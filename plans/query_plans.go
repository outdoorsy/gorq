package plans

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/memcachier/mc"
	"github.com/outdoorsy/gorp"
	"github.com/outdoorsy/gorq/dialects"
	"github.com/outdoorsy/gorq/filters"
	"github.com/outdoorsy/gorq/interfaces"
)

var (
	tableCacheMap  = map[string]map[string]struct{}{}
	tableCacheLock sync.RWMutex
)

func addTableCacheMapEntry(tableKey, cacheKey string) {
	tableCacheLock.Lock()
	if tableCacheMap[tableKey] == nil {
		tableCacheMap[tableKey] = map[string]struct{}{cacheKey: {}}
	} else {
		tableCacheMap[tableKey][cacheKey] = struct{}{}
	}
	tableCacheLock.Unlock()
}
func getTableCacheMapEntry(tableKey string) []string {
	var cacheKeys map[string]struct{}
	tableCacheLock.RLock()
	cacheKeys = tableCacheMap[tableKey]
	tableCacheLock.RUnlock()

	entries := make([]string, len(cacheKeys))
	i := 0
	for k := range cacheKeys {
		entries[i] = k
		i++
	}

	return entries
}

type fieldColumnMap struct {
	// addr should be the address (pointer value) of the field within
	// the struct being used to construct this query.
	addr interface{}

	// column should be the column that matches the field that addr
	// points to.
	column *gorp.ColumnMap

	// alias is used in the query as an alias for this column.
	alias string

	// quotedTable should be the pre-quoted table string for this
	// column.
	quotedTable string

	// quotedColumn should be the pre-quoted column string for this
	// column.
	quotedColumn string
}

type structColumnMap []fieldColumnMap

// LocateColumn takes an interface value (which should be a
// pointer to one of the fields on the value that is being used as a
// reference for query construction) and returns the pre-quoted column
// name that should be used to reference that value in queries.
func (structMap structColumnMap) LocateColumn(fieldPtr interface{}) (string, error) {
	fieldMap, err := structMap.fieldMapForPointer(fieldPtr)
	if err != nil {
		return "", err
	}
	return fieldMap.quotedColumn, nil
}

// LocateTableAndColumn takes an interface value (which should be a
// pointer to one of the fields on the value that is being used as a
// reference for query construction) and returns the pre-quoted
// table.column name that should be used to reference that value in
// some types of queries (mostly where statements and select queries).
func (structMap structColumnMap) LocateTableAndColumn(fieldPtr interface{}) (string, error) {
	fieldMap, err := structMap.fieldMapForPointer(fieldPtr)
	if err != nil {
		return "", err
	}
	return fieldMap.quotedTable + "." + fieldMap.quotedColumn, nil
}

func (structMap structColumnMap) joinMapForPointer(fieldPtr interface{}) (*fieldColumnMap, error) {
	for _, fieldMap := range structMap {
		if fieldMap.addr == fieldPtr {
			return &fieldMap, nil
		}
	}
	fieldPtrVal := reflect.ValueOf(fieldPtr)
	addr, value := fieldPtrVal.Pointer(), fieldPtrVal.Elem().Interface()
	return nil, fmt.Errorf("gorp: Cannot find a field matching the passed in pointer %d (value %v)", addr, value)
}

// fieldMapForPointer takes a pointer to a struct field and returns
// the fieldColumnMap for that struct field.
func (structMap structColumnMap) fieldMapForPointer(fieldPtr interface{}) (*fieldColumnMap, error) {
	m, err := structMap.joinMapForPointer(fieldPtr)
	if err != nil {
		return nil, err
	}
	if m.column.Transient {
		return nil, errors.New("gorp: Cannot run queries against transient columns")
	}
	return m, nil
}

type order struct {
	fieldOrWrapper interface{}
	direction      string
}

func (o order) OrderBy(dialect gorp.Dialect, colMap structColumnMap, bindIdx int) (string, []interface{}, error) {
	var (
		wrapper      filters.SqlWrapper
		allFields    []interface{}
		multiWrapper filters.MultiSqlWrapper
	)
	switch t := o.fieldOrWrapper.(type) {
	case filters.SqlWrapper:
		wrapper = t
		allFields = []interface{}{wrapper.ActualValue()}
	case filters.MultiSqlWrapper:
		multiWrapper = t
		allFields = multiWrapper.ActualValues()
	default:
		allFields = []interface{}{o.fieldOrWrapper}
	}
	// OrderBy needs at least one reference to a column of some sort.
	fieldFound := false
	columnsAndFields := make([]string, 0, len(allFields))
	params := make([]interface{}, 0, len(allFields))
	for _, field := range allFields {
		if reflect.TypeOf(field).Kind() == reflect.Ptr {
			column, err := colMap.LocateTableAndColumn(field)
			if err != nil {
				return "", nil, err
			}
			columnsAndFields = append(columnsAndFields, column)
			fieldFound = true
		} else {
			columnsAndFields = append(columnsAndFields, dialect.BindVar(bindIdx))
			params = append(params, field)
			bindIdx++
		}
	}
	if !fieldFound {
		return "", nil, errors.New("OrderBy requires a pointer to a struct field or " +
			"a wrapper with at least one struct field pointer as an actual value.")
	}
	var orderStr string
	if wrapper != nil {
		orderStr = wrapper.WrapSql(columnsAndFields[0])
	} else if multiWrapper != nil {
		orderStr = multiWrapper.WrapSql(columnsAndFields...)
	} else {
		orderStr = columnsAndFields[0]
	}
	direction := strings.ToLower(o.direction)
	switch direction {
	case "asc", "desc":
		orderStr += " " + direction
	case "":
	default:
		return "", nil, errors.New(`gorp: Order by direction must be empty string, "asc", or "desc"`)
	}
	return orderStr, params, nil
}

// subQuery is provided to use plan types as sub-queries in from/join
// clauses.
type subQuery interface {
	QuotedTable() string
	getTable() *gorp.TableMap
	getTarget() reflect.Value
	getColMap() structColumnMap
	errors() []error
	selectQuery() (string, error)
	getArgs() []interface{}
}

// A QueryPlan is a Query.  It returns itself on most method calls;
// the one exception is Assign(), which returns an AssignQueryPlan (a type of
// QueryPlan that implements AssignQuery instead of Query).  The return
// types of the methods on this struct help prevent silly errors like
// trying to run a SELECT statement that tries to Assign() values - that
// type of nonsense will result in compile errors.
//
// QueryPlans must be prepared and executed using an allocated struct
// as reference.  Again, this is intended to catch stupid mistakes
// (like typos in column names) at compile time.  Unfortunately, it
// makes the syntax a little unintuitive; but I haven't been able to
// come up with a better way to do it.
//
// For details about what you need in order to generate a query with
// this logic, see DbMap.Query().
type QueryPlan struct {
	// Errors is a slice of error valuues encountered during query
	// construction.  This is to allow cascading method calls, e.g.
	//
	//     someModel := new(OurModel)
	//     results, err := dbMap.Query(someModel).
	//         Where().
	//         Greater(&someModel.CreatedAt, yesterday).
	//         Less(&someModel.CreatedAt, time.Now()).
	//         Order(&someModel.CreatedAt, gorp.Descending).
	//         Select()
	//
	// The first time that a method call returns an error (most likely
	// Select(), Insert(), Delete(), or Update()), this field will be
	// checked for errors that occurred during query construction, and
	// if it is non-empty, the first error in the list will be
	// returned immediately.
	Errors []error

	table          *gorp.TableMap
	dbMap          *gorp.DbMap
	quotedTable    string
	executor       gorp.SqlExecutor
	target         reflect.Value
	colMap         structColumnMap
	joins          []*filters.JoinFilter
	lastRefs       []filters.Filter
	assignCols     []string
	assignBindVars []string
	assignArgs     []interface{}
	filters        filters.MultiFilter
	orderBy        []order
	groupBy        []string
	limit          int64
	offset         int64
	args           []interface{}
	memCache       *mc.Conn
	cacheable      bool
	invalidate     []interface{}
}

// Query generates a Query for a target model.  The target that is
// passed in must be a pointer to a struct, and will be used as a
// reference for query construction.
func Query(m *gorp.DbMap, exec gorp.SqlExecutor, target interface{}, cache *mc.Conn, cacheable bool, invalidate []interface{}) interfaces.Query {
	// Handle non-standard dialects
	switch src := m.Dialect.(type) {
	case gorp.MySQLDialect:
		m.Dialect = dialects.MySQLDialect{src}
	case gorp.SqliteDialect:
		m.Dialect = dialects.SqliteDialect{src}
	default:
	}
	plan := &QueryPlan{
		dbMap:      m,
		executor:   exec,
		memCache:   cache,
		cacheable:  cacheable,
		invalidate: invalidate,
	}

	targetVal := reflect.ValueOf(target)
	if targetVal.Kind() != reflect.Ptr || targetVal.Elem().Kind() != reflect.Struct {
		plan.Errors = append(plan.Errors, errors.New("A query target must be a pointer to struct"))
	}
	targetTable, _, err := plan.mapTable(targetVal)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	plan.target = targetVal
	plan.table = targetTable
	return plan
}

func (plan *QueryPlan) getTarget() reflect.Value {
	return plan.target
}

func (plan *QueryPlan) getColMap() structColumnMap {
	return plan.colMap
}

func (plan *QueryPlan) errors() []error {
	return plan.Errors
}

func (plan *QueryPlan) getArgs() []interface{} {
	return plan.args
}

func (plan *QueryPlan) getTable() *gorp.TableMap {
	return plan.table
}

func (plan *QueryPlan) mapSubQuery(q subQuery) *gorp.TableMap {
	if len(q.errors()) != 0 {
		plan.Errors = append(plan.Errors, q.errors()...)
	}
	query, err := q.selectQuery()
	if err != nil {
		plan.Errors = append(plan.Errors, err)
	}
	alias := q.QuotedTable()
	plan.quotedTable = fmt.Sprintf("(%s) as %s", query, alias)
	for _, m := range q.getColMap() {
		m.quotedTable = alias
		plan.colMap = append(plan.colMap, m)
	}
	return q.getTable()
}

func (plan *QueryPlan) mapTable(targetVal reflect.Value) (*gorp.TableMap, string, error) {
	if targetVal.Kind() != reflect.Ptr {
		return nil, "", errors.New("All query targets must be pointer types")
	}

	if subQuery, ok := targetVal.Interface().(subQuery); ok {
		return plan.mapSubQuery(subQuery), subQuery.QuotedTable(), nil
	}

	alias := ""
	if plan.table != nil {
		alias = "-"
	}
	var targetTable *gorp.TableMap
	if m, err := plan.colMap.joinMapForPointer(targetVal.Interface()); err == nil {
		if m.column.TargetTable() != nil {
			alias = m.alias
			targetTable = m.column.TargetTable()
		}
	}

	// targetVal could feasibly be a slice or array, to store
	// *-to-many results in.
	elemType := targetVal.Type().Elem()
	if elemType.Kind() == reflect.Slice || elemType.Kind() == reflect.Array {
		targetVal = targetVal.Elem()
		if targetVal.IsNil() {
			targetVal.Set(reflect.MakeSlice(elemType, 0, 1))
		}
		if targetVal.Len() == 0 {
			newElem := reflect.New(elemType.Elem()).Elem()
			if newElem.Kind() == reflect.Ptr {
				newElem.Set(reflect.New(newElem.Type().Elem()))
			}
			targetVal.Set(reflect.Append(targetVal, newElem))
		}
		targetVal = targetVal.Index(0)
		if targetVal.Kind() != reflect.Ptr {
			targetVal = targetVal.Addr()
		}
	}
	// It could also be a pointer to a pointer to a struct, if the
	// struct field is a pointer, itself.  This is *only* allowed when
	// the passed in value mapped to a field, though, so targetTable
	// must already be set.
	if targetTable != nil && elemType.Kind() == reflect.Ptr {
		targetVal = targetVal.Elem()
		if targetVal.IsNil() {
			targetVal.Set(reflect.New(targetVal.Type().Elem()))
		}
	}

	if targetVal.Elem().Kind() != reflect.Struct {
		return nil, "", errors.New("gorp: Cannot create query plan - no struct found to map to")
	}

	var err error
	if targetTable == nil {
		targetTable, err = plan.dbMap.TableFor(targetVal.Type().Elem(), false)
		if err != nil {
			return nil, "", err
		}
	}

	plan.lastRefs = make([]filters.Filter, 0, 2)

	if err = plan.mapColumns(targetTable, targetVal, alias); err != nil {
		return nil, "", err
	}
	return targetTable, alias, nil
}

// fieldByIndex is a copy of v.FieldByIndex, except that it will
// initialize nil pointers while descending the indexes.
func fieldByIndex(v reflect.Value, index []int) reflect.Value {
	for _, idx := range index {
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		switch v.Kind() {
		case reflect.Struct:
			v = v.Field(idx)
		default:
			panic("gorp: found unsupported type using fieldByIndex")
		}
	}
	return v
}

type referenceFilter struct {
	clause string
}

func (filter *referenceFilter) Where(structMap filters.TableAndColumnLocater, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error) {
	return filter.clause, nil, nil
}

func reference(leftTable, leftCol, rightTable, rightCol string) filters.Filter {
	return &referenceFilter{
		clause: fmt.Sprintf("%s.%s = %s.%s", leftTable, leftCol, rightTable, rightCol),
	}
}

// mapColumns creates a list of field addresses and column maps, to
// make looking up the column for a field address easier.  Note that
// it doesn't do any special handling for overridden fields, because
// passing the address of a field that has been overridden is
// difficult to do accidentally.
func (plan *QueryPlan) mapColumns(table *gorp.TableMap, value reflect.Value, prefix string, parents ...string) (err error) {
	value = value.Elem()
	if plan.colMap == nil {
		plan.colMap = make(structColumnMap, 0, value.NumField())
	}
	queryableFields := 0
	quotedTableName := plan.dbMap.Dialect.QuoteField(strings.TrimSuffix(prefix, "_"))
	if prefix == "" || prefix == "-" {
		quotedTableName = plan.dbMap.Dialect.QuotedTableForQuery(table.SchemaName, table.TableName)
	}
	for _, col := range table.Columns {
		if value.Type().FieldByIndex(col.FieldIndex()).PkgPath != "" {
			// TODO: What about anonymous fields?
			// Don't map unexported fields
			continue
		}
		field := fieldByIndex(value, col.FieldIndex())
		alias := prefix + col.ColumnName
		if prefix == "-" {
			alias = "-"
		} else if col.JoinAlias() != "" {
			alias = prefix + col.JoinAlias()
		}
		fieldRef := field.Addr().Interface()
		quotedCol := plan.dbMap.Dialect.QuoteField(col.ColumnName)
		if prefix != "-" {
			// This means we're mapping an embedded struct, so we can
			// sort of autodetect some reference columns.
			if len(col.ReferencedBy()) > 0 {
				// The way that foreign keys work, columns that are
				// referenced by other columns will have the same
				// field reference.
				fieldMap, err := plan.colMap.fieldMapForPointer(fieldRef)
				if err == nil {
					plan.lastRefs = append(plan.lastRefs, reference(fieldMap.quotedTable, fieldMap.quotedColumn, quotedTableName, quotedCol))
					alias = "-"
				}
			}
		}
		fieldMap := fieldColumnMap{
			addr:         fieldRef,
			column:       col,
			alias:        alias,
			quotedTable:  quotedTableName,
			quotedColumn: quotedCol,
		}
		plan.colMap = append(plan.colMap, fieldMap)
		if !col.Transient {
			queryableFields++
		}
	}
	if queryableFields == 0 {
		return errors.New("No fields in the target struct are mappable.")
	}
	return
}

// Extend returns an extended query, using extensions for the
// gorp.Dialect stored as your dbmap's Dialect field.  You will need
// to use a type assertion on the return value.  As an example,
// postgresql supports a form of joining tables for use in an update
// statement.  You can still only *assign* values on the main
// reference table, but you can use values from other joined tables
// both during assignment and in the where clause.  Here's what it
// would look like:
//
//     updateCount, err := dbMap.Query(ref).Extend().(extensions.Postgres).
//         Assign(&ref.Date, time.Now()).
//         Join(mapRef).On().
//         Equal(&mapRef.Foreign, &ref.Id).
//         Update()
//
// If you want to make your own extensions, just make sure to register
// the constructor using RegisterExtension().
func (plan *QueryPlan) Extend() interface{} {
	extendedQuery, err := LoadExtension(plan.dbMap.Dialect, plan)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return nil
	}
	return extendedQuery
}

// Assign sets up an assignment operation to assign the passed in
// value to the passed in field pointer.  This is used for creating
// UPDATE or INSERT queries.
func (plan *QueryPlan) Assign(fieldPtr interface{}, value interface{}) interfaces.AssignQuery {
	assignPlan := &AssignQueryPlan{QueryPlan: plan}
	return assignPlan.Assign(fieldPtr, value)
}

func (plan *QueryPlan) storeJoin() {
	if lastJoinFilter, ok := plan.filters.(*filters.JoinFilter); ok {
		if plan.joins == nil {
			plan.joins = make([]*filters.JoinFilter, 0, 2)
		}
		plan.joins = append(plan.joins, lastJoinFilter)
		plan.filters = nil
	}
}

func (plan *QueryPlan) JoinType(joinType string, target interface{}) (joinPlan interfaces.JoinQuery) {
	joinPlan = &JoinQueryPlan{QueryPlan: plan}
	plan.storeJoin()
	table, alias, err := plan.mapTable(reflect.ValueOf(target))
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		// Add a filter just so the rest of the query methods won't panic
		plan.filters = &filters.JoinFilter{Type: joinType, QuotedJoinTable: "Error: no table found"}
		return
	}
	quotedTable := plan.dbMap.Dialect.QuotedTableForQuery(table.SchemaName, table.TableName)
	quotedAlias := ""
	if alias != "" && alias != "-" {
		quotedAlias = plan.dbMap.Dialect.QuoteField(strings.TrimSuffix(alias, "_"))
	}
	plan.filters = &filters.JoinFilter{Type: joinType, QuotedJoinTable: quotedTable, QuotedAlias: quotedAlias}
	return
}

func (plan *QueryPlan) Join(target interface{}) interfaces.JoinQuery {
	return plan.JoinType("inner", target)
}

func (plan *QueryPlan) LeftJoin(target interface{}) interfaces.JoinQuery {
	return plan.JoinType("left outer", target)
}

func (plan *QueryPlan) On(filters ...filters.Filter) interfaces.JoinQuery {
	plan.filters.Add(filters...)
	return &JoinQueryPlan{QueryPlan: plan}
}

// Where stores any join filter and allocates a new and filter to use
// for WHERE clause creation.  If you pass filters to it, they will be
// passed to plan.Filter().
func (plan *QueryPlan) Where(filterSlice ...filters.Filter) interfaces.WhereQuery {
	plan.storeJoin()
	plan.filters = new(filters.AndFilter)
	plan.Filter(filterSlice...)
	return plan
}

// Filter will add a Filter to the list of filters on this query.  The
// default method of combining filters on a query is by AND - if you
// want OR, you can use the following syntax:
//
//     query.Filter(gorp.Or(gorp.Equal(&field.Id, id), gorp.Less(&field.Priority, 3)))
//
func (plan *QueryPlan) Filter(filters ...filters.Filter) interfaces.WhereQuery {
	plan.filters.Add(filters...)
	return plan
}

// In adds a column IN (values...) comparison to the where clause.
func (plan *QueryPlan) In(fieldPtr interface{}, values ...interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.In(fieldPtr, values...))
}

// Like adds a column LIKE pattern comparison to the where clause.
func (plan *QueryPlan) Like(fieldPtr interface{}, pattern string) interfaces.WhereQuery {
	return plan.Filter(filters.Like(fieldPtr, pattern))
}

// Equal adds a column = value comparison to the where clause.
func (plan *QueryPlan) Equal(fieldPtr interface{}, value interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.Equal(fieldPtr, value))
}

// NotEqual adds a column != value comparison to the where clause.
func (plan *QueryPlan) NotEqual(fieldPtr interface{}, value interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.NotEqual(fieldPtr, value))
}

// Less adds a column < value comparison to the where clause.
func (plan *QueryPlan) Less(fieldPtr interface{}, value interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.Less(fieldPtr, value))
}

// LessOrEqual adds a column <= value comparison to the where clause.
func (plan *QueryPlan) LessOrEqual(fieldPtr interface{}, value interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.LessOrEqual(fieldPtr, value))
}

// Greater adds a column > value comparison to the where clause.
func (plan *QueryPlan) Greater(fieldPtr interface{}, value interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.Greater(fieldPtr, value))
}

// GreaterOrEqual adds a column >= value comparison to the where clause.
func (plan *QueryPlan) GreaterOrEqual(fieldPtr interface{}, value interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.GreaterOrEqual(fieldPtr, value))
}

// Null adds a column IS NULL comparison to the where clause
func (plan *QueryPlan) Null(fieldPtr interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.Null(fieldPtr))
}

// NotNull adds a column IS NOT NULL comparison to the where clause
func (plan *QueryPlan) NotNull(fieldPtr interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.NotNull(fieldPtr))
}

// True adds a column comparison to the where clause (tests for
// column's truthiness)
func (plan *QueryPlan) True(fieldPtr interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.True(fieldPtr))
}

// False adds a NOT column comparison to the where clause (tests for
// column's negated truthiness)
func (plan *QueryPlan) False(fieldPtr interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.False(fieldPtr))
}

// OrderBy adds a column to the order by clause.  The direction is
// optional - you may pass in an empty string to order in the default
// direction for the given column.
func (plan *QueryPlan) OrderBy(fieldPtrOrWrapper interface{}, direction string) interfaces.SelectQuery {
	plan.orderBy = append(plan.orderBy, order{fieldPtrOrWrapper, direction})
	return plan
}

// DiscardOrderBy discards all entries in the order by clause.
func (plan *QueryPlan) DiscardOrderBy() interfaces.SelectQuery {
	plan.orderBy = []order{}
	return plan
}

// GroupBy adds a column to the group by clause.
func (plan *QueryPlan) GroupBy(fieldPtr interface{}) interfaces.SelectQuery {
	column, err := plan.colMap.LocateTableAndColumn(fieldPtr)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	plan.groupBy = append(plan.groupBy, column)
	return plan
}

// Limit sets the limit clause of the query.
func (plan *QueryPlan) Limit(limit int64) interfaces.SelectQuery {
	plan.limit = limit
	return plan
}

// DiscardLimit discards any previously set limit clause.
func (plan *QueryPlan) DiscardLimit() interfaces.SelectQuery {
	plan.limit = 0
	return plan
}

// Offset sets the offset clause of the query.
func (plan *QueryPlan) Offset(offset int64) interfaces.SelectQuery {
	plan.offset = offset
	return plan
}

// DiscardOffset discards any previously set offset clause.
func (plan *QueryPlan) DiscardOffset() interfaces.SelectQuery {
	plan.offset = 0
	return plan
}

func (plan *QueryPlan) whereClause() (string, error) {
	if plan.filters == nil {
		return "", nil
	}
	where, whereArgs, err := plan.filters.Where(plan.colMap, plan.dbMap.Dialect, len(plan.args))
	if err != nil {
		return "", err
	}
	if where != "" {
		plan.args = append(plan.args, whereArgs...)
		return " where " + where, nil
	}
	return "", nil
}

func (plan *QueryPlan) selectJoinClause() (string, error) {
	buffer := bytes.Buffer{}
	for _, join := range plan.joins {
		buffer.WriteString(" ")
		joinClause, joinArgs, err := join.JoinClause(plan.colMap, plan.dbMap.Dialect, len(plan.args))
		if err != nil {
			return "", err
		}
		buffer.WriteString(joinClause)
		plan.args = append(plan.args, joinArgs...)
	}
	return buffer.String(), nil
}

func (plan *QueryPlan) resetArgs() {
	plan.args = nil
	if len(plan.assignArgs) > 0 {
		plan.args = append(plan.args, plan.assignArgs...)
	}
	if subQuery, ok := plan.target.Interface().(subQuery); ok {
		plan.args = append(plan.args, subQuery.getArgs()...)
	}
}

// Truncate will run this query plan as a TRUNCATE TABLE statement.
func (plan *QueryPlan) Truncate() error {
	query := fmt.Sprintf("truncate table %s", plan.QuotedTable())
	_, err := plan.dbMap.Exec(query)
	return err
}

// Select will run this query plan as a SELECT statement.
func (plan *QueryPlan) Select() ([]interface{}, error) {
	query, err := plan.selectQuery()
	if err != nil {
		return nil, err
	}
	if plan.cacheable {
		cacheKey := fmt.Sprintf("%s: %v", query, plan.args)
		table, err := plan.dbMap.TableFor(plan.target.Type(), false)
		if err == nil {
			data, err := getCacheData(cacheKey, plan.target, table, plan.memCache)
			if err == nil { // fail silently - graceful fallback
				return data, nil
			}
		}
	}

	target := plan.target.Interface()
	if subQuery, ok := target.(subQuery); ok {
		target = subQuery.getTarget().Interface()
	}
	res, err := plan.executor.Select(target, query, plan.args...)
	if err != nil {
		return nil, err
	}

	if plan.cacheable {
		cacheKey := fmt.Sprintf("%s: %v", query, plan.args)
		tableKey := plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName)
		addTableCacheMapEntry(tableKey, cacheKey)
		for _, join := range plan.joins {
			addTableCacheMapEntry(join.QuotedJoinTable, cacheKey)
		}
		if err == nil {
			setCacheData(cacheKey, res, plan.colMap, plan.memCache) // fail silently - graceful fallback
		}
	}

	return res, nil
}

// SelectToTarget will run this query plan as a SELECT statement, and
// append results directly to the passed in slice pointer.
func (plan *QueryPlan) SelectToTarget(target interface{}) error {
	targetType := reflect.TypeOf(target)
	if targetType.Kind() != reflect.Ptr || targetType.Elem().Kind() != reflect.Slice {
		return errors.New("SelectToTarget must be run with a pointer to a slice as its target")
	}
	query, err := plan.selectQuery()
	if err != nil {
		return err
	}

	if plan.cacheable {
		table, err := plan.dbMap.TableFor(plan.target.Type(), false)
		if err == nil {
			cacheKey := fmt.Sprintf("%s: %v", query, plan.args)
			data, err := getCacheData(cacheKey, reflect.ValueOf(map[string]interface{}{}), table, plan.memCache)
			if err == nil { // fail silently - graceful fallback
				targetVal := reflect.ValueOf(target)
				for _, item := range data {
					empty := reflect.New(targetVal.Type().Elem().Elem())
					from := reflect.ValueOf(item)
					for _, key := range from.MapKeys() {
						field := empty.FieldByIndex(plan.table.ColMap(key).FieldIndex())
						field.Set(from.MapIndex(key))
					}
				}
				return nil
			}
		}
	}

	_, err = plan.executor.Select(target, query, plan.args...)
	if plan.cacheable {
		cacheKey := fmt.Sprintf("%s: %v", query, plan.args)
		tableKey := plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName)
		addTableCacheMapEntry(tableKey, cacheKey)
		for _, join := range plan.joins {
			addTableCacheMapEntry(join.QuotedJoinTable, cacheKey)
		}
		if err == nil {
			setCacheData(cacheKey, target, plan.colMap, plan.memCache) // fail silently - graceful fallback
		}
	}
	return err
}

func (plan *QueryPlan) Count() (int64, error) {
	plan.resetArgs()
	buffer := new(bytes.Buffer)
	buffer.WriteString("select count(*)")
	if err := plan.writeSelectSuffix(buffer); err != nil {
		return -1, err
	}
	return plan.executor.SelectInt(buffer.String(), plan.args...)
}

func (plan *QueryPlan) QuotedTable() string {
	if plan.quotedTable == "" {
		plan.quotedTable = plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName)
	}
	return plan.quotedTable
}

func (plan *QueryPlan) selectQuery() (string, error) {
	plan.resetArgs()
	buffer := new(bytes.Buffer)
	if err := plan.writeSelectColumns(buffer); err != nil {
		return "", err
	}
	if err := plan.writeSelectSuffix(buffer); err != nil {
		return "", err
	}
	return buffer.String(), nil
}

func (plan *QueryPlan) writeSelectColumns(buffer *bytes.Buffer) error {
	if len(plan.Errors) > 0 {
		return plan.Errors[0]
	}
	buffer.WriteString("select ")
	for index, m := range plan.colMap {
		col := m.column
		if !col.Transient && m.alias != "-" {
			if index != 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(m.quotedTable)
			buffer.WriteString(".")
			buffer.WriteString(m.quotedColumn)
			if m.alias != "" {
				buffer.WriteString(" AS ")
				buffer.WriteString(m.alias)
			}
		}
	}
	return nil
}

func (plan *QueryPlan) writeSelectSuffix(buffer *bytes.Buffer) error {
	plan.storeJoin()
	buffer.WriteString(" from ")
	buffer.WriteString(plan.QuotedTable())
	joinClause, err := plan.selectJoinClause()
	if err != nil {
		return err
	}
	buffer.WriteString(joinClause)
	whereClause, err := plan.whereClause()
	if err != nil {
		return err
	}
	buffer.WriteString(whereClause)
	for index, orderBy := range plan.orderBy {
		if index == 0 {
			buffer.WriteString(" order by ")
		} else {
			buffer.WriteString(", ")
		}
		orderStr, args, err := orderBy.OrderBy(plan.dbMap.Dialect, plan.colMap, len(plan.args))
		if err != nil {
			return err
		}
		buffer.WriteString(orderStr)
		plan.args = append(plan.args, args...)
	}
	for index, groupBy := range plan.groupBy {
		if index == 0 {
			buffer.WriteString(" group by ")
		} else {
			buffer.WriteString(", ")
		}
		buffer.WriteString(groupBy)
	}
	// Nonstandard LIMIT clauses seem to have to come *before* the
	// offset clause.
	limiter, nonstandard := plan.dbMap.Dialect.(interfaces.NonstandardLimiter)
	if plan.limit > 0 && nonstandard {
		buffer.WriteString(" ")
		buffer.WriteString(limiter.Limit(plan.dbMap.Dialect.BindVar(len(plan.args))))
		plan.args = append(plan.args, plan.limit)
	}
	if plan.offset > 0 {
		buffer.WriteString(" offset ")
		buffer.WriteString(plan.dbMap.Dialect.BindVar(len(plan.args)))
		plan.args = append(plan.args, plan.offset)
	}
	// Standard FETCH NEXT (n) ROWS ONLY must come after the offset.
	if plan.limit > 0 && !nonstandard {
		// Many dialects seem to ignore the SQL standard when it comes
		// to the limit clause.
		buffer.WriteString(" fetch next (")
		buffer.WriteString(plan.dbMap.Dialect.BindVar(len(plan.args)))
		plan.args = append(plan.args, plan.limit)
		buffer.WriteString(") rows only")
	}
	return nil
}

// Insert will run this query plan as an INSERT statement.
func (plan *QueryPlan) Insert() error {
	plan.resetArgs()
	if len(plan.Errors) > 0 {
		return plan.Errors[0]
	}
	buffer := bytes.Buffer{}
	buffer.WriteString("insert into ")
	buffer.WriteString(plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName))
	buffer.WriteString(" (")
	for i, col := range plan.assignCols {
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(col)
	}
	buffer.WriteString(") values (")
	for i, bindVar := range plan.assignBindVars {
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(bindVar)
	}
	buffer.WriteString(")")
	_, err := plan.executor.Exec(buffer.String(), plan.args...)

	if plan.cacheable {
		tableKey := plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName)
		evictCacheData(getTableCacheMapEntry(tableKey), plan.memCache) // fail gracefully
	}

	return err
}

// joinFromAndWhereClause will return the from and where clauses for
// joined tables, for use in UPDATE and DELETE statements.
func (plan *QueryPlan) joinFromAndWhereClause() (from, where string, err error) {
	fromSlice := make([]string, 0, len(plan.joins))
	whereBuffer := bytes.Buffer{}
	for _, join := range plan.joins {
		fromSlice = append(fromSlice, join.QuotedJoinTable)
		whereClause, whereArgs, err := join.Where(plan.colMap, plan.dbMap.Dialect, len(plan.args))
		if err != nil {
			return "", "", err
		}
		whereBuffer.WriteString(whereClause)
		plan.args = append(plan.args, whereArgs...)
	}
	return strings.Join(fromSlice, ", "), whereBuffer.String(), nil
}

// Update will run this query plan as an UPDATE statement.
func (plan *QueryPlan) Update() (int64, error) {
	plan.resetArgs()
	if len(plan.Errors) > 0 {
		return -1, plan.Errors[0]
	}
	buffer := bytes.Buffer{}
	buffer.WriteString("update ")
	buffer.WriteString(plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName))
	buffer.WriteString(" set ")
	for i, col := range plan.assignCols {
		bindVar := plan.assignBindVars[i]
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(col)
		buffer.WriteString("=")
		buffer.WriteString(bindVar)
	}
	joinTables, joinWhereClause, err := plan.joinFromAndWhereClause()
	if err != nil {
		return -1, nil
	}
	if joinTables != "" {
		buffer.WriteString(" from ")
		buffer.WriteString(joinTables)
	}
	whereClause, err := plan.whereClause()
	if err != nil {
		return -1, err
	}
	if joinWhereClause != "" {
		if whereClause == "" {
			whereClause = " where "
		} else {
			whereClause += " and "
		}
		whereClause += joinWhereClause
	}
	buffer.WriteString(whereClause)
	res, err := plan.executor.Exec(buffer.String(), plan.args...)
	if err != nil {
		return -1, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return -1, err
	}

	if plan.cacheable {
		tableKey := plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName)
		evictCacheData(getTableCacheMapEntry(tableKey), plan.memCache) // fail gracefully
	}

	return rows, nil
}

// Delete will run this query plan as a DELETE statement.
func (plan *QueryPlan) Delete() (int64, error) {
	plan.resetArgs()
	if len(plan.Errors) > 0 {
		return -1, plan.Errors[0]
	}
	buffer := bytes.Buffer{}
	buffer.WriteString("delete from ")
	buffer.WriteString(plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName))
	joinTables, joinWhereClause, err := plan.joinFromAndWhereClause()
	if err != nil {
		return -1, err
	}
	if joinTables != "" {
		buffer.WriteString(" using ")
		buffer.WriteString(joinTables)
	}
	whereClause, err := plan.whereClause()
	if err != nil {
		return -1, err
	}
	if joinWhereClause != "" {
		if whereClause == "" {
			whereClause = " where "
		} else {
			whereClause += " and "
		}
		whereClause += joinWhereClause
	}
	buffer.WriteString(whereClause)
	res, err := plan.executor.Exec(buffer.String(), plan.args...)
	if err != nil {
		return -1, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return -1, err
	}

	if plan.cacheable {
		tableKey := plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName)
		evictCacheData(getTableCacheMapEntry(tableKey), plan.memCache) // fail gracefully
	}

	return rows, nil
}

// A JoinQueryPlan is a QueryPlan, except with some return values
// changed so that it will match the JoinQuery interface.
type JoinQueryPlan struct {
	*QueryPlan
}

func (plan *JoinQueryPlan) References() interfaces.JoinQuery {
	if len(plan.lastRefs) == 0 {
		plan.Errors = append(plan.Errors, errors.New("No references found to join with"))
	}
	plan.QueryPlan.Filter(plan.lastRefs...)
	return plan
}

func (plan *JoinQueryPlan) In(fieldPtr interface{}, values ...interface{}) interfaces.JoinQuery {
	plan.QueryPlan.In(fieldPtr, values...)
	return plan
}

func (plan *JoinQueryPlan) Like(fieldPtr interface{}, pattern string) interfaces.JoinQuery {
	plan.QueryPlan.Like(fieldPtr, pattern)
	return plan
}

func (plan *JoinQueryPlan) Equal(fieldPtr interface{}, value interface{}) interfaces.JoinQuery {
	plan.QueryPlan.Equal(fieldPtr, value)
	return plan
}

func (plan *JoinQueryPlan) NotEqual(fieldPtr interface{}, value interface{}) interfaces.JoinQuery {
	plan.QueryPlan.NotEqual(fieldPtr, value)
	return plan
}

func (plan *JoinQueryPlan) Less(fieldPtr interface{}, value interface{}) interfaces.JoinQuery {
	plan.QueryPlan.Less(fieldPtr, value)
	return plan
}

func (plan *JoinQueryPlan) LessOrEqual(fieldPtr interface{}, value interface{}) interfaces.JoinQuery {
	plan.QueryPlan.LessOrEqual(fieldPtr, value)
	return plan
}

func (plan *JoinQueryPlan) Greater(fieldPtr interface{}, value interface{}) interfaces.JoinQuery {
	plan.QueryPlan.Greater(fieldPtr, value)
	return plan
}

func (plan *JoinQueryPlan) GreaterOrEqual(fieldPtr interface{}, value interface{}) interfaces.JoinQuery {
	plan.QueryPlan.GreaterOrEqual(fieldPtr, value)
	return plan
}

func (plan *JoinQueryPlan) Null(fieldPtr interface{}) interfaces.JoinQuery {
	plan.QueryPlan.Null(fieldPtr)
	return plan
}

func (plan *JoinQueryPlan) NotNull(fieldPtr interface{}) interfaces.JoinQuery {
	plan.QueryPlan.NotNull(fieldPtr)
	return plan
}

func (plan *JoinQueryPlan) True(fieldPtr interface{}) interfaces.JoinQuery {
	plan.QueryPlan.True(fieldPtr)
	return plan
}

func (plan *JoinQueryPlan) False(fieldPtr interface{}) interfaces.JoinQuery {
	plan.QueryPlan.False(fieldPtr)
	return plan
}

// An AssignQueryPlan is, for all intents and purposes, a QueryPlan.
// The only difference is the return type of Where() and all of the
// various where clause operations.  This is intended to be used for
// queries that have had Assign() called, to make it a compile error
// if you try to call Select() on a query that has had both Assign()
// and Where() called.
//
// All documentation for QueryPlan applies to AssignQueryPlan, too.
type AssignQueryPlan struct {
	*QueryPlan
}

func (plan *AssignQueryPlan) Assign(fieldPtr interface{}, value interface{}) interfaces.AssignQuery {
	column, err := plan.colMap.LocateColumn(fieldPtr)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	plan.assignCols = append(plan.assignCols, column)
	plan.assignBindVars = append(plan.assignBindVars, plan.dbMap.Dialect.BindVar(len(plan.assignArgs)))
	plan.assignArgs = append(plan.assignArgs, value)
	return plan
}

func (plan *AssignQueryPlan) Where(filters ...filters.Filter) interfaces.UpdateQuery {
	plan.QueryPlan.Where(filters...)
	return plan
}

func (plan *AssignQueryPlan) Filter(filters ...filters.Filter) interfaces.UpdateQuery {
	plan.QueryPlan.Filter(filters...)
	return plan
}

func (plan *AssignQueryPlan) In(fieldPtr interface{}, values ...interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.In(fieldPtr, values...)
	return plan
}

func (plan *AssignQueryPlan) Like(fieldPtr interface{}, pattern string) interfaces.UpdateQuery {
	plan.QueryPlan.Like(fieldPtr, pattern)
	return plan
}

func (plan *AssignQueryPlan) Equal(fieldPtr interface{}, value interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.Equal(fieldPtr, value)
	return plan
}

func (plan *AssignQueryPlan) NotEqual(fieldPtr interface{}, value interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.NotEqual(fieldPtr, value)
	return plan
}

func (plan *AssignQueryPlan) Less(fieldPtr interface{}, value interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.Less(fieldPtr, value)
	return plan
}

func (plan *AssignQueryPlan) LessOrEqual(fieldPtr interface{}, value interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.LessOrEqual(fieldPtr, value)
	return plan
}

func (plan *AssignQueryPlan) Greater(fieldPtr interface{}, value interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.Greater(fieldPtr, value)
	return plan
}

func (plan *AssignQueryPlan) GreaterOrEqual(fieldPtr interface{}, value interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.GreaterOrEqual(fieldPtr, value)
	return plan
}

func (plan *AssignQueryPlan) Null(fieldPtr interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.Null(fieldPtr)
	return plan
}

func (plan *AssignQueryPlan) NotNull(fieldPtr interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.NotNull(fieldPtr)
	return plan
}

func (plan *AssignQueryPlan) True(fieldPtr interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.True(fieldPtr)
	return plan
}

func (plan *AssignQueryPlan) False(fieldPtr interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.False(fieldPtr)
	return plan
}
