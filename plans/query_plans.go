package plans

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"

	"github.com/outdoorsy/gorp"
	"github.com/outdoorsy/gorq/filters"
	"github.com/outdoorsy/gorq/interfaces"
)

// BindVarPlaceholder is used as a placeholder string for bindVar
// strings.  Wherever it is used in a query, it should be replaced by
// the correct bind variable for the dialect in use.
const BindVarPlaceholder = "%s"

type tableAlias struct {
	*gorp.TableMap
	quotedFromClause string
	dialect          gorp.Dialect
}

func (t tableAlias) tableForFromClause() string {
	if t.quotedFromClause != "" {
		return t.quotedFromClause
	}
	return t.dialect.QuotedTableForQuery(t.SchemaName, t.TableName)
}

// UnmappedSubQuery is an interface that subqueries which do not have
// access to details about the table and struct field maps may
// implement.
type UnmappedSubQuery interface {
	Target() interface{}
	SelectQuery(table *gorp.TableMap, col *gorp.ColumnMap, tableAlias string, tablePrefix string) (query string, columns []string)
}

type JoinFunc func(parent, field interface{}) (joinType string, joinTarget, selectionField interface{}, constraints []filters.Filter)

type JoinOp struct {
	Table  *gorp.TableMap
	Column *gorp.ColumnMap
	Join   JoinFunc
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

	table           *gorp.TableMap
	dbMap           *gorp.DbMap
	quotedTable     string
	executor        gorp.SqlExecutor
	target          reflect.Value
	colMap          structColumnMap
	joins           []*filters.JoinFilter
	lastRefs        []filters.Filter
	assignCols      []string
	assignBindVars  []string
	assignArgs      []interface{}
	filters         filters.MultiFilter
	orderBy         []order
	groupBy         []string
	limit           int64
	offset          int64
	cache           interfaces.Cache
	cachingDisabled bool
	tables          []*gorp.TableMap
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

// Fields restricts the columns being selected in a select query to
// just those matching the passed in field pointers.
func (plan *QueryPlan) Fields(fields ...interface{}) interfaces.SelectionQuery {
	for _, field := range plan.colMap {
		field.doSelect = false
	}
	for _, field := range fields {
		plan.AddField(field)
	}
	return plan
}

// AddField adds a field to the select statement.  Some fields (for
// example, fields that are processed via JoinOp values passed to
// Query()) are not mapped in the query by default, and you may use
// AddField to request that they are selected.
//
// Note that fields handled by JoinOp values should *not* be
// explicitly joined to using methods like Join or LeftJoin, but added
// using AddField instead.
func (plan *QueryPlan) AddField(fieldPtr interface{}) interfaces.SelectionQuery {
	m, err := plan.colMap.joinMapForPointer(fieldPtr)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	m.doSelect = true
	if m.join != nil {
		joinType, joinTarget, joinField, constraints := m.join(m.parent, m.field)
		if joinTarget == nil {
			// This means to not bother joining.
			m.doSelect = false
			return plan
		}
		plan.JoinType(joinType, joinTarget).On(constraints...)
		m.selectTarget = joinField
	}
	return plan
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
		quotedAlias = plan.dbMap.Dialect.QuoteField(alias)
	}
	plan.filters = &filters.JoinFilter{Type: joinType, QuotedJoinTable: quotedTable, QuotedAlias: quotedAlias}
	return
}

func (plan *QueryPlan) Join(target interface{}) interfaces.JoinQuery {
	return plan.JoinType("INNER", target)
}

func (plan *QueryPlan) LeftJoin(target interface{}) interfaces.JoinQuery {
	return plan.JoinType("LEFT OUTER", target)
}

func (plan *QueryPlan) On(filters ...filters.Filter) interfaces.JoinQuery {
	plan.filters.Add(filters...)
	return &JoinQueryPlan{QueryPlan: plan}
}

func (plan *QueryPlan) References() interfaces.JoinQuery {
	joinQuery := &JoinQueryPlan{QueryPlan: plan}
	return joinQuery.References()
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

// InSubQuery adds a column IN (subQuery) comparison to the where
// clause.
func (plan *QueryPlan) InSubQuery(fieldPtr interface{}, subQuery filters.SubQuery) interfaces.WhereQuery {
	return plan.Filter(filters.InSubQuery(fieldPtr, subQuery))
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

// Truncate will run this query plan as a TRUNCATE TABLE statement.
func (plan *QueryPlan) Truncate() error {
	query := fmt.Sprintf("TRUNCATE TABLE %s", plan.QuotedTable())
	_, err := plan.dbMap.Exec(query)
	return err
}

func (plan *QueryPlan) bindVars(statement *Statement) []string {
	bindVars := make([]string, 0, len(statement.args))
	for i := range statement.args {
		bindVars = append(bindVars, plan.dbMap.Dialect.BindVar(i))
	}
	return bindVars
}

func (plan *QueryPlan) unmarshalCachedResults(cached []interface{}, resultSlice reflect.Value, elementType reflect.Type) error {
	for _, res := range cached {
		resMap := res.(map[string]interface{})
		// Make sure newTarget is settable by creating a pointer to it
		// and getting the Elem().
		newTarget := reflect.New(elementType).Elem()
		if newTarget.Kind() == reflect.Ptr {
			newTarget.Set(reflect.New(elementType.Elem()))
		}
		for alias, value := range resMap {
			if err := plan.setField(newTarget, alias, value); err != nil {
				return err
			}
		}
		resultSlice.Set(reflect.Append(resultSlice, newTarget))
	}
	return nil
}

func (plan *QueryPlan) setField(target reflect.Value, alias string, value interface{}) error {
	var col *fieldColumnMap
	for _, col = range plan.colMap {
		if col.alias == alias {
			break
		}
	}
	if col == nil {
		return fmt.Errorf("Cannot find field matching alias %s", alias)
	}
	field := fieldByIndex(target, col.column.FieldIndex())
	v := reflect.ValueOf(value)
	if field.Kind() == reflect.Slice {
		return plan.unmarshalCachedResults(value.([]interface{}), field, field.Type().Elem())
	}
	if !v.Type().ConvertibleTo(field.Type()) {
		return fmt.Errorf("Cannot convert type %v to type %v", v.Type(), field.Type())
	}
	field.Set(v.Convert(field.Type()))
	return nil
}

// cachedSelect will load data from cache for the current query.  If
// target is a pointer to a slice, data will be appended to it;
// otherwise, cachedSelect will return a []interface{} containing
// elements of the same type as target.
//
// The return value will be nil if there is nothing in cache for this
// query, or if there are errors while creating the return value.
func (plan *QueryPlan) cachedSelect(target reflect.Value, statement *Statement, bindVars []string) []interface{} {
	if plan.cache == nil || !plan.cache.Cacheable(plan.table) {
		return nil
	}
	key, err := CacheKey(statement.Query(bindVars...), statement.args)
	if err != nil {
		return nil
	}
	result, err := plan.cache.Get(key)
	if err != nil || result == "" {
		return nil
	}

	cached, err := restoreFromCache(result)
	if err != nil {
		return nil
	}

	var results []interface{}
	targetType := target.Type()
	var selectTarget reflect.Value
	if targetType.Kind() == reflect.Ptr && targetType.Elem().Kind() == reflect.Slice {
		selectTarget = target.Elem()
		targetType = targetType.Elem().Elem()

		// results still needs to be non-nil for the return value, but
		// there's no point in taking up a bunch of extra memory.
		results = []interface{}{}
	} else {
		// Currently, this case is only hit when Select() is called,
		// which will send us the original query target, which
		// *should* be a pointer to a struct.  And that is perfectly
		// valid as targetType.

		results = make([]interface{}, 0, len(cached))
		selectTarget = reflect.ValueOf(&results)
	}
	if err := plan.unmarshalCachedResults(cached, selectTarget, targetType); err != nil {
		return nil
	}
	return results
}

// Select will run this query plan as a SELECT statement.
func (plan *QueryPlan) Select() ([]interface{}, error) {
	statement, err := plan.SelectStatement()
	if err != nil {
		return nil, err
	}
	bindVars := plan.bindVars(statement)

	if !plan.cachingDisabled {
		if result := plan.cachedSelect(plan.target, statement, bindVars); result != nil {
			return result, nil
		}
	}

	target := plan.target.Interface()
	if subQuery, ok := target.(subQuery); ok {
		target = subQuery.getTarget().Interface()
	}

	res, err := plan.executor.Select(target, statement.Query(bindVars...), statement.args...)
	if err != nil {
		return nil, err
	}

	if plan.cache != nil && plan.cache.Cacheable(plan.table) && !plan.cachingDisabled {
		plan.cacheResults(res, statement, bindVars)
	}
	return res, nil
}

func (plan *QueryPlan) toCacheFormat(cacheVal reflect.Value) []interface{} {
	if cacheVal.Kind() != reflect.Slice {
		return nil
	}
	cacheData := make([]interface{}, 0, cacheVal.Len())
	if cacheVal.Len() == 0 {
		return cacheData
	}
	// We're guaranteed to have at least one element, here.
	src := cacheVal.Index(0)
	for src.Kind() == reflect.Interface || src.Kind() == reflect.Ptr {
		src = src.Elem()
	}
	srcType := src.Type()
	cacheMap := &cacheMapping{}
	cacheMap.cacheType = srcType
	for _, m := range plan.colMap {
		if m.doSelect {
			nested := []*fieldColumnMap{m}
			for current := m.parentMap; current != nil; current = current.parentMap {
				nested = append([]*fieldColumnMap{current}, nested...)
			}
			cacheMap.add(nested)
		}
	}
	return cacheMap.valueFor(cacheVal).([]interface{})
}

// cacheResults stores a result slice in cache.  Any failures will be
// silent.
func (plan *QueryPlan) cacheResults(results interface{}, statement *Statement, bindVars []string) {
	defer func() {
		// Don't let reflection panics propagate.
		// if r := recover(); r != nil {
		// 	log.Printf("Recovered from %v", r)
		// }
	}()

	cacheVal := reflect.ValueOf(results)
	if cacheVal.Kind() == reflect.Ptr {
		cacheVal = cacheVal.Elem()
	}
	if cacheVal.Kind() != reflect.Slice {
		return
	}

	key, err := CacheKey(statement.Query(bindVars...), statement.args)
	if err != nil {
		return
	}

	cacheData := plan.toCacheFormat(cacheVal)
	if cacheData == nil {
		// We don't want to cache this.
		return
	}
	// fmt.Println("about to encode to json: ", cacheData)
	raw, err := json.Marshal(cacheData)
	if err != nil {
		log.Printf("Error from marshal: %v", err)
		return
	}

	go func() {
		encoded, err := prepareForCache(string(raw))
		if err != nil {
			log.Printf("Error from prepareForCache: %v", err)
			return
		}
		plan.cache.Set(plan.tables, key, encoded)
	}()
}

// SelectToTarget will run this query plan as a SELECT statement, and
// append results directly to the passed in slice pointer.
func (plan *QueryPlan) SelectToTarget(target interface{}) error {
	targetVal := reflect.ValueOf(target)
	targetType := targetVal.Type()
	if targetType.Kind() != reflect.Ptr || targetType.Elem().Kind() != reflect.Slice {
		return errors.New("SelectToTarget must be run with a pointer to a slice as its target")
	}
	statement, err := plan.SelectStatement()
	if err != nil {
		return err
	}
	bindVars := plan.bindVars(statement)
	if !plan.cachingDisabled {
		if result := plan.cachedSelect(targetVal, statement, bindVars); result != nil {
			// All results have been appended to target.
			return nil
		}
	}

	_, err = plan.executor.Select(target, statement.Query(bindVars...), statement.args...)
	if err != nil {
		return err
	}
	if plan.cache != nil && plan.cache.Cacheable(plan.table) && !plan.cachingDisabled {
		plan.cacheResults(target, statement, bindVars)
	}
	return err
}

func (plan *QueryPlan) Count() (int64, error) {
	statement := new(Statement)
	statement.query.WriteString("SELECT COUNT(*)")
	if err := plan.addSelectSuffix(statement); err != nil {
		return -1, err
	}
	bindVars := plan.bindVars(statement)
	return plan.executor.SelectInt(statement.Query(bindVars...), statement.args...)
}

func (plan *QueryPlan) QuotedTable() string {
	if plan.quotedTable == "" {
		plan.quotedTable = plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName)
	}
	return plan.quotedTable
}

// argOrColumn returns the string that should be used to represent a
// value in a query.  If the value is detected to be a field, an error
// will be returned if the field cannot be selected.  If the value is
// used as an argument, it will be appended to args and the returned
// string will be the bind value.
func (plan *QueryPlan) argOrColumn(value interface{}) (args []interface{}, sqlValue string, err error) {
	switch src := value.(type) {
	case filters.SqlWrapper:
		value = src.ActualValue()
		args, wrapperVal, err := plan.argOrColumn(value)
		if err != nil {
			return nil, "", err
		}
		return args, src.WrapSql(wrapperVal), nil
	case filters.MultiSqlWrapper:
		values := src.ActualValues()
		wrapperVals := make([]string, 0, len(values))
		for _, val := range values {
			newArgs, wrapperVal, err := plan.argOrColumn(val)
			if err != nil {
				return nil, "", err
			}
			wrapperVals = append(wrapperVals, wrapperVal)
			args = append(args, newArgs...)
		}
		return args, src.WrapSql(wrapperVals...), nil
	default:
		if reflect.TypeOf(value).Kind() == reflect.Ptr {
			m, err := plan.colMap.fieldMapForPointer(value)
			if err != nil {
				return nil, "", err
			}
			if m.selectTarget != m.field {
				return plan.argOrColumn(m.selectTarget)
			}
			sqlValue = m.quotedTable + "." + m.quotedColumn
		} else {
			sqlValue = BindVarPlaceholder
			args = append(args, value)
		}
	}
	return
}

// Insert will run this query plan as an INSERT statement.
func (plan *QueryPlan) Insert() error {
	if len(plan.Errors) > 0 {
		return plan.Errors[0]
	}
	statement := new(Statement)
	statement.query.WriteString("INSERT INTO ")
	statement.query.WriteString(plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName))
	statement.query.WriteString(" (")
	for i, col := range plan.assignCols {
		if i > 0 {
			statement.query.WriteString(", ")
		}
		statement.query.WriteString(col)
	}
	statement.query.WriteString(") VALUES (")
	for i, bindVar := range plan.assignBindVars {
		if i > 0 {
			statement.query.WriteString(", ")
		}
		statement.query.WriteString(bindVar)
	}
	statement.query.WriteString(")")
	_, err := plan.executor.Exec(statement.query.String(), statement.args...)

	if plan.cache != nil && !plan.cachingDisabled {
		go plan.cache.DropEntries(plan.tables)
	}

	return err
}

// Update will run this query plan as an UPDATE statement.
func (plan *QueryPlan) Update() (int64, error) {
	if len(plan.Errors) > 0 {
		return -1, plan.Errors[0]
	}
	statement := &Statement{
		args: plan.assignArgs,
	}
	statement.query.WriteString("UPDATE ")
	statement.query.WriteString(plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName))
	statement.query.WriteString(" SET ")
	for i, col := range plan.assignCols {
		if i > 0 {
			statement.query.WriteString(", ")
		}
		statement.query.WriteString(col)
		statement.query.WriteString("=")
		statement.query.WriteString(BindVarPlaceholder)
	}
	if err := plan.addWhereClause(statement); err != nil {
		return -1, err
	}
	bindVars := plan.bindVars(statement)
	res, err := plan.executor.Exec(statement.Query(bindVars...), statement.args...)
	if err != nil {
		return -1, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return -1, err
	}

	if plan.cache != nil && !plan.cachingDisabled {
		go plan.cache.DropEntries(plan.tables)
	}

	return rows, nil
}

// Delete will run this query plan as a DELETE statement.
func (plan *QueryPlan) Delete() (int64, error) {
	if len(plan.Errors) > 0 {
		return -1, plan.Errors[0]
	}
	statement := new(Statement)
	statement.query.WriteString("DELETE FROM ")
	statement.query.WriteString(plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName))
	if err := plan.addWhereClause(statement); err != nil {
		return -1, err
	}
	bindVars := plan.bindVars(statement)
	res, err := plan.executor.Exec(statement.Query(bindVars...), statement.args...)
	if err != nil {
		return -1, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return -1, err
	}

	if plan.cache != nil && !plan.cachingDisabled {
		go plan.cache.DropEntries(plan.tables)
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

func (plan *JoinQueryPlan) InSubQuery(fieldPtr interface{}, subQuery filters.SubQuery) interfaces.JoinQuery {
	plan.QueryPlan.InSubQuery(fieldPtr, subQuery)
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
	plan.assignBindVars = append(plan.assignBindVars, plan.dbMap.Dialect.BindVar(len(plan.assignBindVars)))
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

func (plan *AssignQueryPlan) InSubQuery(fieldPtr interface{}, subQuery filters.SubQuery) interfaces.UpdateQuery {
	plan.QueryPlan.InSubQuery(fieldPtr, subQuery)
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
