package plans

import (
	"errors"
	"reflect"
	"strings"

	"github.com/outdoorsy/gorp"
	"github.com/outdoorsy/gorq/dialects"
	"github.com/outdoorsy/gorq/filters"
	"github.com/outdoorsy/gorq/interfaces"
)

// Query generates a Query for a target model.  The target that is
// passed in must be a pointer to a struct, and will be used as a
// reference for query construction.
func Query(m *gorp.DbMap, exec gorp.SqlExecutor, target interface{}, cache interfaces.Cache, cachingDisabled bool, joinOps ...JoinOp) interfaces.Query {
	// Handle non-standard dialects
	switch src := m.Dialect.(type) {
	case gorp.MySQLDialect:
		m.Dialect = dialects.MySQLDialect{src}
	case gorp.SqliteDialect:
		m.Dialect = dialects.SqliteDialect{src}
	default:
	}
	plan := &QueryPlan{
		dbMap:           m,
		executor:        exec,
		cache:           cache,
		cachingDisabled: cachingDisabled,
	}

	targetVal := reflect.ValueOf(target)
	if targetVal.Kind() != reflect.Ptr || targetVal.Elem().Kind() != reflect.Struct {
		plan.Errors = append(plan.Errors, errors.New("A query target must be a pointer to struct"))
	}
	targetTable, _, err := plan.mapTable(targetVal, joinOps...)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	plan.target = targetVal
	plan.table = targetTable.TableMap
	plan.quotedTable = targetTable.tableForFromClause()
	return plan
}

func (plan *QueryPlan) mapTable(targetVal reflect.Value, joinOps ...JoinOp) (*tableAlias, string, error) {
	if targetVal.Kind() != reflect.Ptr {
		return nil, "", errors.New("All query targets must be pointer types")
	}

	if subQuery, ok := targetVal.Interface().(subQuery); ok {
		// This is one of our QueryPlan types, or an extended version
		// of one of them.
		return plan.mapSubQuery(subQuery), subQuery.QuotedTable(), nil
	}

	// UnmappedSubQuery types are for user-generated sub-queries, so
	// we still have to do a fair bit of mapping work.
	subQuery, isSubQuery := targetVal.Interface().(UnmappedSubQuery)
	if isSubQuery {
		targetVal = reflect.ValueOf(subQuery.Target())
	}

	var prefix, alias string
	if plan.table != nil {
		prefix = "-"
		alias = "-"
	}
	var (
		targetTable *gorp.TableMap
		joinColumn  *gorp.ColumnMap
	)
	parentMap, err := plan.colMap.joinMapForPointer(targetVal.Interface())
	if err == nil && parentMap.column.TargetTable() != nil {
		prefix, alias = parentMap.prefix, parentMap.alias
		joinColumn = parentMap.column
		targetTable = parentMap.column.TargetTable()
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

	if targetTable == nil {
		targetTable, err = plan.dbMap.TableFor(targetVal.Type().Elem(), false)
		if err != nil {
			return nil, "", err
		}
	}
	plan.tables = append(plan.tables, targetTable)

	plan.lastRefs = make([]filters.Filter, 0, 2)

	if isSubQuery {
		// Get the columns from the sub-query's select statement.
		query, columns := subQuery.SelectQuery(targetTable, joinColumn, alias, prefix)
		for _, field := range plan.colMap {
			for i, colName := range columns {
				switch colName {
				case field.column.ColumnName, field.column.JoinAlias():
					field.alias = colName
					columns = append(columns[:i], columns[i+1:]...)
					break
				}
			}
		}
		return &tableAlias{TableMap: targetTable, dialect: plan.dbMap.Dialect, quotedFromClause: query}, alias, nil
	}
	if err = plan.mapColumns(parentMap, targetVal.Interface(), targetTable, targetVal, prefix, joinOps...); err != nil {
		return nil, "", err
	}
	return &tableAlias{TableMap: targetTable, dialect: plan.dbMap.Dialect}, alias, nil
}

// mapColumns creates a list of field addresses and column maps, to
// make looking up the column for a field address easier.  Note that
// it doesn't do any special handling for overridden fields, because
// passing the address of a field that has been overridden is
// difficult to do accidentally.
func (plan *QueryPlan) mapColumns(parentMap *fieldColumnMap, parent interface{}, table *gorp.TableMap, value reflect.Value, prefix string, joinOps ...JoinOp) (err error) {
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
		shouldSelect := !col.Transient && prefix != "-"
		if value.Type().FieldByIndex(col.FieldIndex()).PkgPath != "" {
			// TODO: What about anonymous fields?
			// Don't map unexported fields
			continue
		}
		field := fieldByIndex(value, col.FieldIndex())
		alias := prefix + col.ColumnName
		colPrefix := prefix
		if col.JoinAlias() != "" {
			alias = prefix + col.JoinAlias()
			colPrefix = prefix + col.JoinPrefix()
		}
		fieldRef := field.Addr().Interface()
		quotedCol := plan.dbMap.Dialect.QuoteField(col.ColumnName)
		if prefix != "-" && prefix != "" {
			// This means we're mapping an embedded struct, so we can
			// sort of autodetect some reference columns.
			if len(col.ReferencedBy()) > 0 {
				// The way that foreign keys work, columns that are
				// referenced by other columns will have the same
				// field reference.
				fieldMap, err := plan.colMap.fieldMapForPointer(fieldRef)
				if err == nil {
					plan.lastRefs = append(plan.lastRefs, reference(fieldMap.quotedTable, fieldMap.quotedColumn, quotedTableName, quotedCol))
					shouldSelect = false
				}
			}
		}
		fieldMap := &fieldColumnMap{
			parentMap:    parentMap,
			parent:       parent,
			field:        fieldRef,
			selectTarget: fieldRef,
			column:       col,
			alias:        alias,
			prefix:       colPrefix,
			quotedTable:  quotedTableName,
			quotedColumn: quotedCol,
			doSelect:     shouldSelect,
		}
		for _, op := range joinOps {
			if op.Table == table && op.Column == col {
				fieldMap.join = op.Join
			}
		}
		for _, op := range joinOps {
			if table == op.Table && col == op.Column {
				fieldMap.join = op.Join
				break
			}
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
