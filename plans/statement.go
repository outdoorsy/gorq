package plans

import (
	"bytes"
	"strings"

	"github.com/outdoorsy/gorq/filters"
	"github.com/outdoorsy/gorq/interfaces"
)

// A Statement is a generated SQL statement.
type Statement struct {
	query bytes.Buffer
	args  []interface{}
}

// Query returns the query string for s, replacing bound argument
// placeholders with bindArgs.
func (s *Statement) Query(bindVars ...string) string {
	query := s.query.String()
	for _, v := range bindVars {
		query = strings.Replace(query, BindVarPlaceholder, v, 1)
	}
	return query
}

// Args returns the arguments for s.
func (s *Statement) Args() []interface{} {
	return s.args
}

// SelectStatement generates a select statement.
func (plan *QueryPlan) SelectStatement() (*Statement, error) {
	statement := new(Statement)
	statement.query.WriteString("SELECT ")
	if err := plan.addSelectColumns(statement); err != nil {
		return nil, err
	}
	if err := plan.addSelectSuffix(statement); err != nil {
		return nil, err
	}
	return statement, nil
}

// addSelectColumns adds the select columns, separated by commas, to
// statement.
func (plan *QueryPlan) addSelectColumns(statement *Statement) error {
	if len(plan.Errors) > 0 {
		return plan.Errors[0]
	}
	for index, m := range plan.colMap {
		if m.doSelect {
			if index != 0 {
				statement.query.WriteString(",")
			}
			var err error
			selectClause := m.quotedTable + "." + m.quotedColumn
			if m.selectTarget != m.field {
				switch src := m.selectTarget.(type) {
				case filters.SqlWrapper:
					actualValue := src.ActualValue()
					newArgs, sqlValue, err := plan.argOrColumn(actualValue)
					if err != nil {
						return err
					}
					statement.args = append(statement.args, newArgs...)
					selectClause = src.WrapSql(sqlValue)
				case filters.MultiSqlWrapper:
					values := src.ActualValues()
					sqlValues := make([]string, 0, len(values))
					for _, v := range values {
						newArgs, sqlValue, err := plan.argOrColumn(v)
						if err != nil {
							return err
						}
						sqlValues = append(sqlValues, sqlValue)
						statement.args = append(statement.args, newArgs...)
					}
					selectClause = src.WrapSql(sqlValues...)
				default:
					var newArgs []interface{}
					newArgs, selectClause, err = plan.argOrColumn(m.field)
					if err != nil {
						return err
					}
					statement.args = append(statement.args, newArgs...)
				}
			}
			statement.query.WriteString(selectClause)
			if m.alias != "" {
				statement.query.WriteString(" AS ")
				statement.query.WriteString(m.alias)
			}
		}
	}
	return nil
}

// addWhereClause adds the where clause (including the word "WHERE")
// to a statement, if there is a where clause on plan.
func (plan *QueryPlan) addWhereClause(statement *Statement) error {
	if plan.filters == nil {
		return nil
	}
	whereArgs := plan.filters.ActualValues()
	whereVals := make([]string, 0, len(whereArgs))
	for _, arg := range whereArgs {
		args, val, err := plan.argOrColumn(arg)
		if err != nil {
			return err
		}
		whereVals = append(whereVals, val)
		statement.args = append(statement.args, args...)
	}
	where := plan.filters.Where(whereVals...)

	if where != "" {
		statement.query.WriteString(" WHERE ")
		statement.query.WriteString(where)
	}
	return nil
}

// addJoinClause adds JOIN clauses to statement, if there are any join
// operations applied to plan.
func (plan *QueryPlan) addJoinClause(statement *Statement) error {
	for _, join := range plan.joins {
		joinArgs := join.ActualValues()
		joinVals := make([]string, 0, len(joinArgs))
		for _, arg := range joinArgs {
			args, val, err := plan.argOrColumn(arg)
			if err != nil {
				return err
			}
			joinVals = append(joinVals, val)
			statement.args = append(statement.args, args...)
		}
		joinClause := join.JoinClause(joinVals...)

		statement.query.WriteString(joinClause)
	}
	return nil
}

// addSelectSuffix adds the full suffix of a SELECT statement
// (starting with the FROM clause) to statement.
func (plan *QueryPlan) addSelectSuffix(statement *Statement) error {
	plan.storeJoin()
	statement.query.WriteString(" FROM ")
	statement.query.WriteString(plan.QuotedTable())
	if err := plan.addJoinClause(statement); err != nil {
		return err
	}
	if err := plan.addWhereClause(statement); err != nil {
		return err
	}
	for index, groupBy := range plan.groupBy {
		if index == 0 {
			statement.query.WriteString(" GROUP BY ")
		} else {
			statement.query.WriteString(", ")
		}
		statement.query.WriteString(groupBy)
	}
	for index, orderBy := range plan.orderBy {
		if index == 0 {
			statement.query.WriteString(" ORDER BY ")
		} else {
			statement.query.WriteString(", ")
		}
		args, val, err := plan.argOrColumn(orderBy.ActualValue())
		if err != nil {
			return err
		}
		statement.query.WriteString(orderBy.OrderBy(val))
		statement.args = append(statement.args, args...)
	}
	// Nonstandard LIMIT clauses seem to have to come *before* the
	// offset clause.
	limiter, nonstandard := plan.dbMap.Dialect.(interfaces.NonstandardLimiter)
	if plan.limit > 0 && nonstandard {
		statement.query.WriteString(" ")
		statement.query.WriteString(limiter.Limit(BindVarPlaceholder))
		statement.args = append(statement.args, plan.limit)
	}
	if plan.offset > 0 {
		statement.query.WriteString(" OFFSET ")
		statement.query.WriteString(BindVarPlaceholder)
		statement.args = append(statement.args, plan.offset)
	}
	// Standard FETCH NEXT (n) ROWS ONLY must come after the offset.
	if plan.limit > 0 && !nonstandard {
		// Many dialects seem to ignore the SQL standard when it comes
		// to the limit clause.
		statement.query.WriteString(" FETCH NEXT (")
		statement.query.WriteString(BindVarPlaceholder)
		statement.args = append(statement.args, plan.limit)
		statement.query.WriteString(") ROWS ONLY")
	}
	return nil
}
