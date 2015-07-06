package plans

import (
	"fmt"
	"reflect"

	"github.com/outdoorsy/gorp"
)

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

func (plan *QueryPlan) getTarget() reflect.Value {
	return plan.target
}

func (plan *QueryPlan) getColMap() structColumnMap {
	return plan.colMap
}

func (plan *QueryPlan) errors() []error {
	return plan.Errors
}

func (plan *QueryPlan) getTable() *gorp.TableMap {
	return plan.table
}

func (plan *QueryPlan) mapSubQuery(q subQuery) *tableAlias {
	if len(q.errors()) != 0 {
		plan.Errors = append(plan.Errors, q.errors()...)
	}
	query, err := q.selectQuery()
	if err != nil {
		plan.Errors = append(plan.Errors, err)
	}
	alias := q.QuotedTable()
	quotedFromClause := fmt.Sprintf("(%s) as %s", query, alias)
	for _, m := range q.getColMap() {
		m.quotedTable = alias
		plan.colMap = append(plan.colMap, m)
	}
	return &tableAlias{TableMap: q.getTable(), dialect: plan.dbMap.Dialect, quotedFromClause: quotedFromClause}
}
