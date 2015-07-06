package plans

import (
	"fmt"

	"github.com/outdoorsy/gorq/filters"
)

type referenceFilter struct {
	clause string
}

func (filter *referenceFilter) ActualValues() []interface{} {
	return nil
}

func (filter *referenceFilter) Where(...string) string {
	return filter.clause
}

func reference(leftTable, leftCol, rightTable, rightCol string) filters.Filter {
	return &referenceFilter{
		clause: fmt.Sprintf("%s.%s = %s.%s", leftTable, leftCol, rightTable, rightCol),
	}
}
