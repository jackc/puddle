package puddle

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResList_PanicsWithBugReportIfResourceDoesNotExist(t *testing.T) {
	arr := []*Resource[any]{
		new(Resource[any]),
		new(Resource[any]),
		new(Resource[any]),
	}

	list := resList[any](arr)

	assert.PanicsWithValue(t, "BUG: removeResource could not find res in slice", func() {
		list.remove(new(Resource[any]))
	})
}
