package puddleg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRemoveResourcePanicsWithBugReportIfResourceDoesNotExist(t *testing.T) {
	s := []*Resource[any]{new(Resource[any]), new(Resource[any]), new(Resource[any])}
	assert.PanicsWithValue(t, "BUG: removeResource could not find res in slice", func() { removeResource(s, new(Resource[any])) })
}
