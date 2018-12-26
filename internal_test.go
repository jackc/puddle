package puddle

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRemoveResourcePanicsWithBugReportIfResourceDoesNotExist(t *testing.T) {
	s := []*Resource{new(Resource), new(Resource), new(Resource)}
	assert.PanicsWithValue(t, "BUG: removeResource could not find res in slice", func() { removeResource(s, new(Resource)) })
}
