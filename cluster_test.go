package kafkauniverse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetEnvVariable(t *testing.T) {
	assert.Equal(t, "TO_VARIABLE2", getEnvVariableName("to-variable2"))
	assert.NotNil(t, getEnvVariable("PA", "t", "H")) // will match env variable PATH
}
