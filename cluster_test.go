package kafkauniverse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetEnvVariable(t *testing.T) {
	assert.Equal(t, "PATH_TO_VARIABLE2", getEnvVariableName("PATH_", "to-variable2"))
	assert.NotNil(t, getEnvVariable("PA", "th")) // will match env variable PATH
}
