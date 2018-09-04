package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

// newlogger
// default
// is debug enabled

// 환경변수로 패스 잡고
// 파일 생성해서 설정을 읽자
// 다양한 설정: 서브

func resetLogger() {
	// set clear logger as default
	baseLogger = zerolog.New(os.Stdout)
	// set flag off
	isLogInit = false
}

func createConfigAndSetEnv(text string) error {
	byteText := []byte(text)

	// create and write config text to a file
	tmpfile, err := ioutil.TempFile("", "dummy")
	if err != nil {
		return err
	}
	if _, err := tmpfile.Write(byteText); err != nil {
		return err
	}
	if err := tmpfile.Close(); err != nil {
		return err
	}

	// set environment var
	envKey := confEnvPrefix + "_" + confFilePathKey
	os.Unsetenv(envKey)
	os.Setenv(envKey, tmpfile.Name())

	return nil
}

func createCleanLogger(configText string, moduleName string) (*Logger, error) {
	resetLogger()
	if err := createConfigAndSetEnv(configText); err != nil {
		return nil, err
	}
	return NewLogger(moduleName), nil
}

func TestDefaultConfig(t *testing.T) {
	logger := Default()
	assert.Equal(t, "info", logger.Level())
}

func TestBasicLevel(t *testing.T) {

	var logger *Logger
	var err error

	configStr := `
	level = "error"
	`

	if logger, err = createCleanLogger(configStr, "test_logger"); err != nil {
		assert.Fail(t, err.Error())
	}

	assert.Equal(t, "error", logger.Level())
}

func TestSubLevel(t *testing.T) {

	var logger *Logger
	var err error

	configStr := `
	level = "error"

	[sub_module]
	level = "warn"
	`

	if logger, err = createCleanLogger(configStr, "sub_module"); err != nil {
		assert.Fail(t, err.Error())
	}

	// check global level of default logger
	assert.Equal(t, "error", Default().Level())

	// check sub logger level
	assert.Equal(t, "warn", logger.Level())
}

func TestIsDebugNotEnabled(t *testing.T) {
	var logger *Logger
	var err error

	configStr := `
	level = "warn"
	`

	if logger, err = createCleanLogger(configStr, "info_logger"); err != nil {
		assert.Fail(t, err.Error())
	}

	assert.False(t, logger.IsDebugEnabled())
}

func TestIsDebugEnabled(t *testing.T) {
	var logger *Logger
	var err error

	configStr := `
	level = "debug"
	`
	if logger, err = createCleanLogger(configStr, "debug_logger"); err != nil {
		assert.Fail(t, err.Error())
	}

	assert.True(t, logger.IsDebugEnabled())
}
