package log

import (
	"bytes"
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
	baseLogger = zerolog.New(os.Stderr)
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

func TestGetOutput(t *testing.T) {
	defer func() {
		os.Remove("testfile.log")
	}()

	tests := []struct {
		name string

		arg     string
		wantOut interface{}
		wantErr bool
	}{
		{"TEmpty", "", nil, true},
		{"TStdout", "stdout", os.Stdout, false},
		{"TStderr", "stderr", os.Stderr, false},
		{"TCustomFile", "testfile.log", nil, false},
		{"TFailPermission", "/etc/hosts", nil, true},
		{"TFailCantCreate", "no/where/dir/nofile.log", nil, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := getOutput(test.arg)
			if test.wantOut != nil {
				if got != test.wantOut {
					t.Errorf("getOutput() = %v, want %v", got, test.wantOut)
				}
			}
			if (err != nil) != test.wantErr {
				t.Errorf("getOutput() err = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestFileOutByModule(t *testing.T) {
	var baseLogName = "test_basefile.log"
	var m1LogName = "test_subfile1.log"
	var m2LogName = "test_subfile2.log"

	defer func() {
		// clean up after test
		os.Remove(baseLogName)
		os.Remove(m1LogName)
		os.Remove(m2LogName)
	}()

	// remove if exist
	os.Remove(baseLogName)
	os.Remove(m1LogName)
	os.Remove(m2LogName)

	configStr := `
	out = "test_basefile.log"
	level = "info"

	[m1]
	out = "test_subfile1.log"

	[m2]
	out = "test_subfile2.log"
	`
	if _, err := createCleanLogger(configStr, "m1"); err != nil {
		assert.Fail(t, err.Error())
	}

	sublog1 := NewLogger("m1")
	sublog1.Info().Msg("sub1 write")

	sublog1_1 := NewLogger("m1")
	sublog1_1.Info().Msg("sub1_1 write")

	sublog2 := NewLogger("m2")
	sublog2.Info().Msg("sub2 write")

	// not specified module inherit base logger.
	otherlog := NewLogger("ohter_m")
	otherlog.Info().Msg("other write")

	baseContent, err := ioutil.ReadFile(baseLogName)
	if err != nil {
		assert.Fail(t, err.Error())
	}
	assert.True(t, bytes.Contains(baseContent, []byte("other write")))

	m1Content, err := ioutil.ReadFile(m1LogName)
	if err != nil {
		assert.Fail(t, err.Error())
	}
	assert.True(t, bytes.Contains(m1Content, []byte("sub1 write")))
	assert.True(t, bytes.Contains(m1Content, []byte("sub1_1 write")))

	m2Content, err := ioutil.ReadFile(m2LogName)
	if err != nil {
		assert.Fail(t, err.Error())
	}
	assert.True(t, bytes.Contains(m2Content, []byte("sub2 write")))

}
