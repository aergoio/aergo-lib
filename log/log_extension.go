package log

// WithSkipFrameCount creates new logger with sets the number of frames to skip when determining the caller information in the logger output.
// It is useful for log wrapper.
func (logger *Logger) WithSkipFrameCount(skipFrameCount int) *Logger {
	zLogger := logger.With().CallerWithSkipFrameCount(skipFrameCount).Logger()
	return &Logger{
		Logger: &zLogger,
		name:   logger.name,
		level:  logger.level,
	}
}
