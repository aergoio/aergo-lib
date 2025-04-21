package log

import (
	"github.com/rs/zerolog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
)

// SetRelativeLogPathForProject configures the log caller function to display paths relative to the project root.
func SetRelativeLogPathForProject() {
	SetRelativeLogPath(FindProjectRootWithSkip(1))
}

func SetRelativeLogPath(baseDir string) {
	// customizing CallerMarshalFunc of zerolog
	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		// find relative path based of project root
		relPath, err := filepath.Rel(baseDir, file)
		if err != nil {
			// use original path if failed
			relPath = file
		}
		return relPath + ":" + strconv.Itoa(line)
	}
}

// FindProjectRoot finds project root of caller. It may only work in go module project.
func FindProjectRoot() string {
	return FindProjectRootWithSkip(1)
}

func FindProjectRootWithSkip(skip int) string {
	var skipCount = skip + 1
	_, file, _, ok := runtime.Caller(skipCount)
	if !ok {
		return ""
	}
	return findProjectRoot(file)
}

func findProjectRoot(startPath string) string {
	dir := filepath.Dir(startPath)
	for {
		// find root directory
		if fileExists(filepath.Join(dir, "go.mod")) {
			// Assuming path containing go.mod as a project root.
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break // finish scan for system root
		}
		dir = parent
	}
	return dir // return current directory if scan was failed
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
