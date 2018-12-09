package providechainpoint

import (
	"os"

	logger "github.com/kthomas/go-logger"
)

// Log global
var Log = logger.NewLogger("provide-chainpoint", getLogLevel(), true)

func getLogLevel() string {
	lvl := os.Getenv("LOG_LEVEL")
	if lvl == "" {
		lvl = "debug"
	}
	return lvl
}

func stringOrNil(str string) *string {
	if str == "" {
		return nil
	}
	return &str
}
