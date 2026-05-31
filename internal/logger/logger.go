package logger

import (
	"fmt"
	"sync"
	"time"
)

var mu sync.Mutex

// Print formats structured, thread-safe logs.
func printLog(level, message string) {
	mu.Lock()
	defer mu.Unlock()
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	fmt.Printf("[%s] [%s] %s\n", timestamp, level, message)
}

func Info(format string, args ...interface{}) {
	printLog("INFO", fmt.Sprintf(format, args...))
}

func Success(format string, args ...interface{}) {
	printLog("SUCCESS", fmt.Sprintf(format, args...))
}

func Warn(format string, args ...interface{}) {
	printLog("WARN", fmt.Sprintf(format, args...))
}

func Error(format string, args ...interface{}) {
	printLog("ERROR", fmt.Sprintf(format, args...))
}
