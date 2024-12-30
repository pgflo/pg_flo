package sinks

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/pgflo/pg_flo/pkg/utils"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: "15:04:05.000",
	})
}

// FileSink represents a sink that writes data to files
type FileSink struct {
	outputDir      string
	currentFile    *os.File
	currentSize    int64
	maxFileSize    int64
	rotateInterval time.Duration
	lastRotation   time.Time
	mutex          sync.Mutex
}

// NewFileSink creates a new FileSink instance
func NewFileSink(outputDir string) (*FileSink, error) {
	sink := &FileSink{
		outputDir:      outputDir,
		maxFileSize:    100 * 1024 * 1024, // 100 MB
		rotateInterval: time.Hour,         // Rotate every hour if size limit not reached
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %v", err)
	}

	if err := sink.rotateFile(); err != nil {
		return nil, fmt.Errorf("failed to create initial log file: %v", err)
	}

	return sink, nil
}

// rotateFile creates a new log file and updates the current file pointer
func (s *FileSink) rotateFile() error {
	if s.currentFile != nil {
		err := s.currentFile.Close()
		if err != nil {
			return err
		}
		s.currentFile = nil
	}

	timestamp := time.Now().UTC().Format("20060102T150405Z")
	filename := fmt.Sprintf("pg_flo_log_%s.jsonl", timestamp)
	filepath := filepath.Join(s.outputDir, filename)

	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to create new log file: %v", err)
	}

	s.currentFile = file
	s.currentSize = 0
	s.lastRotation = time.Now()

	log.Info().Str("file", filepath).Msg("Rotated to new log file")
	return nil
}

// WriteBatch writes a batch of data to the current log file
func (s *FileSink) WriteBatch(messages []*utils.CDCMessage) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, message := range messages {
		jsonData, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("failed to marshal data to JSON: %v", err)
		}

		if s.currentFile == nil || s.currentSize >= s.maxFileSize || time.Since(s.lastRotation) >= s.rotateInterval {
			if err := s.rotateFile(); err != nil {
				return err
			}
		}

		jsonData = append(jsonData, '\n')
		n, err := s.currentFile.Write(jsonData)
		if err != nil {
			return fmt.Errorf("failed to write to log file: %v", err)
		}

		s.currentSize += int64(n)
	}
	return nil
}

// Close closes the current log file and performs any necessary cleanup
func (s *FileSink) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.currentFile != nil {
		err := s.currentFile.Close()
		s.currentFile = nil
		return err
	}
	return nil
}
