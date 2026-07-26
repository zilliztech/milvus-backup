// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/zilliztech/milvus-backup/internal/progressbar"
)

var (
	_globalL atomic.Pointer[zap.Logger]
	_globalP atomic.Pointer[ZapProperties]
	_globalS atomic.Pointer[zap.SugaredLogger]
)

func init() {
	l, p := newStdLogger()
	_globalL.Store(l)
	_globalP.Store(p)
	_globalS.Store(l.Sugar())
}

// InitLogger replaces the global logger with one built from conf. The caller
// maps its own configuration onto Config, which keeps this package free of any
// configuration schema — the schema packages log through it.
func InitLogger(conf *Config) {
	lg, p, err := initLogger(conf)
	if err != nil {
		panic(err)
	}
	ReplaceGlobals(lg, p)
}

// InitLogger initializes a zap logger.
func initLogger(cfg *Config, opts ...zap.Option) (*zap.Logger, *ZapProperties, error) {
	outputs := make([]zapcore.WriteSyncer, 0)
	if len(cfg.File.Filename) > 0 {
		lg, err := initFileLog(&cfg.File)
		if err != nil {
			return nil, nil, err
		}
		outputs = append(outputs, zapcore.AddSync(lg))
	}
	if cfg.Console {
		stdOut := zapcore.AddSync(progressbar.Stdout)
		outputs = append(outputs, stdOut)
	}
	writer := zap.CombineWriteSyncers(outputs...)
	return InitLoggerWithWriteSyncer(cfg, writer, opts...)
}

// InitLoggerWithWriteSyncer initializes a zap logger with specified  write syncer.
func InitLoggerWithWriteSyncer(cfg *Config, output zapcore.WriteSyncer, opts ...zap.Option) (*zap.Logger, *ZapProperties, error) {
	level := zap.NewAtomicLevel()
	err := level.UnmarshalText([]byte(cfg.Level))
	if err != nil {
		return nil, nil, fmt.Errorf("initLoggerWithWriteSyncer UnmarshalText cfg.Level err:%w", err)
	}
	core := NewTextCore(newZapTextEncoder(cfg), output, level)
	opts = append(cfg.buildOptions(output), opts...)
	lg := zap.New(core, opts...)
	r := &ZapProperties{
		Core:   core,
		Syncer: output,
		Level:  level,
	}
	return lg, r, nil
}

// initFileLog initializes file based logging options.
func initFileLog(cfg *FileLogConfig) (*lumberjack.Logger, error) {
	if st, err := os.Stat(cfg.Filename); err == nil {
		if st.IsDir() {
			return nil, errors.New("can't use directory as log file name")
		}
	}

	if cfg.MaxSize == 0 {
		cfg.MaxSize = defaultLogMaxSize
	}

	// use lumberjack to logrotate
	return &lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxDays,
		LocalTime:  true,
	}, nil
}

// newStdLogger builds the logger the package starts with, the one in use until
// InitLogger replaces it with the configured one.
//
// It writes to stderr rather than going through initLogger, which builds its
// sinks from the file and console settings and so would have none to build from
// here. Anything logged while the configuration is still being read — an
// unknown key, a deprecated schema version — would otherwise be discarded,
// which is the opposite of what a warning raised at that point is for. stderr
// keeps it clear of the stdout a command writes its own output to.
//
// No caller skip: the package-level Warn and friends already add one for their
// own frame, and the logger InitLogger builds adds none.
func newStdLogger() (*zap.Logger, *ZapProperties) {
	conf := &Config{Level: "info", File: FileLogConfig{}}
	lg, r, _ := InitLoggerWithWriteSyncer(conf, zapcore.AddSync(os.Stderr))
	return lg, r
}

// L returns the global Logger, which can be reconfigured with ReplaceGlobals.
// It's safe for concurrent use.
func L() *zap.Logger {
	return _globalL.Load()
}

// S returns the global SugaredLogger, which can be reconfigured with
// ReplaceGlobals. It's safe for concurrent use.
func S() *zap.SugaredLogger {
	return _globalS.Load()
}

// ReplaceGlobals replaces the global Logger and SugaredLogger.
// It's safe for concurrent use.
func ReplaceGlobals(logger *zap.Logger, props *ZapProperties) {
	_globalL.Store(logger)
	_globalS.Store(logger.Sugar())
	_globalP.Store(props)
}
