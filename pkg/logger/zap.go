package log

import (
	"fmt"
	"os"

	"github.com/dingyu123456/lyra/internal/config"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	errorLogFileName = "error.log"
)

func Init(cfg *config.LogConfig) (err error) {
	// 先构造debug级别日志的core-----------------------------------------------
	writeSyncer := getMultipleSyncWriter(
		cfg.FileName,
		cfg.MaxSize,
		cfg.MaxBackups,
		cfg.MaxAge)
	encoder := getEncoder()

	// 因为下面zapcore.NewCore需要的日志级别参数的类型不是配置信息中的string，需要转换为level类型
	var l = new(zapcore.Level)
	err = l.UnmarshalText([]byte(cfg.Level))
	if err != nil {
		return fmt.Errorf("failed to unmarshal log level, %w", err)
	}

	// zap的核心，创建一个日志对象时需要的参数
	// 该函数需要三个参数，1.编码器（用来规定日志格式）2.同步器（将日志写入磁盘的日志文件）3.日志等级（日志文件中会记录高于或等于该级别的日志）

	// 所有级别日志的core
	coreAll := zapcore.NewCore(encoder, writeSyncer, l)
	// ---------------------------------------------------------------------

	// 构造error级别日志的core，用于将error日志单独输入到error.log-----------------
	errWriteSyncer := getSingleSyncWriter(
		errorLogFileName,
		cfg.MaxSize,
		cfg.MaxBackups,
		cfg.MaxAge)
	errEncoder := getEncoder()
	coreErr := zapcore.NewCore(errEncoder, errWriteSyncer, zapcore.ErrorLevel)
	//----------------------------------------------------------------------

	lg := zap.New(zapcore.NewTee(coreAll, coreErr), zap.AddCaller()) // zap.AddCaller()会让日志信息显示调用者信息，比如文件名和代码行数
	//将zap库中全局的logger替换为lg，这样就可以通过zap.L()在全局访问到lg，当然也可以在本文件创建一个全局变量作为全局logger
	zap.ReplaceGlobals(lg)
	return
}

// 自定义zap日志的输出格式，自定义的方式是先获取一个默认的编码器配置，然后修改自己需要的部分即可
func getEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()                             // zap.NewProductionEncoderConfig()返回一个默认的编码器
	encoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05") // 更改时间格式, 必须使用"2006-01-02 15:04:05"
	encoderConfig.TimeKey = "time"                                                // 将时间的key改为“time”（默认是“ts”）
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder                       // 将日志级别的显示更改为大写
	encoderConfig.EncodeCaller = zapcore.ShortCallerEncoder                       // 调用者信息，ShortCallerEncoder包含文件名和行号
	return zapcore.NewJSONEncoder(encoderConfig)
}

// 返回一个日志同步器（将日志同步到磁盘的日志文件）
func getMultipleSyncWriter(filename string, maxSize, maxBackup, maxAge int) zapcore.WriteSyncer {
	// 因为zap日志库没有自带的日志文件分割功能，所以要借助lumberjack插件来实现，该插件只支持按文件大小分割，不支持按照时间分割
	lumberJackLogger := &lumberjack.Logger{
		Filename:   filename,
		MaxSize:    maxSize,
		MaxBackups: maxBackup,
		MaxAge:     maxAge,
	}
	// 日志只输出到单个文件用下面这行return即可
	// return zapcore.AddSync(lumberJackLogger)

	// 同时将日志输出在文件和终端
	// 创建终端日志输出器
	terminalLogger := zapcore.AddSync(os.Stdout)

	// 使用 MultiWriteSyncer 将日志同时输出到文件和终端
	return zapcore.NewMultiWriteSyncer(zapcore.AddSync(lumberJackLogger), terminalLogger)
}

func getSingleSyncWriter(filename string, maxSize, maxBackup, maxAge int) zapcore.WriteSyncer {
	// 因为zap日志库没有自带的日志文件分割功能，所以要借助lumberjack插件来实现，该插件只支持按文件大小分割，不支持按照时间分割
	lumberJackLogger := &lumberjack.Logger{
		Filename:   filename,
		MaxSize:    maxSize,
		MaxBackups: maxBackup,
		MaxAge:     maxAge,
	}
	// 日志只输出到单个文件
	return zapcore.AddSync(lumberJackLogger)
}
