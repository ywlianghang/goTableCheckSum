package main

import (
	"encoding/json"
	"fmt"
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)


func initLogger(loglevel string) *zap.Logger{
	hook := lumberjack.Logger{
		Filename: "./logs/tableCheckSum.log",   //日志文件路径
		MaxSize:   128,      //megabytes
		MaxBackups:  30,     //最多保留300个备份
		MaxAge: 7,           //days
		Compress:    true,    //是否压缩 disabled by default
	}
	w := zapcore.AddSync(&hook)
	// 设置日志级别，debug可以打印出info，debug，warn，error;info级别可以打印warn，info，error；warn只能打印warn，error
	// debug-->info->warn->error
	var level zapcore.Level
	switch loglevel {
	case "debug":
		level = zap.DebugLevel
	case "info":
		level = zap.InfoLevel
	case "warn":
		level = zap.WarnLevel
	case "error":
		level = zap.ErrorLevel
	default:
		level = zap.InfoLevel
	}
	encoderConfig := zap.NewProductionEncoderConfig()
	//时间格式
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		w,
		level,
		)
    logger := zap.New(core)
    return logger
}
type Test struct {
	Name string `json:"name"`
	Age int `json:"age"`
}
func main(){
	t := &Test{
		Name:"xiaoming",
		Age:12,
	}
	data,err := json.Marshal(t)
	fmt.Println(data)
	if err != nil{
		fmt.Println("marshal is failed,err:",err)
	}
	logger := initLogger("info")
	logger.Info("level")
}



