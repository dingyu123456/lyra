package config

import "gorm.io/gorm/logger"

type Config struct {
	Name            string `mapstructure:"name"`
	Version         string `mapstructure:"version"`
	*DatabaseConfig `mapstructure:"database"`
	*KarmadaConfig  `mapstructure:"karmada"`
	*LogConfig      `mapstructure:"log"`
}

type DatabaseConfig struct {
	Host                      string `mapstructure:"host"`
	Port                      int    `mapstructure:"port"`
	Username                  string `mapstructure:"username"`
	Password                  string `mapstructure:"password"`
	DBName                    string `mapstructure:"dbname"`
	MaxOpenConns              int    `mapstructure:"max_open_conns"`
	MaxIdleConns              int    `mapstructure:"max_idle_conns"`
	ConnMaxLifetimeHours      int    `mapstructure:"conn_max_lifetime"`
	SlowThresholdMs           int    `mapstructure:"slow_threshold_ms"`
	GormLogLevel              string `mapstructure:"gorm_log_level"`
	IgnoreRecordNotFoundError bool   `mapstructure:"ignore_record_not_found_error"`
}

func (dc *DatabaseConfig) ToGormLogLevel() logger.LogLevel {
	switch dc.GormLogLevel {
	case "silent":
		return logger.Silent
	case "error":
		return logger.Error
	case "warn":
		return logger.Warn
	case "info":
		return logger.Info
	default:
		return logger.Info
	}
}

type KarmadaConfig struct {
	KubeConfig string `mapstructure:"kubeconfig"`
}

type LogConfig struct {
	Level      string `mapstructure:"level"`
	FileName   string `mapstructure:"file_name"`
	MaxSize    int    `mapstructure:"max_size"`
	MaxBackups int    `mapstructure:"max_backups"`
	MaxAge     int    `mapstructure:"max_age"`
}
