package log

import (
	"context"

	"go.uber.org/zap"
)

type contextKey string

const loggerKey contextKey = "lyra_logger"

// NewContext 将带有附加上下文（如 PodName）的 Logger 塞入 Context
func NewContext(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// FromContext 从 Context 中提取 Logger。如果不存在，则降级为全局默认 Logger。
func FromContext(ctx context.Context) *zap.Logger {
	if logger, ok := ctx.Value(loggerKey).(*zap.Logger); ok {
		return logger
	}
	return zap.L() // 兜底返回全局 logger
}
