package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"akapledge-indexer/internal/config"
	"akapledge-indexer/internal/eth"
	"akapledge-indexer/internal/indexer"
	"akapledge-indexer/internal/store"

	"go.uber.org/zap"
)

func main() {
	cfgPath := flag.String("config", "./configs/config.yaml", "config path")
	once := flag.Bool("once", false, "run one round then exit")
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		logger.Fatal("load config failed", zap.Error(err))
	}

	st, err := store.NewMySQL(cfg.Database.DSN, cfg.Database.MaxOpenConns, cfg.Database.MaxIdleConns, cfg.Database.ConnMaxLifetimeMinutes, logger)
	if err != nil {
		logger.Fatal("init mysql failed", zap.Error(err))
	}
	defer st.Close()

	ctx := context.Background()
	// if err := st.EnsureSchema(ctx); err != nil {
	// 	logger.Fatal("ensure schema failed", zap.Error(err))
	// }

	ec, err := eth.Dial(cfg.Chain.RPCURL)
	if err != nil {
		logger.Fatal("sync once failed",
			zap.Error(err),
			zap.ByteString("stack", debug.Stack()),
		)
	}
	defer ec.Close()

	svc, err := indexer.New(cfg, logger, st, ec)
	if err != nil {
		logger.Fatal("sync once failed",
			zap.Error(err),
			zap.ByteString("stack", debug.Stack()),
		)
	}

	if *once {
		logger.Info("startup", zap.Bool("once", *once))
		if err := svc.SyncOnce(ctx); err != nil {
			logger.Fatal("sync once failed",
				zap.Error(err),
				zap.ByteString("stack", debug.Stack()),
			)
		}
		return
	}

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := svc.Run(runCtx); err != nil && err != context.Canceled {
		logger.Fatal("sync once failed",
			zap.Error(err),
			zap.ByteString("stack", debug.Stack()),
		)
	}
	defer func() {
		logger.Info("program exit")
	}()
}
