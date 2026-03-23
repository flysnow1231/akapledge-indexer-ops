package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
	App struct {
		Name string `mapstructure:"name"`
		Env  string `mapstructure:"env"`
	} `mapstructure:"app"`
	Chain struct {
		RPCURL              string `mapstructure:"rpc_url"`
		ChainID             int64  `mapstructure:"chain_id"`
		ContractAddress     string `mapstructure:"contract_address"`
		StartBlock          uint64 `mapstructure:"start_block"`
		DelayBlocks         uint64 `mapstructure:"delay_blocks"`
		BatchSize           uint64 `mapstructure:"batch_size"`
		PollIntervalSeconds int    `mapstructure:"poll_interval_seconds"`
	} `mapstructure:"chain"`
	Database struct {
		DSN                    string `mapstructure:"dsn"`
		MaxOpenConns           int    `mapstructure:"max_open_conns"`
		MaxIdleConns           int    `mapstructure:"max_idle_conns"`
		ConnMaxLifetimeMinutes int    `mapstructure:"conn_max_lifetime_minutes"`
	} `mapstructure:"database"`
	Indexer struct {
		TaskName                 string `mapstructure:"task_name"`
		ReorgSafeDistance        uint64 `mapstructure:"reorg_safe_distance"`
		EnableReceiptStatusCheck bool   `mapstructure:"enable_receipt_status_check"`
	} `mapstructure:"indexer"`
}

func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetConfigType("yaml")

	v.SetDefault("chain.delay_blocks", 6)
	v.SetDefault("chain.batch_size", 1500)
	v.SetDefault("chain.poll_interval_seconds", 8)
	v.SetDefault("database.max_open_conns", 20)
	v.SetDefault("database.max_idle_conns", 5)
	v.SetDefault("database.conn_max_lifetime_minutes", 30)
	v.SetDefault("indexer.task_name", "akapledge_ops_indexer")
	v.SetDefault("indexer.reorg_safe_distance", 12)
	v.SetDefault("indexer.enable_receipt_status_check", true)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if cfg.Chain.DelayBlocks == 0 {
		cfg.Chain.DelayBlocks = 6
	}
	if cfg.Chain.PollIntervalSeconds <= 0 {
		cfg.Chain.PollIntervalSeconds = 8
	}
	if cfg.Indexer.TaskName == "" {
		cfg.Indexer.TaskName = "akapledge_ops_indexer"
	}
	return &cfg, nil
}
