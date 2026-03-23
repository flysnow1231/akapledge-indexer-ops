package eth

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"go.uber.org/zap"
)

type Client struct {
	RPC    *ethclient.Client
	logger *zap.Logger
}

func Dial(rpcURL string) (*Client, error) {
	c, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("dial rpc: %w", err)
	}
	return &Client{RPC: c}, nil
}

func (c *Client) Close() { c.RPC.Close() }

func (c *Client) LatestBlock(ctx context.Context) (uint64, error) {
	return retryRPC(ctx, c.logger, "HeaderByNumber(latest)", func() (uint64, error) {
		header, err := c.RPC.HeaderByNumber(ctx, nil)
		if err != nil {
			return 0, err
		}
		return header.Number.Uint64(), nil
	})
}
func (c *Client) FilterLogs(ctx context.Context, contract common.Address, from, to uint64) ([]types.Log, error) {
	q := ethereum.FilterQuery{
		Addresses: []common.Address{contract},
		FromBlock: new(big.Int).SetUint64(from),
		ToBlock:   new(big.Int).SetUint64(to),
	}
	return retryRPC(ctx, c.logger,
		fmt.Sprintf("FilterLogs(%s,%d-%d)", contract.Hex(), from, to),
		func() ([]types.Log, error) {
			return c.RPC.FilterLogs(ctx, q)
		})
}

func (c *Client) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	return retryRPC(ctx, c.logger,
		fmt.Sprintf("TransactionReceipt(%s)", txHash.Hex()),
		func() (*types.Receipt, error) {
			return c.RPC.TransactionReceipt(ctx, txHash)
		})
}

func isRateLimitErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "429 Too Many Requests") ||
		strings.Contains(msg, "\"code\":429") ||
		strings.Contains(msg, "exceeded its compute units per second capacity")
}
func retryRPC[T any](
	ctx context.Context,
	logger *zap.Logger,
	opName string,
	fn func() (T, error),
) (T, error) {
	const maxRetries = 5
	baseDelay := 800 * time.Millisecond

	var zero T
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		v, err := fn()
		if err == nil {
			return v, nil
		}

		lastErr = err

		if !isRateLimitErr(err) {
			return zero, fmt.Errorf("%s: %w", opName, err)
		}

		if attempt == maxRetries {
			break
		}

		delay := baseDelay * time.Duration(1<<attempt)
		jitter := time.Duration(rand.Intn(300)) * time.Millisecond
		wait := delay + jitter

		if logger != nil {
			logger.Warn("rpc hit rate limit, retrying",
				zap.String("op", opName), zap.Int("attempt", attempt+1),
				zap.Duration("wait", wait),
				zap.Error(err),
			)
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, ctx.Err()
		case <-timer.C:
		}
	}

	return zero, fmt.Errorf("%s after retries exhausted: %w", opName, lastErr)
}
