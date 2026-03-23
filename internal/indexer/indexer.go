package indexer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"runtime/debug"
	"strings"
	"time"

	contractabi "akapledge-indexer/internal/abi"
	"akapledge-indexer/internal/config"
	"akapledge-indexer/internal/eth"
	"akapledge-indexer/internal/store"
	"akapledge-indexer/internal/util"

	gethabi "github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"
)

type Service struct {
	cfg      *config.Config
	logger   *zap.Logger
	store    *store.MySQLStore
	eth      *eth.Client
	contract common.Address
	abi      gethabi.ABI
}

func New(cfg *config.Config, logger *zap.Logger, st *store.MySQLStore, ec *eth.Client) (*Service, error) {
	parsed, err := gethabi.JSON(strings.NewReader(contractabi.AkaPledgeABI))
	if err != nil {
		return nil, fmt.Errorf("parse abi: %w", err)
	}
	return &Service{
		cfg:      cfg,
		logger:   logger,
		store:    st,
		eth:      ec,
		contract: common.HexToAddress(cfg.Chain.ContractAddress),
		abi:      parsed,
	}, nil
}

func (s *Service) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Duration(s.cfg.Chain.PollIntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		if err := s.SyncOnce(ctx); err != nil {
			s.logger.Error("sync once failed", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Service) SyncOnce(ctx context.Context) error {
	latest, err := s.eth.LatestBlock(ctx)
	if err != nil {
		s.logger.Error("LatestBlock failed",
			zap.Error(err),
			zap.ByteString("stack", debug.Stack()),
		)
		return err
	}
	if latest <= s.cfg.Chain.DelayBlocks {
		s.logger.Info("waiting blocks for delay window", zap.Uint64("latest", latest), zap.Uint64("delay_blocks", s.cfg.Chain.DelayBlocks))
		return nil
	}
	safeLatest := latest - s.cfg.Chain.DelayBlocks

	contractAddr := strings.ToLower(s.contract.Hex())
	cp, err := s.store.GetCheckpoint(ctx, s.cfg.Indexer.TaskName, s.cfg.Chain.ChainID, contractAddr)
	if err != nil {
		return err
	}

	from := cp + 1
	if cp == 0 && s.cfg.Chain.StartBlock > 0 {
		from = s.cfg.Chain.StartBlock
	}
	if from > safeLatest {
		return s.saveIdleCheckpoint(ctx, from, cp, safeLatest)
	}

	for start := from; start <= safeLatest; start += s.cfg.Chain.BatchSize {
		end := start + s.cfg.Chain.BatchSize - 1
		if end > safeLatest {
			end = safeLatest
		}
		if err := s.syncRange(ctx, start, end, safeLatest); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) saveIdleCheckpoint(ctx context.Context, from, scanned, safeLatest uint64) error {
	tx, err := s.store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.store.SaveCheckpointTx(ctx, tx, s.cfg.Indexer.TaskName, s.cfg.Chain.ChainID, strings.ToLower(s.contract.Hex()), from, scanned, safeLatest, s.cfg.Chain.DelayBlocks, "IDLE"); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) syncRange(ctx context.Context, from, to, safeLatest uint64) error {
	logs, err := s.eth.FilterLogs(ctx, s.contract, from, to)
	if err != nil {
		return fmt.Errorf("filter logs %d-%d: %w", from, to, err)
	}

	tx, err := s.store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, lg := range logs {
		if s.cfg.Indexer.EnableReceiptStatusCheck {
			rcpt, err := s.eth.TransactionReceipt(ctx, lg.TxHash)
			if err != nil {
				return fmt.Errorf("get receipt: %w", err)
			}
			// debug
			evt, err := s.abi.EventByID(lg.Topics[0])
			if err != nil {
				panic(err)
			}
			s.logger.Info("tx=%s logIndex=%d topic0=%s eventName=%s dataLen=%d topics=%d",
				zap.String("txHash", lg.TxHash.Hex()),
				zap.Uint("logIndex", lg.Index),
				zap.String("topic0", lg.Topics[0].Hex()),
				zap.String("eventName", evt.Name),
				zap.Int("dataLen", len(lg.Data)),
				zap.Int("topics", len(lg.Topics)),
			)

			if len(lg.Topics) == 0 || lg.Topics[0] != evt.ID {
				panic(fmt.Errorf("log is not BorrowerClaimed: tx=%s logIndex=%d", lg.TxHash.Hex(), lg.Index))
			}

			values, err := s.abi.Unpack("BorrowerClaimed", lg.Data)
			// fmt.Printf("UNPACK OK tx=%s logIndex=%d p0=%v p1=%v p2=%v\n",
			// 	lg.TxHash.Hex(),
			// 	lg.Index,
			// 	values[0],
			// 	values[1],
			// 	values[2],
			// )
			// if err != nil {
			// 	fmt.Printf("UNPACK FAIL tx=%s logIndex=%d data=%x\n", lg.TxHash.Hex(), lg.Index, lg.Data)
			// 	panic(err)
			// }

			// debug
			for i, v := range values {
				s.logger.Info("value param", zap.Int("index", i), zap.String("value", fmt.Sprintf("%v", v)))
			}

			if rcpt.Status != types.ReceiptStatusSuccessful {
				continue
			}
		}
		if err := s.processLogTx(ctx, tx, lg); err != nil {
			return fmt.Errorf("process log tx=%s logIndex=%d: %w", lg.TxHash.Hex(), lg.Index, err)
		}
	}

	if err := s.store.SaveCheckpointTx(ctx, tx, s.cfg.Indexer.TaskName, s.cfg.Chain.ChainID, strings.ToLower(s.contract.Hex()), from, to, safeLatest, s.cfg.Chain.DelayBlocks, "RUNNING"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	s.logger.Info("sync range ok",
		zap.Uint64("from", from),
		zap.Uint64("to", to),
		zap.Uint64("safe_latest", safeLatest),
		zap.Int("logs", len(logs)),
	)
	return nil
}

func (s *Service) processLogTx(ctx context.Context, tx *sql.Tx, lg types.Log) error {
	if len(lg.Topics) == 0 {
		return nil
	}
	event, err := s.abi.EventByID(lg.Topics[0])
	if err != nil {
		return nil
	}

	poolID := ""
	userAddress := ""
	payload := map[string]any{}
	contractAddr := strings.ToLower(s.contract.Hex())
	chainID := s.cfg.Chain.ChainID

	switch event.Name {
	case "PoolCreated":
		var e struct {
			LendToken               common.Address
			CollateralToken         common.Address
			SettleTime              *big.Int
			ExecutionEndTime        *big.Int
			FixedInterestRateBps    *big.Int
			CollateralRatioBps      *big.Int
			LiquidationThresholdBps *big.Int
			ProtocolFeeBps          *big.Int
			SlippageBps             *big.Int
		}
		if err := s.unpack(lg, event.Name, &e); err != nil {
			return err
		}
		poolID = topicBig(lg.Topics[1]).String()
		admin := topicAddr(lg.Topics[2])
		payload = map[string]any{
			"poolId": poolID, "admin": admin,
			"lendToken":       strings.ToLower(e.LendToken.Hex()),
			"collateralToken": strings.ToLower(e.CollateralToken.Hex()),
			"settleTime":      e.SettleTime.String(), "executionEndTime": e.ExecutionEndTime.String(),
			"fixedInterestRateBps": e.FixedInterestRateBps.String(), "collateralRatioBps": e.CollateralRatioBps.String(),
			"liquidationThresholdBps": e.LiquidationThresholdBps.String(), "protocolFeeBps": e.ProtocolFeeBps.String(), "slippageBps": e.SlippageBps.String(),
		}
		_, err = tx.ExecContext(ctx, `
INSERT INTO akapledge_pool(
  chain_id, contract_address, pool_id, admin_address, lend_token, collateral_token, stage,
  settle_time, execution_end_time, fixed_interest_rate_bps, collateral_ratio_bps,
  liquidation_threshold_bps, protocol_fee_bps, slippage_bps, last_event_block
) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE
  admin_address=VALUES(admin_address), lend_token=VALUES(lend_token), collateral_token=VALUES(collateral_token),
  settle_time=VALUES(settle_time), execution_end_time=VALUES(execution_end_time),
  fixed_interest_rate_bps=VALUES(fixed_interest_rate_bps), collateral_ratio_bps=VALUES(collateral_ratio_bps),
  liquidation_threshold_bps=VALUES(liquidation_threshold_bps), protocol_fee_bps=VALUES(protocol_fee_bps),
  slippage_bps=VALUES(slippage_bps), stage=VALUES(stage), last_event_block=VALUES(last_event_block)`,
			chainID, contractAddr, poolID, admin, strings.ToLower(e.LendToken.Hex()), strings.ToLower(e.CollateralToken.Hex()), "MATCH",
			e.SettleTime.Uint64(), e.ExecutionEndTime.Uint64(), e.FixedInterestRateBps.String(), e.CollateralRatioBps.String(),
			e.LiquidationThresholdBps.String(), e.ProtocolFeeBps.String(), e.SlippageBps.String(), lg.BlockNumber,
		)
		if err != nil {
			return err
		}

	case "PoolStageUpdated":
		var e struct{ FromStage, ToStage uint8 }
		if err := s.unpack(lg, event.Name, &e); err != nil {
			return err
		}
		poolID = topicBig(lg.Topics[1]).String()
		payload = map[string]any{"poolId": poolID, "fromStage": e.FromStage, "toStage": e.ToStage, "fromStageName": util.StageName(e.FromStage), "toStageName": util.StageName(e.ToStage)}
		_, err = tx.ExecContext(ctx, `UPDATE akapledge_pool SET stage=?, last_event_block=? WHERE chain_id=? AND contract_address=? AND pool_id=?`, util.StageName(e.ToStage), lg.BlockNumber, chainID, contractAddr, poolID)
		if err != nil {
			return err
		}

	case "LendDeposited":
		var e struct{ Amount *big.Int }
		if err := s.unpack(lg, event.Name, &e); err != nil {
			return err
		}
		poolID = topicBig(lg.Topics[1]).String()
		userAddress = topicAddr(lg.Topics[2])
		payload = map[string]any{"poolId": poolID, "lender": userAddress, "amount": e.Amount.String()}
		if err := s.upsertLender(ctx, tx, poolID, userAddress, map[string]string{"lend_deposited": e.Amount.String()}, lg.BlockNumber); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE akapledge_pool SET total_lend_deposited = CAST(total_lend_deposited AS DECIMAL(65,0)) + ?, last_event_block=? WHERE chain_id=? AND contract_address=? AND pool_id=?`, e.Amount.String(), lg.BlockNumber, chainID, contractAddr, poolID)
		if err != nil {
			return err
		}

	case "BorrowDeposited":
		var e struct{ Amount *big.Int }
		if err := s.unpack(lg, event.Name, &e); err != nil {
			return err
		}
		poolID = topicBig(lg.Topics[1]).String()
		userAddress = topicAddr(lg.Topics[2])
		payload = map[string]any{"poolId": poolID, "borrower": userAddress, "amount": e.Amount.String()}
		if err := s.upsertBorrower(ctx, tx, poolID, userAddress, map[string]string{"collateral_deposited": e.Amount.String()}, lg.BlockNumber); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE akapledge_pool SET total_collateral_deposited = CAST(total_collateral_deposited AS DECIMAL(65,0)) + ?, last_event_block=? WHERE chain_id=? AND contract_address=? AND pool_id=?`, e.Amount.String(), lg.BlockNumber, chainID, contractAddr, poolID)
		if err != nil {
			return err
		}

	case "Settled":
		var e struct {
			TotalLendDeposited, TotalCollateralDeposited, MatchedPrincipal, UsedCollateralAmount, UnmatchedLendTotal, UnmatchedCollateralTotal *big.Int
		}
		if err := s.unpack(lg, event.Name, &e); err != nil {
			return err
		}
		poolID = topicBig(lg.Topics[1]).String()
		payload = map[string]any{"poolId": poolID, "totalLendDeposited": e.TotalLendDeposited.String(), "totalCollateralDeposited": e.TotalCollateralDeposited.String(), "matchedPrincipal": e.MatchedPrincipal.String(), "usedCollateralAmount": e.UsedCollateralAmount.String(), "unmatchedLendTotal": e.UnmatchedLendTotal.String(), "unmatchedCollateralTotal": e.UnmatchedCollateralTotal.String()}
		_, err = tx.ExecContext(ctx, `UPDATE akapledge_pool SET total_lend_deposited=?, total_collateral_deposited=?, matched_principal=?, used_collateral_amount=?, unmatched_lend_total=?, unmatched_collateral_total=?, last_event_block=? WHERE chain_id=? AND contract_address=? AND pool_id=?`, e.TotalLendDeposited.String(), e.TotalCollateralDeposited.String(), e.MatchedPrincipal.String(), e.UsedCollateralAmount.String(), e.UnmatchedLendTotal.String(), e.UnmatchedCollateralTotal.String(), lg.BlockNumber, chainID, contractAddr, poolID)
		if err != nil {
			return err
		}

	case "LenderRefunded":
		var e struct{ Amount *big.Int }
		if err := s.unpack(lg, event.Name, &e); err != nil {
			return err
		}
		poolID = topicBig(lg.Topics[1]).String()
		userAddress = topicAddr(lg.Topics[2])
		payload = map[string]any{"poolId": poolID, "lender": userAddress, "amount": e.Amount.String()}
		if err := s.upsertLender(ctx, tx, poolID, userAddress, map[string]string{"lend_refunded": e.Amount.String()}, lg.BlockNumber); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE akapledge_pool SET stable_lend_refund_paid_total = CAST(stable_lend_refund_paid_total AS DECIMAL(65,0)) + ?, last_event_block=? WHERE chain_id=? AND contract_address=? AND pool_id=?`, e.Amount.String(), lg.BlockNumber, chainID, contractAddr, poolID)
		if err != nil {
			return err
		}

	case "LenderClaimed":
		var e struct{ MatchedPrincipalShare, SpAmountMinted *big.Int }
		if err := s.unpack(lg, event.Name, &e); err != nil {
			return err
		}
		poolID = topicBig(lg.Topics[1]).String()
		userAddress = topicAddr(lg.Topics[2])
		payload = map[string]any{"poolId": poolID, "lender": userAddress, "matchedPrincipalShare": e.MatchedPrincipalShare.String(), "spAmountMinted": e.SpAmountMinted.String()}
		if err := s.upsertLender(ctx, tx, poolID, userAddress, map[string]string{"matched_principal_share": e.MatchedPrincipalShare.String(), "sp_minted": e.SpAmountMinted.String()}, lg.BlockNumber); err != nil {
			return err
		}

	case "BorrowerRefunded":
		var e struct{ Amount *big.Int }
		if err := s.unpack(lg, event.Name, &e); err != nil {
			return err
		}
		poolID = topicBig(lg.Topics[1]).String()
		userAddress = topicAddr(lg.Topics[2])
		payload = map[string]any{"poolId": poolID, "borrower": userAddress, "amount": e.Amount.String()}
		if err := s.upsertBorrower(ctx, tx, poolID, userAddress, map[string]string{"collateral_refunded": e.Amount.String()}, lg.BlockNumber); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE akapledge_pool SET collateral_borrow_refund_paid_total = CAST(collateral_borrow_refund_paid_total AS DECIMAL(65,0)) + ?, last_event_block=? WHERE chain_id=? AND contract_address=? AND pool_id=?`, e.Amount.String(), lg.BlockNumber, chainID, contractAddr, poolID)
		if err != nil {
			return err
		}

	case "BorrowerClaimed":
		var e struct{ CollateralUsedShare, LoanPaidAmount, JpAmountMinted *big.Int }
		if err := s.unpack(lg, event.Name, &e); err != nil {
			return err
		}
		poolID = topicBig(lg.Topics[1]).String()
		userAddress = topicAddr(lg.Topics[2])
		payload = map[string]any{"poolId": poolID, "borrower": userAddress, "collateralUsedShare": e.CollateralUsedShare.String(), "loanPaidAmount": e.LoanPaidAmount.String(), "jpAmountMinted": e.JpAmountMinted.String()}
		if err := s.upsertBorrower(ctx, tx, poolID, userAddress, map[string]string{"collateral_used_share": e.CollateralUsedShare.String(), "loan_paid_amount": e.LoanPaidAmount.String(), "jp_minted": e.JpAmountMinted.String()}, lg.BlockNumber); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE akapledge_pool SET stable_loan_paid_total = CAST(stable_loan_paid_total AS DECIMAL(65,0)) + ?, last_event_block=? WHERE chain_id=? AND contract_address=? AND pool_id=?`, e.LoanPaidAmount.String(), lg.BlockNumber, chainID, contractAddr, poolID)
		if err != nil {
			return err
		}

	case "HealthChecked":
		var e struct {
			CollateralValueStable, DebtTotal, RatioBps *big.Int
			Sufficient                                 bool
		}
		if err := s.unpack(lg, event.Name, &e); err != nil {
			return err
		}
		poolID = topicBig(lg.Topics[1]).String()
		payload = map[string]any{"poolId": poolID, "collateralValueStable": e.CollateralValueStable.String(), "debtTotal": e.DebtTotal.String(), "ratioBps": e.RatioBps.String(), "sufficient": e.Sufficient}
		health := 0
		if e.Sufficient {
			health = 1
		}
		_, err = tx.ExecContext(ctx, `UPDATE akapledge_pool SET latest_collateral_value_stable=?, latest_debt_total=?, latest_ratio_bps=?, latest_health_result=?, last_event_block=? WHERE chain_id=? AND contract_address=? AND pool_id=?`, e.CollateralValueStable.String(), e.DebtTotal.String(), e.RatioBps.String(), health, lg.BlockNumber, chainID, contractAddr, poolID)
		if err != nil {
			return err
		}

	case "Finished":
		var e struct{ CollateralBalanceBeforeSwap, CollateralSold, StableBalanceBefore, StableBalanceAfter, StableToLendersTotal, StableToBorrowersSurplusTotal, CollateralToBorrowersTotal *big.Int }
		if err := s.unpack(lg, event.Name, &e); err != nil {
			return err
		}
		poolID = topicBig(lg.Topics[1]).String()
		payload = map[string]any{"poolId": poolID, "collateralBalanceBeforeSwap": e.CollateralBalanceBeforeSwap.String(), "collateralSold": e.CollateralSold.String(), "stableBalanceBefore": e.StableBalanceBefore.String(), "stableBalanceAfter": e.StableBalanceAfter.String(), "stableToLendersTotal": e.StableToLendersTotal.String(), "stableToBorrowersSurplusTotal": e.StableToBorrowersSurplusTotal.String(), "collateralToBorrowersTotal": e.CollateralToBorrowersTotal.String()}
		_, err = tx.ExecContext(ctx, `UPDATE akapledge_pool SET stable_to_lenders_total=?, stable_to_borrowers_surplus_total=?, collateral_to_borrowers_total=?, finish_collateral_sold=?, finish_stable_balance_before=?, finish_stable_balance_after=?, finish_block_number=?, last_event_block=? WHERE chain_id=? AND contract_address=? AND pool_id=?`, e.StableToLendersTotal.String(), e.StableToBorrowersSurplusTotal.String(), e.CollateralToBorrowersTotal.String(), e.CollateralSold.String(), e.StableBalanceBefore.String(), e.StableBalanceAfter.String(), lg.BlockNumber, lg.BlockNumber, chainID, contractAddr, poolID)
		if err != nil {
			return err
		}

	case "Liquidated":
		var e struct{ CollateralBalanceBeforeSwap, CollateralSold, StableBalanceBefore, StableBalanceAfter, StableToLendersTotal, StableToBorrowersSurplusTotal *big.Int }
		if err := s.unpack(lg, event.Name, &e); err != nil {
			return err
		}
		poolID = topicBig(lg.Topics[1]).String()
		payload = map[string]any{"poolId": poolID, "collateralBalanceBeforeSwap": e.CollateralBalanceBeforeSwap.String(), "collateralSold": e.CollateralSold.String(), "stableBalanceBefore": e.StableBalanceBefore.String(), "stableBalanceAfter": e.StableBalanceAfter.String(), "stableToLendersTotal": e.StableToLendersTotal.String(), "stableToBorrowersSurplusTotal": e.StableToBorrowersSurplusTotal.String()}
		_, err = tx.ExecContext(ctx, `UPDATE akapledge_pool SET stable_to_lenders_total=?, stable_to_borrowers_surplus_total=?, collateral_to_borrowers_total='0', finish_collateral_sold=?, finish_stable_balance_before=?, finish_stable_balance_after=?, finish_block_number=?, last_event_block=? WHERE chain_id=? AND contract_address=? AND pool_id=?`, e.StableToLendersTotal.String(), e.StableToBorrowersSurplusTotal.String(), e.CollateralSold.String(), e.StableBalanceBefore.String(), e.StableBalanceAfter.String(), lg.BlockNumber, lg.BlockNumber, chainID, contractAddr, poolID)
		if err != nil {
			return err
		}

	case "LenderWithdrawn":
		var e struct{ SpBurned, StableAmount *big.Int }
		if err := s.unpack(lg, event.Name, &e); err != nil {
			return err
		}
		poolID = topicBig(lg.Topics[1]).String()
		userAddress = topicAddr(lg.Topics[2])
		payload = map[string]any{"poolId": poolID, "lender": userAddress, "spBurned": e.SpBurned.String(), "stableAmount": e.StableAmount.String()}
		if err := s.upsertLender(ctx, tx, poolID, userAddress, map[string]string{"sp_burned": e.SpBurned.String(), "stable_withdrawn": e.StableAmount.String()}, lg.BlockNumber); err != nil {
			return err
		}

	case "BorrowerWithdrawn":
		var e struct{ JpBurned, CollateralAmount, StableSurplusAmount *big.Int }
		if err := s.unpack(lg, event.Name, &e); err != nil {
			return err
		}
		poolID = topicBig(lg.Topics[1]).String()
		userAddress = topicAddr(lg.Topics[2])
		payload = map[string]any{"poolId": poolID, "borrower": userAddress, "jpBurned": e.JpBurned.String(), "collateralAmount": e.CollateralAmount.String(), "stableSurplusAmount": e.StableSurplusAmount.String()}
		if err := s.upsertBorrower(ctx, tx, poolID, userAddress, map[string]string{"jp_burned": e.JpBurned.String(), "collateral_withdrawn": e.CollateralAmount.String(), "stable_surplus_withdrawn": e.StableSurplusAmount.String()}, lg.BlockNumber); err != nil {
			return err
		}

	default:
		return nil
	}

	return s.insertEventLog(ctx, tx, lg, event.Name, poolID, userAddress, payload)
}

func (s *Service) insertEventLog(ctx context.Context, tx *sql.Tx, lg types.Log, eventName, poolID, userAddress string, payload map[string]any) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO akapledge_event_log(
  chain_id, contract_address, block_number, block_hash, tx_hash, tx_index, log_index,
  event_name, pool_id, user_address, topic0, payload_json, removed
) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
ON DUPLICATE KEY UPDATE payload_json=VALUES(payload_json), removed=VALUES(removed)`,
		s.cfg.Chain.ChainID,
		strings.ToLower(s.contract.Hex()),
		lg.BlockNumber,
		strings.ToLower(lg.BlockHash.Hex()),
		strings.ToLower(lg.TxHash.Hex()),
		lg.TxIndex,
		lg.Index,
		eventName,
		poolID,
		userAddress,
		strings.ToLower(lg.Topics[0].Hex()),
		string(payloadBytes),
		lg.Removed,
	)
	return err
}

func (s *Service) upsertLender(ctx context.Context, tx *sql.Tx, poolID, lender string, increments map[string]string, lastBlock uint64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO akapledge_lender_position(chain_id, contract_address, pool_id, lender_address, last_event_block) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE last_event_block=GREATEST(last_event_block, VALUES(last_event_block))`, s.cfg.Chain.ChainID, strings.ToLower(s.contract.Hex()), poolID, lender, lastBlock)
	if err != nil {
		return err
	}
	for col, val := range increments {
		q := fmt.Sprintf(`UPDATE akapledge_lender_position SET %s = CAST(%s AS DECIMAL(65,0)) + ?, last_event_block=? WHERE chain_id=? AND contract_address=? AND pool_id=? AND lender_address=?`, col, col)
		if _, err := tx.ExecContext(ctx, q, val, lastBlock, s.cfg.Chain.ChainID, strings.ToLower(s.contract.Hex()), poolID, lender); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) upsertBorrower(ctx context.Context, tx *sql.Tx, poolID, borrower string, increments map[string]string, lastBlock uint64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO akapledge_borrower_position(chain_id, contract_address, pool_id, borrower_address, last_event_block) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE last_event_block=GREATEST(last_event_block, VALUES(last_event_block))`, s.cfg.Chain.ChainID, strings.ToLower(s.contract.Hex()), poolID, borrower, lastBlock)
	if err != nil {
		return err
	}
	for col, val := range increments {
		q := fmt.Sprintf(`UPDATE akapledge_borrower_position SET %s = CAST(%s AS DECIMAL(65,0)) + ?, last_event_block=? WHERE chain_id=? AND contract_address=? AND pool_id=? AND borrower_address=?`, col, col)
		if _, err := tx.ExecContext(ctx, q, val, lastBlock, s.cfg.Chain.ChainID, strings.ToLower(s.contract.Hex()), poolID, borrower); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) unpack(lg types.Log, eventName string, out any) error {
	return s.abi.UnpackIntoInterface(out, eventName, lg.Data)
}

func topicBig(topic common.Hash) *big.Int {
	return new(big.Int).SetBytes(topic.Bytes())
}

func topicAddr(topic common.Hash) string {
	return strings.ToLower(common.BytesToAddress(topic.Bytes()).Hex())
}
