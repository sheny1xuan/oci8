package oci8

// #include "oci8.go.h"
import "C"
import (
	"log"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/transaction-wg/seata-golang/pkg/base/meta"
	"github.com/transaction-wg/seata-golang/pkg/client/config"
)

// Commit transaction commit
func (tx *Tx) Commit() error {
	if tx.conn.ctx != nil {
		branchID, err := tx.register()
		if err != nil {
			return tx.localRollback()
		}

		tx.conn.ctx.branchID = branchID

		if len(tx.conn.ctx.sqlUndoItemsBuffer) > 0 {
			err = GetUndoLogManager().FlushUndoLogs(tx.conn)
			if err != nil {
				err1 := tx.report(false)
				// ? 如果分支事务undolog生成失败，回滚事务本地事务，
				localRollbackErr := tx.localRollback()
				if localRollbackErr != nil {
					return localRollbackErr
				}
				if err1 != nil {
					return err1
				}

				return err
			}
			err = tx.localCommit()
			if err != nil {
				err1 := tx.report(false)
				if err1 != nil {
					return err1
				}
				return err
			}
		} else {
			err = tx.localCommit()
			return err
		}
	} else {
		err := tx.localCommit()
		return err
	}
	return nil
}

func (tx *Tx) localCommit() error {
	tx.conn.inTransaction = false
	if rv := C.OCITransCommit(
		tx.conn.svc,
		tx.conn.errHandle,
		0,
	); rv != C.OCI_SUCCESS {
		return tx.conn.getError(rv)
	}
	return nil
}

// Rollback transaction rollback
func (tx *Tx) Rollback() error {
	err := tx.localRollback()
	if tx.conn.ctx != nil {
		branchID, err := tx.register()
		if err != nil {
			return err
		}
		tx.conn.ctx.branchID = branchID
		tx.report(false)
	}
	return err
}

func (tx *Tx) localRollback() error {
	tx.conn.inTransaction = false
	if rv := C.OCITransRollback(
		tx.conn.svc,
		tx.conn.errHandle,
		0,
	); rv != C.OCI_SUCCESS {
		return tx.conn.getError(rv)
	}
	return nil
}

func (tx *Tx) register() (int64, error) {
	var branchID int64
	var err error

	for retryCount := 0; retryCount < config.GetATConfig().LockRetryTimes; retryCount++ {
		branchID, err = dataSourceManager.BranchRegister(meta.BranchTypeAT, tx.conn.cfg.Username, "", tx.conn.ctx.xid,
			nil, strings.Join(tx.conn.ctx.lockKeys, ";"))
		if err == nil {
			break
		}
		log.Printf("branch register err: %v \n", err)
		var tex *meta.TransactionException
		if errors.As(err, &tex) {
			if tex.Code == meta.TransactionExceptionCodeGlobalTransactionNotExist {
				break
			}
		}
		time.Sleep(config.GetATConfig().LockRetryInterval)
	}
	return branchID, err
}

func (tx *Tx) report(commitDone bool) error {
	retry := config.GetATConfig().LockRetryTimes
	for retry > 0 {
		var err error
		if commitDone {
			err = dataSourceManager.BranchReport(meta.BranchTypeAT, tx.conn.ctx.xid, tx.conn.ctx.branchID,
				meta.BranchStatusPhaseoneDone, nil)
		} else {
			err = dataSourceManager.BranchReport(meta.BranchTypeAT, tx.conn.ctx.xid, tx.conn.ctx.branchID,
				meta.BranchStatusPhaseoneFailed, nil)
		}
		if err != nil {
			log.Printf("Failed to report [%d/%s] commit done [%t] Retry Countdown: %d \n",
				tx.conn.ctx.branchID, tx.conn.ctx.xid, commitDone, retry)
		}
		retry = retry - 1
		if retry == 0 {
			return errors.WithMessagef(err, "Failed to report branch status %t", commitDone)
		}
	}
	return nil
}
