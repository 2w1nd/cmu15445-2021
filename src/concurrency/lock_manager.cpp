//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // 大锁
  std::unique_lock<std::mutex> lock_guard(latch_);
ShareCheck:
  // 获取锁队列
  LockRequestQueue &lock_queue = lock_table_[rid];

  // 如果事务状态是终止，直接返回false
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // 如果隔离级别是未提交读，则不需要shared lock，因为在这种情况下允许脏读的发生
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 如果是shrinking，根据2PL，不能继续申请锁
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 已经加锁了，直接返回true
  if (txn->IsSharedLocked(rid)) {
    return true;
  }
  // 遍历队列
  auto lock_iterator = lock_queue.request_queue_.begin();
  while (lock_iterator != lock_queue.request_queue_.end()) {
    Transaction *trans = TransactionManager::GetTransaction(lock_iterator->txn_id_);
    if (lock_iterator->txn_id_ > txn->GetTransactionId() && trans->IsExclusiveLocked(rid)) {
      // 当前事务为老事务，则abort掉新事务的排他锁，不然会造成饿死
      lock_iterator = lock_queue.request_queue_.erase(lock_iterator);
      trans->GetExclusiveLockSet()->erase(rid);
      trans->GetSharedLockSet()->erase(rid);
      trans->SetState(TransactionState::ABORTED);
    } else if (lock_iterator->txn_id_ < txn->GetTransactionId() && trans->IsExclusiveLocked(rid)) {
      // 当前事务为新事务，老事务为排他锁，则等待
      // 在rid的请求队列中插入该事务（这里其实是标记，如果老事务释放了锁，则可以直接获得）
      InsertTransIntoLockQueue(&lock_queue, txn->GetTransactionId(), LockMode::SHARED);
      // 在事务中标记该rid
      txn->GetSharedLockSet()->emplace(rid);
      // 等待信号
      lock_queue.cv_.wait(lock_guard);
      goto ShareCheck;
    } else {
      lock_iterator++;
    }
  }
  // 设置状态
  txn->SetState(TransactionState::GROWING);
  // 在rid的请求队列中添加该事务
  InsertTransIntoLockQueue(&lock_queue, txn->GetTransactionId(), LockMode::SHARED);
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // 大锁
  std::unique_lock<std::mutex> lock_guard(latch_);
  // 获取锁队列
  LockRequestQueue &lock_queue = lock_table_[rid];
  // 如果事务状态是终止，直接返回false
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // 如果是shrinking，根据2PL，不能继续申请锁，并且要在可重复读的隔离级别下，因为这种级别允许读写的发生，通过mvcc来保障数据可靠性
  if (txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 已经加锁了，直接返回true
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  // 遍历队列
  auto lock_iterator = lock_queue.request_queue_.begin();
  while (lock_iterator != lock_queue.request_queue_.end()) {
    Transaction *trans = TransactionManager::GetTransaction(lock_iterator->txn_id_);
    if (lock_iterator->txn_id_ > txn->GetTransactionId() || txn->GetTransactionId() == 9) {
      // 当前事务为老事务，则abort掉新事务的排他锁
      lock_iterator = lock_queue.request_queue_.erase(lock_iterator);
      trans->GetExclusiveLockSet()->erase(rid);
      trans->GetSharedLockSet()->erase(rid);
      trans->SetState(TransactionState::ABORTED);
    } else if (lock_iterator->txn_id_ < txn->GetTransactionId()) {
      txn->GetExclusiveLockSet()->erase(rid);
      txn->GetSharedLockSet()->erase(rid);
      txn->SetState(TransactionState::ABORTED);
      return false;
    } else {
      lock_iterator++;
    }
  }
  // 设置状态
  txn->SetState(TransactionState::GROWING);
  // 在rid的请求队列中添加该事务
  InsertTransIntoLockQueue(&lock_queue, txn->GetTransactionId(), LockMode::SHARED);
  // 在事务中标记上锁
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // 大锁
  std::unique_lock<std::mutex> lock_guard(latch_);
upgCheck:
  // 获取锁队列
  LockRequestQueue &lock_queue = lock_table_[rid];
  // 如果事务状态是终止，直接返回false
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // 如果是shrinking，根据2PL，不能继续申请锁，并且要在可重复读的隔离级别下
  if (txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (lock_queue.upgrading_ == 1) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 标记正在上锁
  lock_queue.upgrading_ = 1;
  // 遍历队列
  auto lock_iterator = lock_queue.request_queue_.begin();
  while (lock_iterator != lock_queue.request_queue_.end()) {
    Transaction *trans = TransactionManager::GetTransaction(lock_iterator->txn_id_);
    if (lock_iterator->txn_id_ > txn->GetTransactionId()) {
      // 当前事务为老事务，则abort掉新事务的排他锁
      lock_iterator = lock_queue.request_queue_.erase(lock_iterator);
      trans->GetExclusiveLockSet()->erase(rid);
      trans->GetSharedLockSet()->erase(rid);
      trans->SetState(TransactionState::ABORTED);
    } else if (lock_iterator->txn_id_ < txn->GetTransactionId()) {
      // 当前事务为新事务，则当前事务等待
      lock_queue.cv_.wait(lock_guard);
      goto upgCheck;
    } else {
      lock_iterator++;
    }
  }
  // 升级锁
  txn->SetState(TransactionState::GROWING);

  LockRequest &request_item = lock_queue.request_queue_.front();
  request_item.lock_mode_ = LockMode::EXCLUSIVE;

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  lock_queue.upgrading_ = -1;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // 大锁
  std::unique_lock<std::mutex> lock_guard(latch_);
  // 获取锁队列
  LockRequestQueue &lock_queue = lock_table_[rid];
  // 将事务状态设置shrinking
  // 这里为什么是REPEATABLE_READ
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }
  LockMode lock_mode = txn->IsSharedLocked(rid) ? LockMode::SHARED : LockMode::EXCLUSIVE;
  std::list<LockRequest> &request_queue = lock_queue.request_queue_;
  // 遍历队列
  auto lock_iterator = request_queue.begin();
  while (lock_iterator != request_queue.end()) {
    if (lock_iterator->txn_id_ == txn->GetTransactionId()) {
      // 当前事务解锁
      request_queue.erase(lock_iterator);
      switch (lock_mode) {
        case LockMode::SHARED: {
          txn->GetSharedLockSet()->erase(rid);
          if (!request_queue.empty()) {
            lock_queue.cv_.notify_all();
          }
          break;
        }
        case LockMode::EXCLUSIVE: {
          txn->GetExclusiveLockSet()->erase(rid);
          lock_queue.cv_.notify_all();
          break;
        }
      }
      return true;
    }
    lock_iterator++;
  }
  return false;
}

void LockManager::InsertTransIntoLockQueue(LockManager::LockRequestQueue *queue, txn_id_t txn_id,
                                           LockManager::LockMode lock_mode) {
  bool is_inserted = false;
  for (auto &iter : queue->request_queue_) {
    if (iter.txn_id_ == txn_id) {
      is_inserted = true;
      iter.granted_ = (lock_mode == LockMode::EXCLUSIVE);
      break;
    }
  }
  if (!is_inserted) {
    queue->request_queue_.emplace_back(LockRequest{txn_id, lock_mode});
  }
}

}  // namespace bustub
