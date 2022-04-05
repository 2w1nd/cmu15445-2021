//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple cur_tuple;
  RID cur_rid;

  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
  Transaction *txn = GetExecutorContext()->GetTransaction();
  while (true) {
    try {
      if (!child_executor_->Next(&cur_tuple, &cur_rid)) {
        break;
      }
    } catch (Exception &e) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "deleteExecutor: child execute error");
    }

    // 加锁
    if (lock_mgr != nullptr) {
      if (txn->IsSharedLocked(cur_rid)) {
        lock_mgr->LockUpgrade(txn, cur_rid);
      } else if (txn->IsExclusiveLocked(cur_rid)) {
        lock_mgr->LockExclusive(txn, cur_rid);
      }
    }

    // 根据子查询器的结果来调用TableHeap标记删除状态
    TableHeap *table_heap = table_info_->table_.get();
    table_heap->MarkDelete(cur_rid, exec_ctx_->GetTransaction());

    // 更新索引
    for (const auto &index : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      auto index_info = index->index_.get();
      index_info->DeleteEntry(
          cur_tuple.KeyFromTuple(table_info_->schema_, *index_info->GetKeySchema(), index_info->GetKeyAttrs()), cur_rid,
          exec_ctx_->GetTransaction());
      // 在事务中记录下变更
      txn->GetIndexWriteSet()->emplace_back(IndexWriteRecord(cur_rid, table_info_->oid_, WType::DELETE, cur_tuple,
                                                             index->index_oid_, exec_ctx_->GetCatalog()));
    }

    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mgr != nullptr) {  // 提交读才需要解锁
      lock_mgr->Unlock(txn, cur_rid);
    }
  }

  return false;
}

}  // namespace bustub
