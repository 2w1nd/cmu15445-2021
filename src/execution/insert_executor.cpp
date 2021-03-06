//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();
}

void InsertExecutor::InsertIntoTableWithIndex(Tuple *cur_tuple) {
  RID cur_id;

  // 直接调用table_heap_的api插入
  if (!table_heap_->InsertTuple(*cur_tuple, &cur_id, exec_ctx_->GetTransaction())) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertExecutor: no enough space for this tuple");
  }

  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
  Transaction *txn = GetExecutorContext()->GetTransaction();
  // 加锁
  if (lock_mgr != nullptr) {
    if (txn->IsSharedLocked(cur_id)) {
      lock_mgr->LockUpgrade(txn, cur_id);
    } else if (txn->IsExclusiveLocked(cur_id)) {
      lock_mgr->LockExclusive(txn, cur_id);
    }
  }

  // 更新索引
  for (const auto &index : catalog_->GetTableIndexes(table_info_->name_)) {
    index->index_->InsertEntry(
        cur_tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()),
        cur_id, exec_ctx_->GetTransaction());
    // 在事务中记录下变更
    IndexWriteRecord write_record(cur_id, table_info_->oid_, WType::INSERT, *cur_tuple, index->index_oid_,
                                  exec_ctx_->GetCatalog());
    write_record.tuple_ = *cur_tuple;
    txn->GetIndexWriteSet()->emplace_back(write_record);
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mgr != nullptr) {  // 提交读才需要解锁
    lock_mgr->Unlock(txn, cur_id);
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // 判断有无子计划，没有的话直接插入
  if (plan_->IsRawInsert()) {
    for (const auto &row_value : plan_->RawValues()) {
      Tuple cur_tuple(row_value, &(table_info_->schema_));
      InsertIntoTableWithIndex(&cur_tuple);
    }
    return false;
  }

  std::vector<Tuple> child_tuples;
  child_executor_->Init();
  try {
    Tuple cur_tuple;
    RID cur_rid;
    while (child_executor_->Next(&cur_tuple, &cur_rid)) {
      child_tuples.push_back(cur_tuple);
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "InsertExecutor:child execute error.");
    return false;
  }

  for (auto &child_tuple : child_tuples) {
    InsertIntoTableWithIndex(&child_tuple);
  }

  return false;
}

}  // namespace bustub
