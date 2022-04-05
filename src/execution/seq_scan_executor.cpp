//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), iter_(nullptr, RID{}, nullptr) {}

void SeqScanExecutor::Init() {
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  iter_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (iter_ == table_heap_->End()) {
    return false;
  }

  RID original_rid = iter_->GetRid();
  const Schema *output_schema = plan_->OutputSchema();

  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
  Transaction *txn = GetExecutorContext()->GetTransaction();
  if (lock_mgr != nullptr) {
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {  // 未提交读情况不需要加锁
      if (!txn->IsSharedLocked(original_rid) && !txn->IsExclusiveLocked(original_rid)) {
        lock_mgr->LockShared(txn, original_rid);
      }
    }
  }

  // 筛选哪些列将要返回
  std::vector<Value> values;
  values.reserve(output_schema->GetColumnCount());
  for (size_t i = 0; i < values.capacity(); ++i) {
    values.push_back(output_schema->GetColumn(i).GetExpr()->Evaluate(
        // 注意这里由于TableIterator重载了*运算符，这里返回的是其指向tuple
        &(*iter_), &(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_)));
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mgr != nullptr) {  // 提交读才需要解锁
    lock_mgr->Unlock(txn, original_rid);
  }

  // 迭代器+1
  ++iter_;

  Tuple temp_tuple(values, output_schema);

  // 看看该行是否符合条件，符合则返回，不符合递归找下一行
  const AbstractExpression *predict = plan_->GetPredicate();
  if (predict == nullptr || predict->Evaluate(&temp_tuple, output_schema).GetAs<bool>()) {
    *tuple = temp_tuple;
    *rid = original_rid;
    return true;
  }
  return Next(tuple, rid);
}

}  // namespace bustub
