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
  while (true) {
    try {
      if (!child_executor_->Next(&cur_tuple, &cur_rid)) {
        break;
      }
    } catch (Exception &e) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "deleteExecutor: child execute error");
      return false;
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
    }
  }

  return false;
}

}  // namespace bustub
