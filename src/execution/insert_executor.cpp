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

/**
 * INSERT执行器 - 负责将数据插入到表中
 * 例如: INSERT INTO table VALUES (1, 'a'), (2, 'b') 
 * 或者: INSERT INTO table SELECT * FROM another_table
 * 
 * 工作流程：
 * 1. 从子执行器获取要插入的数据（可能是VALUES或SELECT的结果）
 * 2. 将每一行数据插入到目标表中
 * 3. 更新相关索引（如果有的话）
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),  // 初始化基类 AbstractExecutor
      plan_(plan),                 // 初始化插入计划节点
      child_executor_(std::move(child_executor))  // 初始化子执行器（提供要插入的数据）
{
    // 获取目标表的堆文件对象，用于实际的数据插入操作
    table_heap_ = exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_.get();
   
    // TODO: 初始化表的索引列表，用于后续更新索引    
    // 初始化状态标记，表示是否已经发出了结果
    emitted_ = false;
}

/**
 * 初始化插入执行器
 * 主要工作是初始化子执行器，准备获取要插入的数据
 */
void InsertExecutor::Init() { 
    child_executor_->Init();
}

/**
 * 执行插入操作
 * @param tuple 输出参数，用于存储插入的元组（在INSERT中通常不使用）
 * @param rid 输出参数，用于存储插入的行ID（在INSERT中通常不使用）
 * @return 如果插入成功返回true，否则返回false
 */
auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    // 检查是否有子执行器（例如VALUES或SELECT）
    if (child_executor_ != nullptr) {
        // 从子执行器获取下一行数据
        if (child_executor_->Next(tuple, rid)) {
                // 获取事务和锁管理器
                Transaction *txn = exec_ctx_->GetTransaction();
                LockManager *lock_mgr = exec_ctx_->GetLockManager();
                auto table_oid = plan_->GetTableOid();

                // 设置元组元数据
                TupleMeta tuple_meta;
                tuple_meta.is_deleted_ = false;  // 新插入的元组标记为未删除

                // 执行实际的插入操作
                auto inserted_rid = table_heap_->InsertTuple(tuple_meta, *tuple, lock_mgr, txn, table_oid);
                
                // 检查插入是否成功
                if (inserted_rid.has_value()) {
                    auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(
                    exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->name_);
                    *rid = inserted_rid.value();
                    for (auto &index_info : table_indexes) {
                    // 插入到索引中
                        if(index_info->index_type_ == IndexType::VectorIVFFlatIndex) {
                            auto vector_index = dynamic_cast<IVFFlatIndex*>(index_info->index_.get());
                            vector_index->InsertVectorEntry(tuple->GetValue(&child_executor_->GetOutputSchema(),0).GetVector(), *rid);
                        } else if(index_info->index_type_ == IndexType::VectorHNSWIndex) {
                            auto vector_index = dynamic_cast<HNSWIndex*>(index_info->index_.get());
                            vector_index->InsertVectorEntry(tuple->GetValue(&child_executor_->GetOutputSchema(),0).GetVector(), *rid);
                        }   
                    }
                    return true;
                }
                return false;
        }
        return false;
    }
    return false;
}
}  // namespace bustub
