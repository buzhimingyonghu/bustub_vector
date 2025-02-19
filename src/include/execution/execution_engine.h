//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_engine.h
//
// Identification: src/include/execution/execution_engine.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/executor_context.h"
#include "execution/executor_factory.h"
#include "execution/executors/init_check_executor.h"
#include "execution/plans/abstract_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * ExecutionEngine 类负责执行查询计划
 * 
 * 主要功能：
 * 1. 接收经过优化器优化后的查询计划
 * 2. 为查询计划创建相应的执行器
 * 3. 在事务上下文中执行查询
 * 4. 收集查询结果并返回
 * 
 * 执行过程：
 * 1. 通过ExecutorFactory为查询计划节点创建对应的执行器
 * 2. 初始化执行器
 * 3. 反复调用执行器的Next()方法获取结果元组
 * 4. 对于嵌套循环连接等操作进行正确性检查
 * 
 * 错误处理：
 * - 执行过程中如果发生异常，会清空结果集并返回失败
 * - 提供事务回滚机制确保数据一致性
 */
class ExecutionEngine {
 public:
  /**
   * 构造一个新的执行引擎实例
   * @param bpm 执行引擎使用的缓冲池管理器
   * @param txn_mgr 执行引擎使用的事务管理器
   * @param catalog 执行引擎使用的系统目录
   */
  ExecutionEngine(BufferPoolManager *bpm, TransactionManager *txn_mgr, Catalog *catalog)
      : bpm_{bpm}, txn_mgr_{txn_mgr}, catalog_{catalog} {}

  DISALLOW_COPY_AND_MOVE(ExecutionEngine);

  /**
   * 执行查询计划
   * @param plan 要执行的查询计划
   * @param result_set 执行计划产生的元组集合
   * @param txn 查询执行所在的事务上下文
   * @param exec_ctx 查询执行所在的执行器上下文
   * @return 如果查询计划执行成功返回true，否则返回false
   */
  auto Execute(const AbstractPlanNodeRef &plan, std::vector<Tuple> *result_set, Transaction *txn,
               ExecutorContext *exec_ctx) -> bool {
    // 确保事务一致性
    BUSTUB_ASSERT((txn == exec_ctx->GetTransaction()), "Broken Invariant");

    // 为抽象计划节点构造执行器
    auto executor = ExecutorFactory::CreateExecutor(exec_ctx, plan);

    // 初始化执行器
    auto executor_succeeded = true;

    try {
      // 初始化执行器
      executor->Init();
      // 轮询执行器获取结果
      PollExecutor(executor.get(), plan, result_set);
      // 执行检查
      PerformChecks(exec_ctx);
    } catch (const ExecutionException &ex) {
      // 执行失败时清理结果集
      executor_succeeded = false;
      if (result_set != nullptr) {
        result_set->clear();
      }
    }

    return executor_succeeded;
  }

  /**
   * 执行嵌套循环连接检查
   * 确保右表执行器在每个左表元组处理时都被正确初始化
   */
  void PerformChecks(ExecutorContext *exec_ctx) {
    for (const auto &[left_executor, right_executor] : exec_ctx->GetNLJCheckExecutorSet()) {
      auto casted_left_executor = dynamic_cast<const InitCheckExecutor *>(left_executor);
      auto casted_right_executor = dynamic_cast<const InitCheckExecutor *>(right_executor);
      BUSTUB_ASSERT(casted_right_executor->GetInitCount() + 1 >= casted_left_executor->GetNextCount(),
                    "nlj check failed, are you initialising the right executor every time when there is a left tuple? "
                    "(off-by-one is okay)");
    }
  }

 private:
  /**
   * 持续轮询执行器直到所有结果都被获取，或发生异常
   * @param executor 根执行器
   * @param plan 要执行的计划
   * @param result_set 元组结果集
   */
  static void PollExecutor(AbstractExecutor *executor, const AbstractPlanNodeRef &plan,
                           std::vector<Tuple> *result_set) {
    RID rid{};
    Tuple tuple{};
    // 不断获取下一个元组直到没有更多结果
    while (executor->Next(&tuple, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(tuple);
      }
    }
  }

  [[maybe_unused]] BufferPoolManager *bpm_;      // 缓冲池管理器
  [[maybe_unused]] TransactionManager *txn_mgr_; // 事务管理器
  [[maybe_unused]] Catalog *catalog_;            // 系统目录
};

}  // namespace bustub
