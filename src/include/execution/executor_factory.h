//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// executor_factory.h
//
// Identification: src/include/execution/executor_factory.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "execution/executors/abstract_executor.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

/**
 * ExecutorFactory 是执行器工厂类，负责为不同类型的查询计划节点创建对应的执行器
 * 
 * 工作原理：
 * 1. 接收一个查询计划节点（如InsertPlanNode、ValuesPlanNode等）
 * 2. 根据计划节点的类型（PlanType）创建对应的执行器
 * 3. 如果计划节点有子节点，会递归创建子执行器
 * 
 * 例如对于 INSERT INTO t1 VALUES (1), (2), (3) 语句：
 * - 首先为 InsertPlanNode 创建 InsertExecutor
 * - 然后为其子节点 ValuesPlanNode 创建 ValuesExecutor
 */
class ExecutorFactory {
 public:
  /**
   * 为给定的计划节点创建对应的执行器
   * @param exec_ctx 执行器上下文，包含事务、目录等信息
   * @param plan 需要执行的计划节点
   * @return 返回创建好的执行器
   * 
   * 支持的执行器类型：
   * - InsertExecutor：处理插入操作
   * - ValuesExecutor：生成常量值
   * - SeqScanExecutor：表扫描
   * - IndexScanExecutor：索引扫描
   * - 等等...
   */
  static auto CreateExecutor(ExecutorContext *exec_ctx, const AbstractPlanNodeRef &plan)
      -> std::unique_ptr<AbstractExecutor>;
};

}  // namespace bustub
