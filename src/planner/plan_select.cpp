#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "binder/bound_expression.h"
#include "binder/bound_order_by.h"
#include "binder/bound_statement.h"
#include "binder/bound_table_ref.h"
#include "binder/expressions/bound_constant.h"
#include "binder/statement/insert_statement.h"
#include "binder/statement/select_statement.h"
#include "binder/tokens.h"
#include "catalog/schema.h"
#include "common/enums/statement_type.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/util/string_util.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/values_plan.h"
#include "fmt/format.h"
#include "planner/planner.h"
#include "type/type_id.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * 为SELECT语句生成查询计划
 * 处理顺序：FROM -> WHERE -> GROUP BY -> HAVING -> SELECT -> DISTINCT -> ORDER BY -> LIMIT
 */
auto Planner::PlanSelect(const SelectStatement &statement) -> AbstractPlanNodeRef {
  auto ctx_guard = NewContext();
  // 处理 WITH 子句（CTE：Common Table Expression）
  if (!statement.ctes_.empty()) {
    ctx_.cte_list_ = &statement.ctes_;
  }

  AbstractPlanNodeRef plan = nullptr;

  // 处理 FROM 子句
  switch (statement.table_->type_) {
    case TableReferenceType::EMPTY:
      // 处理无表查询，如: SELECT 1
      plan = std::make_shared<ValuesPlanNode>(
          std::make_shared<Schema>(std::vector<Column>{}),
          std::vector<std::vector<AbstractExpressionRef>>{std::vector<AbstractExpressionRef>{}});
      break;
    default:
      // 处理普通表查询
      plan = PlanTableRef(*statement.table_);
      break;
  }

  // 处理 WHERE 子句
  if (!statement.where_->IsInvalid()) {
    auto schema = plan->OutputSchema();
    auto [_, expr] = PlanExpression(*statement.where_, {plan});
    plan = std::make_shared<FilterPlanNode>(std::make_shared<Schema>(schema), std::move(expr), std::move(plan));
  }

  // 检查是否有聚合函数或窗口函数
  bool has_agg = false;
  bool has_window_agg = false;
  // 聚合函数和窗口函数不能同时使用
  for (const auto &item : statement.select_list_) {
    if (item->HasAggregation()) {
      has_agg = true;
      break;
    }
    if (item->HasWindowFunction()) {
      has_window_agg = true;
      break;
    }
  }

  // 处理窗口函数
  if (has_window_agg) {
    if (!statement.having_->IsInvalid()) {
      throw Exception("HAVING on window function is not supported yet.");
    }
    if (!statement.group_by_.empty()) {
      throw Exception("Group by is not allowed to use with window function.");
    }
    plan = PlanSelectWindow(statement, std::move(plan));
  } 
  // 处理聚合函数和 GROUP BY
  else if (!statement.having_->IsInvalid() || !statement.group_by_.empty() || has_agg) {
    plan = PlanSelectAgg(statement, std::move(plan));
  } 
  // 处理普通 SELECT
  else {
    std::vector<AbstractExpressionRef> exprs;
    std::vector<std::string> column_names;
    std::vector<AbstractPlanNodeRef> children = {plan};
    // 处理选择列表
    for (const auto &item : statement.select_list_) {
      auto [name, expr] = PlanExpression(*item, {plan});
      if (name == UNNAMED_COLUMN) {
        name = fmt::format("__unnamed#{}", universal_id_++);
      }
      exprs.emplace_back(std::move(expr));
      column_names.emplace_back(std::move(name));
    }
    plan = std::make_shared<ProjectionPlanNode>(
        std::make_shared<Schema>(ProjectionPlanNode::RenameSchema(
            ProjectionPlanNode::InferProjectionSchema(exprs), column_names)),
        std::move(exprs), std::move(plan));
  }

  // 处理 DISTINCT
  // 将 DISTINCT 转换为 GROUP BY 实现
  if (statement.is_distinct_) {
    auto child = std::move(plan);
    std::vector<AbstractExpressionRef> distinct_exprs;
    size_t col_idx = 0;
    for (const auto &col : child->OutputSchema().GetColumns()) {
      distinct_exprs.emplace_back(std::make_shared<ColumnValueExpression>(0, col_idx++, col));
    }
    plan = std::make_shared<AggregationPlanNode>(
        std::make_shared<Schema>(child->OutputSchema()), 
        child,
        std::move(distinct_exprs), 
        std::vector<AbstractExpressionRef>{},
        std::vector<AggregationType>{});
  }

  // 处理 ORDER BY
  if (!statement.sort_.empty()) {
    std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys;
    for (const auto &order_by : statement.sort_) {
      auto [_, expr] = PlanExpression(*order_by->expr_, {plan});
      auto abstract_expr = std::move(expr);
      order_bys.emplace_back(std::make_pair(order_by->type_, abstract_expr));
    }
    plan = std::make_shared<SortPlanNode>(
        std::make_shared<Schema>(plan->OutputSchema()), 
        plan, 
        std::move(order_bys));
  }

  // 处理 LIMIT
  if (!statement.limit_count_->IsInvalid() || !statement.limit_offset_->IsInvalid()) {
    std::optional<size_t> offset = std::nullopt;
    std::optional<size_t> limit = std::nullopt;

    // 处理 LIMIT 子句
    if (!statement.limit_count_->IsInvalid()) {
      if (statement.limit_count_->type_ == ExpressionType::CONSTANT) {
        const auto &constant_expr = dynamic_cast<BoundConstant &>(*statement.limit_count_);
        const auto val = constant_expr.val_.CastAs(TypeId::INTEGER);
        if (constant_expr.val_.GetTypeId() == TypeId::INTEGER) {
          limit = std::make_optional(constant_expr.val_.GetAs<int32_t>());
        } else {
          throw NotImplementedException("LIMIT clause must be an integer constant.");
        }
      } else {
        throw NotImplementedException("LIMIT clause must be an integer constant.");
      }
    }

    // 处理 OFFSET 子句
    if (!statement.limit_offset_->IsInvalid()) {
      if (statement.limit_offset_->type_ == ExpressionType::CONSTANT) {
        const auto &constant_expr = dynamic_cast<BoundConstant &>(*statement.limit_offset_);
        const auto val = constant_expr.val_.CastAs(TypeId::INTEGER);
        if (constant_expr.val_.GetTypeId() == TypeId::INTEGER) {
          offset = std::make_optional(constant_expr.val_.GetAs<int32_t>());
        } else {
          throw NotImplementedException("OFFSET clause must be an integer constant.");
        }
      } else {
        throw NotImplementedException("OFFSET clause must be an integer constant.");
      }
    }

    // OFFSET 暂不支持
    if (offset != std::nullopt) {
      throw NotImplementedException("OFFSET clause is not supported yet.");
    }

    plan = std::make_shared<LimitPlanNode>(
        std::make_shared<Schema>(plan->OutputSchema()), 
        plan, 
        *limit);
  }

  return plan;
}

}  // namespace bustub
