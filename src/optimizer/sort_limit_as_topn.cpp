#include "optimizer/optimizer.h"
#include "execution/plans/projection_plan.h"


namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  //TopNPlanNode(SchemaRef output, AbstractPlanNodeRef child,
  //             std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys, std::size_t n)
    // 1. 保存原始输出模式

  SchemaRef schema_ref = std::make_shared<const Schema>(plan->OutputSchema());
    // 2. 检查是否是 Limit 节点

  if(plan->GetType() == PlanType::Limit)
  {
    auto limit_plan = std::dynamic_pointer_cast<const LimitPlanNode>(plan);
    // 3. 检查 Limit 的子节点是否是 Sort 节点

    if(plan->children_[0]->GetType() == PlanType::Sort)
    {
      auto sort_plan = std::dynamic_pointer_cast<const SortPlanNode>(plan->children_[0]);
      if (!sort_plan) {
        return plan; // 如果转换失败，返回原计划
      }
      // 4. 获取 Sort 的子节点（数据源）

      auto pro_plan = sort_plan->children_[0];
      // 5. 创建 TopN 计划节点

      auto res = std::make_shared<TopNPlanNode>(schema_ref,pro_plan, 
      sort_plan->GetOrderBy(),limit_plan->limit_);
      return res;
    }
    return plan;
    
  }
  return plan;
    
    
  
}

}  // namespace bustub