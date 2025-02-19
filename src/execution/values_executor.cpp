#include "execution/executors/values_executor.h"

namespace bustub {

/**
 * VALUES执行器 - 用于执行VALUES子句，直接生成常量值的行
 * 例如: INSERT INTO table VALUES (1, 'a'), (2, 'b') 中的 VALUES (1, 'a'), (2, 'b') 部分
 * 或者: SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t 中的VALUES部分
 */
ValuesExecutor::ValuesExecutor(ExecutorContext *exec_ctx, const ValuesPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), dummy_schema_(Schema({})) {}

/**
 * 初始化执行器
 * 将游标重置为0，准备开始遍历VALUES中的行
 */
void ValuesExecutor::Init() { cursor_ = 0; }

/**
 * 获取下一行数据
 * @param tuple 输出参数，用于存储下一行的元组
 * @param rid 输出参数，行标识符(在VALUES执行器中不使用)
 * @return 如果还有下一行返回true，否则返回false
 */
auto ValuesExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 检查是否已经遍历完所有行
  if (cursor_ >= plan_->GetValues().size()) {
    return false;
  }

  // 准备存储当前行的所有列值
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  // 获取当前行的表达式并求值
  const auto &row_expr = plan_->GetValues()[cursor_];
  for (const auto &col : row_expr) {
    // 对每个列表达式求值，因为是常量，不需要输入元组，所以传入nullptr和空schema
    values.push_back(col->Evaluate(nullptr, dummy_schema_));
  }

  // 根据模式创建新的元组
  *tuple = Tuple{values, &GetOutputSchema()};
  // 移动到下一行
  cursor_ += 1;

  return true;
}

}  // namespace bustub
