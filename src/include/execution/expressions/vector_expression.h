#pragma once

#include <cmath>
#include <string>
#include <utility>
#include <vector>
#include <assert.h>

#include "catalog/schema.h"
#include "execution/expressions/abstract_expression.h"
#include "fmt/format.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * 向量表达式类型，表示要执行的向量计算类型
 * - L2Dist: L2距离（欧几里得距离）
 * - InnerProduct: 内积（点积）
 * - CosineSimilarity: 余弦相似度
 */
enum class VectorExpressionType { L2Dist, InnerProduct, CosineSimilarity };

/**
 * 计算两个向量之间的距离/相似度
 * @param left 左侧向量
 * @param right 右侧向量
 * @param dist_fn 计算方法（L2距离、内积或余弦相似度）
 * @return 计算结果
 */
inline auto ComputeDistance(const std::vector<double> &left, const std::vector<double> &right,
                            VectorExpressionType dist_fn) {
  auto sz = left.size();
  // 确保两个向量长度相同
  BUSTUB_ASSERT(sz == right.size(), "vector length mismatched!");
  
  switch (dist_fn) {
    case VectorExpressionType::L2Dist: {
      // 计算L2距离（欧几里得距离）
      // distance = sqrt((x1-y1)^2 + (x2-y2)^2 + ...)
      double distance = 0.0;
      assert(left.size() == right.size());
      for(size_t i = 0; i < left.size(); i++) {
        distance += pow(left[i] - right[i], 2);
      }
      return sqrt(distance);
    }
    case VectorExpressionType::InnerProduct: {
      // 计算内积（点积）
      // distance = -(x1*y1 + x2*y2 + ...)
      // 注意这里返回负值是为了与相似度计算保持一致（越小表示越相似）
      double distance = 0.0;
      for(size_t i = 0; i < left.size(); i++) {
        distance += left[i] * right[i];
      }
      return -1.0 * distance;
    }
    case VectorExpressionType::CosineSimilarity: {
      // 计算余弦相似度
      // similarity = 1 - (x·y)/(|x|*|y|)
      // 其中 x·y 是内积，|x| 和 |y| 是向量的L2范数
      double distance = 0.0;          // 内积
      double left_distance = 0.0;     // 左向量的平方和
      double right_distance = 0.0;    // 右向量的平方和
      assert(left.size() == right.size());
      for(size_t i = 0; i < left.size(); i++) {
        distance += left[i] * right[i];
        left_distance += pow(left[i], 2);
        right_distance += pow(right[i], 2);
      }
      return 1 - distance/sqrt(left_distance * right_distance);
    }
    default:
      BUSTUB_ASSERT(false, "Unsupported vector expr type.");
  }
}

/**
 * 向量表达式类，用于处理向量之间的计算
 * 支持在SQL中进行向量距离/相似度计算
 */
class VectorExpression : public AbstractExpression {
 public:
  /**
   * 构造向量表达式
   * @param expr_type 计算类型（L2距离、内积或余弦相似度）
   * @param left 左侧向量表达式
   * @param right 右侧向量表达式
   */
  VectorExpression(VectorExpressionType expr_type, AbstractExpressionRef left, AbstractExpressionRef right)
      : AbstractExpression({std::move(left), std::move(right)}, Column{"<val>", TypeId::DECIMAL}),
        expr_type_{expr_type} {}

  /**
   * 计算表达式的值
   * @param tuple 输入元组
   * @param schema 元组的模式
   * @return 计算结果
   */
  auto Evaluate(const Tuple *tuple, const Schema &schema) const -> Value override {
    Value lhs = GetChildAt(0)->Evaluate(tuple, schema);
    Value rhs = GetChildAt(1)->Evaluate(tuple, schema);
    return ValueFactory::GetDecimalValue(PerformComputation(lhs, rhs));
  }

  /**
   * 在连接操作中计算表达式的值
   */
  auto EvaluateJoin(const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
                    const Schema &right_schema) const -> Value override {
    Value lhs = GetChildAt(0)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    Value rhs = GetChildAt(1)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    return ValueFactory::GetDecimalValue(PerformComputation(lhs, rhs));
  }

  /** 返回表达式的字符串表示 */
  auto ToString() const -> std::string override {
    return fmt::format("{}({}, {})", expr_type_, *GetChildAt(0), *GetChildAt(1));
  }

  BUSTUB_EXPR_CLONE_WITH_CHILDREN(VectorExpression);

  VectorExpressionType expr_type_;  // 向量计算类型

 private:
  /**
   * 执行实际的向量计算
   * @param lhs 左操作数
   * @param rhs 右操作数
   * @return 计算结果
   */
  auto PerformComputation(const Value &lhs, const Value &rhs) const -> double {
    auto left_vec = lhs.GetVector();
    auto right_vec = rhs.GetVector();
    return ComputeDistance(left_vec, right_vec, expr_type_);
  }
};

}  // namespace bustub

/**
 * 向量表达式类型的格式化器
 * 用于将VectorExpressionType转换为可读的字符串
 */
template <>
struct fmt::formatter<bustub::VectorExpressionType> : formatter<string_view> {
  template <typename FormatContext>
  auto format(bustub::VectorExpressionType c, FormatContext &ctx) const {
    string_view name;
    switch (c) {
      case bustub::VectorExpressionType::L2Dist:
        name = "l2_dist";
        break;
      case bustub::VectorExpressionType::CosineSimilarity:
        name = "cosine_similarity";
        break;
      case bustub::VectorExpressionType::InnerProduct:
        name = "inner_product";
        break;
      default:
        name = "Unknown";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
