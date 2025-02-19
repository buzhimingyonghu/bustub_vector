//===----------------------------------------------------------------------===//
//                         BusTub
//
// binder/select_statement.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "binder/bound_expression.h"
#include "binder/bound_order_by.h"
#include "binder/bound_statement.h"
#include "binder/bound_table_ref.h"
#include "binder/table_ref/bound_subquery_ref.h"
namespace bustub {

class Catalog;

// 选择语句类，继承自 BoundStatement
class SelectStatement : public BoundStatement {
 public:
  // 构造函数，初始化 SELECT 语句的各个部分
  explicit SelectStatement(std::unique_ptr<BoundTableRef> table,  // FROM 子句，表示要查询的表
                           std::vector<std::unique_ptr<BoundExpression>> select_list,  // SELECT 列表，表示查询的字段
                           std::unique_ptr<BoundExpression> where,  // WHERE 子句，表示查询条件
                           std::vector<std::unique_ptr<BoundExpression>> group_by,  // GROUP BY 子句，表示分组字段
                           std::unique_ptr<BoundExpression> having,  // HAVING 子句，表示过滤分组后的条件
                           std::unique_ptr<BoundExpression> limit_count,  // LIMIT 子句，表示查询结果的最大数量
                           std::unique_ptr<BoundExpression> limit_offset,  // OFFSET 子句，表示跳过的行数
                           std::vector<std::unique_ptr<BoundOrderBy>> sort,  // ORDER BY 子句，表示排序规则
                           CTEList ctes,  // 公用表表达式（CTE）
                           bool is_distinct)  // 是否使用 DISTINCT 去重
      : BoundStatement(StatementType::SELECT_STATEMENT),  // 初始化基类 BoundStatement
        table_(std::move(table)),
        select_list_(std::move(select_list)),
        where_(std::move(where)),
        group_by_(std::move(group_by)),
        having_(std::move(having)),
        limit_count_(std::move(limit_count)),
        limit_offset_(std::move(limit_offset)),
        sort_(std::move(sort)),
        ctes_(std::move(ctes)),
        is_distinct_(is_distinct) {}

  /** 绑定的 FROM 子句，表示查询的数据来源 */
  std::unique_ptr<BoundTableRef> table_;

  /** 绑定的 SELECT 列表，表示要查询的字段 */
  std::vector<std::unique_ptr<BoundExpression>> select_list_;

  /** 绑定的 WHERE 子句，表示筛选条件 */
  std::unique_ptr<BoundExpression> where_;

  /** 绑定的 GROUP BY 子句，表示分组依据 */
  std::vector<std::unique_ptr<BoundExpression>> group_by_;

  /** 绑定的 HAVING 子句，表示对分组后的结果进行筛选 */
  std::unique_ptr<BoundExpression> having_;

  /** 绑定的 LIMIT 子句，表示查询返回的最大行数 */
  std::unique_ptr<BoundExpression> limit_count_;

  /** 绑定的 OFFSET 子句，表示查询跳过的行数 */
  std::unique_ptr<BoundExpression> limit_offset_;

  /** 绑定的 ORDER BY 子句，表示排序规则 */
  std::vector<std::unique_ptr<BoundOrderBy>> sort_;

  /** 绑定的 CTE（公用表表达式） */
  CTEList ctes_;

  /** 是否为 SELECT DISTINCT 语句，决定是否去重 */
  bool is_distinct_;

  // 将 SELECT 语句转换为字符串表示
  auto ToString() const -> std::string override;
};

}  // namespace bustub