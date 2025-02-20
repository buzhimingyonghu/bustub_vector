#pragma once

#include <memory>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "execution/expressions/vector_expression.h"
#include "storage/index/index.h"
#include "storage/index/vector_index.h"

namespace bustub {

/**
 * IVFFlat (Inverted File Flat) 索引实现类
 * 这是一个基于聚类的向量索引结构，用于高效的向量相似度搜索
 * 
 * 工作原理：
 * 1. 使用K-means算法将向量空间划分为多个簇
 * 2. 每个簇有一个聚类中心(centroid)
 * 3. 查询时只需要搜索最近的几个簇，而不是整个数据集
 * 
 * 主要参数：
 * - lists_: 聚类的数量
 * - probe_lists_: 查询时检查的聚类数量
 */
class IVFFlatIndex : public VectorIndex {
 public:
  /**
   * 构造函数
   * @param metadata 索引元数据，包含索引的基本信息
   * @param buffer_pool_manager 缓冲池管理器，用于管理索引数据的缓存
   * @param distance_fn 距离计算函数类型(L2/内积/余弦相似度)
   * @param options 索引配置选项，必须包含：
   *               - lists: 聚类中心的数量
   *               - probe_lists: 查询时检查的聚类数量
   */
  IVFFlatIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
               VectorExpressionType distance_fn, const std::vector<std::pair<std::string, int>> &options);

  /** 析构函数 */
  ~IVFFlatIndex() override = default;

  /**
   * 构建索引
   * @param initial_data 用于构建索引的初始数据集，每个元素是(向量,RID)对
   */
  void BuildIndex(std::vector<std::pair<std::vector<double>, RID>> initial_data) override;

  /**
   * 执行向量近邻搜索
   * @param base_vector 查询向量
   * @param limit 需要返回的最近邻向量数量
   * @return 返回按距离排序的最近邻向量的RID列表
   */
  auto ScanVectorKey(const std::vector<double> &base_vector, size_t limit) -> std::vector<RID> override;

  /**
   * 插入新的向量到索引中
   * @param key 要插入的向量
   * @param rid 向量对应的行标识符(Row ID)
   */
  void InsertVectorEntry(const std::vector<double> &key, RID rid) override;

  /** 缓冲池管理器指针 */
  BufferPoolManager *bpm_;

  /** 聚类的数量（构建索引时创建的桶/列表数量）*/
  size_t lists_{0};

  /** 查询时检查的聚类数量（探测的桶/列表数量）*/
  size_t probe_lists_{0};

  /** 类型别名，用于简化向量的表示 */
  using Vector = std::vector<double>;

  /** 存储所有聚类中心的向量 */
  std::vector<Vector> centroids_;

  /** 
   * 倒排索引结构：每个聚类中心对应的向量列表
   * 外层vector的索引对应聚类中心的索引
   * 内层vector存储属于该聚类的所有(向量,RID)对
   */
  std::vector<std::vector<std::pair<Vector, RID>>> centroids_buckets_;

 private:
  /**
   * 从数据集中随机采样作为初始聚类中心
   * @param data 原始数据集
   * @param num_samples 需要采样的数量
   * @return 返回采样得到的向量集合
   */
  std::vector<Vector> RandomSample(const std::vector<std::pair<Vector, RID>> &data, size_t num_samples);

  /**
   * 找到与给定向量最近的num_centroids个聚类中心
   * @param base_vector 查询向量
   * @param num_centroids 需要返回的最近聚类中心数量
   * @return 返回最近聚类中心的索引列表
   */
  std::vector<size_t> FindNearestCentroids(const std::vector<double> &base_vector, size_t num_centroids);
};

}  // namespace bustub
