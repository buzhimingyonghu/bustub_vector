#include "storage/index/ivfflat_index.h"
#include <algorithm>
#include <optional>
#include <random>
#include "common/exception.h"
#include "execution/expressions/vector_expression.h"
#include "storage/index/index.h"
#include "storage/index/vector_index.h"

namespace bustub {
using Vector = std::vector<double>;

/**
 * 从数据集中随机采样一些向量作为聚类中心
 * @param data 原始数据集，包含向量和对应的RID
 * @param num_samples 需要采样的数量
 * @return 采样得到的向量集合，作为初始聚类中心
 */
std::vector<Vector> IVFFlatIndex::RandomSample(const std::vector<std::pair<Vector, RID>> &data, size_t num_samples) {
    std::vector<Vector> sampled_centroids;

    // 检查数据量是否足够进行采样
    if (data.size() < num_samples) {
        throw std::invalid_argument("Not enough data to sample the required number of centroids");
    }

    // 创建索引数组用于随机打乱
    std::vector<size_t> indices(data.size());
    for (size_t i = 0; i < data.size(); ++i) {
        indices[i] = i;
    }

    // 使用随机数生成器打乱索引
    std::random_device rd;
    std::mt19937 rng(rd());  // 随机数生成器
    std::shuffle(indices.begin(), indices.end(), rng);

    // 选择前num_samples个向量作为初始聚类中心
    for (size_t i = 0; i < num_samples; ++i) {
        sampled_centroids.push_back(data[indices[i]].first);
    }

    return sampled_centroids;
}

/**
 * IVFFlat索引构造函数
 * @param metadata 索引元数据
 * @param buffer_pool_manager 缓冲池管理器
 * @param distance_fn 距离计算函数类型
 * @param options 索引选项，包括聚类数量(lists)和查询时探测的聚类数量(probe_lists)
 */
IVFFlatIndex::IVFFlatIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
                           VectorExpressionType distance_fn, const std::vector<std::pair<std::string, int>> &options)
    : VectorIndex(std::move(metadata), distance_fn) {
  std::optional<size_t> lists;
  std::optional<size_t> probe_lists;
  for (const auto &[k, v] : options) {
    if (k == "lists") {
      lists = v;  // 聚类中心的数量
    } else if (k == "probe_lists") {
      probe_lists = v;  // 查询时检查的聚类数量
    }
  }
  if (!lists.has_value() || !probe_lists.has_value()) {
    throw Exception("missing options: lists / probe_lists for ivfflat index");
  }
  lists_ = *lists;
  probe_lists_ = *probe_lists;
}

/**
 * 向量加法：a = a + b
 */
void VectorAdd(Vector &a, const Vector &b) {
  for (size_t i = 0; i < a.size(); i++) {
    a[i] += b[i];
  }
}

/**
 * 向量除以标量：a = a / x
 */
void VectorScalarDiv(Vector &a, double x) {
  for (auto &y : a) {
    y /= x;
  }
}

/**
 * 找到向量最近的聚类中心
 * @param vec 待查询向量
 * @param centroids 所有聚类中心
 * @param dist_fn 距离计算函数
 * @return 最近聚类中心的索引
 */
auto FindCentroid(const Vector &vec, const std::vector<Vector> &centroids, VectorExpressionType dist_fn) -> size_t {
  int min_index = -1;
  int min_distance = 0;
  for(size_t i = 0; i < centroids.size(); i++) {
    if(min_distance < ComputeDistance(vec, centroids[i], dist_fn)) {
      min_index = i;
      min_distance = ComputeDistance(vec, centroids[i], dist_fn);
    }
  }
  return min_index;
}

/**
 * 更新聚类中心
 * @param data 所有数据点,每个元素是(向量,RID)对
 * @param centroids 当前的聚类中心列表
 * @param dist_fn 距离计算函数类型
 * @return 返回更新后的聚类中心列表,每个聚类中心是其所属数据点的平均值
 */
auto FindCentroids(const std::vector<std::pair<Vector, RID>> &data, const std::vector<Vector> &centroids,
                   VectorExpressionType dist_fn) -> std::vector<Vector> {
  std::vector<Vector> res = centroids;
  std::vector<size_t> count(res.size(), 0);
  // 将每个向量分配到最近的聚类中心
  for(auto &it : data) {
    size_t index = FindCentroid(it.first, res, dist_fn);   
    VectorAdd(res[index],it.first);
    count[index]++;
  }
  // 计算新的聚类中心（取平均值）
  for(size_t i = 0; i < res.size(); i++) {
    if(i == 0) continue;
    VectorScalarDiv(res[i], count[i]);  
  }
  return res;
}

/**
 * 构建IVFFlat索引
 * @param initial_data 用于构建索引的初始数据集,每个元素是(向量,RID)对
 * @note 如果数据量小于配置的聚类数量,则不会构建索引
 */
void IVFFlatIndex::BuildIndex(std::vector<std::pair<Vector, RID>> initial_data) {
  if (initial_data.size() < lists_) {
    return;
  }
  centroids_buckets_.resize(lists_);
  // 随机选择初始聚类中心
  centroids_ = RandomSample(initial_data, lists_);

  // 执行K-means迭代
  const size_t max_iterations = 500;
  for (size_t iter = 0; iter < max_iterations; ++iter) {
      centroids_ = FindCentroids(initial_data, centroids_, VectorExpressionType::L2Dist);
  }

  // 将所有向量分配到最近的聚类中心
  for (const auto& pair : initial_data) {
        const Vector& vec = pair.first;
        size_t nearest_centroid_idx = FindCentroid(vec, centroids_, VectorExpressionType::L2Dist);
        centroids_buckets_[nearest_centroid_idx].push_back(pair);
  }
}

/**
 * 插入新的向量到索引中
 * @param key 要插入的向量
 * @param rid 向量对应的RID
 */
void IVFFlatIndex::InsertVectorEntry(const std::vector<double> &key, RID rid) {
  size_t nearest_centroid_idx = FindCentroid(key, centroids_, VectorExpressionType::L2Dist);
  centroids_buckets_[nearest_centroid_idx].emplace_back(key,rid);
} 

/**
 * 找到与给定向量最近的num_centroids个聚类中心
 * @param base_vector 查询向量
 * @param num_centroids 需要返回的最近聚类中心数量
 * @return 最近聚类中心的索引列表
 */
std::vector<size_t> IVFFlatIndex::FindNearestCentroids(const std::vector<double> &base_vector, size_t num_centroids) {
    std::vector<std::pair<double, size_t>> distances;

    // 计算查询向量到所有聚类中心的距离
    for (size_t i = 0; i < centroids_.size(); ++i) {
        double dist = ComputeDistance(base_vector, centroids_[i], VectorExpressionType::L2Dist);
        distances.emplace_back(dist, i);
    }

    // 按距离升序排序
    std::sort(distances.begin(), distances.end());

    // 返回最近的num_centroids个聚类中心的索引
    std::vector<size_t> nearest_centroids;
    for (size_t i = 0; i < num_centroids && i < distances.size(); ++i) {
        nearest_centroids.push_back(distances[i].second);
    }

    return nearest_centroids;
}

/**
 * 执行向量近邻搜索
 * @param base_vector 查询向量
 * @param limit 返回的最近邻数量
 * @return 最近邻向量的RID列表
 */
auto IVFFlatIndex::ScanVectorKey(const std::vector<double> &base_vector, size_t limit) -> std::vector<RID> {
    std::vector<RID> global_result;
    std::vector<std::pair<double, RID>> local_results;

    // 1. 找到最近的probe_lists_个聚类中心
    std::vector<size_t> nearest_centroids = FindNearestCentroids(base_vector, probe_lists_);

    // 2. 在选中的聚类中搜索最近邻
    for (size_t centroid_idx : nearest_centroids) {
        for (const auto& entry : centroids_buckets_[centroid_idx]) {
            const Vector& vec = entry.first;
            RID rid = entry.second;
            double distance = ComputeDistance(base_vector, vec, VectorExpressionType::L2Dist);
            local_results.emplace_back(distance, rid);
        }
    }

    // 3. 对所有候选结果排序
    std::sort(local_results.begin(), local_results.end(),
        [](const auto &a, const auto &b) {
            return a.first < b.first;  // 只比较距离（first字段）
        });

    // 4. 返回前limit个最近邻
    for (size_t i = 0; i < std::min(limit, local_results.size()); ++i) {
        global_result.push_back(local_results[i].second);
    }

    return global_result;
}

}  // namespace bustub