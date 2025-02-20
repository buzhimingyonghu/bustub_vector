#include <algorithm>
#include <cmath>
#include <functional>
#include <iterator>
#include <memory>
#include <queue>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/macros.h"
#include "execution/expressions/vector_expression.h"
#include "fmt/format.h"
#include "fmt/std.h"
#include "storage/index/hnsw_index.h"
#include "storage/index/index.h"
#include "storage/index/vector_index.h"

namespace bustub {

/**
 * 用于比较距离的函数对象
 * 在优先队列中用于维护最大堆，距离大的在堆顶
 */
struct CompareByDistance {
  bool operator()(const std::pair<double, size_t> &a, const std::pair<double, size_t> &b) const {
    return a.first < b.first;  // 这样将保持最大堆，距离大的在堆顶
  }
};

/**
 * HNSW索引构造函数
 * @param metadata 索引元数据
 * @param buffer_pool_manager 缓冲池管理器
 * @param distance_fn 距离计算函数类型
 * @param options 索引配置选项，必须包含：
 *               - m: 每个节点的最大邻居数
 *               - ef_construction: 构建时的候选集大小
 *               - ef_search: 搜索时的候选集大小
 */
HNSWIndex::HNSWIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager,
                     VectorExpressionType distance_fn, const std::vector<std::pair<std::string, int>> &options)
    : VectorIndex(std::move(metadata), distance_fn),
      vertices_(std::make_unique<std::vector<Vector>>()),
      layers_{{*vertices_, distance_fn}} {
  // 解析配置选项
  std::optional<size_t> m;
  std::optional<size_t> ef_construction;
  std::optional<size_t> ef_search;
  for (const auto &[k, v] : options) {
    if (k == "m") {
      m = v;  // 每层节点的最大邻居数
    } else if (k == "ef_construction") {
      ef_construction = v;  // 构建时的候选集大小
    } else if (k == "ef_search") {
      ef_search = v;  // 搜索时的候选集大小
    }
  }
  if (!m.has_value() || !ef_construction.has_value() || !ef_search.has_value()) {
    throw Exception("missing options: m / ef_construction / ef_search for hnsw index");
  }

  // 初始化HNSW参数
  ef_construction_ = *ef_construction;
  m_ = *m;
  ef_search_ = *ef_search;
  m_max_ = m_;
  m_max_0_ = m_ * m_;  // 底层的最大邻居数更大
  layers_[0].m_max_ = m_max_0_;
  m_l_ = 1.0 / std::log(m_);  // 用于计算节点层级的参数

  // 初始化随机数生成器
  std::random_device rand_dev;
  generator_ = std::mt19937(rand_dev());
}

/**
 * 选择最近的邻居节点
 * @param vec 查询向量
 * @param vertex_ids 候选顶点ID列表
 * @param vertices 所有顶点的向量数据
 * @param m 需要选择的邻居数量
 * @param dist_fn 距离计算函数
 * @return 返回最近的m个邻居的ID列表
 */
auto SelectNeighbors(const std::vector<double> &vec, const std::vector<size_t> &vertex_ids,
                     const std::vector<std::vector<double>> &vertices, size_t m, VectorExpressionType dist_fn)
    -> std::vector<size_t> {
  // 定义比较器，根据距离排序
  auto cmp = [&](size_t left_id, size_t right_id) {
    double dist_left = ComputeDistance(vec, vertices[left_id], dist_fn);
    double dist_right = ComputeDistance(vec, vertices[right_id], dist_fn);
    return dist_left > dist_right;  // 按距离升序排列
  };

  // 使用优先队列维护最近的m个邻居
  std::priority_queue<size_t, std::vector<size_t>, decltype(cmp)> min_heap(cmp);

  for (const auto &vertex_id : vertex_ids) {
    min_heap.push(vertex_id);
    if (min_heap.size() > m) {
      min_heap.pop();  // 保持堆大小为m
    }
  }

  // 提取结果
  std::vector<size_t> nearest_neighbors;
  while (!min_heap.empty()) {
    nearest_neighbors.push_back(min_heap.top());
    min_heap.pop();
  }

  return nearest_neighbors;
}

/**
 * 在单层中搜索最近邻
 * @param base_vector 查询向量
 * @param limit 返回的邻居数量
 * @param entry_points 搜索的入口点
 * @return 返回最近邻的ID列表
 */
auto NSW::SearchLayer(const std::vector<double> &base_vector, size_t limit, const std::vector<size_t> &entry_points)
    -> std::vector<size_t> {
  std::queue<size_t> candidate_queue;
  std::priority_queue<std::pair<double, size_t>, std::vector<std::pair<double, size_t>>, CompareByDistance> result_set;
  std::unordered_set<size_t> visited;

  double max_result_dist = std::numeric_limits<double>::min();
  double min_candidate_dist = std::numeric_limits<double>::max();

  // 初始化搜索：处理入口点
  for (const auto &entry_point : entry_points) {
    double dist = ComputeDistance(base_vector, vertices_[entry_point], dist_fn_);
    candidate_queue.push(entry_point);
    result_set.emplace(dist, entry_point);
    visited.insert(entry_point);

    if (result_set.size() == limit) {
      max_result_dist = result_set.top().first;
    }
    min_candidate_dist = std::min(min_candidate_dist, dist);
  }

  // 贪心搜索过程
  while (!candidate_queue.empty()) {
    size_t curr_vertex = candidate_queue.front();
    candidate_queue.pop();

    // 获取当前节点的邻居并选择最近的
    const auto &neighbors = edges_[curr_vertex];
    auto nearest_neighbors = SelectNeighbors(base_vector, neighbors, vertices_, limit, dist_fn_);

    // 处理每个邻居
    for (const auto &neighbor : nearest_neighbors) {
      if (visited.find(neighbor) != visited.end()) continue;

      double neighbor_dist = ComputeDistance(base_vector, vertices_[neighbor], dist_fn_);
      visited.insert(neighbor);
      candidate_queue.push(neighbor);
      result_set.emplace(neighbor_dist, neighbor);

      if (result_set.size() > limit) {
        result_set.pop();
      }

      if (result_set.size() == limit) {
        max_result_dist = result_set.top().first;
      }
      min_candidate_dist = std::min(min_candidate_dist, neighbor_dist);
    }

    // 提前终止条件：如果候选集中最近的距离已经大于结果集中最远的距离
    if (result_set.size() == limit && min_candidate_dist > max_result_dist) {
      break;
    }
  }

  // 整理结果
  std::vector<size_t> nearest_neighbors;
  while (!result_set.empty()) {
    nearest_neighbors.push_back(result_set.top().second);
    result_set.pop();
  }
  
  std::reverse(nearest_neighbors.begin(), nearest_neighbors.end());
  return nearest_neighbors;
}

/**
 * 向图中添加新顶点
 * @param vertex_id 新顶点的ID
 */
auto NSW::AddVertex(size_t vertex_id) { in_vertices_.push_back(vertex_id); }

/**
 * 在单层图中插入新向量
 * @param vec 要插入的向量
 * @param vertex_id 向量的ID
 * @param ef_construction 构建时的候选集大小
 * @param m 最大邻居数
 */
auto NSW::Insert(const std::vector<double> &vec, size_t vertex_id, size_t ef_construction, size_t m) {
  AddVertex(vertex_id);
  
  // 找到最近的m个邻居
  auto nearest_neighbors = SearchLayer(vec, m, {DefaultEntryPoint()});
  
  // 建立连接
  for (const auto &neighbor_id : nearest_neighbors) {
    Connect(vertex_id, neighbor_id);
  }

  // 优化连接：确保每个节点的连接数不超过最大值
  for (const auto &neighbor_id : nearest_neighbors) {
    if (edges_[neighbor_id].size() > m_max_) {
      auto neighbors_to_keep = SelectNeighbors(vertices_[neighbor_id], edges_[neighbor_id], vertices_, m_max_, dist_fn_);
      edges_[neighbor_id] = neighbors_to_keep;
    }
  }
}

/**
 * 在图中连接两个顶点
 * @param vertex_a 第一个顶点的ID
 * @param vertex_b 第二个顶点的ID
 */
void NSW::Connect(size_t vertex_a, size_t vertex_b) {
  edges_[vertex_a].push_back(vertex_b);
  edges_[vertex_b].push_back(vertex_a);
}

/**
 * 添加新的向量到索引中
 * @param vec 要添加的向量
 * @param rid 向量对应的RID
 * @return 返回分配给新向量的ID
 */
auto HNSWIndex::AddVertex(const std::vector<double> &vec, RID rid) -> size_t {
  auto id = vertices_->size();
  vertices_->emplace_back(vec);
  rids_.emplace_back(rid);
  return id;
}

/**
 * 构建HNSW索引
 * @param initial_data 初始数据集
 */
void HNSWIndex::BuildIndex(std::vector<std::pair<std::vector<double>, RID>> initial_data) {
  // 随机打乱数据
  std::shuffle(initial_data.begin(), initial_data.end(), generator_);

  // 逐个插入数据点
  for (const auto &[vec, rid] : initial_data) {
    InsertVectorEntry(vec, rid);
  }
}

/**
 * 生成随机层级
 * @return 返回新节点应该插入的层级
 */
int HNSWIndex::GenerateRandomLevel() {
    std::uniform_real_distribution<> uniform_dist(0.0, 1.0);
    double random_value = uniform_dist(generator_);
    int level = static_cast<int>(-std::log(random_value) * m_l_);
    return level;
}

/**
 * 执行向量近邻搜索
 * @param base_vector 查询向量
 * @param limit 返回的最近邻数量
 * @return 返回最近邻的RID列表
 */
auto HNSWIndex::ScanVectorKey(const std::vector<double> &base_vector, size_t limit) -> std::vector<RID> {
  // 从最高层开始搜索
  std::vector<size_t> entry_points = {layers_.rbegin()->DefaultEntryPoint()};
  int i = layers_.size() - 1;
  
  // 逐层向下搜索
  while(i > 0) {
    entry_points = layers_[i--].SearchLayer(base_vector, limit, entry_points);
  }
  
  // 在底层进行最终搜索
  entry_points = layers_[0].SearchLayer(base_vector, limit, entry_points);
  
  // 转换结果为RID列表
  std::vector<RID> result;
  result.reserve(entry_points.size());
  for (const auto &id : entry_points) {
    result.push_back(rids_[id]);
  }
  return result;
}

/**
 * 插入新的向量到索引中
 * @param key 要插入的向量
 * @param rid 向量对应的RID
 */
void HNSWIndex::InsertVectorEntry(const std::vector<double> &key, RID rid) {
  // 生成随机层级
  std::uniform_real_distribution<double> level_dist(0.0, 1.0);
  auto vertex_id = AddVertex(key, rid);
  int target_level = static_cast<int>(std::floor(-std::log(level_dist(generator_)) * m_l_));
  
  // 如果索引非空，执行插入操作
  std::vector<size_t> nearest_elements;
  if (!layers_[0].in_vertices_.empty()) {
    std::vector<size_t> entry_points{layers_[layers_.size() - 1].DefaultEntryPoint()};
    
    // 从高层向下插入
    int level = layers_.size() - 1;
    // 在高于目标层的层中只更新entry_points
    for (; level > target_level; level--) {
      nearest_elements = layers_[level].SearchLayer(key, ef_search_, entry_points);
      nearest_elements = SelectNeighbors(key, nearest_elements, *vertices_, 1, distance_fn_);
      entry_points = {nearest_elements[0]};
    }
    // 在目标层及以下的层中建立连接
    for (; level >= 0; level--) {
      auto &layer = layers_[level];
      nearest_elements = layer.SearchLayer(key, ef_construction_, entry_points);
      auto neighbors = SelectNeighbors(key, nearest_elements, *vertices_, m_, distance_fn_);
      layer.AddVertex(vertex_id);
      // 建立双向连接
      for (const auto neighbor : neighbors) {
        layer.Connect(vertex_id, neighbor);
      }
      // 优化邻居连接
      for (const auto neighbor : neighbors) {
        auto &edges = layer.edges_[neighbor];
        if (edges.size() > m_max_) {
          auto new_neighbors = SelectNeighbors((*vertices_)[neighbor], edges, *vertices_, layer.m_max_, distance_fn_);
          edges = new_neighbors;
        }
      }
      entry_points = nearest_elements;
    }
  } else {
    // 如果是第一个节点，直接添加到底层
    layers_[0].AddVertex(vertex_id);
  }

  // 如果需要，创建新的层
  while (static_cast<int>(layers_.size()) <= target_level) {
    auto layer = NSW{*vertices_, distance_fn_, m_max_};
    layer.AddVertex(vertex_id);
    layers_.emplace_back(std::move(layer));
  }
}

}  // namespace bustub