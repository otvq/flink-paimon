# Flink 内核 + Paimon 深度学习路线图（28 周 / 7 个月）

> **2026-11-12 补充**：新增阶段 5「AI 数据底座」（W17–20），总周期 24 周 → 28 周（文件名保留 `24weeks` 不变，以本标注为准）。

> **定位**：30 岁 / 3 年大数据 + 3 年 Java / 非大厂 → 32 岁前完成"数据基础设施 / AI 数据底座工程师"关键一跳
> **时间预算**：工作日 1h/天 + 周末 3h×2 ≈ 11h/周（低于 8h/周则整体顺延 25%）
> **铁律**：每周必须有产出物（笔记/文章/代码），只看不写 = 没学
> **源码方法论**：断点跟数据走——让一条数据流过作业，在关键算子打断点看调用栈，不逐行读

---

## 环境基线

- [ ] JDK 17
- [ ] IDEA（社区版即可）
- [ ] Flink 2.3.0 源码导入（重点模块：`flink-core`、`flink-runtime`、`flink-streaming-java`、`flink-table`）
- [ ] Python 3.11+（uv 即可）+ Docker（W16 湖仓 demo 起使用，AI 阶段 W17–20 必需）
- [ ] GitHub 建 `flink-paimon-notes` 笔记仓库（所有产出物归档于此，它就是公开作品集）

---

## 阶段总览

| 阶段 | 周次 | 主题 | 产出物 | 状态 |
|---|---|---|---|---|
| 0 | W1 | 地基与基线 | 源码环境 + 笔记仓库 | ☐ |
| 1 | W2–4 | Flink 运行时骨架 | 架构链路图 + 1 篇文章 | ☐ |
| 2 | W5–8 | 状态与 Checkpoint ⭐ | 1–2 篇深度文 | ☐ |
| 3 | W9–11 | 时间、窗口与反压 | 反压排查手册 | ☐ |
| 4 | W12–16 | Paimon 深入 ⭐ | 湖仓 demo + 2 篇文章 | ☐ |
| 5 | W17–20 | AI 数据底座 ⭐（新增） | RAG 数据管线 demo + 向量库选型表 + 1 篇文章 | ☐ |
| 6 | W21–24 | 查询层与生产化 | GitHub 开源项目 + 首个 PR | ☐ |
| 7 | W25–28 | 输出与变现 | 6–8 篇系列文 + 面试摸底 | ☐ |

⭐ = 面试考察密度最高的三段

---

## 阶段 0（W1）：地基与基线

- [ ] W1-1 环境：JDK 17 + IDEA + Flink 2.3.0 源码导入，本地起 mini-cluster
- [ ] W1-2 第一刀：跑通 DataStream WordCount，在 `keyBy` 之后打断点，观察调用栈一路回到 `main()`
- [ ] W1-3 必读：Flink 2.0 官方博客（中英文）
- [ ] W1-4 建笔记仓库 `flink-paimon-notes`

**里程碑自测**：环境全通，能说清 Flink 2.0 三大变化（分离式状态管理 ForSt / Source-Sink V2 / DataSet API 移除）

---

## 阶段 1（W2–4）：Flink 运行时骨架

- [ ] W2 作业提交链路：`StreamGraph → JobGraph → ExecutionGraph` 三层图的生成与优化；operator chain 发生在哪一步
- [ ] W3 运行时组件：JobManager（Dispatcher/JobMaster 分工）、TaskManager、slot 分配；FLIP-27 Source 模型（`SplitEnumerator` 与 `SourceReader` 分离的设计动机）
- [ ] W4 网络与内存：ResultPartition/InputGate、credit-based 流控、network buffers vs managed memory 分配
- [ ] 产出：架构链路图 + 1 篇文章

**里程碑自测**：不看资料画出"一条数据从 `main()` 到 TaskManager 执行"的完整链路图，讲出每个组件为什么存在

---

## 阶段 2（W5–8）：状态与 Checkpoint ⭐核心

- [ ] W5 State Backend：Heap vs RocksDB；RocksDB 基础打牢（memtable / SST / LSM 合并）⚠️ 知识将全额迁移到 Paimon，一鱼两吃
- [ ] W6 Checkpoint 机制：`CheckpointCoordinator` 源码；barrier 对齐 vs 非对齐（unaligned）；两阶段提交与 exactly-once
- [ ] W7 进阶：增量 checkpoint、savepoint 与状态演进、changelog state backend；精读阿里 VLDB 论文（Disaggregated State Management）
- [ ] W8 故障实战周：制造大状态慢 checkpoint / barrier 对齐超时 / 状态膨胀，逐个排查调优，写成第一篇深度文章
- [ ] 产出：1–2 篇深度文

**里程碑自测**：从源码层面讲清 barrier 从哪发出、怎么流动、非对齐时数据去了哪、RocksDB 增量快照为什么快

---

## 阶段 3（W9–11）：时间、窗口与反压

- [ ] W9 时间语义：watermark 生成与传播（periodic/punctuated）、空闲源问题、迟到数据三件套（allowedLateness / sideOutput / 更新结果语义）
- [ ] W10 窗口与优化：`WindowOperator` 源码、mini-batch、local-global 聚合如何解决数据倾斜
- [ ] W11 反压专题：metrics（`backPressuredTimeMs` / `inPoolUsage`）+ 火焰图定位算子；网络栈与算子两类根因的解法清单，整理成手册发布
- [ ] 产出：反压排查手册（这类文章搜索量极大）

**里程碑自测**：拿到任意反压作业，20 分钟内定位瓶颈算子并给出 3 种以上解法及适用条件

---

## 阶段 4（W12–16）：Paimon 深入 ⭐核心

- [ ] W12 表格式基础：snapshot / manifest / schema / data file 概念体系；用 Iceberg 概念做对照表
- [ ] W13 LSM 写入模型：Primary-Key Table 的 L0 sorted run、刷盘时机（每个 checkpoint 一次 L0）；四种 merge engine（deduplicate / partial-update / aggregation / first-row）的选择
- [ ] W14 Compaction 深入：Full / Universal Compaction、`compaction.trigger` 参数族、小文件问题、独立 Compaction 服务；对照 RocksDB compaction 加深理解
- [ ] W15 流批一体：changelog producer 四档（none/input/lookup/full-compaction）代价对比、consumer 机制、watermark 对齐、lookup join 维表关联
- [ ] W16 实战周：搭完整 demo（Flink 实时写 Paimon partial-update 场景 + StarRocks 外表查询），调优前后指标记录成文
- [ ] 产出：湖仓 demo + 2 篇文章

**里程碑自测**：能回答"为什么 Paimon 用 LSM 而 Iceberg 用 COW/MOR"；能对着参数表解释每一次 compaction 调优决策的权衡

---

## 阶段 5（W17–20）：AI 数据底座 ⭐（2026-11-12 新增）

> **插入理由**：职业终点是"AI 数据底座方向"，但原路线只建了湖仓技能、没建 AI 数据栈技能。此阶段刻意设计为"知识迁移"而非"从零学 AI"——向量库/ANN 索引的存储设计与你刚学完的 LSM 高度同构（都是用预处理和空间换查询延迟），是整条路线里性价比最高的一段。

- [ ] W17 向量检索基础：embedding 与相似度度量（cosine / L2 / 内积）；ANN 三大家（HNSW / IVF-PQ / DiskANN）的召回率-延迟-内存三角权衡；本地用 faiss 或 pgvector 跑通第一个向量检索 demo。迁移视角：对照刚学完的 Paimon，体会 ANN 与 LSM 同属"用预处理和空间换查询延迟"的存储设计
- [ ] W18 向量数据库架构：Milvus 深入（growing/sealed segment 模型、对象存储 + binlog 的存算分离形态）——segment 设计与 Paimon sorted run/snapshot 高度同构，一鱼两吃；快速过 pgvector / Elasticsearch / StarRocks vector index，产出向量库选型对照表初稿
- [ ] W19 RAG 数据管线：文档 → chunk → embedding → upsert 完整链路；删除与更新语义（源文档变更后向量库如何同步——RAG 管线最大的坑）；湖仓为 source of truth、向量索引为派生物的一致性设计；批流实战：Flink 实时写湖仓 + 同步 embedding 到向量库，搭一条最小可跑的 RAG 数据管线
- [ ] W20 湖仓 × AI：Paimon 作为训练数据底座（time travel → 可复现训练集、schema 演进、列统计辅助数据筛选）；非结构化/半结构化数据入湖；视野层（了解即可）：LLM serving 的 KV-cache 存储层（Mooncake 类）、DeepSeek 3FS、agent 时代数据访问模式
- [ ] 产出：向量库选型对照表 + RAG 数据管线 demo（并入 W24 开源项目）+ 1 篇文章（建议角度：《从 LSM 到 ANN：大数据工程师的向量存储入门》，用已有知识做迁移讲解，差异化最强）

**里程碑自测**：能讲清"向量库与 Paimon 在存储设计上的同构与差异"；能给一个 RAG 业务画出完整数据管线（含删除同步路径）并指出一致性风险点

---

## 阶段 6（W21–24）：查询层与生产化

- [ ] W21 StarRocks：external catalog 挂 Paimon、异步物化视图加速湖上查询、向量索引与湖上向量检索
- [ ] W22 可观测性：Flink metrics + Paimon snapshot 监控 + 数据质量校验（2026 年 JD 新标配）；延伸：RAG 管线新鲜度与向量-源一致性监控
- [ ] W23 选型视野：Fluss（流式存储新物种，了解即可）+ Paimon/Iceberg/Hudi 三方对比，写成选型文；向量库选型对照表（W18 初稿）在此定稿并入
- [ ] W24 资产化：demo 整理成 GitHub 开源项目（好 README + 架构图 + docker-compose 一键启动，含 W19 的 RAG 数据管线模块）；去 Paimon/StarRocks GitHub 挑 `good first issue` 或文档错误，提交第一个 PR
- [ ] 产出：开源项目 + 首个 PR

**里程碑自测**：GitHub 有陌生人能一键跑起来的湖仓项目；第一个 PR 已提交（contributor 身份是雪球起点）

---

## 阶段 7（W25–28）：输出与变现

- [ ] W25–26 整理系列文章至 6–8 篇（checkpoint 源码 ×2–3、Paimon LSM/compaction ×2、湖仓实战 ×1–2、AI 数据底座 ×1–2），掘金/知乎/公众号同步分发
- [ ] W27 高频面试题自测（见下方清单），每题白板讲 15 分钟
- [ ] W28 面试摸底：投 2–3 个目标岗位（数据基础设施 / AI 平台方向）试水，校准市场温度
- [ ] 产出：系列文 + 摸底结果复盘

**终极自测**：45 分钟白板讲透"一条数据从 Kafka 进入 Flink 到落 Paimon 再被 StarRocks 查到的完整生命周期，以及每一步可能出什么故障；扩展题：这条数据同时派生 embedding 写入向量库支撑 RAG 时，一致性与新鲜度如何保证"

---

## 高频面试自测清单（W23 用）

- [ ] barrier 从哪发出、怎么流动、非对齐 checkpoint 时数据去了哪
- [ ] RocksDB 增量快照为什么快；LSM 读写放大如何权衡
- [ ] watermark 生成与传播；空闲源怎么处理
- [ ] 反压的定位方法与两类根因的解法
- [ ] 为什么 Paimon 用 LSM 而 Iceberg 用 COW/MOR
- [ ] 四种 merge engine 的选型；partial-update 多流写入的语义
- [ ] changelog producer 四档的语义与代价
- [ ] compaction 参数调优的决策逻辑；小文件问题怎么治
- [ ] Flink 2.0 分离式状态管理（ForSt）解决什么问题
- [ ] StarRocks external catalog + 物化视图的湖仓加速原理
- [ ] ANN 索引（HNSW / IVF）的召回率-延迟-内存三角权衡；向量检索为什么不用 B+ 树
- [ ] Milvus 的 segment 模型与 Paimon sorted run 的同构与差异
- [ ] RAG 管线：源文档更新/删除时向量库的同步语义；向量-湖仓一致性怎么保证
- [ ] 湖仓如何支撑可复现训练数据集（time travel / schema 演进 / 列统计）

---

## 最终产出物清单（32 岁跳槽的弹药库）

- [ ] 6–8 篇深度技术文章（含 1–2 篇 AI 数据底座主题，可被搜索到）
- [ ] 1 个一键跑起来的 GitHub 湖仓开源项目（含 RAG 数据管线模块）
- [ ] 1 个 Apache 社区 PR（contributor 身份）
- [ ] 12 个月以上 runway（财务线，与学习线并行）
- [ ] 面试摸底 2–3 个岗位，校准定价

---

## 每周打卡记录

| 周次 | 日期 | 完成任务 | 产出物链接 | 备注 |
|---|---|---|---|---|
| W1 | | | | |
| W2 | | | | |
| W3 | | | | |
| W4 | | | | |
| W5 | | | | |
| W6 | | | | |
| W7 | | | | |
| W8 | | | | |
| W9 | | | | |
| W10 | | | | |
| W11 | | | | |
| W12 | | | | |
| W13 | | | | |
| W14 | | | | |
| W15 | | | | |
| W16 | | | | |
| W17 | | | | |
| W18 | | | | |
| W19 | | | | |
| W20 | | | | |
| W21 | | | | |
| W22 | | | | |
| W23 | | | | |
| W24 | | | | |
| W25 | | | | |
| W26 | | | | |
| W27 | | | | |
| W28 | | | | |

---

## 参考资料

### Flink
- Flink 2.0.0 官方发布博客：https://flink.apache.org/2025/03/24/apache-flink-2.0.0-a-new-era-of-real-time-data-processing
- 中文解读（知乎）：https://zhuanlan.zhihu.com/p/32574793785
- 袋鼠云：Flink 2.0 新特性解读与迁移指南：https://www.dtstack.com/zh-cn/blogs/apache-flink-2-new-features-guide
- VLDB 论文：Disaggregated State Management in Apache Flink 2.0：https://www.vldb.org/pvldb/vol18/p4846-mei.pdf
- Flink 官方下载页（当前稳定版 2.3.0）：https://flink.apache.org/zh/downloads

### Paimon
- 官方文档总入口：https://paimon.apache.org/docs/master
- Primary-Key Table：https://paimon.apache.org/docs/master/primary-key-table
- Compaction：https://paimon.apache.org/docs/master/primary-key-table/compaction
- Dedicated Compaction：https://paimon.apache.org/docs/master/maintenance/dedicated-compaction

### AI 数据底座（2026-11-12 新增）
- Milvus 文档总入口：https://milvus.io/docs
- pgvector：https://github.com/pgvector/pgvector
- faiss：https://github.com/facebookresearch/faiss
- ann-benchmarks（ANN 算法公开实测榜单）：http://ann-benchmarks.com/
- HNSW 论文：https://arxiv.org/abs/1603.09320
- DiskANN 论文：https://arxiv.org/abs/1911.11081
- StarRocks 官方文档（搜"向量检索"）：https://docs.starrocks.io/docs/
- DeepSeek 3FS（LLM 时代存储新物种，视野层）：https://github.com/deepseek-ai/3FS
- Mooncake 论文（KVCache 为中心的 LLM serving 存算分离）：https://arxiv.org/abs/2407.00079

### 湖仓与生态
- 2026 数仓技术趋势（FineDataLink）：https://www.finedatalink.com/blog/article/69c7343c1916e24b22e858f3
- 湖仓一体 2026 选型对比（掘金）：https://juejin.cn/post/7639592961111310386
- 腾讯云：千亿级流量实时湖仓 Flink + Iceberg + Paimon：https://developer.cloud.tencent.com/article/2729355
- 阿里集团 Lakehouse 实践（StarRocks + Paimon）：https://developer.volcengine.com/articles/7482707349367750682
- Apache Fluss 发展研究报告（2024–2026）：https://nightcat.cloudns.asia:9981/misc/2026/03/
- Paimon LSM 源码级导览（pyshine）：https://www.pyshine.com/lsm-tree-flink/

---

*生成于 2026-11-12 · 同日补充阶段 5「AI 数据底座」（24 周 → 28 周）· 配套职业策略见对话记录（实时湖仓 → AI 数据底座方向）*
