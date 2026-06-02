# Osat 大规模数据测试报告 — Round 1

**测试日期**: 2026-05-31 ~ 2026-06-01
**项目目录**: `/home/chengyu/pepXplorer/ms_analysis/Osat/`
**测试目的**: 3296 样本（16 个 PXD 项目，Oryza_sativa）大规模流程验证

---

## 1. 测试环境

| 项目 | 值 |
|------|-----|
| 集群 | GinPie PBS |
| Snakemake 版本 | Python 3.9 / snakemake (conda env: ms) |
| 调度参数 | `--cores 48 --jobs 8` |
| 搜库引擎 | msgf, comet, msfragger |
| 数据类型 | DDA (1517) + DIA (1779), 混合碎裂模式 HCD/CID/SID |
| 总任务数 | 126,766 |

---

## 2. 运行历史

### 2.1 第一次提交（已终止）

- **时间**: 2026-05-31 20:39
- **参数**: `--cores 6 --jobs 8`
- **问题**: `--cores 6` 限制总线程数为 6，4 线程搜库任务同一时间只能跑 1 个，严重制约后续阶段
- **处理**: 终止后重新提交

### 2.2 第二次提交（当前运行中）

- **时间**: 2026-05-31 21:11（DAG 构建约 5 分钟，占用 1.8GB 内存）
- **参数**: `--cores 48 --jobs 8`
- **状态**: 运行中

---

## 3. 当前进度（截至 2026-06-01 10:30）

### 3.1 总体进度

| 指标 | 数值 |
|------|------|
| 运行时长 | ~13 小时 |
| 总任务 | 126,766 |
| 已完成 | 902 (1%) |
| 已提交 PBS | 923 |
| 转换完成 | 885 / 3296 样本 |
| 转换失败 | 15 样本 |
| 下游任务 | 0（全部阻塞在转换阶段） |
| mzML 总大小 | 1.7 TB |

### 3.2 各项目转换进度

| 项目 | 总数 | 已转换 | 失败 | 剩余 |
|------|------|--------|------|------|
| PXD055573 | 900 | 275 | 0 | 625 |
| PXD071972 | 615 | 169 | 11 | 435 |
| PXD014979 | 560 | 167 | 0 | 393 |
| PXD028032 | 300 | 89 | 2 | 209 |
| PXD054204 | 270 | 69 | 0 | 201 |
| PXD016013 | 169 | 44 | 0 | 125 |
| PXD040365 | 110 | 11 | 1 | 98 |
| PXD066634 | 81 | 18 | 0 | 63 |
| PXD015063 | 63 | 13 | 0 | 50 |
| PXD076063 | 54 | 0 | 0 | 54 |
| PXD043593 | 54 | 15 | 0 | 39 |
| PXD076354 | 27 | 6 | 0 | 21 |
| PXD068723 | 27 | 11 | 1 | 15 |
| PXD039588 | 27 | 1 | 0 | 26 |
| PXD071707 | 21 | 2 | 0 | 19 |
| PXD049027 | 18 | 0 | 0 | 18 |

---

## 4. 发现的问题

### 4.1 [严重] DAG 串行瓶颈：转换阶段阻塞全部下游分析

**现象**: 已转换的 885 个样本无法开始 QC 或搜库，所有下游任务数为 0。

**根因**: 工作流依赖链存在全局同步屏障：

```
convert_input_sample (×3296, 可并行)
        ↓ 必须全部完成 ↓
realize_input_conversions → conversion_report.tsv
        ↓
infer_mzml_metadata → mzml_inference.tsv
        ↓
normalize_sample_metadata, build_manual_review_recommendations
        ↓
build_engine_input_manifest → engine_input_manifest.tsv    ← 全局瓶颈
        ↓
quality_control_raw, first_search_openms_adapter, ...      ← 全部等待
```

关键代码位置：

1. **`rules/00_input_resolver.smk:132-161`** — `realize_input_conversions` 规则使用 `expand()` 收集全部 3296 个样本的转换报告，必须全部就位后才产出 `conversion_report.tsv`
2. **`rules/00_input_resolver.smk:163-189`** — `infer_mzml_metadata` 依赖 `conversion_report.tsv`
3. **`rules/00_input_resolver.smk:227-269`** — `build_engine_input_manifest` checkpoint 依赖上述所有产物，是所有下游规则的入口
4. **`rules/0_preprocessing.smk:14`** — `quality_control_raw` 依赖 `engine_input_manifest.tsv`
5. **`rules/1_first_search.smk:13`** — `first_search_openms_adapter` 依赖 `engine_input_manifest.tsv`

**影响**: 按当前 8 并行 msconvert 速度（~5-10 min/样本），3296 样本转换需要约 **35-70 小时**。期间集群上所有搜库、QC、分析资源完全空闲，`--cores 48` 的多线程能力无法利用。

**优化方案讨论**:

**方案 A — 预生成 manifest，解耦 conversion**

将 `build_engine_input_manifest` 提前执行（基于 `sample_format_detection.tsv`），生成"预期 manifest"。`convert_input_sample` 完成后仅更新对应行路径，失败样本标记为 skipped。下游规则可在首个样本转换完成后立即启动。

- 优点：manifest 立即可用，最充分的并行化
- 难点：manifest 路径需占位符机制；脚本需支持增量更新

**方案 B — 按项目/批次流水线**

按 PXD 项目分组，每批独立走完全流程。小项目（PXD071707 21样本）先完成先分析，不等待大项目（PXD055573 900样本）。

- 优点：改动小，自然分组
- 难点：需 batching 层；最终聚合仍需等全部批次

**方案 C — 最小改动：manifest 不依赖 conversion_report**

观察发现 `sample_format_detection.tsv` 已包含 format/acquisition_mode 等元信息，`conversion_report.tsv` 主要确认转换成功与否。可让 manifest 生成仅依赖 detection 输出，失败样本在后续步骤中自然跳过。

- 优点：改动最小，只需调整依赖关系
- 难点：需验证 `build_engine_manifest.py` 能否不依赖 conversion_report 正常工作

### 4.2 [严重] PXD076354 timsTOF 数据产生巨型 mzML 文件

**现象**: PXD076354（timsTOF PASEF）的 6 个已转换 mzML 文件，每个 117-120 GB。

```
PXD076354_sample_2.mzML  120G
PXD076354_sample_5.mzML  120G
PXD076354_sample_3.mzML  119G
PXD076354_sample_6.mzML  119G
PXD076354_sample_1.mzML  117G
PXD076354_sample_4.mzML  117G
```

6 个文件已占 ~700 GB。剩余 21 个样本预计消耗 ~2.5 TB，总计将超过 3 TB。

**与 test 项目对照**: Round 18-19 测试中已发现同一问题（117GB mzML 导致 OOM），当时排除了 PXD076354。

**建议**: 
- 从 `samples.csv` 中排除 PXD076354 全部 27 个样本
- 已生成的 6 个巨型 mzML 文件（~700GB）建议清理回收空间
- timsTOF PASEF 数据需要专门的转换/搜库策略，不在当前 DDA/DIA 流程中处理

### 4.3 [中等] 15 个样本转换失败（Corrupt RAW file）

**错误信息**: `msconvert failed; returncode=1; Corrupt RAW file`

失败样本列表：

| 项目 | 失败样本 |
|------|----------|
| PXD071972 (11) | sample_484, 506, 547, 661, 666, 744, 843, 845, 891, 893, 894 |
| PXD028032 (2) | sample_244, 293 |
| PXD040365 (1) | sample_49 |
| PXD068723 (1) | sample_22 |

**原因**: 原始 RAW 文件损坏（Thermo `RawFileImpl::ctor()` 报告 Corrupt RAW），非流程 bug。

**影响**: 这些样本会标记为 `failed`，后续步骤自动跳过，不影响其他样本处理。

### 4.4 [低] --cores 参数理解与调度行为

**发现**: `--cores` 在 Snakemake 中表示**所有运行中任务的总线程上限**，而非每个任务的线程数。

- `--cores 6 --jobs 8`: 最多 8 个 PBS 任务，但总线程 ≤ 6。4 线程搜库任务仅能并行 1 个。
- `--cores 48 --jobs 8`: 最多 8 个 PBS 任务，总线程 ≤ 48。4 线程搜库任务可同时并行 12 个。

当前 `convert_input_sample` 规则硬编码 `threads: 1`（Docker msconvert 单线程），因此 PBS 显示 `ncpus=1` 是正确行为。后续搜库规则使用 `get_tool_threads(wildcards.tool)` 动态分配（默认 4 线程）。

### 4.5 [低] PXD014979_sample_173 转换卡住超 11 小时

**现象**: job 2848（PXD014979_sample_173）运行 11.5 小时，CPU 时间仅 2:44，PBS 日志为空。

**可能原因**: Docker msconvert I/O 等待（大文件 + NFS），或 Docker 进程挂起。

**建议**: 排查该样本原始 RAW 文件大小；如果异常大建议标记跳过。

---

## 5. 资源消耗

| 资源 | 当前值 |
|------|--------|
| mzML 总大小 | 1.7 TB |
| 磁盘剩余 | 18 TB / 42 TB (58% used) |
| PXD076354 预估额外消耗 | ~2.5 TB（建议排除） |
| DAG 构建内存 | ~2.7 GB |
| DAG 构建时间 | ~5 分钟 |

---

## 6. 建议操作

### 6.1 立即处理

1. **排除 PXD076354**: 从 `samples.csv` 移除 27 个样本，终止当前运行，清理已有巨型 mzML（回收 ~700GB）
2. **重启流程**: 排除后重新 `--unlock` 并提交，DAG 规模从 126K 降至约 115K

### 6.2 后续优化

3. **解耦转换与分析**: 实施 DAG 串行瓶颈的优化方案（建议先尝试方案 C 最小改动）
4. **增加并行度**: 转换阶段完成后，`--cores 48` 可支持 12 个 4 线程搜库任务并行，确认集群有足够计算节点
5. **清理损坏样本**: 记录 15 个 Corrupt RAW 样本，从后续分析中排除

---

## 7. 关键文件路径

| 文件 | 路径 |
|------|------|
| 工作流主日志 | `Osat/logs/workflow_main.log` |
| PBS 日志目录 | `Osat/logs/pbs/` |
| 转换失败日志 | `Osat/logs/input_resolver/conversion_reports/*.failed.tsv` |
| 转换报告 | `Osat/results/input/conversion_reports/` |
| 已转换 mzML | `Osat/results/input/converted/` |
| 样本配置 | `Osat/samples.csv` |
| 项目配置 | `Osat/project_config.yaml` |
