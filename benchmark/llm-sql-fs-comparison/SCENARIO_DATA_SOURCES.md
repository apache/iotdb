<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# 可用于 IoTDB 场景任务的数据源核实

核实日期：2026-09-14。以下内容来自 UCI 官方数据集页面和 API 响应；本次只读取元数据，没有下载完整数据文件。

## 1. Individual Household Electric Power Consumption（UCI id 235）

- 一手来源：[UCI 数据集页面](https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption)、[UCI API](https://archive.ics.uci.edu/api/dataset?id=235)、[数据文件地址](https://archive.ics.uci.edu/static/public/235/data.csv)。
- 数据属性：单个家庭（法国巴黎附近 Sceaux）的电力测量，约 4 年（2006-12 至 2010-11），**每分钟**采样；API 给出 2,075,259 条实例、9 个字段，属于多变量时间序列。它是实际家庭用电记录，不是实验室仿真。
- 字段和单位：`Date`、`Time`；`Global_active_power`（kW）、`Global_reactive_power`（kW）、`Voltage`（V）、`Global_intensity`（A）；`Sub_metering_1/2/3`（Wh）。官方说明 sub-metering 1 对应厨房（洗碗机、烤箱、微波炉），2 对应洗衣房（洗衣机、烘干机、冰箱和灯），3 对应热水器和空调。
- 缺失语义：附加说明指出约 1.25% 行的测量值缺失，日历时间戳仍在，空值表现为相邻分号之间没有值。API 的 `has_missing_values` 字段显示为 `no`，因此 fixture 应以附加说明的空字段规则为准并在报告中标注这一元数据不一致。
- 许可与引用：数据集页面标明 [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/legalcode)，DOI `10.24432/C58K54`；使用时须署名 Georges Hebrail、Alice Berard 和 UCI。

可落地的业务问题与机械验收：

1. **家庭负荷异常与节能排查**：在指定日期找出每分钟总有功功率超过基线（例如同一小时过去 7 天中位数的 150%）的连续区间，并指出主要 sub-metering 来源。验收器按 `Date+Time` 生成分钟桶，检查闭区间端点、空值不当作 0、区间合并规则、峰值与 sub-metering 求和是否与独立 oracle 一致。
2. **电压质量与用电量核算**：筛出电压低于阈值且 `Global_intensity` 较高的分钟，计算每个事件的 Wh（kW×1/60h），并列出受影响日期。验收器检查单位换算、缺失分钟与事件持续时长；不能由该数据源判断电器故障原因或电费金额（缺少电价和设备级完整回路信息）。

## 2. Occupancy Detection（UCI id 357）

- 一手来源：[UCI 数据集页面](https://archive.ics.uci.edu/dataset/357/occupancy+detection)、[UCI API](https://archive.ics.uci.edu/api/dataset?id=357)、[数据文件地址](https://archive.ics.uci.edu/static/public/357/data.csv)。
- 数据属性：办公室房间占用检测的**实验数据**，API 给出 20,560 条实例、6 个特征并标注二分类任务；官方摘要说明 ground truth 来自每分钟拍摄的带时间戳图片。页面说明提交了 3 个数据集（用于训练和测试）。
- 字段和单位：`id`；`date`（日期时间）；`Temperature`（°C）、`Humidity`（%）、`Light`（Lux）、`CO2`（ppm）、`HumidityRatio`（kg water-vapor/kg air）；`Occupancy` 为标签，0=未占用、1=占用。API 未声明缺失值。
- 许可与引用：数据集页面标明 [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/legalcode)，DOI `10.24432/C5X01N`；创建者 Luis Candanedo，相关论文为 Candanedo 与 Feldheim 2016 年 *Energy and Buildings* 文章。

可落地的业务问题与机械验收：

1. **会议室空调节能**：按小时计算占用率，在有人时段比较温度/CO2 与设定阈值，找出“无人但高能耗代理信号”（Light 或 CO2 持续偏高）的时段。验收器按时间戳重建分钟/小时桶，检查 `Occupancy` 只取 0/1、占用率分母、阈值比较和连续时段边界。
2. **通风巡检优先级**：找出占用状态为 1 且 CO2 超过阈值的连续区间，按持续时间和峰值排序并返回证据行。验收器核对每个区间首末时间、CO2 峰值、排序并列规则；该数据源不能直接证明人员健康风险或 HVAC 故障（没有风量、设定点和室外空气数据）。

## 3. Condition Monitoring of Hydraulic Systems（UCI id 447）

- 一手来源：[UCI 数据集页面](https://archive.ics.uci.edu/dataset/447/condition+monitoring+of+hydraulic+systems)、[UCI API](https://archive.ics.uci.edu/api/dataset?id=447)。该数据集页面未提供单一 `data_url`，文件通过页面链接获取；本次未下载文件。
- 数据属性：在液压试验台上实验采集的多传感器数据，不是现场生产设备数据，也不是模拟器生成。API 给出 2,205 个实例、43,680 个特征，支持分类和回归。试验台有主工作回路和次级冷却过滤回路，**每个恒定负载循环 60 秒**；改变冷却器、阀、泵、蓄能器的状态和严重度。
- 原始数据结构：官方变量说明写明原始过程传感器数据以制表符分隔矩阵保存，行=循环，列=循环内数据点；未做特征提取。传感器及采样率：PS1–PS6 压力（bar，100 Hz）；EPS1 电机功率（W，100 Hz）；FS1–FS2 流量（l/min，10 Hz）；TS1–TS4 温度（°C，1 Hz）；VS1 振动（mm/s，1 Hz）；CE 冷却效率（虚拟，%，1 Hz）；CP 冷却功率（虚拟，kW，1 Hz）；SE 效率因子（%，1 Hz）。
- 标签语义：`profile.txt` 按循环编号逐行标注：冷却器效率 3/20/100%；阀切换状态 100/90/80/73%；泵内泄漏 0/1/2（无/弱/严重）；蓄能器压力 130/115/100/90 bar；stable flag 0/1（稳定/可能尚未达到静态条件）。这些是试验台设定或标注的部件状态，不能直接当作现场告警。
- 许可与引用：数据集页面标明 [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/legalcode)，DOI `10.24432/C5CW21`；创建者 Nikolai Helwig、Eliseo Pignanelli、Andreas Schütze。

可落地的业务问题与机械验收：

1. **液压试验台部件健康分级**：针对最近一个 60 秒负载循环，按传感器采样率计算压力/流量/温度/振动摘要，并依据 profile 标签输出冷却器、阀、泵泄漏和蓄能器等级。验收器检查按循环分组、各采样率对应的点数（100 Hz→约 6000 点、10 Hz→约 600 点、1 Hz→约 60 点）、单位换算和标签映射。
2. **维修前复现与稳定性筛查**：找出 `stable flag=1` 的循环，比较其与稳定循环的压力/振动统计差异，列出需要重复试验的循环编号。验收器检查只使用 profile 的稳定标记、循环编号对齐和固定统计公式；该数据源不能估计真实生产线停机损失、现场剩余寿命或安全风险，因为数据来自受控试验台且无成本、维修和环境变量。

## 使用边界

三个数据源都适合构造“发现 schema→按时间窗口查询→处理缺失/标签→给出带证据业务结论”的 IoTDB 任务。它们分别代表实际家庭记录、办公环境实验记录和液压试验台实验记录；不能把实验室标签宣传为生产现场事实。将其导入 IoTDB fixture 时，应保留原始时间、单位、缺失表示、循环/设备标识及 DOI/CC BY 4.0 署名信息。
