/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
 */

export const zhSidebar = {
  '/zh/UserGuide/V0.13.x/': [
    {
      text: 'IoTDB用户手册 (V0.13.x)',
      collapsible: false,
      children: [],
    },
    {
      text: 'IoTDB简介',
      collapsible: true,
      prefix: 'IoTDB-Introduction/',
      children: [
        { text: 'IoTDB简介', link: 'What-is-IoTDB' },
        { text: '主要功能特点', link: 'Features' },
        { text: '系统架构', link: 'Architecture' },
        { text: '应用场景', link: 'Scenario' },
        { text: '研究论文', link: 'Publication' },
      ],
    },
    {
      text: '快速上手',
      collapsible: true,
      prefix: 'QuickStart/',
      children: [
        { text: '快速上手', link: 'QuickStart' },
        { text: '数据文件存储', link: 'Files' },
        { text: '下载与安装', link: 'WayToGetIoTDB' },
        { text: 'SQL命令行终端(CLI)', link: 'Command-Line-Interface' },
      ],
    },
    {
      text: '数据模式与概念',
      collapsible: true,
      prefix: 'Data-Concept/',
      children: [
        { text: '数据模型', link: 'Data-Model-and-Terminology' },
        { text: '元数据模板', link: 'Schema-Template' },
        { text: '数据类型', link: 'Data-Type' },
        { text: '死区处理', link: 'Deadband-Process' },
        { text: '编码方式', link: 'Encoding' },
        { text: '压缩方式', link: 'Compression' },
        { text: '时间分区', link: 'Time-Partition' },
        { text: '时区', link: 'Time-zone' },
      ],
    },
    {
      text: '语法约定',
      collapsible: true,
      prefix: 'Reference/',
      children: [
        { text: '语法约定', link: 'Syntax-Conventions' },
      ],
    },
    {
      text: '应用编程接口',
      collapsible: true,
      prefix: 'API/',
      children: [
        { text: 'Java 原生接口', link: 'Programming-Java-Native-API' },
        { text: 'Python 原生接口', link: 'Programming-Python-Native-API' },
        { text: 'C++ 原生接口', link: 'Programming-Cpp-Native-API' },
        { text: 'Go 原生接口', link: 'Programming-Go-Native-API' },
        { text: 'JDBC (不推荐)', link: 'Programming-JDBC' },
        { text: 'MQTT', link: 'Programming-MQTT' },
        { text: 'REST API', link: 'RestService' },
        { text: 'TsFile API', link: 'Programming-TsFile-API' },
        { text: '状态码', link: 'Status-Codes' },
      ],
    },
    {
      text: '元数据操作',
      collapsible: true,
      prefix: 'Operate-Metadata/',
      children: [
        { text: '存储组操作', link: 'Storage-Group' },
        { text: '节点操作', link: 'Node' },
        { text: '时间序列操作', link: 'Timeseries' },
        { text: '元数据模板', link: 'Template' },
        { text: 'TTL', link: 'TTL' },
        { text: '自动创建元数据', link: 'Auto-Create-MetaData' },
      ],
    },
    {
      text: '数据写入和删除',
      collapsible: true,
      prefix: 'Write-And-Delete-Data/',
      children: [
        { text: '写入数据', link: 'Write-Data' },
        { text: '加载 TsFile', link: 'Load-External-Tsfile' },
        { text: '导入导出 CSV', link: 'CSV-Tool' },
        { text: '删除数据', link: 'Delete-Data' },
      ],
    },
    {
      text: '数据查询',
      collapsible: true,
      prefix: 'Query-Data/',
      children: [
        { text: '概述', link: 'Overview' },
        { text: '选择表达式', link: 'Select-Expression' },
        { text: '查询过滤条件', link: 'Query-Filter' },
        { text: '查询结果分页', link: 'Pagination' },
        { text: '查询结果对齐格式', link: 'Result-Format' },
        { text: '聚合查询', link: 'Aggregate-Query' },
        { text: '最新点查询', link: 'Last-Query' },
        { text: '空值填充', link: 'Fill-Null-Value' },
        { text: '空值过滤', link: 'Without-Null' },
        { text: '查询性能追踪', link: 'Tracing-Tool' },
      ],
    },
    {
      text: '数据处理',
      collapsible: true,
      prefix: 'Process-Data/',
      children: [
        { text: '用户定义函数(UDF)', link: 'UDF-User-Defined-Function' },
        { text: '查询写回(SELECT INTO)', link: 'Select-Into' },
        { text: '连续查询(CQ)', link: 'Continuous-Query' },
        { text: '触发器', link: 'Triggers' },
        { text: '告警机制', link: 'Alerting' },
      ],
    },
    {
      text: '权限管理',
      collapsible: true,
      prefix: 'Administration-Management/',
      children: [
        { text: '权限管理', link: 'Administration' },
      ],
    },
    {
      text: '运维工具',
      collapsible: true,
      prefix: 'Maintenance-Tools/',
      children: [
        { text: '运维命令', link: 'Maintenance-Command' },
        { text: '日志工具', link: 'Log-Tool' },
        { text: 'JMX 工具', link: 'JMX-Tool' },
        { text: 'MLog 解析工具', link: 'MLogParser-Tool' },
        { text: 'MLog 加载工具', link: 'MLogLoad-Tool' },
        { text: '元数据导出工具', link: 'Export-Schema-Tool' },
        { text: '节点工具', link: 'NodeTool' },
        { text: '水印工具', link: 'Watermark-Tool' },
        { text: '监控工具', link: 'Metric-Tool' },
        { text: 'TsFile 同步工具', link: 'Sync-Tool' },
        { text: 'TsFile 拆分工具', link: 'TsFile-Split-Tool' },
      ],
    },
    {
      text: '系统集成',
      collapsible: true,
      prefix: 'Ecosystem-Integration/',
      children: [
        { text: 'Grafana Plugin', link: 'Grafana-Plugin' },
        { text: 'Grafana Connector（不推荐）', link: 'Grafana-Connector' },
        { text: 'Zeppelin-IoTDB', link: 'Zeppelin-IoTDB' },
        { text: 'DBeaver-IoTDB', link: 'DBeaver' },
        { text: 'Spark-TsFile', link: 'Spark-TsFile' },
        { text: 'Hadoop-TsFile', link: 'MapReduce-TsFile' },
        { text: 'Spark-IoTDB', link: 'Spark-IoTDB' },
        { text: 'Hive-TsFile', link: 'Hive-TsFile' },
        { text: 'Flink-TsFile', link: 'Flink-TsFile' },
        { text: 'Flink-IoTDB', link: 'Flink-IoTDB' },
        { text: 'NiFi IoTDB', link: 'NiFi-IoTDB' },
      ],
    },
    {
      text: 'UDF 资料库',
      collapsible: true,
      prefix: 'UDF-Library/',
      children: [
        { text: '快速开始', link: 'Quick-Start' },
        { text: '数据画像', link: 'Data-Profiling' },
        { text: '异常检测', link: 'Anomaly-Detection' },
        { text: '数据匹配', link: 'Data-Matching' },
        { text: '频域分析', link: 'Frequency-Domain' },
        { text: '数据质量', link: 'Data-Quality' },
        { text: '数据修复', link: 'Data-Repairing' },
        { text: '序列发现', link: 'Series-Discovery' },
        { text: '字符串处理', link: 'String-Processing' }
      ],
    },
    {
      text: '参考',
      collapsible: true,
      prefix: 'Reference/',
      children: [
        { text: '配置参数', link: 'Config-Manual' },
        { text: '关键字', link: 'Keywords' },
        { text: '常见问题', link: 'Frequently-asked-questions' },
        { text: '时间序列数据库比较', link: 'TSDB-Comparison' },
      ],
    },
  ],
};
