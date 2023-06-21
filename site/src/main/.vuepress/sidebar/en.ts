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

import { sidebar } from 'vuepress-theme-hope';
import { enSidebar as V102xSidebar } from './V1.2.x/en.js';
import { enSidebar as V101xSidebar } from './V1.1.x/en.js';
import { enSidebar as V100xSidebar } from './V1.0.x/en.js';
import { enSidebar as V013xSidebar } from './V0.13.x/en.js';

export const enSidebar = sidebar({
  // '/UserGuide/Master/': [
  //   {
  //     text: 'IoTDB User Guide (latest)',
  //     children: [],
  //   },
  //   {
  //     text: 'About IoTDB',
  //     collapsible: true,
  //     prefix: 'IoTDB-Introduction/',
  //     // children: 'structure',
  //     children: [
  //       { text: 'What is IoTDB', link: 'What-is-IoTDB' },
  //       { text: 'Architecture', link: 'Architecture' },
  //       { text: 'Scenario', link: 'Scenario' },
  //       { text: 'Features', link: 'Features' },
  //       { text: 'Publication', link: 'Publication' },
  //     ],
  //   },
  //   {
  //     text: 'Quick Start',
  //     collapsible: true,
  //     prefix: 'QuickStart/',
  //     // children: 'structure',
  //     children: [
  //       { text: 'Quick Start', link: 'QuickStart' },
  //       { text: 'Cluster Quick Start', link: 'ClusterQuickStart' },
  //       { text: 'Download and Setup', link: 'WayToGetIoTDB' },
  //       { text: 'Command Line Interface', link: 'Command-Line-Interface' },
  //       { text: 'Data storage', link: 'Files' },
  //     ],
  //   },
  //   {
  //     text: 'Data Concept',
  //     collapsible: true,
  //     prefix: 'Data-Concept/',
  //     // children: 'structure',
  //     children: [
  //       { text: 'Data Model and Terminology', link: 'Data-Model-and-Terminology' },
  //       { text: 'Schema Template', link: 'Schema-Template' },
  //       { text: 'Data Type', link: 'Data-Type' },
  //       { text: 'Deadband Process', link: 'Deadband-Process' },
  //       { text: 'Encoding', link: 'Encoding' },
  //       { text: 'Compression', link: 'Compression' },
  //       { text: 'Time Partition of Data', link: 'Time-Partition' },
  //       { text: 'Time zone', link: 'Time-zone' },
  //     ],
  //   },
  //   {
  //     text: 'Syntax Conventions',
  //     collapsible: true,
  //     prefix: 'Syntax-Conventions/',
  //     // children: 'structure',
  //     children: [
  //       { text: 'Literal Values', link: 'Literal-Values' },
  //       { text: 'Identifier', link: 'Identifier' },
  //       { text: 'NodeName in Path', link: 'NodeName-In-Path' },
  //       { text: 'Key-Value Pair', link: 'KeyValue-Pair' },
  //       { text: 'Keywords', link: 'Keywords-And-Reserved-Words' },
  //       { text: 'Session And TsFile API', link: 'Session-And-TsFile-API' },
  //       { text: 'Detailed Definitions of Lexical and Grammar', link: 'Detailed-Grammar' },
  //     ],
  //   },
  //   {
  //     text: 'API',
  //     collapsible: true,
  //     prefix: 'API/',
  //     // children: 'structure',
  //     children: [
  //       { text: 'Java Native API', link: 'Programming-Java-Native-API' },
  //       { text: 'Python Native API', link: 'Programming-Python-Native-API' },
  //       { text: 'C++ Native API', link: 'Programming-Cpp-Native-API' },
  //       { text: 'Go Native API', link: 'Programming-Go-Native-API' },
  //       { text: 'JDBC (Not Recommend)', link: 'Programming-JDBC' },
  //       { text: 'MQTT', link: 'Programming-MQTT' },
  //       { text: 'REST API V1 (Not Recommend)', link: 'RestServiceV1' },
  //       { text: 'REST API V2', link: 'RestServiceV2' },
  //       { text: 'TsFile API', link: 'Programming-TsFile-API' },
  //       { text: 'Interface Comparison', link: 'Interface-Comparison' },
  //     ],
  //   },
  //   {
  //     text: 'Operate Metadata',
  //     collapsible: true,
  //     prefix: 'Operate-Metadata/',
  //     // children: 'structure',
  //     children: [
  //       { text: 'Database', link: 'Database' },
  //       { text: 'Node', link: 'Node' },
  //       { text: 'Timeseries', link: 'Timeseries' },
  //       { text: 'Schema Template', link: 'Template' },
  //       { text: 'Auto Create Metadata', link: 'Auto-Create-MetaData' },
  //     ],
  //   },
  //   {
  //     text: 'Write Data (Update Data)',
  //     collapsible: true,
  //     prefix: 'Write-Data/',
  //     // children: 'structure',
  //     children: [
  //       { text: 'CLI Write', link: 'Write-Data' },
  //       { text: 'Native API Write', link: 'Session' },
  //       { text: 'REST API', link: 'REST-API' },
  //       { text: 'MQTT Write', link: 'MQTT' },
  //       { text: 'Batch Data Load', link: 'Batch-Load-Tool' },
  //     ],
  //   },
  //   {
  //     text: 'Delete Data',
  //     collapsible: true,
  //     prefix: 'Delete-Data/',
  //     // children: 'structure',
  //     children: [
  //       { text: 'Delete Data', link: 'Delete-Data' },
  //       { text: 'TTL', link: 'TTL' },
  //     ],
  //   },
  //   {
  //     text: 'Query Data',
  //     collapsible: true,
  //     prefix: 'Query-Data/',
  //     // children: 'structure',
  //     children: [
  //       { text: 'Overview', link: 'Overview' },
  //       { text: 'Select Expression', link: 'Select-Expression' },
  //       { text: 'Last Query', link: 'Last-Query' },
  //       { text: 'Query Alignment Mode', link: 'Align-By' },
  //       { text: 'Where Condition', link: 'Where-Condition' },
  //       { text: 'Group By', link: 'Group-By' },
  //       { text: 'Having Condition', link: 'Having-Condition' },
  //       { text: 'Order By', link: 'Order-By' },
  //       { text: 'Fill Null Value', link: 'Fill' },
  //       { text: 'Pagination', link: 'Pagination' },
  //       { text: 'Select Into', link: 'Select-Into' },
  //       { text: 'Continuous Query', link: 'Continuous-Query' },
  //     ],
  //   },
  //   {
  //     text: 'Operators and Functions',
  //     collapsible: true,
  //     prefix: 'Operators-Functions/',
  //     // children: 'structure',
  //     children: [
  //       { text: 'Overview', link: 'Overview' },
  //       { text: 'UDF (User Defined Function)', link: 'User-Defined-Function' },
  //       { text: 'Aggregation', link: 'Aggregation' },
  //       { text: 'Mathematical', link: 'Mathematical' },
  //       { text: 'Comparison', link: 'Comparison' },
  //       { text: 'Logical', link: 'Logical' },
  //       { text: 'Conversion', link: 'Conversion' },
  //       { text: 'Constant', link: 'Constant' },
  //       { text: 'Selection', link: 'Selection' },
  //       { text: 'Continuous Interval', link: 'Continuous-Interval' },
  //       { text: 'Variation Trend', link: 'Variation-Trend' },
  //       { text: 'Sample', link: 'Sample' },
  //       { text: 'Time-Series', link: 'Time-Series' },
  //       { text: 'Lambda', link: 'Lambda' },
  //       { text: 'Conditional Expression', link: 'Conditional' },

  //       // IoTDB-Quality
  //       { text: 'Data Profiling', link: 'Data-Profiling' },
  //       { text: 'Anomaly Detection', link: 'Anomaly-Detection' },
  //       { text: 'Data Matching', link: 'Data-Matching' },
  //       { text: 'Frequency Domain', link: 'Frequency-Domain' },
  //       { text: 'Data Quality', link: 'Data-Quality' },
  //       { text: 'Data Repairing', link: 'Data-Repairing' },
  //       { text: 'Series Discovery', link: 'Series-Discovery' },
  //       { text: 'Machine Learning', link: 'Machine-Learning' },
  //     ],
  //   },
  //   {
  //     text: 'Trigger',
  //     collapsible: true,
  //     prefix: 'Trigger/',
  //     // children: 'structure',
  //     children: [
  //       { text: 'Instructions', link: 'Instructions' },
  //       { text: 'How to implement a trigger', link: 'Implement-Trigger' },
  //       { text: 'Trigger Management', link: 'Trigger-Management' },
  //       { text: 'Notes', link: 'Notes' },
  //       { text: 'Configuration-Parameters', link: 'Configuration-Parameters' },
  //     ],
  //   },
  //   {
  //     text: 'Monitor and Alert',
  //     collapsible: true,
  //     prefix: 'Monitor-Alert/',
  //     // children: 'structure',
  //     children: [
  //       { text: 'Metric Tool', link: 'Metric-Tool' },
  //       { text: 'Alerting', link: 'Alerting' },
  //     ],
  //   },
  //   {
  //     text: 'Administration Management',
  //     collapsible: true,
  //     prefix: 'Administration-Management/',
  //     // children: 'structure',
  //     children: [
  //       { text: 'Administration', link: 'Administration' },
  //     ],
  //   },
  //   {
  //     text: 'Maintenance Tools',
  //     collapsible: true,
  //     prefix: 'Maintenance-Tools/',
  //     // children: 'structure',
  //     children: [
  //       { text: 'Maintenance Command', link: 'Maintenance-Command' },
  //       { text: 'Log Tool', link: 'Log-Tool' },
  //       { text: 'JMX Tool', link: 'JMX-Tool' },
  //       { text: 'MLogParser Tool', link: 'MLogParser-Tool' },
  //       { text: 'IoTDB Data Directory Overview Tool', link: 'IoTDB-Data-Dir-Overview-Tool' },
  //       { text: 'TsFile Sketch Tool', link: 'TsFile-Sketch-Tool' },
  //       { text: 'TsFile Resource Sketch Tool', link: 'TsFile-Resource-Sketch-Tool' },
  //       { text: 'TsFile Split Tool', link: 'TsFile-Split-Tool' },
  //       { text: 'TsFile Load Export Tool', link: 'TsFile-Load-Export-Tool' },
  //       { text: 'CSV Load Export Tool', link: 'CSV-Tool' },
  //     ],
  //   },
  //   {
  //     text: 'Ecosystem Integration',
  //     collapsible: true,
  //     prefix: 'Ecosystem-Integration/',
  //     // children: 'structure',
  //     children: [
  //       { text: 'Grafana-Plugin', link: 'Grafana-Plugin' },
  //       { text: 'Grafana-Connector (Not Recommended)', link: 'Grafana-Connector' },
  //       { text: 'Zeppelin-IoTDB', link: 'Zeppelin-IoTDB' },
  //       { text: 'DBeaver-IoTDB', link: 'DBeaver' },
  //       { text: 'MapReduce-TsFile', link: 'MapReduce-TsFile' },
  //       { text: 'Spark-TsFile', link: 'Spark-TsFile' },
  //       { text: 'Spark-IoTDB', link: 'Spark-IoTDB' },
  //       { text: 'Hive-TsFile', link: 'Hive-TsFile' },
  //       { text: 'Flink-IoTDB', link: 'Flink-IoTDB' },
  //       { text: 'Flink-TsFile', link: 'Flink-TsFile' },
  //       { text: 'NiFi-IoTDB', link: 'NiFi-IoTDB' },
  //     ],
  //   },
  //   {
  //     text: 'Cluster',
  //     collapsible: true,
  //     prefix: 'Cluster/',
  //     // children: 'structure',
  //     children: [
  //       { text: 'Cluster Concept', link: 'Cluster-Concept' },
  //       { text: 'Cluster Setup', link: 'Cluster-Setup' },
  //       { text: 'Cluster Maintenance', link: 'Cluster-Maintenance' },
  //       { text: 'Deployment Recommendation', link: 'Deployment-Recommendation' },
  //     ],
  //   },
  //   {
  //     text: 'FAQ',
  //     collapsible: true,
  //     prefix: 'FAQ/',
  //     // children: 'structure',
  //     children: [
  //       { text: 'Frequently asked questions', link: 'Frequently-asked-questions' },
  //       { text: 'FAQ for cluster setup', link: 'FAQ-for-cluster-setup' },
  //     ],
  //   },
  //   {
  //     text: 'Reference',
  //     collapsible: true,
  //     prefix: 'Reference/',
  //     // children: 'structure',
  //     children: [
  //       { text: 'Common Config Manual', link: 'Common-Config-Manual' },
  //       { text: 'ConfigNode Config Manual', link: 'ConfigNode-Config-Manual' },
  //       { text: 'DataNode Config Manual', link: 'DataNode-Config-Manual' },
  //       { text: 'SQL Reference', link: 'SQL-Reference' },
  //       { text: 'Status Codes', link: 'Status-Codes' },
  //       { text: 'Keywords', link: 'Keywords' },
  //       { text: 'TSDB Comparison', link: 'TSDB-Comparison' },
  //     ],
  //   },
  // ],
  // ...V102xSidebar,
  ...V101xSidebar,
  ...V100xSidebar,
  ...V013xSidebar,
});
