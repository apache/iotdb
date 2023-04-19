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

export const enSidebar = {
  '/UserGuide/V0.13.x/': [
    {
      text: 'IoTDB User Guide (V0.13.x)',
      collapsible: false,
      children: [],
    },
    {
      text: 'IoTDB Introduction',
      collapsible: true,
      prefix: 'IoTDB-Introduction/',
      children: [
        { text: 'What is IoTDB', link: 'What-is-IoTDB' },
        { text: 'Architecture', link: 'Architecture' },
        { text: 'Scenario', link: 'Scenario' },
        { text: 'Features', link: 'Features' },
        { text: 'Publication', link: 'Publication' },
      ],
    },
    {
      text: 'Quick Start',
      collapsible: true,
      prefix: 'QuickStart/',
      children: [
        { text: 'Quick Start', link: 'QuickStart' },
        { text: 'Data storage', link: 'Files' },
        { text: 'Download and Setup', link: 'WayToGetIoTDB' },
        { text: 'Command Line Interface', link: 'Command-Line-Interface' },
      ],
    },
    {
      text: 'Data Concept',
      collapsible: true,
      prefix: 'Data-Concept/',
      children: [
        { text: 'Data Model and Terminology', link: 'Data-Model-and-Terminology' },
        { text: 'Schema Template', link: 'Schema-Template' },
        { text: 'Data Type', link: 'Data-Type' },
        { text: 'Deadband Process', link: 'Deadband-Process' },
        { text: 'Encoding', link: 'Encoding' },
        { text: 'Compression', link: 'Compression' },
        { text: 'Time Partition', link: 'Time-Partition' },
        { text: 'Time zone', link: 'Time-zone' },
      ],
    },
    {
      text: 'Syntax Conventions',
      collapsible: true,
      prefix: 'Reference/',
      children: [
        { text: 'Syntax Conventions', link: 'Syntax-Conventions' },
      ],
    },
    {
      text: 'API',
      collapsible: true,
      prefix: 'API/',
      children: [
        { text: 'Java Native API', link: 'Programming-Java-Native-API' },
        { text: 'Python Native API', link: 'Programming-Python-Native-API' },
        { text: 'C++ Native API', link: 'Programming-Cpp-Native-API' },
        { text: 'Go Native API', link: 'Programming-Go-Native-API' },
        { text: 'JDBC (Not Recommend)', link: 'Programming-JDBC' },
        { text: 'MQTT', link: 'Programming-MQTT' },
        { text: 'REST API', link: 'RestService' },
        { text: 'TsFile API', link: 'Programming-TsFile-API' },
        { text: 'Status Codes', link: 'Status-Codes' },
      ],
    },
    {
      text: 'Operate Metadata',
      collapsible: true,
      prefix: 'Operate-Metadata/',
      children: [
        { text: 'Storage Group', link: 'Storage-Group' },
        { text: 'Node', link: 'Node' },
        { text: 'Timeseries', link: 'Timeseries' },
        { text: 'Schema Template', link: 'Template' },
        { text: 'TTL', link: 'TTL' },
        { text: 'Auto Create Metadata', link: 'Auto-Create-MetaData' },
      ],
    },
    {
      text: 'Write and Delete Data',
      collapsible: true,
      prefix: 'Write-And-Delete-Data/',
      children: [
        { text: 'Write Data', link: 'Write-Data' },
        { text: 'Load External Tsfile', link: 'Load-External-Tsfile' },
        { text: 'CSV Tool', link: 'CSV-Tool' },
        { text: 'Delete Data', link: 'Delete-Data' },
      ],
    },
    {
      text: 'Query Data',
      collapsible: true,
      prefix: 'Query-Data/',
      children: [
        { text: 'Overview', link: 'Overview' },
        { text: 'Select Expression', link: 'Select-Expression' },
        { text: 'Query Filter', link: 'Query-Filter' },
        { text: 'Pagination', link: 'Pagination' },
        { text: 'Query Result Formats', link: 'Result-Format' },
        { text: 'Aggregate Query', link: 'Aggregate-Query' },
        { text: 'Last Query', link: 'Last-Query' },
        { text: 'Fill Null Value', link: 'Fill-Null-Value' },
        { text: 'Without Null', link: 'Without-Null' },
        { text: 'Tracing Tool', link: 'Tracing-Tool' },
      ],
    },
    {
      text: 'Process Data',
      collapsible: true,
      prefix: 'Process-Data/',
      children: [
        { text: 'UDF (User Defined Function)', link: 'UDF-User-Defined-Function' },
        { text: 'Query Write-back (SELECT INTO)', link: 'Select-Into' },
        { text: 'CQ (Continuous Query)', link: 'Continuous-Query' },
        { text: 'Triggers', link: 'Triggers' },
        { text: 'Alerting', link: 'Alerting' },
      ],
    },
    {
      text: 'Administration Management',
      collapsible: true,
      prefix: 'Administration-Management/',
      children: [
        { text: 'Administration', link: 'Administration' },
      ],
    },
    {
      text: 'Maintenance Tools',
      collapsible: true,
      prefix: 'Maintenance-Tools/',
      children: [
        { text: 'Maintenance Command', link: 'Maintenance-Command' },
        { text: 'Log Tool', link: 'Log-Tool' },
        { text: 'JMX Tool', link: 'JMX-Tool' },
        { text: 'MLogParser Tool', link: 'MLogParser-Tool' },
        { text: 'MLogLoad Tool', link: 'MLogLoad-Tool' },
        { text: 'Export Schema Tool', link: 'Export-Schema-Tool' },
        { text: 'Node Tool', link: 'NodeTool' },
        { text: 'Watermark Tool', link: 'Watermark-Tool' },
        { text: 'Metric Tool', link: 'Metric-Tool' },
        { text: 'Sync Tool', link: 'Sync-Tool' },
        { text: 'TsFile Split Tool', link: 'TsFile-Split-Tool' },
      ],
    },
    {
      text: 'Ecosystem Integration',
      collapsible: true,
      prefix: 'Ecosystem-Integration/',
      children: [
        { text: 'Grafana Plugin', link: 'Grafana-Plugin' },
        { text: 'Grafana Connector (Not Recommended)', link: 'Grafana-Connector' },
        { text: 'Zeppelin-IoTDB', link: 'Zeppelin-IoTDB' },
        { text: 'DBeaver-IoTDB', link: 'DBeaver' },
        { text: 'MapReduce TsFile', link: 'MapReduce-TsFile' },
        { text: 'Spark TsFile', link: 'Spark-TsFile' },
        { text: 'Spark IoTDB', link: 'Spark-IoTDB' },
        { text: 'Hive TsFile', link: 'Hive-TsFile' },
        { text: 'Flink IoTDB', link: 'Flink-IoTDB' },
        { text: 'Flink TsFile', link: 'Flink-TsFile' },
        { text: 'NiFi IoTDB', link: 'NiFi-IoTDB' },
      ],
    },
    {
      text: 'UDF Library',
      collapsible: true,
      prefix: 'UDF-Library/',
      children: [
        { text: 'Quick Start', link: 'Quick-Start' },
        { text: 'Data Profiling', link: 'Data-Profiling' },
        { text: 'Anomaly Detection', link: 'Anomaly-Detection' },
        { text: 'Data Matching', link: 'Data-Matching' },
        { text: 'Frequency Domain Analysis', link: 'Frequency-Domain' },
        { text: 'Data Quality', link: 'Data-Quality' },
        { text: 'Data Repairing', link: 'Data-Repairing' },
        { text: 'Series Discovery', link: 'Series-Discovery' },
        { text: 'String Processing', link: 'String-Processing' }
      ],
    },
    {
      text: 'Reference',
      collapsible: true,
      prefix: 'Reference/',
      children: [
        { text: 'Config Manual', link: 'Config-Manual' },
        { text: 'Keywords', link: 'Keywords' },
        { text: 'Frequently asked questions', link: 'Frequently-asked-questions' },
        { text: 'TSDB Comparison', link: 'TSDB-Comparison' },
      ],
    },
  ],
};
