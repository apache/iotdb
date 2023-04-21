/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const config = {
  plugins: [
    '@vuepress/back-to-top',
    ['@vuepress/plugin-html-redirect', {
      countdown: 0,
    }],
    ['sitemap', {
      hostname: 'https://iotdb.apache.org',
    }],
  ],
  head: [
    ['link', { rel: 'icon', href: '/favicon.ico' }],
    ['meta', { name: 'Description', content: 'Apache IoTDB: Time Series Database for IoT' }],
    ['meta', { name: 'Keywords', content: 'TSDB, time series, time series database, IoTDB, IoT database, IoT data management,时序数据库, 时间序列管理, IoTDB, 物联网数据库, 实时数据库, 物联网数据管理, 物联网数据' }],
    ['meta', { name: 'baidu-site-verification', content: 'wfKETzB3OT' }],
    ['meta', { name: 'google-site-verification', content: 'mZWAoRY0yj_HAr-s47zHCGHzx5Ju-RVm5wDbPnwQYFo' }],
    ['script', { async: true, src: 'https://www.googletagmanager.com/gtag/js?id=G-5MM3J6X84E' }],
    ['script', {}, `
          window.dataLayer = window.dataLayer || [];
          function gtag(){dataLayer.push(arguments);}
          gtag('js', new Date());
          gtag('config', 'G-5MM3J6X84E');`,
    ],
  ],

  // 静态网站部署的目录
  base: '',

  // 网站标题
  title: 'IoTDB Website',

  description: 'Apache IoTDB',

  markdown: {
    // 显示代码行号
    lineNumbers: true,
    toc: {
      includeLevel: [2, 2, 3, 4, 5, 6],
    },
    extractHeaders: ['h2', 'h3', 'h4', 'h5', 'h6'],
  },
  themeConfig: {

    // 项目的 github 地址
    repo: 'https://github.com/apache/iotdb.git',

    // github 地址的链接名
    repoLabel: 'GitHub',

    logo: '/img/logo.png',

    searchMaxSuggestions: 10,

    displayAllHeaders: true,

    sidebarDepth: 0,

    editLinks: true,

    docsRepo: 'apache/iotdb',

    docsDir: 'docs',

    algolia: {
      apiKey: 'f1f30c0df04d74534e066d07786bce05',
      indexName: 'iotdb-apache',
      // 如果 Algolia 没有为你提供 `appId` ，使用 `BH4D9OD16A` 或者移除该配置项
      appId: 'JLT9R2YGAE',
    },

    locales: {
      '/': {
        selectText: 'Languages',
        label: 'English',
        ariaLabel: 'Languages',
        editLinkText: 'Found Error, Edit this page on GitHub',
        serviceWorker: {
          updatePopup: {
            message: 'New content is available.',
            buttonText: 'Refresh',
          },
        },
        nav: [
          {
            text: 'Documentation',
            items: [
              { text: 'latest', link: '/UserGuide/Master/QuickStart/QuickStart' },
              { text: 'v1.0.x', link: '/UserGuide/V1.0.x/QuickStart/QuickStart' },
              { text: 'v0.13.x', link: '/UserGuide/V0.13.x/QuickStart/QuickStart' },
            ],
          },
          {
            text: 'Design',
            link: 'https://cwiki.apache.org/confluence/display/IOTDB/System+Design',
          },
          {
            text: 'Download',
            link: '/Download/',
          },
          {
            text: 'Community',
            items: [
              { text: 'About', link: '/Community/About' },
              { text: 'Wiki', link: 'https://cwiki.apache.org/confluence/display/iotdb' },
              { text: 'People', link: '/Community/Community-Project Committers' },
              { text: 'Powered By', link: '/Community/Community-Powered By' },
              { text: 'Resources', link: '/Community/Materials' },
              { text: 'Feedback', link: '/Community/Feedback' },
            ],
          },
          {
            text: 'Development',
            items: [
              { text: 'How to vote', link: '/Development/VoteRelease' },
              { text: 'How to Commit', link: '/Development/HowToCommit' },
              { text: 'Become a Contributor', link: '/Development/HowToJoin' },
              { text: 'Become a Committer', link: '/Development/Committer' },
              { text: 'ContributeGuide', link: '/Development/ContributeGuide' },
              { text: 'How to Contribute Code', link: '/Development/HowtoContributeCode' },
              { text: 'Changelist of TsFile', link: '/Development/format-changelist' },
              { text: 'Changelist of RPC', link: '/Development/rpc-changelist' },
            ],
          },
          // {
          //   text: 'Blog',
          //   items: [
          //     { text: 'Overview', link: '/Blog/Index'},
          //     { text: 'Some Notes on Release 0.9.3 and upcoming 0.10.0', link: '/Blog/Release0_93'}
          //   ]
          // },
          {
            text: 'ASF',
            items: [
              { text: 'Foundation', link: 'https://www.apache.org/' },
              { text: 'License', link: 'https://www.apache.org/licenses/' },
              { text: 'Security', link: 'https://www.apache.org/security/' },
              { text: 'Sponsorship', link: 'https://www.apache.org/foundation/sponsorship.html' },
              { text: 'Thanks', link: 'https://www.apache.org/foundation/thanks.html' },
              { text: 'Current Events', link: 'https://www.apache.org/events/current-event' },
            ],
          },
        ],
        sidebar: {
          '/UserGuide/V0.13.x/': [
            {
              title: 'IoTDB User Guide (V0.13.x)',
              collapsable: false,
            },
            {
              title: 'IoTDB Introduction',
              children: [
                ['IoTDB-Introduction/What-is-IoTDB', 'What is IoTDB'],
                ['IoTDB-Introduction/Architecture', 'Architecture'],
                ['IoTDB-Introduction/Scenario', 'Scenario'],
                ['IoTDB-Introduction/Features', 'Features'],
                ['IoTDB-Introduction/Publication', 'Publication'],
              ],
            },
            {
              title: 'Quick Start',
              children: [
                ['QuickStart/QuickStart', 'Quick Start'],
                ['QuickStart/Files', 'Data storage'],
                ['QuickStart/WayToGetIoTDB', 'Download and Setup'],
                ['QuickStart/Command-Line-Interface', 'Command Line Interface'],
              ],
            },
            {
              title: 'Data Concept',
              sidebarDepth: 1,
              children: [
                ['Data-Concept/Data-Model-and-Terminology', 'Data Model and Terminology'],
                ['Data-Concept/Schema-Template', 'Schema Template'],
                ['Data-Concept/Data-Type', 'Data Type'],
                ['Data-Concept/Deadband-Process', 'Deadband Process'],
                ['Data-Concept/Encoding', 'Encoding'],
                ['Data-Concept/Compression', 'Compression'],
                ['Data-Concept/Time-Partition', 'Time Partition'],
                ['Data-Concept/Time-zone', 'Time zone'],
              ],
            },
            {
              title: 'Syntax Conventions',
              sidebarDepth: 2,
              children: [
                ['Reference/Syntax-Conventions', 'Syntax Conventions'],
              ],
            },
            {
              title: 'API',
              children: [
                ['API/Programming-Java-Native-API', 'Java Native API'],
                ['API/Programming-Python-Native-API', 'Python Native API'],
                ['API/Programming-Cpp-Native-API', 'C++ Native API'],
                ['API/Programming-Go-Native-API', 'Go Native API'],
                ['API/Programming-JDBC', 'JDBC (Not Recommend)'],
                ['API/Programming-MQTT', 'MQTT'],
                ['API/RestService', 'REST API'],
                ['API/Programming-TsFile-API', 'TsFile API'],
                ['API/Status-Codes', 'Status Codes'],
              ],
            },
            {
              title: 'Operate Metadata',
              sidebarDepth: 1,
              children: [
                ['Operate-Metadata/Storage-Group', 'Storage Group'],
                ['Operate-Metadata/Node', 'Node'],
                ['Operate-Metadata/Timeseries', 'Timeseries'],
                ['Operate-Metadata/Template', 'Schema Template'],
                ['Operate-Metadata/TTL', 'TTL'],
                ['Operate-Metadata/Auto-Create-MetaData', 'Auto Create Metadata'],
              ],
            },
            {
              title: 'Write and Delete Data',
              sidebarDepth: 1,
              children: [
                ['Write-And-Delete-Data/Write-Data', 'Write Data'],
                ['Write-And-Delete-Data/Load-External-Tsfile', 'Load External Tsfile'],
                ['Write-And-Delete-Data/CSV-Tool', 'CSV Tool'],
                ['Write-And-Delete-Data/Delete-Data', 'Delete Data'],
              ],
            },
            {
              title: 'Query Data',
              sidebarDepth: 1,
              children: [
                ['Query-Data/Overview.md', 'Overview'],
                ['Query-Data/Select-Expression.md', 'Select Expression'],
                ['Query-Data/Query-Filter.md', 'Query Filter'],
                ['Query-Data/Pagination.md', 'Pagination'],
                ['Query-Data/Result-Format.md', 'Query Result Formats'],
                ['Query-Data/Aggregate-Query.md', 'Aggregate Query'],
                ['Query-Data/Last-Query.md', 'Last Query'],
                ['Query-Data/Fill-Null-Value.md', 'Fill Null Value'],
                ['Query-Data/Without-Null.md', 'Without Null'],
                ['Query-Data/Tracing-Tool.md', 'Tracing Tool'],
              ],
            },
            {
              title: 'Process Data',
              sidebarDepth: 1,
              children: [
                ['Process-Data/UDF-User-Defined-Function', 'UDF (User Defined Function)'],
                ['Process-Data/Select-Into', 'Query Write-back (SELECT INTO)'],
                ['Process-Data/Continuous-Query', 'CQ (Continuous Query)'],
                ['Process-Data/Triggers', 'Triggers'],
                ['Process-Data/Alerting', 'Alerting'],
              ],
            },
            {
              title: 'Administration Management',
              children: [
                ['Administration-Management/Administration', 'Administration'],
              ],
            },
            {
              title: 'Maintenance Tools',
              children: [
                ['Maintenance-Tools/Maintenance-Command', 'Maintenance Command'],
                ['Maintenance-Tools/Log-Tool', 'Log Tool'],
                ['Maintenance-Tools/JMX-Tool', 'JMX Tool'],
                ['Maintenance-Tools/MLogParser-Tool', 'MLogParser Tool'],
                ['Maintenance-Tools/MLogLoad-Tool', 'MLogLoad Tool'],
                ['Maintenance-Tools/Export-Schema-Tool', 'Export Schema Tool'],
                ['Maintenance-Tools/NodeTool', 'Node Tool'],
                ['Maintenance-Tools/Watermark-Tool', 'Watermark Tool'],
                ['Maintenance-Tools/Metric-Tool', 'Metric Tool'],
                ['Maintenance-Tools/Sync-Tool', 'Sync Tool'],
                ['Maintenance-Tools/TsFile-Split-Tool', 'TsFile Split Tool'],
              ],
            },
            {
              title: 'Ecosystem Integration',
              children: [
                ['Ecosystem Integration/Grafana Plugin', 'Grafana Plugin'],
                ['Ecosystem Integration/Grafana Connector', 'Grafana Connector (Not Recommended)'],
                ['Ecosystem Integration/Zeppelin-IoTDB', 'Zeppelin-IoTDB'],
                ['Ecosystem Integration/DBeaver', 'DBeaver-IoTDB'],
                ['Ecosystem Integration/MapReduce TsFile', 'MapReduce TsFile'],
                ['Ecosystem Integration/Spark TsFile', 'Spark TsFile'],
                ['Ecosystem Integration/Spark IoTDB', 'Spark IoTDB'],
                ['Ecosystem Integration/Hive TsFile', 'Hive TsFile'],
                ['Ecosystem Integration/Flink IoTDB', 'Flink IoTDB'],
                ['Ecosystem Integration/Flink TsFile', 'Flink TsFile'],
                ['Ecosystem Integration/NiFi-IoTDB', 'NiFi IoTDB'],
              ],
            },
            {
              title: 'UDF Library',
              sidebarDepth: 1,
              children: [
                ['UDF-Library/Quick-Start', 'Quick Start'],
                ['UDF-Library/Data-Profiling', 'Data Profiling'],
                ['UDF-Library/Anomaly-Detection', 'Anomaly Detection'],
                ['UDF-Library/Data-Matching', 'Data Matching'],
                ['UDF-Library/Frequency-Domain', 'Frequency Domain Analysis'],
                ['UDF-Library/Data-Quality', 'Data Quality'],
                ['UDF-Library/Data-Repairing', 'Data Repairing'],
                ['UDF-Library/Series-Discovery', 'Series Discovery'],
                ['UDF-Library/String-Processing', 'String Processing'],
              ],
            },
            {
              title: 'Reference',
              children: [
                ['Reference/Config-Manual', 'Config Manual'],
                ['Reference/Keywords', 'Keywords'],
                ['Reference/Frequently-asked-questions', 'Frequently asked questions'],
                ['Reference/TSDB-Comparison', 'TSDB Comparison'],
              ],
            },
          ],
          '/UserGuide/V1.0.x/': [
            {
              title: 'IoTDB User Guide (V1.0.x)',
              collapsable: false,
            },
            {
              title: 'About IoTDB',
              children: [
                ['IoTDB-Introduction/What-is-IoTDB', 'What is IoTDB'],
                ['IoTDB-Introduction/Architecture', 'Architecture'],
                ['IoTDB-Introduction/Scenario', 'Scenario'],
                ['IoTDB-Introduction/Features', 'Features'],
                ['IoTDB-Introduction/Publication', 'Publication'],
              ],
            },
            {
              title: 'Quick Start',
              children: [
                ['QuickStart/QuickStart', 'Quick Start'],
                ['QuickStart/WayToGetIoTDB', 'Download and Setup'],
                ['QuickStart/Command-Line-Interface', 'Command Line Interface'],
                ['QuickStart/Files', 'Data storage'],
              ],
            },
            {
              title: 'Data Concept',
              sidebarDepth: 1,
              children: [
                ['Data-Concept/Data-Model-and-Terminology', 'Data Model and Terminology'],
                ['Data-Concept/Schema-Template', 'Schema Template'],
                ['Data-Concept/Data-Type', 'Data Type'],
                ['Data-Concept/Deadband-Process', 'Deadband Process'],
                ['Data-Concept/Encoding', 'Encoding'],
                ['Data-Concept/Compression', 'Compression'],
                ['Data-Concept/Time-Partition', 'Time Partition of Data'],
                ['Data-Concept/Time-zone', 'Time zone'],
              ],
            },
            {
              title: 'Syntax Conventions',
              sidebarDepth: 1,
              children: [
                ['Syntax-Conventions/Literal-Values', 'Literal Values'],
                ['Syntax-Conventions/Identifier', 'Identifier'],
                ['Syntax-Conventions/NodeName-In-Path', 'NodeName in Path'],
                ['Syntax-Conventions/KeyValue-Pair', 'Key-Value Pair'],
                ['Syntax-Conventions/Keywords-And-Reserved-Words', 'Keywords'],
                ['Syntax-Conventions/Session-And-TsFile-API', 'Session And TsFile API'],
                ['Syntax-Conventions/Detailed-Grammar', 'Detailed Definitions of Lexical and Grammar'],
              ],
            },
            {
              title: 'API',
              children: [
                ['API/Programming-Java-Native-API', 'Java Native API'],
                ['API/Programming-Python-Native-API', 'Python Native API'],
                ['API/Programming-Cpp-Native-API', 'C++ Native API'],
                ['API/Programming-Go-Native-API', 'Go Native API'],
                ['API/Programming-JDBC', 'JDBC (Not Recommend)'],
                ['API/Programming-MQTT', 'MQTT'],
                ['API/RestService', 'REST API'],
                ['API/Programming-TsFile-API', 'TsFile API'],
                ['API/InfluxDB-Protocol', 'InfluxDB Protocol'],
                ['API/Interface-Comparison', 'Interface Comparison'],
              ],
            },
            {
              title: 'Operate Metadata',
              sidebarDepth: 1,
              children: [
                ['Operate-Metadata/Database', 'Database'],
                ['Operate-Metadata/Node', 'Node'],
                ['Operate-Metadata/Timeseries', 'Timeseries'],
                ['Operate-Metadata/Template', 'Schema Template'],
                ['Operate-Metadata/Auto-Create-MetaData', 'Auto Create Metadata'],
              ],
            },
            {
              title: 'Write Data (Update Data)',
              sidebarDepth: 1,
              children: [
                ['Write-Data/Write-Data', 'CLI Write'],
                ['Write-Data/Session', 'Native API Write'],
                ['Write-Data/REST-API', 'REST API'],
                ['Write-Data/MQTT', 'MQTT Write'],
                ['Write-Data/Batch-Load-Tool', 'Batch Data Load'],
              ],
            },
            {
              title: 'Delete Data',
              sidebarDepth: 1,
              children: [
                ['Delete-Data/Delete-Data', 'Delete Data'],
                ['Delete-Data/TTL', 'TTL'],
              ],
            },
            {
              title: 'Query Data',
              sidebarDepth: 1,
              children: [
                ['Query-Data/Overview', 'Overview'],
                ['Query-Data/Select-Expression', 'Select Expression'],
                ['Query-Data/Last-Query', 'Last Query'],
                ['Query-Data/Align-By', 'Query Alignment Mode'],
                ['Query-Data/Where-Condition', 'Where Condition'],
                ['Query-Data/Group-By', 'Group By'],
                ['Query-Data/Having-Condition', 'Having Condition'],
                // ['Query-Data/Order-By','Order By'],
                ['Query-Data/Fill', 'Fill Null Value'],
                ['Query-Data/Pagination', 'Pagination'],
                ['Query-Data/Select-Into', 'Select Into'],
                ['Query-Data/Continuous-Query', 'Continuous Query'],
              ],
            },
            {
              title: 'Operators and Functions',
              sidebarDepth: 1,
              children: [
                ['Operators-Functions/Overview', 'Overview'],
                ['Operators-Functions/User-Defined-Function', 'UDF (User Defined Function)'],
                ['Operators-Functions/Aggregation', 'Aggregation'],
                ['Operators-Functions/Mathematical', 'Mathematical'],
                ['Operators-Functions/Comparison', 'Comparison'],
                ['Operators-Functions/Logical', 'Logical'],
                ['Operators-Functions/String', 'Conversion'],
                ['Operators-Functions/Conversion', 'Conversion'],
                ['Operators-Functions/Constant', 'Constant'],
                ['Operators-Functions/Selection', 'Selection'],
                ['Operators-Functions/Continuous-Interval', 'Continuous Interval'],
                ['Operators-Functions/Variation-Trend', 'Variation Trend'],
                ['Operators-Functions/Sample', 'Sample'],
                ['Operators-Functions/Time-Series', 'Time-Series'],
                ['Operators-Functions/Lambda', 'Lambda'],

                // IoTDB-Quality
                ['Operators-Functions/Data-Profiling', 'Data Profiling'],
                ['Operators-Functions/Anomaly-Detection', 'Anomaly Detection'],
                ['Operators-Functions/Data-Matching', 'Data Matching'],
                ['Operators-Functions/Frequency-Domain', 'Frequency Domain'],
                ['Operators-Functions/Data-Quality', 'Data Quality'],
                ['Operators-Functions/Data-Repairing', 'Data Repairing'],
                ['Operators-Functions/Series-Discovery', 'Series Discovery'],
              ],
            },
            {
              title: 'Trigger',
              sidebarDepth: 1,
              children: [
                ['Trigger/Instructions', 'Instructions'],
                ['Trigger/Implement-Trigger', 'How to implement a trigger'],
                ['Trigger/Trigger-Management', 'Trigger Management'],
                ['Trigger/Notes', 'Notes'],
                ['Trigger/Configuration-Parameters', 'Configuration-Parameters'],
              ],
            },
            {
              title: 'Monitor and Alert',
              sidebarDepth: 1,
              children: [
                ['Monitor-Alert/Metric-Tool', 'Metric Tool'],
                ['Monitor-Alert/Alerting', 'Alerting'],
              ],
            },
            {
              title: 'Administration Management',
              children: [
                ['Administration-Management/Administration', 'Administration'],
              ],
            },
            {
              title: 'Maintenance Tools',
              children: [
                ['Maintenance-Tools/Maintenance-Command', 'Maintenance Command'],
                ['Maintenance-Tools/Log-Tool', 'Log Tool'],
                ['Maintenance-Tools/JMX-Tool', 'JMX Tool'],
                ['Maintenance-Tools/MLogParser-Tool', 'MLogParser Tool'],
                ['Maintenance-Tools/IoTDB-Data-Dir-Overview-Tool', 'IoTDB Data Directory Overview Tool'],
                ['Maintenance-Tools/TsFile-Sketch-Tool', 'TsFile Sketch Tool'],
                ['Maintenance-Tools/TsFile-Resource-Sketch-Tool', 'TsFile Resource Sketch Tool'],
                ['Maintenance-Tools/TsFile-Split-Tool', 'TsFile Split Tool'],
                ['Maintenance-Tools/TsFile-Load-Export-Tool', 'TsFile Load Export Tool'],
                ['Maintenance-Tools/CSV-Tool', 'CSV Load Export Tool'],
              ],
            },
            {
              title: 'Collaboration of Edge and Cloud',
              children: [
                ['Edge-Cloud-Collaboration/Sync-Tool', 'TsFile Sync Tool'],
              ],
            },
            {
              title: 'Ecosystem Integration',
              children: [
                ['Ecosystem-Integration/Grafana-Plugin', 'Grafana-Plugin'],
                ['Ecosystem-Integration/Grafana-Connector', 'Grafana-Connector (Not Recommended)'],
                ['Ecosystem-Integration/Zeppelin-IoTDB', 'Zeppelin-IoTDB'],
                ['Ecosystem-Integration/DBeaver', 'DBeaver-IoTDB'],
                ['Ecosystem-Integration/MapReduce-TsFile', 'MapReduce-TsFile'],
                ['Ecosystem-Integration/Spark-TsFile', 'Spark-TsFile'],
                ['Ecosystem-Integration/Spark-IoTDB', 'Spark-IoTDB'],
                ['Ecosystem-Integration/Hive-TsFile', 'Hive-TsFile'],
                ['Ecosystem-Integration/Flink-IoTDB', 'Flink-IoTDB'],
                ['Ecosystem-Integration/Flink-TsFile', 'Flink-TsFile'],
                ['Ecosystem-Integration/NiFi-IoTDB', 'NiFi-IoTDB'],
              ],
            },
            {
              title: 'Cluster',
              children: [
                ['Cluster/Cluster-Concept', 'Cluster Concept'],
                ['Cluster/Cluster-Setup', 'Cluster Setup'],
                ['Cluster/Cluster-Maintenance', 'Cluster Maintenance'],
                ['Cluster/Deployment-Recommendation', 'Deployment Recommendation'],
              ],
            },
            {
              title: 'FAQ',
              children: [
                ['FAQ/Frequently-asked-questions', 'Frequently asked questions'],
                ['FAQ/FAQ-for-cluster-setup', 'FAQ for cluster setup'],
              ],
            },
            {
              title: 'Reference',
              children: [
                ['Reference/Common-Config-Manual', 'Common Config Manual'],
                ['Reference/ConfigNode-Config-Manual', 'ConfigNode Config Manual'],
                ['Reference/DataNode-Config-Manual', 'DataNode Config Manual'],
                ['Reference/SQL-Reference','SQL Reference'],
                ['Reference/Status-Codes', 'Status Codes'],
                ['Reference/Keywords', 'Keywords'],
                ['Reference/TSDB-Comparison', 'TSDB Comparison'],
              ],
            },
          ],
          '/UserGuide/Master/': [
            {
              title: 'IoTDB User Guide (latest)',
              collapsable: false,
            },
            {
              title: 'About IoTDB',
              children: [
                ['IoTDB-Introduction/What-is-IoTDB', 'What is IoTDB'],
                ['IoTDB-Introduction/Architecture', 'Architecture'],
                ['IoTDB-Introduction/Scenario', 'Scenario'],
                ['IoTDB-Introduction/Features', 'Features'],
                ['IoTDB-Introduction/Publication', 'Publication'],
              ],
            },
            {
              title: 'Quick Start',
              children: [
                ['QuickStart/QuickStart', 'Quick Start'],
                ['QuickStart/ClusterQuickStart', 'Cluster Quick Start'],
                ['QuickStart/WayToGetIoTDB', 'Download and Setup'],
                ['QuickStart/Command-Line-Interface', 'Command Line Interface'],
                ['QuickStart/Files', 'Data storage'],
              ],
            },
            {
              title: 'Data Concept',
              sidebarDepth: 1,
              children: [
                ['Data-Concept/Data-Model-and-Terminology', 'Data Model and Terminology'],
                ['Data-Concept/Schema-Template', 'Schema Template'],
                ['Data-Concept/Data-Type', 'Data Type'],
                ['Data-Concept/Deadband-Process', 'Deadband Process'],
                ['Data-Concept/Encoding', 'Encoding'],
                ['Data-Concept/Compression', 'Compression'],
                ['Data-Concept/Time-Partition', 'Time Partition of Data'],
                ['Data-Concept/Time-zone', 'Time zone'],
              ],
            },
            {
              title: 'Syntax Conventions',
              sidebarDepth: 1,
              children: [
                ['Syntax-Conventions/Literal-Values', 'Literal Values'],
                ['Syntax-Conventions/Identifier', 'Identifier'],
                ['Syntax-Conventions/NodeName-In-Path', 'NodeName in Path'],
                ['Syntax-Conventions/KeyValue-Pair', 'Key-Value Pair'],
                ['Syntax-Conventions/Keywords-And-Reserved-Words', 'Keywords'],
                ['Syntax-Conventions/Session-And-TsFile-API', 'Session And TsFile API'],
                ['Syntax-Conventions/Detailed-Grammar', 'Detailed Definitions of Lexical and Grammar'],
              ],
            },
            {
              title: 'API',
              children: [
                ['API/Programming-Java-Native-API', 'Java Native API'],
                ['API/Programming-Python-Native-API', 'Python Native API'],
                ['API/Programming-Cpp-Native-API', 'C++ Native API'],
                ['API/Programming-Go-Native-API', 'Go Native API'],
                ['API/Programming-JDBC', 'JDBC (Not Recommend)'],
                ['API/Programming-MQTT', 'MQTT'],
                ['API/RestServiceV1', 'REST API V1 (Not Recommend)'],
                ['API/RestServiceV2', 'REST API V2'],
                ['API/Programming-TsFile-API', 'TsFile API'],
                ['API/InfluxDB-Protocol', 'InfluxDB Protocol'],
                ['API/Interface-Comparison', 'Interface Comparison'],
              ],
            },
            {
              title: 'Operate Metadata',
              sidebarDepth: 1,
              children: [
                ['Operate-Metadata/Database', 'Database'],
                ['Operate-Metadata/Node', 'Node'],
                ['Operate-Metadata/Timeseries', 'Timeseries'],
                ['Operate-Metadata/Template', 'Schema Template'],
                ['Operate-Metadata/Auto-Create-MetaData', 'Auto Create Metadata'],
              ],
            },
            {
              title: 'Write Data (Update Data)',
              sidebarDepth: 1,
              children: [
                ['Write-Data/Write-Data', 'CLI Write'],
                ['Write-Data/Session', 'Native API Write'],
                ['Write-Data/REST-API', 'REST API'],
                ['Write-Data/MQTT', 'MQTT Write'],
                ['Write-Data/Batch-Load-Tool', 'Batch Data Load'],
              ],
            },
            {
              title: 'Delete Data',
              sidebarDepth: 1,
              children: [
                ['Delete-Data/Delete-Data', 'Delete Data'],
                ['Delete-Data/TTL', 'TTL'],
              ],
            },
            {
              title: 'Query Data',
              sidebarDepth: 1,
              children: [
                ['Query-Data/Overview', 'Overview'],
                ['Query-Data/Select-Expression', 'Select Expression'],
                ['Query-Data/Last-Query', 'Last Query'],
                ['Query-Data/Align-By', 'Query Alignment Mode'],
                ['Query-Data/Where-Condition', 'Where Condition'],
                ['Query-Data/Group-By', 'Group By'],
                ['Query-Data/Having-Condition', 'Having Condition'],
                // ['Query-Data/Order-By','Order By'],
                ['Query-Data/Fill', 'Fill Null Value'],
                ['Query-Data/Pagination', 'Pagination'],
                ['Query-Data/Select-Into', 'Select Into'],
                ['Query-Data/Continuous-Query', 'Continuous Query'],
              ],
            },
            {
              title: 'Operators and Functions',
              sidebarDepth: 1,
              children: [
                ['Operators-Functions/Overview', 'Overview'],
                ['Operators-Functions/User-Defined-Function', 'UDF (User Defined Function)'],
                ['Operators-Functions/Aggregation', 'Aggregation'],
                ['Operators-Functions/Mathematical', 'Mathematical'],
                ['Operators-Functions/Comparison', 'Comparison'],
                ['Operators-Functions/Logical', 'Logical'],
                ['Operators-Functions/String', 'Conversion'],
                ['Operators-Functions/Conversion', 'Conversion'],
                ['Operators-Functions/Constant', 'Constant'],
                ['Operators-Functions/Selection', 'Selection'],
                ['Operators-Functions/Continuous-Interval', 'Continuous Interval'],
                ['Operators-Functions/Variation-Trend', 'Variation Trend'],
                ['Operators-Functions/Sample', 'Sample'],
                ['Operators-Functions/Time-Series', 'Time-Series'],
                ['Operators-Functions/Lambda', 'Lambda'],

                // IoTDB-Quality
                ['Operators-Functions/Data-Profiling', 'Data Profiling'],
                ['Operators-Functions/Anomaly-Detection', 'Anomaly Detection'],
                ['Operators-Functions/Data-Matching', 'Data Matching'],
                ['Operators-Functions/Frequency-Domain', 'Frequency Domain'],
                ['Operators-Functions/Data-Quality', 'Data Quality'],
                ['Operators-Functions/Data-Repairing', 'Data Repairing'],
                ['Operators-Functions/Series-Discovery', 'Series Discovery'],
                ['Operators-Functions/Machine-Learning', 'Machine Learning'],
              ],
            },
            {
              title: 'Trigger',
              sidebarDepth: 1,
              children: [
                ['Trigger/Instructions', 'Instructions'],
                ['Trigger/Implement-Trigger', 'How to implement a trigger'],
                ['Trigger/Trigger-Management', 'Trigger Management'],
                ['Trigger/Notes', 'Notes'],
                ['Trigger/Configuration-Parameters', 'Configuration-Parameters'],
              ],
            },
            {
              title: 'Monitor and Alert',
              sidebarDepth: 1,
              children: [
                ['Monitor-Alert/Metric-Tool', 'Metric Tool'],
                ['Monitor-Alert/Alerting', 'Alerting'],
              ],
            },
            {
              title: 'Administration Management',
              children: [
                ['Administration-Management/Administration', 'Administration'],
              ],
            },
            {
              title: 'Maintenance Tools',
              children: [
                ['Maintenance-Tools/Maintenance-Command', 'Maintenance Command'],
                ['Maintenance-Tools/Log-Tool', 'Log Tool'],
                ['Maintenance-Tools/JMX-Tool', 'JMX Tool'],
                ['Maintenance-Tools/MLogParser-Tool', 'MLogParser Tool'],
                ['Maintenance-Tools/IoTDB-Data-Dir-Overview-Tool', 'IoTDB Data Directory Overview Tool'],
                ['Maintenance-Tools/TsFile-Sketch-Tool', 'TsFile Sketch Tool'],
                ['Maintenance-Tools/TsFile-Resource-Sketch-Tool', 'TsFile Resource Sketch Tool'],
                ['Maintenance-Tools/TsFile-Split-Tool', 'TsFile Split Tool'],
                ['Maintenance-Tools/TsFile-Load-Export-Tool', 'TsFile Load Export Tool'],
                ['Maintenance-Tools/CSV-Tool', 'CSV Load Export Tool'],
              ],
            },
            {
              title: 'Collaboration of Edge and Cloud',
              children: [
                ['Edge-Cloud-Collaboration/Sync-Tool', 'TsFile Sync Tool'],
              ],
            },
            {
              title: 'Ecosystem Integration',
              children: [
                ['Ecosystem-Integration/Grafana-Plugin', 'Grafana-Plugin'],
                ['Ecosystem-Integration/Grafana-Connector', 'Grafana-Connector (Not Recommended)'],
                ['Ecosystem-Integration/Zeppelin-IoTDB', 'Zeppelin-IoTDB'],
                ['Ecosystem-Integration/DBeaver', 'DBeaver-IoTDB'],
                ['Ecosystem-Integration/MapReduce-TsFile', 'MapReduce-TsFile'],
                ['Ecosystem-Integration/Spark-TsFile', 'Spark-TsFile'],
                ['Ecosystem-Integration/Spark-IoTDB', 'Spark-IoTDB'],
                ['Ecosystem-Integration/Hive-TsFile', 'Hive-TsFile'],
                ['Ecosystem-Integration/Flink-IoTDB', 'Flink-IoTDB'],
                ['Ecosystem-Integration/Flink-TsFile', 'Flink-TsFile'],
                ['Ecosystem-Integration/NiFi-IoTDB', 'NiFi-IoTDB'],
              ],
            },
            {
              title: 'Cluster',
              children: [
                ['Cluster/Cluster-Concept', 'Cluster Concept'],
                ['Cluster/Cluster-Setup', 'Cluster Setup'],
                ['Cluster/Cluster-Maintenance', 'Cluster Maintenance'],
                ['Cluster/Deployment-Recommendation', 'Deployment Recommendation'],
              ],
            },
            {
              title: 'FAQ',
              children: [
                ['FAQ/Frequently-asked-questions', 'Frequently asked questions'],
                ['FAQ/FAQ-for-cluster-setup', 'FAQ for cluster setup'],
              ],
            },
            {
              title: 'Reference',
              children: [
                ['Reference/Common-Config-Manual', 'Common Config Manual'],
                ['Reference/ConfigNode-Config-Manual', 'ConfigNode Config Manual'],
                ['Reference/DataNode-Config-Manual', 'DataNode Config Manual'],
                ['Reference/SQL-Reference','SQL Reference'],
                ['Reference/Status-Codes', 'Status Codes'],
                ['Reference/Keywords', 'Keywords'],
                ['Reference/TSDB-Comparison', 'TSDB Comparison'],
              ],
            },
          ],
        },
      },
      '/zh/': {
        // 多语言下拉菜单的标题
        selectText: '语言',
        // 该语言在下拉菜单中的标签
        label: '简体中文',
        // 编辑链接文字
        editLinkText: '发现错误？在 GitHub 上编辑此页',
        // Service Worker 的配置
        serviceWorker: {
          updatePopup: {
            message: '发现新内容可用.',
            buttonText: '刷新',
          },
        },
        nav: [
          {
            text: '文档',
            items: [
              { text: 'latest', link: '/zh/UserGuide/Master/QuickStart/QuickStart' },
              { text: 'v1.0.x', link: '/zh/UserGuide/V1.0.x/QuickStart/QuickStart' },
              { text: 'v0.13.x', link: '/zh/UserGuide/V0.13.x/QuickStart/QuickStart' },
            ],
          },
          {
            text: '系统设计',
            link: 'https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=177051872',
          },
          {
            text: '下载',
            link: '/zh/Download/',
          },
          {
            text: '社区',
            items: [
              { text: '关于社区', link: '/zh/Community/About' },
              { text: 'Wiki', link: 'https://cwiki.apache.org/confluence/display/iotdb' },
              { text: '开发人员', link: '/zh/Community/Community-Project Committers' },
              { text: '技术支持', link: '/zh/Community/Community-Powered By' },
              { text: '活动与报告', link: '/Community/Materials' },
              { text: '交流与反馈', link: '/zh/Community/Feedback' },
            ],
          },
          {
            text: '开发',
            items: [
              { text: '如何投票', link: '/zh/Development/VoteRelease' },
              { text: '如何提交代码', link: '/zh/Development/HowToCommit' },
              { text: '成为Contributor', link: '/zh/Development/HowToJoin' },
              { text: '成为Committer', link: '/zh/Development/Committer' },
              { text: '项目开发指南', link: '/zh/Development/ContributeGuide' },
              { text: '技术贡献指南', link: '/zh/Development/HowtoContributeCode' },
              { text: 'TsFile的更改列表', link: '/zh/Development/format-changelist' },
              { text: 'RPC变更清单', link: '/zh/Development/rpc-changelist' },
            ],
          },
          {
            text: 'ASF',
            items: [
              { text: '基金会', link: 'https://www.apache.org/' },
              { text: '许可证', link: 'https://www.apache.org/licenses/' },
              { text: '安全', link: 'https://www.apache.org/security/' },
              { text: '赞助', link: 'https://www.apache.org/foundation/sponsorship.html' },
              { text: '致谢', link: 'https://www.apache.org/foundation/thanks.html' },
              { text: '活动', link: 'https://www.apache.org/events/current-event' },
            ],
          },
        ],
        sidebar: {
          '/zh/UserGuide/V0.13.x/': [
            {
              title: 'IoTDB用户手册 (V0.13.x)',
              collapsable: false,
            },
            {
              title: 'IoTDB简介',
              children: [
                ['IoTDB-Introduction/What-is-IoTDB', 'IoTDB简介'],
                ['IoTDB-Introduction/Features', '主要功能特点'],
                ['IoTDB-Introduction/Architecture', '系统架构'],
                ['IoTDB-Introduction/Scenario', '应用场景'],
                ['IoTDB-Introduction/Publication', '研究论文'],
              ],
            },
            {
              title: '快速上手',
              children: [
                ['QuickStart/QuickStart', '快速上手'],
                ['QuickStart/Files', '数据文件存储'],
                ['QuickStart/WayToGetIoTDB', '下载与安装'],
                ['QuickStart/Command-Line-Interface', 'SQL命令行终端(CLI)'],
              ],
            },
            {
              title: '数据模式与概念',
              sidebarDepth: 1,
              children: [
                ['Data-Concept/Data-Model-and-Terminology', '数据模型'],
                ['Data-Concept/Schema-Template', '元数据模板'],
                ['Data-Concept/Data-Type', '数据类型'],
                ['Data-Concept/Deadband-Process', '死区处理'],
                ['Data-Concept/Encoding', '编码方式'],
                ['Data-Concept/Compression', '压缩方式'],
                ['Data-Concept/Time-Partition', '时间分区'],
                ['Data-Concept/Time-zone', '时区'],
              ],
            },
            {
              title: '语法约定',
              sidebarDepth: 1,
              children: [
                ['Reference/Syntax-Conventions', '语法约定'],
              ],
            },
            {
              title: '应用编程接口',
              children: [
                ['API/Programming-Java-Native-API', 'Java 原生接口'],
                ['API/Programming-Python-Native-API', 'Python 原生接口'],
                ['API/Programming-Cpp-Native-API', 'C++ 原生接口'],
                ['API/Programming-Go-Native-API', 'Go 原生接口'],
                ['API/Programming-JDBC', 'JDBC (不推荐)'],
                ['API/Programming-MQTT', 'MQTT'],
                ['API/RestServiceV1', 'REST API V1 (不推荐)'],
                ['API/RestServiceV2', 'REST API V2'],
                ['API/Programming-TsFile-API', 'TsFile API'],
                ['API/Status-Codes', '状态码'],
              ],
            },
            {
              title: '元数据操作',
              sidebarDepth: 1,
              children: [
                ['Operate-Metadata/Storage-Group', '存储组操作'],
                ['Operate-Metadata/Node', '节点操作'],
                ['Operate-Metadata/Timeseries', '时间序列操作'],
                ['Operate-Metadata/Template', '元数据模板'],
                ['Operate-Metadata/TTL', 'TTL'],
                ['Operate-Metadata/Auto-Create-MetaData', '自动创建元数据'],
              ],
            },
            {
              title: '数据写入和删除',
              sidebarDepth: 1,
              children: [
                ['Write-And-Delete-Data/Write-Data', '写入数据'],
                ['Write-And-Delete-Data/Load-External-Tsfile', '加载 TsFile'],
                ['Write-And-Delete-Data/CSV-Tool', '导入导出 CSV'],
                ['Write-And-Delete-Data/Delete-Data', '删除数据'],
              ],
            },
            {
              title: '数据查询',
              sidebarDepth: 1,
              children: [
                ['Query-Data/Overview.md', '概述'],
                ['Query-Data/Select-Expression.md', '选择表达式'],
                ['Query-Data/Query-Filter.md', '查询过滤条件'],
                ['Query-Data/Pagination.md', '查询结果分页'],
                ['Query-Data/Result-Format.md', '查询结果对齐格式'],
                ['Query-Data/Aggregate-Query.md', '聚合查询'],
                ['Query-Data/Last-Query.md', '最新点查询'],
                ['Query-Data/Fill-Null-Value.md', '空值填充'],
                ['Query-Data/Without-Null.md', '空值过滤'],
                ['Query-Data/Tracing-Tool.md', '查询性能追踪'],
              ],
            },
            {
              title: '数据处理',
              sidebarDepth: 1,
              children: [
                ['Process-Data/UDF-User-Defined-Function', '用户定义函数(UDF)'],
                ['Process-Data/Select-Into', '查询写回(SELECT INTO)'],
                ['Process-Data/Continuous-Query', '连续查询(CQ)'],
                ['Process-Data/Triggers', '触发器'],
                ['Process-Data/Alerting', '告警机制'],
              ],
            },
            {
              title: '权限管理',
              children: [
                ['Administration-Management/Administration', '权限管理'],
              ],
            },
            {
              title: '运维工具',
              children: [
                ['Maintenance-Tools/Maintenance-Command', '运维命令'],
                ['Maintenance-Tools/Log-Tool', '日志工具'],
                ['Maintenance-Tools/JMX-Tool', 'JMX 工具'],
                ['Maintenance-Tools/MLogParser-Tool', 'MLog 解析工具'],
                ['Maintenance-Tools/MLogLoad-Tool', 'MLog 加载工具'],
                ['Maintenance-Tools/Export-Schema-Tool', '元数据导出工具'],
                ['Maintenance-Tools/NodeTool', '节点工具'],
                ['Maintenance-Tools/Watermark-Tool', '水印工具'],
                ['Maintenance-Tools/Metric-Tool', '监控工具'],
                ['Maintenance-Tools/Sync-Tool', 'TsFile 同步工具'],
                ['Maintenance-Tools/TsFile-Split-Tool', 'TsFile 拆分工具'],
              ],
            },
            {
              title: '系统集成',
              children: [
                ['Ecosystem Integration/Grafana Plugin', 'Grafana Plugin'],
                ['Ecosystem Integration/Grafana Connector', 'Grafana Connector（不推荐）'],
                ['Ecosystem Integration/Zeppelin-IoTDB', 'Zeppelin-IoTDB'],
                ['Ecosystem Integration/DBeaver', 'DBeaver-IoTDB'],
                ['Ecosystem Integration/Spark TsFile', 'Spark TsFile'],
                ['Ecosystem Integration/MapReduce TsFile', 'Hadoop-TsFile'],
                ['Ecosystem Integration/Spark IoTDB', 'Spark-IoTDB'],
                ['Ecosystem Integration/Hive TsFile', 'Hive-TsFile'],
                ['Ecosystem Integration/Flink TsFile', 'Flink-TsFile'],
                ['Ecosystem Integration/Flink IoTDB', 'Flink-IoTDB'],
                ['Ecosystem Integration/NiFi-IoTDB', 'NiFi IoTDB'],
              ],
            },
            {
              title: 'UDF 资料库',
              sidebarDepth: 1,
              children: [
                ['UDF-Library/Quick-Start', '快速开始'],
                ['UDF-Library/Data-Profiling', '数据画像'],
                ['UDF-Library/Anomaly-Detection', '异常检测'],
                ['UDF-Library/Data-Matching', '数据匹配'],
                ['UDF-Library/Frequency-Domain', '频域分析'],
                ['UDF-Library/Data-Quality', '数据质量'],
                ['UDF-Library/Data-Repairing', '数据修复'],
                ['UDF-Library/Series-Discovery', '序列发现'],
                ['UDF-Library/String-Processing', '字符串处理'],
              ],
            },
            {
              title: '参考',
              children: [
                ['Reference/Config-Manual', '配置参数'],
                ['Reference/Keywords', '关键字'],
                ['Reference/Frequently-asked-questions', '常见问题'],
                ['Reference/TSDB-Comparison', '时间序列数据库比较'],
              ],
            },
          ],
          '/zh/UserGuide/V1.0.x/': [
            {
              title: 'IoTDB用户手册 (V1.0.x)',
              collapsable: false,
            },
            {
              title: '关于IoTDB',
              children: [
                ['IoTDB-Introduction/What-is-IoTDB', 'IoTDB简介'],
                ['IoTDB-Introduction/Features', '主要功能特点'],
                ['IoTDB-Introduction/Architecture', '系统架构'],
                ['IoTDB-Introduction/Scenario', '应用场景'],
                ['IoTDB-Introduction/Publication', '研究论文'],
              ],
            },
            {
              title: '快速上手',
              children: [
                ['QuickStart/QuickStart', '快速上手'],
                ['QuickStart/WayToGetIoTDB', '下载与安装'],
                ['QuickStart/Command-Line-Interface', 'SQL命令行终端(CLI)'],
                ['QuickStart/Files', '数据文件存储'],
              ],
            },
            {
              title: '数据模式与概念',
              sidebarDepth: 1,
              children: [
                ['Data-Concept/Data-Model-and-Terminology', '数据模型'],
                ['Data-Concept/Schema-Template', '元数据模板'],
                ['Data-Concept/Data-Type', '数据类型'],
                ['Data-Concept/Deadband-Process', '死区处理'],
                ['Data-Concept/Encoding', '编码方式'],
                ['Data-Concept/Compression', '压缩方式'],
                ['Data-Concept/Time-Partition', '数据的时间分区'],
                ['Data-Concept/Time-zone', '时区'],
              ],
            },
            {
              title: '语法约定',
              sidebarDepth: 1,
              children: [
                ['Syntax-Conventions/Literal-Values', '字面值常量'],
                ['Syntax-Conventions/Identifier', '标识符'],
                ['Syntax-Conventions/NodeName-In-Path', '路径结点名'],
                ['Syntax-Conventions/KeyValue-Pair', '键值对'],
                ['Syntax-Conventions/Keywords-And-Reserved-Words', '关键字'],
                ['Syntax-Conventions/Session-And-TsFile-API', 'Session And TsFile API'],
                ['Syntax-Conventions/Detailed-Grammar', '词法和文法详细定义'],
              ],
            },
            {
              title: '应用编程接口',
              children: [
                ['API/Programming-Java-Native-API', 'Java 原生接口'],
                ['API/Programming-Python-Native-API', 'Python 原生接口'],
                ['API/Programming-Cpp-Native-API', 'C++ 原生接口'],
                ['API/Programming-Go-Native-API', 'Go 原生接口'],
                ['API/Programming-JDBC', 'JDBC (不推荐)'],
                ['API/Programming-MQTT', 'MQTT'],
                ['API/RestService', 'REST API'],
                ['API/Programming-TsFile-API', 'TsFile API'],
                ['API/InfluxDB-Protocol', 'InfluxDB 协议适配器'],
                ['API/Interface-Comparison', '原生接口对比'],
              ],
            },
            {
              title: '元数据操作',
              sidebarDepth: 1,
              children: [
                ['Operate-Metadata/Database', '数据库操作'],
                ['Operate-Metadata/Node', '节点操作'],
                ['Operate-Metadata/Timeseries', '时间序列操作'],
                ['Operate-Metadata/Template', '元数据模板'],
                ['Operate-Metadata/Auto-Create-MetaData', '自动创建元数据'],
              ],
            },
            {
              title: '数据写入（数据更新）',
              sidebarDepth: 1,
              children: [
                ['Write-Data/Write-Data', 'CLI 工具写入'],
                ['Write-Data/Session', '原生接口写入'],
                ['Write-Data/REST-API', 'REST 服务'],
                ['Write-Data/MQTT', 'MQTT写入'],
                ['Write-Data/Batch-Load-Tool', '批量数据导入'],
              ],
            },
            {
              title: '数据删除',
              sidebarDepth: 1,
              children: [
                ['Delete-Data/Delete-Data', '删除数据'],
                ['Delete-Data/TTL', 'TTL'],
              ],
            },
            {
              title: '数据查询',
              sidebarDepth: 1,
              children: [
                ['Query-Data/Overview', '概述'],
                ['Query-Data/Select-Expression', '选择表达式'],
                ['Query-Data/Last-Query', '最新点查询'],
                ['Query-Data/Align-By', '查询对齐模式'],
                ['Query-Data/Where-Condition', '查询过滤条件'],
                ['Query-Data/Group-By', '分段分组聚合'],
                ['Query-Data/Having-Condition', '聚合结果过滤'],
                // ['Query-Data/Order-By','结果集排序'],
                ['Query-Data/Fill', '结果集补空值'],
                ['Query-Data/Pagination', '结果集分页'],
                ['Query-Data/Select-Into', '查询写回'],
                ['Query-Data/Continuous-Query', '连续查询'],
              ],
            },
            {
              title: '运算符和函数',
              sidebarDepth: 1,
              children: [
                ['Operators-Functions/Overview', '概述'],
                ['Operators-Functions/User-Defined-Function', '用户自定义函数'],
                ['Operators-Functions/Aggregation', '聚合函数'],
                ['Operators-Functions/Mathematical', '算数运算符和函数'],
                ['Operators-Functions/Comparison', '比较运算符和函数'],
                ['Operators-Functions/Logical', '逻辑运算符'],
                ['Operators-Functions/String', '字符串处理'],
                ['Operators-Functions/Conversion', '数据类型转换'],
                ['Operators-Functions/Constant', '常序列生成'],
                ['Operators-Functions/Selection', '选择函数'],
                ['Operators-Functions/Continuous-Interval', '区间查询'],
                ['Operators-Functions/Variation-Trend', '趋势计算'],
                ['Operators-Functions/Sample', '采样函数'],
                ['Operators-Functions/Time-Series', '时间序列处理'],
                ['Operators-Functions/Lambda', 'Lambda 表达式'],

                // IoTDB-Quality
                ['Operators-Functions/Data-Profiling', '数据画像'],
                ['Operators-Functions/Anomaly-Detection', '异常检测'],
                ['Operators-Functions/Data-Matching', '数据匹配'],
                ['Operators-Functions/Frequency-Domain', '频域分析'],
                ['Operators-Functions/Data-Quality', '数据质量'],
                ['Operators-Functions/Data-Repairing', '数据修复'],
                ['Operators-Functions/Series-Discovery', '序列发现'],
              ],
            },
            {
              title: '触发器',
              sidebarDepth: 1,
              children: [
                ['Trigger/Instructions', '使用说明'],
                ['Trigger/Implement-Trigger', '编写触发器'],
                ['Trigger/Trigger-Management', '管理触发器'],
                ['Trigger/Notes', '重要注意事项'],
                ['Trigger/Configuration-Parameters', '配置参数'],
              ],
            },
            {
              title: '监控告警',
              sidebarDepth: 1,
              children: [
                ['Monitor-Alert/Metric-Tool', '监控工具'],
                ['Monitor-Alert/Alerting', '告警机制'],
              ],
            },
            {
              title: '权限管理',
              children: [
                ['Administration-Management/Administration', '权限管理'],
              ],
            },
            {
              title: '运维工具',
              children: [
                ['Maintenance-Tools/Maintenance-Command', '运维命令'],
                ['Maintenance-Tools/Log-Tool', '日志工具'],
                ['Maintenance-Tools/JMX-Tool', 'JMX 工具'],
                ['Maintenance-Tools/MLogParser-Tool', 'Mlog解析工具'],
                ['Maintenance-Tools/IoTDB-Data-Dir-Overview-Tool', 'IoTDB数据文件夹概览工具'],
                ['Maintenance-Tools/TsFile-Sketch-Tool', 'TsFile概览工具'],
                ['Maintenance-Tools/TsFile-Resource-Sketch-Tool', 'TsFile Resource概览工具'],
                ['Maintenance-Tools/TsFile-Split-Tool', 'TsFile 拆分工具'],
                ['Maintenance-Tools/TsFile-Load-Export-Tool', 'TsFile 导入导出工具'],
                ['Maintenance-Tools/CSV-Tool', 'CSV 导入导出工具'],
              ],
            },
            {
              title: '端云协同',
              children: [
                ['Edge-Cloud-Collaboration/Sync-Tool', 'TsFile 同步工具'],
              ],
            },
            {
              title: '系统集成',
              children: [
                ['Ecosystem-Integration/Grafana-Plugin', 'Grafana-Plugin'],
                ['Ecosystem-Integration/Grafana-Connector', 'Grafana-Connector（不推荐）'],
                ['Ecosystem-Integration/Zeppelin-IoTDB', 'Zeppelin-IoTDB'],
                ['Ecosystem-Integration/DBeaver', 'DBeaver-IoTDB'],
                ['Ecosystem-Integration/Spark-TsFile', 'Spark-TsFile'],
                ['Ecosystem-Integration/MapReduce-TsFile', 'Hadoop-TsFile'],
                ['Ecosystem-Integration/Spark-IoTDB', 'Spark-IoTDB'],
                ['Ecosystem-Integration/Hive-TsFile', 'Hive-TsFile'],
                ['Ecosystem-Integration/Flink-TsFile', 'Flink-TsFile'],
                ['Ecosystem-Integration/Flink-IoTDB', 'Flink-IoTDB'],
                ['Ecosystem-Integration/NiFi-IoTDB', 'NiFi-IoTDB'],
              ],
            },
            {
              title: '分布式',
              children: [
                ['Cluster/Cluster-Concept', '基本概念'],
                ['Cluster/Cluster-Setup', '分布式部署'],
                ['Cluster/Cluster-Maintenance', '分布式运维命令'],
                ['Cluster/Deployment-Recommendation', '部署推荐'],
              ],
            },
            {
              title: 'FAQ',
              children: [
                ['FAQ/Frequently-asked-questions', '常见问题'],
                ['FAQ/FAQ-for-cluster-setup', '分布式部署FAQ'],
              ],
            },
            {
              title: '参考',
              children: [
                ['Reference/Common-Config-Manual', '公共配置参数'],
                ['Reference/ConfigNode-Config-Manual', 'ConfigNode配置参数'],
                ['Reference/DataNode-Config-Manual', 'DataNode配置参数'],
                ['Reference/SQL-Reference','SQL参考文档'],
                ['Reference/Status-Codes', '状态码'],
                ['Reference/Keywords', '关键字'],
                ['Reference/TSDB-Comparison', '时间序列数据库比较'],
              ],
            },
          ],
          '/zh/UserGuide/Master/': [
            {
              title: 'IoTDB用户手册 (In progress)',
              collapsable: false,
            },
            {
              title: '关于IoTDB',
              children: [
                ['IoTDB-Introduction/What-is-IoTDB', 'IoTDB简介'],
                ['IoTDB-Introduction/Features', '主要功能特点'],
                ['IoTDB-Introduction/Architecture', '系统架构'],
                ['IoTDB-Introduction/Scenario', '应用场景'],
                ['IoTDB-Introduction/Publication', '研究论文'],
              ],
            },
            {
              title: '快速上手',
              children: [
                ['QuickStart/QuickStart', '快速上手'],
                ['QuickStart/ClusterQuickStart', '集群快速上手'],
                ['QuickStart/WayToGetIoTDB', '下载与安装'],
                ['QuickStart/Command-Line-Interface', 'SQL命令行终端(CLI)'],
                ['QuickStart/Files', '数据文件存储'],
              ],
            },
            {
              title: '数据模式与概念',
              sidebarDepth: 1,
              children: [
                ['Data-Concept/Data-Model-and-Terminology', '数据模型'],
                ['Data-Concept/Schema-Template', '元数据模板'],
                ['Data-Concept/Data-Type', '数据类型'],
                ['Data-Concept/Deadband-Process', '死区处理'],
                ['Data-Concept/Encoding', '编码方式'],
                ['Data-Concept/Compression', '压缩方式'],
                ['Data-Concept/Time-Partition', '数据的时间分区'],
                ['Data-Concept/Time-zone', '时区'],
              ],
            },
            {
              title: '语法约定',
              sidebarDepth: 1,
              children: [
                ['Syntax-Conventions/Literal-Values', '字面值常量'],
                ['Syntax-Conventions/Identifier', '标识符'],
                ['Syntax-Conventions/NodeName-In-Path', '路径结点名'],
                ['Syntax-Conventions/KeyValue-Pair', '键值对'],
                ['Syntax-Conventions/Keywords-And-Reserved-Words', '关键字'],
                ['Syntax-Conventions/Session-And-TsFile-API', 'Session And TsFile API'],
                ['Syntax-Conventions/Detailed-Grammar', '词法和文法详细定义'],
              ],
            },
            {
              title: '应用编程接口',
              children: [
                ['API/Programming-Java-Native-API', 'Java 原生接口'],
                ['API/Programming-Python-Native-API', 'Python 原生接口'],
                ['API/Programming-Cpp-Native-API', 'C++ 原生接口'],
                ['API/Programming-Go-Native-API', 'Go 原生接口'],
                ['API/Programming-JDBC', 'JDBC (不推荐)'],
                ['API/Programming-MQTT', 'MQTT'],
                ['API/RestService', 'REST API'],
                ['API/Programming-TsFile-API', 'TsFile API'],
                ['API/InfluxDB-Protocol', 'InfluxDB 协议适配器'],
                ['API/Interface-Comparison', '原生接口对比'],
              ],
            },
            {
              title: '元数据操作',
              sidebarDepth: 1,
              children: [
                ['Operate-Metadata/Database', '数据库操作'],
                ['Operate-Metadata/Node', '节点操作'],
                ['Operate-Metadata/Timeseries', '时间序列操作'],
                ['Operate-Metadata/Template', '元数据模板'],
                ['Operate-Metadata/Auto-Create-MetaData', '自动创建元数据'],
              ],
            },
            {
              title: '数据写入（数据更新）',
              sidebarDepth: 1,
              children: [
                ['Write-Data/Write-Data', 'CLI 工具写入'],
                ['Write-Data/Session', '原生接口写入'],
                ['Write-Data/REST-API', 'REST 服务'],
                ['Write-Data/MQTT', 'MQTT写入'],
                ['Write-Data/Batch-Load-Tool', '批量数据导入'],
              ],
            },
            {
              title: '数据删除',
              sidebarDepth: 1,
              children: [
                ['Delete-Data/Delete-Data', '删除数据'],
                ['Delete-Data/TTL', 'TTL'],
              ],
            },
            {
              title: '数据查询',
              sidebarDepth: 1,
              children: [
                ['Query-Data/Overview', '概述'],
                ['Query-Data/Select-Expression', '选择表达式'],
                ['Query-Data/Last-Query', '最新点查询'],
                ['Query-Data/Align-By', '查询对齐模式'],
                ['Query-Data/Where-Condition', '查询过滤条件'],
                ['Query-Data/Group-By', '分段分组聚合'],
                ['Query-Data/Having-Condition', '聚合结果过滤'],
                // ['Query-Data/Order-By','结果集排序'],
                ['Query-Data/Fill', '结果集补空值'],
                ['Query-Data/Pagination', '结果集分页'],
                ['Query-Data/Select-Into', '查询写回'],
                ['Query-Data/Continuous-Query', '连续查询'],
              ],
            },
            {
              title: '运算符和函数',
              sidebarDepth: 1,
              children: [
                ['Operators-Functions/Overview', '概述'],
                ['Operators-Functions/User-Defined-Function', '用户自定义函数'],
                ['Operators-Functions/Aggregation', '聚合函数'],
                ['Operators-Functions/Mathematical', '算数运算符和函数'],
                ['Operators-Functions/Comparison', '比较运算符和函数'],
                ['Operators-Functions/Logical', '逻辑运算符'],
                ['Operators-Functions/String', '字符串处理'],
                ['Operators-Functions/Conversion', '数据类型转换'],
                ['Operators-Functions/Constant', '常序列生成'],
                ['Operators-Functions/Selection', '选择函数'],
                ['Operators-Functions/Continuous-Interval', '区间查询'],
                ['Operators-Functions/Variation-Trend', '趋势计算'],
                ['Operators-Functions/Sample', '采样函数'],
                ['Operators-Functions/Time-Series', '时间序列处理'],
                ['Operators-Functions/Lambda', 'Lambda 表达式'],

                // IoTDB-Quality
                ['Operators-Functions/Data-Profiling', '数据画像'],
                ['Operators-Functions/Anomaly-Detection', '异常检测'],
                ['Operators-Functions/Data-Matching', '数据匹配'],
                ['Operators-Functions/Frequency-Domain', '频域分析'],
                ['Operators-Functions/Data-Quality', '数据质量'],
                ['Operators-Functions/Data-Repairing', '数据修复'],
                ['Operators-Functions/Series-Discovery', '序列发现'],
                ['Operators-Functions/Machine-Learning', '机器学习'],
              ],
            },
            {
              title: '触发器',
              sidebarDepth: 1,
              children: [
                ['Trigger/Instructions', '使用说明'],
                ['Trigger/Implement-Trigger', '编写触发器'],
                ['Trigger/Trigger-Management', '管理触发器'],
                ['Trigger/Notes', '重要注意事项'],
                ['Trigger/Configuration-Parameters', '配置参数'],
              ],
            },
            {
              title: '监控告警',
              sidebarDepth: 1,
              children: [
                ['Monitor-Alert/Metric-Tool', '监控工具'],
                ['Monitor-Alert/Alerting', '告警机制'],
              ],
            },
            {
              title: '权限管理',
              children: [
                ['Administration-Management/Administration', '权限管理'],
              ],
            },
            {
              title: '运维工具',
              children: [
                ['Maintenance-Tools/Maintenance-Command', '运维命令'],
                ['Maintenance-Tools/Log-Tool', '日志工具'],
                ['Maintenance-Tools/JMX-Tool', 'JMX 工具'],
                ['Maintenance-Tools/MLogParser-Tool', 'Mlog解析工具'],
                ['Maintenance-Tools/IoTDB-Data-Dir-Overview-Tool', 'IoTDB数据文件夹概览工具'],
                ['Maintenance-Tools/TsFile-Sketch-Tool', 'TsFile概览工具'],
                ['Maintenance-Tools/TsFile-Resource-Sketch-Tool', 'TsFile Resource概览工具'],
                ['Maintenance-Tools/TsFile-Split-Tool', 'TsFile 拆分工具'],
                ['Maintenance-Tools/TsFile-Load-Export-Tool', 'TsFile 导入导出工具'],
                ['Maintenance-Tools/CSV-Tool', 'CSV 导入导出工具'],
              ],
            },
            {
              title: '端云协同',
              children: [
                ['Edge-Cloud-Collaboration/Sync-Tool', 'TsFile 同步工具'],
              ],
            },
            {
              title: '系统集成',
              children: [
                ['Ecosystem-Integration/Grafana-Plugin', 'Grafana-Plugin'],
                ['Ecosystem-Integration/Grafana-Connector', 'Grafana-Connector（不推荐）'],
                ['Ecosystem-Integration/Zeppelin-IoTDB', 'Zeppelin-IoTDB'],
                ['Ecosystem-Integration/DBeaver', 'DBeaver-IoTDB'],
                ['Ecosystem-Integration/Spark-TsFile', 'Spark-TsFile'],
                ['Ecosystem-Integration/MapReduce-TsFile', 'Hadoop-TsFile'],
                ['Ecosystem-Integration/Spark-IoTDB', 'Spark-IoTDB'],
                ['Ecosystem-Integration/Hive-TsFile', 'Hive-TsFile'],
                ['Ecosystem-Integration/Flink-TsFile', 'Flink-TsFile'],
                ['Ecosystem-Integration/Flink-IoTDB', 'Flink-IoTDB'],
                ['Ecosystem-Integration/NiFi-IoTDB', 'NiFi-IoTDB'],
              ],
            },
            {
              title: '分布式',
              children: [
                ['Cluster/Cluster-Concept', '基本概念'],
                ['Cluster/Cluster-Setup', '分布式部署'],
                ['Cluster/Cluster-Maintenance', '分布式运维命令'],
                ['Cluster/Deployment-Recommendation', '部署推荐'],
              ],
            },
            {
              title: 'FAQ',
              children: [
                ['FAQ/Frequently-asked-questions', '常见问题'],
                ['FAQ/FAQ-for-cluster-setup', '分布式部署FAQ'],
              ],
            },
            {
              title: '参考',
              children: [
                ['Reference/Common-Config-Manual', '公共配置参数'],
                ['Reference/ConfigNode-Config-Manual', 'ConfigNode配置参数'],
                ['Reference/DataNode-Config-Manual', 'DataNode配置参数'],
                ['Reference/SQL-Reference','SQL参考文档'],
                ['Reference/Status-Codes', '状态码'],
                ['Reference/Keywords', '关键字'],
                ['Reference/TSDB-Comparison', '时间序列数据库比较'],
              ],
            },
          ],
        },
      },
    },
  },
  locales: {
    '/': {
      lang: 'en-US',
      title: ' ',
      description: ' ',
    },
    '/zh/': {
      lang: 'zh-CN',
      title: ' ',
      description: ' ',
    },
  },
};

module.exports = config;
