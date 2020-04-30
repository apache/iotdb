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

var config = {
    head: [
		['link', { rel: 'icon', href: '/favicon.ico' }],
		["meta", {name: "Description", content: "Apache IoTDB: Time Series Database for IoT"}],
        ["meta", {name: "Keywords", content: "TSDB, time series, time series database, IoTDB, IoT database, IoT data management,时序数据库, 时间序列管理, IoTDB, 物联网数据库, 实时数据库, 物联网数据管理, 物联网数据"}],
        ["meta", {name: "baidu-site-verification", content: "wfKETzB3OT"}],
        ["meta", {name: "google-site-verification", content: "mZWAoRY0yj_HAr-s47zHCGHzx5Ju-RVm5wDbPnwQYFo"}],
      ],

    // 静态网站部署的目录
    base: '',
  
    // 网站标题
    title: 'IoTDB Website',
  
    // <meta name="description" content="...">
    description: 'Apache IoTDB', 
  
    markdown: {
      
      // 显示代码行号
      lineNumbers: true
    },
    themeConfig: {

        // 项目的 github 地址
        repo: 'https://github.com/apache/incubator-iotdb.git',
    
        // github 地址的链接名
        repoLabel: 'gitHub',
		
		logo: '/img/logo.png',
		
		searchMaxSuggestions:10,

		displayAllHeaders: true,
		
		sidebarDepth: 0,
    
		locales: {
		  '/': {
			selectText: 'Languages',
			label: 'English',
			ariaLabel: 'Languages',
			editLinkText: 'Edit this page on GitHub',
			serviceWorker: {
			  updatePopup: {
				message: "New content is available.",
				buttonText: "Refresh"
			  }
			},
			algolia: {},
			nav: [
				 {
					text: 'Document',
					items: [
						{ text: 'In progress', link: '/UserGuide/Master/0-Get Started/1-QuickStart' },
						{ text: 'V0.9.x', link: '/UserGuide/V0.9.x/0-Get Started/1-QuickStart' },
					    { text: 'V0.8.x', link: '/UserGuide/V0.8.x/0-Get Started/1-QuickStart'},
					]
				  },
				  {
					text: 'System design',
					link: '/SystemDesign/0-Architecture/1-Architecture'
				  },
				  {
					text: 'Download',
					link: '/Download/'
				  },
				  {
					text: 'Community',
					items: [
					  { text: 'Wiki', link: 'https://cwiki.apache.org/confluence/display/iotdb'},
					  { text: 'People', link: '/Community/Community-Project Committers'},
					  { text: 'Powered By', link: '/Community/Community-Powered By'},
					  { text: 'Resources', link: '/Community/Materials'},
						{ text: 'Feedback', link: '/Community/Feedback'},
					]
				  },
				  {
					text: 'Development',
					items: [
					  { text: 'How to vote', link: '/Development/VoteRelease'},
					  { text: 'How to Commit', link: '/Development/HowToCommit'},
					  { text: 'Become a Committer', link: '/Development/Committer'},
					  { text: 'ContributeGuide', link: '/Development/ContributeGuide'},
					  { text: 'Changelist of TsFile', link: '/Development/format-changelist'},
					  { text: 'Changelist of RPC', link: '/Development/rpc-changelist'},
					]
				  },
				  {
					text: 'ASF',
					items: [
					  { text: 'Foundation', link: 'http://www.apache.org/'},
					  { text: 'License', link: 'http://www.apache.org/licenses/'},
					  { text: 'Security', link: 'http://www.apache.org/security/'},
					  { text: 'Sponsorship', link: 'http://www.apache.org/foundation/sponsorship.html'},
					  { text: 'Thanks', link: 'http://www.apache.org/foundation/thanks.html'},
					  { text: 'Current Events', link: 'http://www.apache.org/events/current-event'},
					]
				  },
			],
			sidebar: {
				'/UserGuide/V0.8.x/': [
					{
						title:'IoTDB User Guide (V0.8.x)',
						collapsable: false,
					},
					{
						title: '0-Get Started',
						children: [
							['0-Get Started/1-QuickStart','QuickStart'],
							['0-Get Started/2-Frequently asked questions','Frequently asked questions'],
							['0-Get Started/3-Publication','Research Papers']
						]
					},
					{
						title: '1-Overview',
						children: [
							['1-Overview/1-What is IoTDB','What is IoTDB'],
							['1-Overview/2-Architecture','Architecture'],
							['1-Overview/3-Scenario','Scenario'],
							['1-Overview/4-Features','Features']
						]
					},
					{
						title: '2-Concept Key Concepts and Terminology',
						children: [
							['2-Concept Key Concepts and Terminology/1-Key Concepts and Terminology','Key Concepts and Terminology'],
							['2-Concept Key Concepts and Terminology/2-Data Type','Data Type'],
							['2-Concept Key Concepts and Terminology/3-Encoding','Encoding'],
							['2-Concept Key Concepts and Terminology/4-Compression','Compression']
						]
					},
					{
						title: '3-Operation Manual',
						children: [
							['3-Operation Manual/1-Sample Data','Sample Data'],
							['3-Operation Manual/2-Data Model Selection','Data Model Selection'],
							['3-Operation Manual/3-Data Import','Data Import'],
							['3-Operation Manual/4-Data Query','Data Query'],
							['3-Operation Manual/5-Data Maintenance','Data Maintenance'],
							['3-Operation Manual/6-Priviledge Management','Priviledge Management']
						]
					},
					{
						title: '4-Deployment and Management',
						children: [
							['4-Deployment and Management/1-Deployment','Deployment'],
							['4-Deployment and Management/2-Configuration','Configuration'],
							['4-Deployment and Management/3-System Monitor','System Monitor'],
							['4-Deployment and Management/4-Performance Monitor','Performance Monitor'],
							['4-Deployment and Management/5-System log','System log'],
							['4-Deployment and Management/6-Data Management','Data Management'],
							['4-Deployment and Management/7-Build and use IoTDB by Dockerfile','Dockerfile']
						]
					},
					{
						title: '5-IoTDB SQL Documentation',
						children: [
							['5-IoTDB SQL Documentation/1-IoTDB Query Statement','IoTDB Query Statement'],
							['5-IoTDB SQL Documentation/2-Reference','Reference']
						]
					},
					{
						title: '6-JDBC API',
						children: [
							['6-JDBC API/1-JDBC API','JDBC API']
						]
					},
					{
						title: '7-TsFile',
						children: [
							['7-TsFile/1-Installation','Installation'],
							['7-TsFile/2-Usage','Usage'],
							['7-TsFile/3-Hierarchy','Hierarchy']
						]
					},
					{
						title: '8-System Tools',
						children: [
							['8-System Tools/1-Sync','Sync'],
							['8-System Tools/2-Memory Estimation Tool','Memory Estimation Tool']
						]
					},
				],
				'/UserGuide/V0.9.x/': [
					{
						title:'IoTDB User Guide (V0.9.x)',
						collapsable: false,
					},
					{
						title: '0-Get Started',
						children: [
							['0-Get Started/1-QuickStart','QuickStart'],
							['0-Get Started/2-Frequently asked questions','Frequently asked questions'],
							['0-Get Started/3-Publication','Research Papers']
						]
					},
					{
						title: '1-Overview',
						children: [
							['1-Overview/1-What is IoTDB','What is IoTDB'],
							['1-Overview/2-Architecture','Architecture'],
							['1-Overview/3-Scenario','Scenario'],
							['1-Overview/4-Features','Features']
						]
					},
					{
						title: '2-Concept',
						children: [
							['2-Concept/1-Data Model and Terminology','Data Model and Terminology'],
							['2-Concept/2-Data Type','Data Type'],
							['2-Concept/3-Encoding','Encoding'],
							['2-Concept/4-Compression','Compression']
						]
					},
					{
						title: '3-Server',
						children: [
							['3-Server/1-Download','Download'],
							['3-Server/2-Single Node Setup','Single Node Setup'],
							['3-Server/3-Cluster Setup','Cluster Setup'],
							['3-Server/4-Config Manual','Config Manual'],
							['3-Server/5-Docker Image','Docker Image']
						]
					},
					{
						title: '4-Client',
						children: [
							['4-Client/1-Command Line Interface','Command Line Interface'],
							['4-Client/2-Programming - JDBC','JDBC'],
							['4-Client/3-Programming - Session','Session'],
							['4-Client/4-Programming - Other Languages','Other Languages'],
							['4-Client/5-Programming - TsFile API','TsFile API']
						]
					},
					{
						title: '5-Operation Manual',
						children: [
							['5-Operation Manual/1-DDL Data Definition Language','DDL (Data Definition Language)'],
							['5-Operation Manual/2-DML Data Manipulation Language','DML (Data Manipulation Language)'],
							['5-Operation Manual/3-Account Management Statements','Account Management Statements'],
							['5-Operation Manual/4-SQL Reference','SQL Reference']
						]
					},
					{
						title: '6-System Tools',
						children: [
							['6-System Tools/1-Sync Tool','Sync Tool'],
							['6-System Tools/2-Memory Estimation Tool','Memory Estimation Tool'],
							['6-System Tools/3-JMX Tool','JMX Tool'],
							['6-System Tools/4-Watermark Tool','Watermark Tool'],
							['6-System Tools/5-Log Visualizer','Log Visualizer'],
							['6-System Tools/6-Query History Visualization Tool','Query History Visualization Tool'],
							['6-System Tools/7-Monitor and Log Tools','Monitor and Log Tools']
						]
					},
					{
						title: '7-Ecosystem Integration',
						children: [
							['7-Ecosystem Integration/1-Grafana','Grafana'],
							['7-Ecosystem Integration/2-MapReduce TsFile','MapReduce TsFile'],
							['7-Ecosystem Integration/3-Spark TsFile','Spark TsFile'],
							['7-Ecosystem Integration/4-Spark IoTDB','Spark IoTDB'],
							['7-Ecosystem Integration/5-Hive TsFile','Hive TsFile']
						]
					},
					{
						title: '8-System Design',
						children: [
							['8-System Design/1-Hierarchy','Hierarchy'],
							['8-System Design/2-Files','Files'],
							['8-System Design/3-Writing Data on HDFS','Writing Data on HDFS'],
							['8-System Design/4-Shared Nothing Cluster','Shared Nothing Cluster'],
						]
					},
				],
				'/UserGuide/Master/': [
					{
						title:'IoTDB User Guide (In progress)',
						collapsable: false,
					},
					{
						title: '0-Get Started',
						children: [
							['0-Get Started/1-QuickStart','QuickStart'],
							['0-Get Started/2-Frequently asked questions','Frequently asked questions'],
							['0-Get Started/3-Publication','Research Papers']
						]
					},
					{
						title: '1-Overview',
						children: [
							['1-Overview/1-What is IoTDB','What is IoTDB'],
							['1-Overview/2-Architecture','Architecture'],
							['1-Overview/3-Scenario','Scenario'],
							['1-Overview/4-Features','Features']
						]
					},
					{
						title: '2-Concept',
						children: [
							['2-Concept/1-Data Model and Terminology','Data Model and Terminology'],
							['2-Concept/2-Data Type','Data Type'],
							['2-Concept/3-Encoding','Encoding'],
							['2-Concept/4-Compression','Compression']
						]
					},
					{
						title: '3-Server',
						children: [
							['3-Server/1-Download','Download'],
							['3-Server/2-Single Node Setup','Single Node Setup'],
							['3-Server/3-Cluster Setup','Cluster Setup'],
							['3-Server/4-Config Manual','Config Manual'],
							['3-Server/5-Docker Image','Docker Image']
						]
					},
					{
						title: '4-Client',
						children: [
							['4-Client/1-Command Line Interface','Command Line Interface'],
							['4-Client/2-Programming - Native API','Native API'],
							['4-Client/3-Programming - JDBC','JDBC'],
							['4-Client/4-Programming - Other Languages','Other Languages'],
							['4-Client/5-Programming - TsFile API','TsFile API'],
							['4-Client/6-Programming - MQTT','MQTT'],
							['4-Client/7-Status Codes','Status Codes']
						]
					},
					{
						title: '5-Operation Manual',
						children: [
							['5-Operation Manual/1-DDL Data Definition Language','DDL (Data Definition Language)'],
							['5-Operation Manual/2-DML Data Manipulation Language','DML (Data Manipulation Language)'],
							['5-Operation Manual/3-Account Management Statements','Account Management Statements'],
							['5-Operation Manual/4-SQL Reference','SQL Reference']
						]
					},
					{
						title: '6-System Tools',
						children: [
							['6-System Tools/1-Sync Tool','Sync Tool'],
							['6-System Tools/2-Memory Estimation Tool','Memory Estimation Tool'],
							['6-System Tools/3-JMX Tool','JMX Tool'],
							['6-System Tools/4-Watermark Tool','Watermark Tool'],
							['6-System Tools/5-Log Visualizer','Log Visualizer'],
							['6-System Tools/6-Query History Visualization Tool','Query History Visualization Tool'],
							['6-System Tools/7-Monitor and Log Tools','Monitor and Log Tools'],
							['6-System Tools/8-Load External Tsfile','Load External Tsfile']
						]
					},
					{
						title: '7-Ecosystem Integration',
						children: [
							['7-Ecosystem Integration/1-Grafana','Grafana'],
							['7-Ecosystem Integration/2-MapReduce TsFile','MapReduce TsFile'],
							['7-Ecosystem Integration/3-Spark TsFile','Spark TsFile'],
							['7-Ecosystem Integration/4-Spark IoTDB','Spark IoTDB'],
							['7-Ecosystem Integration/5-Hive TsFile','Hive TsFile']
						]
					},
					{
						title: '8-Architecture',
						children: [
							['8-Architecture/1-Files','Files'],
							['8-Architecture/2-Writing Data on HDFS','Writing Data on HDFS'],
							['8-Architecture/3-Shared Nothing Cluster','Shared Nothing Cluster']
						]
					},
				],
				'/SystemDesign/': [
					{
						title: 'System design',
						collapsable: false,
					},
					{
						title: '0-Architecture',
						children: [
							['0-Architecture/1-Architecture','Architecture']
						]
					},
					{
						title: '1-TsFile',
						children: [
							['1-TsFile/1-TsFile','TsFile'],
							['1-TsFile/2-Format','Format'],
							['1-TsFile/3-Write','Write'],
							['1-TsFile/4-Read','Read']
						]
					},
					{
						title: '2-QueryEngine',
						children: [
							['2-QueryEngine/1-QueryEngine','QueryEngine'],
							['2-QueryEngine/2-Planner','Planner'],
							['2-QueryEngine/3-PlanExecutor','PlanExecutor']
						]
					},
					{
						title: '3-SchemaManager',
						children: [
							['3-SchemaManager/1-SchemaManager','SchemaManager'],
						]
					},
					{
						title: '4-StorageEngine',
						children: [
							['4-StorageEngine/1-StorageEngine','StorageEngine'],
							['4-StorageEngine/2-WAL','WAL'],
							['4-StorageEngine/3-FlushManager','FlushManager'],
							['4-StorageEngine/4-MergeManager','MergeManager'],
							['4-StorageEngine/5-DataPartition','DataPartition'],
							['4-StorageEngine/6-DataManipulation','DataManipulation']
						]
					},
					{
						title: '5-DataQuery',
						children: [
							['5-DataQuery/1-DataQuery','DataQuery'],
							['5-DataQuery/2-QueryFundamentals','QueryFundamentals'],
							['5-DataQuery/3-SeriesReader','SeriesReader'],
							['5-DataQuery/4-RawDataQuery','RawDataQuery'],
							['5-DataQuery/5-AggregationQuery','AggregationQuery'],
							['5-DataQuery/6-GroupByQuery','GroupByQuery'],
							['5-DataQuery/7-LastQuery','LastQuery'],
							['5-DataQuery/8-AlignByDeviceQuery','AlignByDeviceQuery'],
							['5-DataQuery/9-FillFunction','FillFunction'],
	                        ['5-DataQuery/10-GroupByFillQuery', 'GroupByFillQuery']
						]
					},
					{
						title: '6-Tools',
						children: [
							['6-Tools/1-Sync','Sync']
						]
					},
					{
						title: '7-Connector',
						children: [
							['7-Connector/2-Hive-TsFile','Hive-TsFile'],
							['7-Connector/3-Spark-TsFile','Spark-TsFile'],
							['7-Connector/4-Spark-IOTDB','Spark-IOTDB']
						]
					},
				],
			}
		  },
		  '/zh/': {
			// 多语言下拉菜单的标题
			selectText: '语言',
			// 该语言在下拉菜单中的标签
			label: '简体中文',
			// 编辑链接文字
			editLinkText: '在 GitHub 上编辑此页',
			// Service Worker 的配置
			serviceWorker: {
			  updatePopup: {
				message: "发现新内容可用.",
				buttonText: "刷新"
			  }
			},
			// 当前 locale 的 algolia docsearch 选项
			algolia: {
			},
			nav: [
				 {
					text: '文档',
					items: [
					  { text: 'In progress', link: '/zh/UserGuide/Master/0-Get Started/1-QuickStart' },
					  { text: 'V0.9.x', link: '/zh/UserGuide/V0.9.x/0-Get Started/1-QuickStart' },
					  { text: 'V0.8.x', link: '/zh/UserGuide/V0.8.x/0-Get Started/1-QuickStart'},
					]
				  },
				  	{
					text: '系统设计',
					link: '/zh/SystemDesign/0-Architecture/1-Architecture'
				  },
				  {
					text: '下载',
					link: '/zh/Download/'
				  },
				  {
					text: '社区',
					items: [
						{ text: 'Wiki', link: 'https://cwiki.apache.org/confluence/display/iotdb'},
					    { text: '开发人员', link: '/zh/Community/Community-Project Committers'},
					    { text: '技术支持', link: '/zh/Community/Community-Powered By'},
							{ text: '活动与报告', link: '/zh/Community/Materials'},
							{ text: '交流与反馈', link: '/zh/Community/Feedback'},
					]
				  },
				  {
					text: '开发',
					items: [
					  { text: '如何投票', link: '/zh/Development/VoteRelease'},
					  { text: '如何提交代码', link: '/Development/HowToCommit'},
					  { text: '成为Committer', link: '/Development/Committer'},
					  { text: '项目开发指南', link: '/zh/Development/ContributeGuide'},
					  { text: 'TsFile的更改列表', link: '/zh/Development/format-changelist'},
					  { text: 'RPC变更清单', link: '/zh/Development/rpc-changelist'},
					]
				  },
				  {
					text: 'ASF',
					items: [
					  { text: '基金会', link: 'http://www.apache.org/'},
					  { text: '许可证', link: 'http://www.apache.org/licenses/'},
					  { text: '安全', link: 'http://www.apache.org/security/'},
					  { text: '赞助', link: 'http://www.apache.org/foundation/sponsorship.html'},
					  { text: '致谢', link: 'http://www.apache.org/foundation/thanks.html'},
					  { text: '活动', link: 'http://www.apache.org/events/current-event'},
					]
				  },
			],
			sidebar: {
				'/zh/UserGuide/V0.8.x/': [
					{
						title: 'IoTDB用户手册 (V0.8.x)',
						collapsable: false,
					},
					{
						title: '0-开始使用',
						children: [
							['0-Get Started/1-QuickStart','快速入门'],
							['0-Get Started/2-Frequently asked questions','经常问的问题'],
							['0-Get Started/3-Publication','调查报告']
						]
					},
					{
						title: '1-概述',
						children: [
							['1-Overview/1-What is IoTDB','什么是IoTDB'],
							['1-Overview/2-Architecture','架构'],
							['1-Overview/3-Scenario','应用场景'],
							['1-Overview/4-Features','特征']
						]
					},
					{
						title: '2-基本概念',
						children: [
							['2-Concept Key Concepts and Terminology/1-Key Concepts and Terminology','主要概念及术语'],
							['2-Concept Key Concepts and Terminology/2-Data Type','数据类型'],
							['2-Concept Key Concepts and Terminology/3-Encoding','编码方式'],
							['2-Concept Key Concepts and Terminology/4-Compression','压缩方式']
						]
					},
					{
						title: '3-操作指南',
						children: [
							['3-Operation Manual/1-Sample Data','样例数据'],
							['3-Operation Manual/2-Data Model Selection','数据模型选用与创建'],
							['3-Operation Manual/3-Data Import','数据接入'],
							['3-Operation Manual/4-Data Query','数据查询'],
							['3-Operation Manual/5-Data Maintenance','数据维护'],
							['3-Operation Manual/6-Priviledge Management','权限管理']
						]
					},
					{
						title: '4-系统部署与管理',
						children: [
							['4-Deployment and Management/1-Deployment','系统部署'],
							['4-Deployment and Management/2-Configuration','系统配置'],
							['4-Deployment and Management/3-System Monitor','系统监控'],
							['4-Deployment and Management/4-Performance Monitor','性能监控'],
							['4-Deployment and Management/5-System log','系统日志'],
							['4-Deployment and Management/6-Data Management','数据管理'],
							['4-Deployment and Management/7-Build and use IoTDB by Dockerfile','通过Dockerfile构建和使用IoTDB']
						]
					},
					{
						title: '5-IoTDB SQL文档',
						children: [
							['5-IoTDB SQL Documentation/1-IoTDB Query Statement','IoTDB查询语句'],
							['5-IoTDB SQL Documentation/2-Reference','参考']
						]
					},
					{
						title: '6-JDBC API',
						children: [
							['6-JDBC API/1-JDBC API','JDBC API']
						]
					},
					{
						title: '7-TsFile',
						children: [
							['7-TsFile/1-Installation','安装'],
							['7-TsFile/2-Usage','用法'],
							['7-TsFile/3-Hierarchy','TsFile层次结构']
						]
					},
					{
						title: '8-系统工具',
						children: [
							['8-System Tools/1-Sync','同步工具'],
							['8-System Tools/2-Memory Estimation Tool','内存预估工具']
						]
					},		   
				],
				'/zh/UserGuide/V0.9.x/': [
					{
						title: 'IoTDB用户手册 (V0.9.x)',
						collapsable: false,
					},
					{
						title: '0-开始',
						children: [
							['0-Get Started/1-QuickStart','快速入门'],
							['0-Get Started/2-Frequently asked questions','常见问题'],
							['0-Get Started/3-Publication','研究论文']
						]
					},
					{
						title: '1-概览',
						children: [
							['1-Overview/1-What is IoTDB','什么是IoTDB'],
							['1-Overview/2-Architecture','架构'],
							['1-Overview/3-Scenario','场景'],
							['1-Overview/4-Features','特征']
						]
					},
					{
						title: '2-概念',
						children: [
							['2-Concept/1-Data Model and Terminology','数据模型与技术'],
							['2-Concept/2-Data Type','数据类型'],
							['2-Concept/3-Encoding','编码方式'],
							['2-Concept/4-Compression','压缩方式']
						]
					},
					{
						title: '3-服务器端',
						children: [
							['3-Server/1-Download','下载'],
							['3-Server/2-Single Node Setup','单节点设置'],
							['3-Server/3-Cluster Setup','集群设置'],
							['3-Server/4-Config Manual','系统配置'],
							['3-Server/5-Docker Image','Docker镜像']
						]
					},
					{
						title: '4-客户端',
						children: [
							['4-Client/1-Command Line Interface','命令行接口 (CLI)'],
							['4-Client/2-Programming - JDBC','JDBC'],
							['4-Client/3-Programming - Session','Session'],
							['4-Client/4-Programming - Other Languages','其他语言'],
							['4-Client/5-Programming - TsFile API','TsFile API']
						]
					},
					{
						title: '5-操作指南',
						children: [
							['5-Operation Manual/1-DDL Data Definition Language','DDL (数据定义语言)'],
							['5-Operation Manual/2-DML Data Manipulation Language','DML (数据操作语言)'],
							['5-Operation Manual/3-Account Management Statements','账户管理语句'],
							['5-Operation Manual/4-SQL Reference','SQL 参考文档']
						]
					},
					{
						title: '6-系统工具',
						children: [
							['6-System Tools/1-Sync Tool','同步工具'],
							['6-System Tools/2-Memory Estimation Tool','内存预估'],
							['6-System Tools/3-JMX Tool','JMX工具'],
							['6-System Tools/4-Watermark Tool','水印工具'],
							['6-System Tools/5-Log Visualizer','日志可视化工具'],
							['6-System Tools/6-Query History Visualization Tool','查询历史可视化工具'],
							['6-System Tools/7-Monitor and Log Tools','监控与日志工具']
						]
					},
					{
						title: '7-生态集成',
						children: [
							['7-Ecosystem Integration/1-Grafana','Grafana'],
							['7-Ecosystem Integration/2-MapReduce TsFile','MapReduce TsFile'],
							['7-Ecosystem Integration/3-Spark TsFile','Spark TsFile'],
							['7-Ecosystem Integration/4-Spark IoTDB','Spark IoTDB'],
							['7-Ecosystem Integration/5-Hive TsFile','Hive TsFile']
						]
					},
					{
						title: '8-系统设计',
						children: [
							['8-System Design/1-Hierarchy','层次结构'],
							['8-System Design/2-Files','文件'],
							['8-System Design/3-Writing Data on HDFS','使用HDFS存储数据'],
							['8-System Design/4-Shared Nothing Cluster','Shared-nothing 架构']
						]
					},
				],
				'/zh/UserGuide/Master/': [
					{
						title: 'IoTDB用户手册 (In progress)',
						collapsable: false,
					},
					{
						title: '0-开始',
						children: [
							['0-Get Started/1-QuickStart','快速入门'],
							['0-Get Started/2-Frequently asked questions','经常问的问题'],
							['0-Get Started/3-Publication','调查报告']
						]
					},
					{
						title: '1-概述',
						children: [
							['1-Overview/1-What is IoTDB','什么是IoTDB'],
							['1-Overview/2-Architecture','架构'],
							['1-Overview/3-Scenario','场景'],
							['1-Overview/4-Features','特征']
						]
					},
					{
						title: '2-概念',
						children: [
							['2-Concept/1-Data Model and Terminology','数据模型与技术'],
							['2-Concept/2-Data Type','数据类型'],
							['2-Concept/3-Encoding','编码方式'],
							['2-Concept/4-Compression','压缩方式']
						]
					},
					{
						title: '3-服务器端',
						children: [
							['3-Server/1-Download','下载'],
							['3-Server/2-Single Node Setup','单节点安装'],
							['3-Server/3-Cluster Setup','集群设置'],
							['3-Server/4-Config Manual','配置手册'],
							['3-Server/5-Docker Image','Docker镜像']
						]
					},
					{
						title: '4-客户端',
						children: [
							['4-Client/1-Command Line Interface','命令行接口(CLI)'],
							['4-Client/2-Programming - Native API','原生接口'],
							['4-Client/3-Programming - JDBC','JDBC'],
							['4-Client/4-Programming - Other Languages','其他语言'],
							['4-Client/5-Programming - TsFile API','TsFile API'],
							['4-Client/6-Programming - MQTT','MQTT'],
							['4-Client/7-Status Codes','状态码']
						]
					},
					{
						title: '5-操作指南',
						children: [
							['5-Operation Manual/1-DDL Data Definition Language','DDL (数据定义语言)'],
							['5-Operation Manual/2-DML Data Manipulation Language','DML (数据操作语言)'],
							['5-Operation Manual/3-Account Management Statements','账户管理语句'],
							['5-Operation Manual/4-SQL Reference','SQL 参考文档']
						]
					},
					{
						title: '6-系统工具',
						children: [
							['6-System Tools/1-Sync Tool','同步工具'],
							['6-System Tools/2-Memory Estimation Tool','内存预估'],
							['6-System Tools/3-JMX Tool','JMX工具'],
							['6-System Tools/4-Watermark Tool','水印工具'],
							['6-System Tools/5-Log Visualizer','日志可视化工具'],
							['6-System Tools/6-Query History Visualization Tool','查询历史可视化工具'],
							['6-System Tools/7-Monitor and Log Tools','监控与日志工具'],
							['6-System Tools/8-Load External Tsfile','加载外部tsfile文件']
						]
					},
					{
						title: '7-生态集成',
						children: [
							['7-Ecosystem Integration/1-Grafana','Grafana'],
							['7-Ecosystem Integration/2-MapReduce TsFile','MapReduce TsFile'],
							['7-Ecosystem Integration/3-Spark TsFile','Spark TsFile'],
							['7-Ecosystem Integration/4-Spark IoTDB','Spark IoTDB'],
							['7-Ecosystem Integration/5-Hive TsFile','Hive TsFile']
						]
					},
					{
						title: '8-系统设计',
						children: [
							['8-Architecture/1-Files','文件'],
							['8-Architecture/2-Writing Data on HDFS','使用HDFS存储数据'],
							['8-Architecture/3-Shared Nothing Cluster','Shared-nothing 架构']
						]
					}
				],
				'/zh/SystemDesign/': [
					{
						title: '系统设计',
						collapsable: false,
					},
					{
						title: '0-应用概览',
						children: [
							['0-Architecture/1-Architecture','应用概览']
						]
					},
					{
						title: '1-TsFile',
						children: [
							['1-TsFile/1-TsFile','TsFile'],
							['1-TsFile/2-Format','格式'],
							['1-TsFile/3-Write','写流程'],
							['1-TsFile/4-Read','读流程']
						]
					},
					{
						title: '2-查询引擎',
						children: [
							['2-QueryEngine/1-QueryEngine','查询引擎'],
							['2-QueryEngine/2-Planner','执行计划生成器'],
							['2-QueryEngine/3-PlanExecutor','计划执行器']
						]
					},
					{
						title: '3-元数据管理',
						children: [
							['3-SchemaManager/1-SchemaManager','元数据管理']
						]
					},
					{
						title: '4-存储引擎',
						children: [
							['4-StorageEngine/1-StorageEngine','存储引擎'],
							['4-StorageEngine/2-WAL','写前日志'],
							['4-StorageEngine/3-FlushManager','FlushManager'],
							['4-StorageEngine/4-MergeManager','文件合并机制'],
							['4-StorageEngine/5-DataPartition','数据分区'],
							['4-StorageEngine/6-DataManipulation','数据增删改']
						]
					},
					{
						title: '5-数据查询',
						children: [
							['5-DataQuery/1-DataQuery','数据查询'],
							['5-DataQuery/2-QueryFundamentals','查询基础介绍'],
							['5-DataQuery/3-SeriesReader','查询基础组件'],
							['5-DataQuery/4-RawDataQuery','原始数据查询'],
							['5-DataQuery/5-AggregationQuery','聚合查询'],
							['5-DataQuery/6-GroupByQuery','降采样查询'],
							['5-DataQuery/7-LastQuery','最近时间戳 Last 查询'],
							['5-DataQuery/8-AlignByDeviceQuery','按设备对齐查询'],
							['5-DataQuery/9-FillFunction','空值填充'],
		                    ['5-DataQuery/10-GroupByFillQuery', '降采样补空值查询']
						]
					},
					{
						title: '6-工具',
						children: [
							['6-Tools/1-Sync','同步工具']
						]
					},
					{
						title: '7-连接器',
						children: [
							['7-Connector/2-Hive-TsFile','Hive-TsFile'],
							['7-Connector/3-Spark-TsFile','Spark-TsFile'],
							['7-Connector/4-Spark-IOTDB','Spark-IOTDB']
						]
					},
				],
			}
		  }
		}
      },
	locales: {
		'/': {
		  lang: 'en-US', 
		  title: ' ',
		  description: ' '
		},
		'/zh/': {
		  lang: 'zh-CN',
		  title: ' ',
		  description: ' '
		}
	  },
  }
  
  module.exports = config
  
