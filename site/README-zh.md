<!--

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

-->

[TOC]

# 网站说明

## IoTDB 网址

见 https://iotdb.apache.org/

## 如何建立

跑`mvn package -DskipTests` 

## 如何调试

当跑完 `mvn package -DskipTests`, 所有的源文档都会复制到target/vue-source目录下

如果你想要调试：

```
# 在localhost:8080打开
npm run dev

# 在.vuepress/dist生成项目文件（html形式）
npm run build
```

请记住，Git只能记录site / src的更改。
目标文件夹中的所有更改将被Git忽略。

## 手动部署

跑`mvn package scm-publish:publish-scm`.

Apache ID和密码是必须的

或直接运行`mvn package scm-publish:publish-scm -Dusername={你的Apache ID} -Dpassword={你的Apache账号密码}`

## 常见问题

如果你在MacOS遇到以下问题:

> gyp: No Xcode or CLT version detected! 

然后，安装Xcode的CommandLine: `sudo xcode-select --install`.
如果您已安装并且仍然出现错误，则 `sudo xcode-select --reset`

# 文档格式

- 所有的md都会被编译成html，REDEME.md编译为index.html，xx.md编译为xx.html
- md内标签必须有开头有结尾，比如\<tr>必须有\</tr>与之对应，而且是严格对应；如果文档中需要插入标签，比如List\<String>，可以加这个放入代码块中，也可以在**俩个**尖括号前加上\，如\<\\String\\>
- 标签不能交叉嵌套，比如\<p>\<center>\</p>\</center>这是不允许的
- 文章的一级标题就是该文档对应sidebar的标题，所以文档最大的章节标题就不需要了

## 文档内添加vue组件

将组件放在.vuepress/components目录下

直接在md文件中使用\<组件名/>即可

关于vue组件的使用教程https://cn.vuejs.org/v2/guide/components.html

# 贡献文档

- 修改已有文档：只需要对文档里的内容进行修改，不要修改文件名（修改文件名就需要到config.js修改）
- 增加文档：将增加的文档放在自己想放的目录，记录下目录的链接路径，到.vuepress/config.js中修改

# 修改config.js

## head,base,title,description

```js
    head: [
		["meta", {name: "Description", content: "Apache IoTDB: Time Series Database for IoT"}],
        ["meta", {name: "Keywords", content: "TSDB, time series, time series database, IoTDB, IoT database, IoT data management,时序数据库, 时间序列管理, IoTDB, 物联网数据库, 实时数据库, 物联网数据管理, 物联网数据"}],
        ["meta", {name: "baidu-site-verification", content: "wfKETzB3OT"}],
        ["meta", {name: "google-site-verification", content: "mZWAoRY0yj_HAr-s47zHCGHzx5Ju-RVm5wDbPnwQYFo"}],
      ],
```

额外的需要被注入到当前页面的 HTML `<head>` 中的标签，每个标签都可以以 `[tagName, { attrName: attrValue }, innerHTML?]` 的格式指定

base:部署站点的基础路径，如果你想让你的网站部署到一个子路径下，你将需要设置它。

title:网站的标题，它将会被用作所有页面标题的前缀，同时，默认主题下，它将显示在导航栏（navbar）上

description:网站的描述，它将会以 `<meta>` 标签渲染到当前页面的 HTML 中。

具体配置信息看 https://www.vuepress.cn/config/

 ## locales

```js
	locales: {
		'/': {
		  lang: 'en-US', // 将会被设置为 <html> 的 lang 属性
		  title: ' ',
		  description: ' '
		},
		'/zh/': {
		  lang: 'zh-CN',
		  title: ' ',
		  description: ' '
		}
	  },
```

表示/跳转英文链接，/zh/跳转中文链接。

文件的存储结构如下,zh下必须与英文的一一对应

```
docs
├─ Userguide
├─ SystemDesign
├─ Download
│  └─ README.md
├─ etc.
└─ zh
   ├─ Userguide
   ├─ SystemDesign
   └─ Download
      └─ README.md
   ├─ etc.
```

## 导航栏

nav表示最顶端的导航栏，修改nav注意自己修改的中文还是英文版本

其中title表示该nav其中一项的标题，items就是下拉列表

![1585829024864](https://user-images.githubusercontent.com/37333508/78358190-57d8ad80-75e5-11ea-87cb-48da2eb5383a.png)

```js
			nav: [
				 {
					text: 'Document',
					items: [
						{ text: 'In progress', link: '/UserGuide/master/0-Get Started/1-QuickStart' },
						{ text: 'V0.9.x', link: '/UserGuide/V0.9.x/0-Get Started/1-QuickStart' },
					    { text: 'V0.8.x', link: '/UserGuide/V0.8.x/0-Get Started/1-QuickStart'},
					]
				  },
```

## 侧边栏

最前面的是跳转链接，children是补充该文档的跳转链接

**总的链接就是 该网站域名+指定的语言版本（/或/zh/）+sidebar的链接+children的链接**

```js
			sidebar: {
				'sidebar的链接': [
					{
						title:'sidebar的标题',
						collapsable: false,//这句表示默认不折叠，即一直保持打开状态
                        children: [
							'children的链接1',
							'children的链接2',
							'children的链接3'
						]
					},
```

关于sidebar其他属性配置，见vuepress教程https://www.vuepress.cn/zh/theme/default-theme-config.html#%E4%BE%A7%E8%BE%B9%E6%A0%8F

如果觉得找到in progress较为麻烦，直接ctrl+F搜索**/UserGuide/master/**跳转到那一块，中文是**/zh/UserGuide/master/**

![1585829139646](https://user-images.githubusercontent.com/37333508/78358128-42638380-75e5-11ea-84bd-76e3864f191e.png)

```js
			sidebar: {
				'/UserGuide/master/': [
					{
						title:'User Guide(In progress)',
						collapsable: false,
					},
					{
						title: '0-Get Started',
						children: [
							'0-Get Started/1-QuickStart',
							'0-Get Started/2-Frequently asked questions',
							'0-Get Started/3-Publication'
						]
					},
```
