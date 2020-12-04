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

# Website description

## IoTDB website

[![Build Status](https://builds.apache.org/view/I/view/IoTDB/job/IoTDB%20Website/badge/icon)](https://builds.apache.org/view/I/view/IoTDB/job/IoTDB%20Website/)

See https://iotdb.apache.org/

## Build Setup


run `mvn compile -DskipTests  -P compile-site -P download-website` for doing the following steps automatically:

- get docs from the master branch and all lagecy docs remotely.
- download node.js and npm;
- run `npm install` and `npm run build`
- the final website will be generated in `target/vue-source/src/.vuepress/dist`

Notice that we will only compile the docs from the master branch, and just copy static htmls from 
iotdb-website. If you want to compile lagecy docs, using `-P compile-site-<version>`, 
e.g., `mvn compile -P compile-site -P compile-site-0.11 -P compile-site-0.10`


## How to Debug

after running `mvn compile -DskipTests`, all source codes are copied into target/vue-source

then if you want to debug, just run 

```
# serve with hot reload at localhost:8080
npm run dev

# build for production with minification
npm run build
```

Remeber, only the changes of site/src can be logged by Git. 
All changes in the target folder will be ignored by Git.

## Deploy Manually

run `mvn compile scm-publish:publish-scm -P compile-site`.

Apache ID and passwored is needed.

Or run `mvn compile scm-publish:publish-scm -Dusername={YOUR_APACHE_ID} -Dpassword={YOUR_APACHE_PASSWORD}  -P compile-site`

## FAQ

If you get an error on your MacOS:

> gyp: No Xcode or CLT version detected! 

Then, install CommandLine of Xcode: `sudo xcode-select --install`.
If you have installed and the error still occurs, then `sudo xcode-select --reset`

# Document format

- All mds will be compiled into html, REDEME.md will be compiled into index.html, and xx.md will be compiled into xx.html
- The tags in .md must have a beginning and an end. For example, \<tr> must have a \</ tr> corresponding to it, and it is strictly corresponding; if you need to insert tags in the document, such as List \<String>, you can add this into the code block, You can also add \ before the two  angle brackets, such as \<\\String\\>
- Tags cannot be cross-nested, such as \<p> \<center>\</p>\</center> This is not allowed
- The first-level title of the article is the title of the sidebar of the document, so the largest chapter title of the document is not needed

## Add vue component to the document

Place the components in the .vuepress / components directory

Use \<component/> directly in the md file

Tutorial on the use of vue componentshttps://cn.vuejs.org/v2/guide/components.html

# Contributing documents

- Only modify existing documents: only need to modify the content of the document, do not modify the file name (change the file name to config.js)
- Adding documents: Put the added documents in the directory where you put them together, record the link path of the directory, and modify it in .vuepress / config.js

# Modify config.js

Head, base, title, description, etc. need hardly be modified

See specific configuration information https://www.vuepress.cn/config/

 head,base,title,description

```js
    head: [
		["meta", {name: "Description", content: "Apache IoTDB: Time Series Database for IoT"}],
        ["meta", {name: "Keywords", content: "TSDB, time series, time series database, IoTDB, IoT database, IoT data management,时序数据库, 时间序列管理, IoTDB, 物联网数据库, 实时数据库, 物联网数据管理, 物联网数据"}],
        ["meta", {name: "baidu-site-verification", content: "wfKETzB3OT"}],
        ["meta", {name: "google-site-verification", content: "mZWAoRY0yj_HAr-s47zHCGHzx5Ju-RVm5wDbPnwQYFo"}],
      ],
```

Additional tags that need to be injected into the HTML `<head>` of the current page, each tag can be specified in the format of `[tagName, {attrName: attrValue}, innerHTML?]`

base:The base path of the deployment site. If you want to deploy your site to a sub-path, you will need to set it.

title:The title of the website, it will be used as the prefix of all page titles. At the same time, under the default theme, it will be displayed on the navigation bar (navbar)

description:The description of the website, which will be rendered into the HTML of the current page with the `<meta>` tag.

See specific configuration information https://www.vuepress.cn/config/

## locales

```js
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
```

Represents / jumps English links, / zh / jumps Chinese links.

The storage structure of the file is as follows.

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

## nav

nav indicates the top navigation bar. Modify nav and pay attention to the Chinese or English version you modify.

Where title represents the title of one of the nav, and items is the drop-down list

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

## sidebar

The first one is a jump link, and children is the jump link that supplements this document

**The total link is the domain name of the site + the specified language version (/ or / zh /) + the link of the sidebar + the link of the children**

For other sidebar property configuration, see vuepress tutorialhttps://vuepress.vuejs.org/theme/default-theme-config.html#sidebar

If you find it difficult to find in progress, directly ctrl + F search **/UserGuide/master/** Jump to that piece, Chinese is **/zh/UserGuide/master/**

![1585915743956](https://user-images.githubusercontent.com/37333508/78359162-121ce480-75e7-11ea-87ab-7ab61d5cda39.png)

```js
			sidebar: {
				'/UserGuide/V0.8.x/': [
					{
						title:'User Guide(V0.8.x)',
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



