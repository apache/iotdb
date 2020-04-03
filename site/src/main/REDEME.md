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
# Document format

- All mds will be compiled into html, REDEME.md will be compiled into index.html, and xx.md will be compiled into xx.html
- The tags in md must have a beginning and an end. For example, <tr> must have a </ tr> corresponding to it, and it is strictly corresponding; if you need to insert tags in the document, such as List <String>, you can add this into the code block.  ``, You can also add \ before the two ** angle brackets, such as \<String\>
-  Tags cannot be cross-nested, such as <p> <center> </ p> </ center> This is not allowed
- The first-level title of the article is the title of the sidebar of the document, so the largest chapter title of the document is not needed

# Contributing documents

- Only modify existing documents: only need to modify the content of the document, do not modify the file name (change the file name to config.js)
- Adding documents: Put the added documents in the directory where you put them together, record the link path of the directory, and modify it in .vuepress / config.js

# Modify config.js

Head, base, title, description, etc. need hardly be modified

See specific configuration information https://www.vuepress.cn/config/

 

Where locales

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

README.md is compiled to index.html by default, and the rest of xx.md is compiled to xx.html

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

sidebar: the first one is a jump link, and children is the jump link that supplements this document

The total link is the domain name of the site + the specified language version (/ or / zh /) + the link of the sidebar + the link of the children

For other sidebar property configuration, see vuepress tutorialhttps://vuepress.vuejs.org/theme/default-theme-config.html#sidebar

If you find it difficult to find in progress, directly ctrl + F search / UserGuide / master / Jump to that piece, Chinese is / zh / UserGuide / master /

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



