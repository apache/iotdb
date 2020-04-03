# 文档格式

- 所有的md都会被编译成html，REDEME.md编译为index.html，xx.md编译为xx.html
- md内标签必须有开头有结尾，比如<tr>必须有</tr>与之对应，而且是严格对应；如果文档中需要插入标签，比如List<String>，可以加这个放入代码块中```，也可以在**俩个**尖括号前加上\，如\<String\>
-  标签不能交叉嵌套，比如<p><center></p></center>这是不允许的
- 文章的一级标题就是该文档对应sidebar的标题，所以文档最大的章节标题就不需要了

# 贡献文档

- 只修改已有文档：只需要对文档里的内容进行修改，不要修改文件名（修改文件名就需要到config.js修改）
- 增加文档：将增加的文档放在自己相放的目录，记录下目录的链接路径，到.vuepress/config.js中修改

# 修改config.js

head,base, title,description等几乎不用修改

具体配置信息看 https://www.vuepress.cn/config/

 

其中locales

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

README.md默认编译为index.html，其余的xx.md编译为xx.html

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

sidebar:最前面的是跳转链接，children是补充该文档的跳转链接

总的链接就是 该网站域名+指定的语言版本（/或/zh/）+sidebar的链接+children的链接

关于sidebar其他属性配置，见vuepress教程https://www.vuepress.cn/zh/theme/default-theme-config.html#%E4%BE%A7%E8%BE%B9%E6%A0%8F

如果觉得找到in progress较为麻烦，直接ctrl+F搜索/UserGuide/master/跳转到那一块，中文是/zh/UserGuide/master/

![1585829139646](https://user-images.githubusercontent.com/37333508/78358128-42638380-75e5-11ea-84bd-76e3864f191e.png)

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



