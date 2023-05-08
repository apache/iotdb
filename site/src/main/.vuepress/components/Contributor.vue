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

<template>
  <div>

    <h1 style="text-align: center;">Project Mentors</h1>
    <div v-for="(item,i) in mentors" class="box">
      <img :src="item.avatar">
      <div class="right">
        <p class="con-name">{{item.name}}</p>
        <div class="con-detailed-info">
          <p>{{item.role}}</p>
        </div>
        <div class="con-apache-info">
          <p class="con-id">Apache ID: {{item.id}}</p>
          <p class="con-date">Starting date: {{item.date}}</p>
        </div>
      </div>
    </div>

    <h1 style="text-align: center;">Project Committers</h1>
    <div v-for="(item,i) in conList" class="box">
      <img :src="item.avatar">
      <div class="right">
        <p class="con-name">{{item.name}}</p>
        <div class="con-detailed-info">
          <p>{{item.role}}</p>
        </div>
        <div class="con-apache-info">
          <p class="con-id">Apache ID: {{item.id}}</p>
          <p class="con-date">Starting date: {{item.date}}</p>
        </div>
      </div>
    </div>

  </div>
</template>

<script lang="ts">

  export default {
    name: "Community",
    data() {
      return {
        msg: 'Welcome to Community Page',
        md: "",
        toc: "",
        mentors:[
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Justin Mclean',
          role: 'Mentor',
          id: 'jmclean',
          date: '2018/11/18'
        },
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Christofer Dutz',
          role: 'Mentor',
          id: 'cdutz',
          date: '2018/11/18'
        },
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Willem Ning Jiang',
          role: 'Mentor',
          id: 'ningjiang',
          date: '2018/11/18'
        },
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Kevin A. McGrail',
          role: 'Champion',
          id: 'kmcgrail',
          date: '2018/11/18'
        },
        ],
        conList: [
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Gaofei Cao',
          role: 'PMC, Committer',
          id: 'caogaofei',
          date: '2018/11/18'
        },
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Yi Xu',
          role: 'PMC, Committer',
          id: 'dope',
          date: '2018/11/18'
        }, 
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Dongfang Mao',
          role: 'PMC, Committer',
          id: 'east',
          date: '2018/11/18'
        }, 
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Wangminhao Gou',
          role: 'Committer',
          id: 'ejttianyu',
          date: '2020/03/03'
        }, 
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Boris Zhu',
          role: 'Committer',
          id: 'geniuspig',
          date: '2019/12/19'
        }, 
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Haonan Hou',
          role: 'PMC, Committer',
          id: 'haonan',
          date: '2020/01/30'
        },
        {
          avatar: '/img/contributor-avatar/xdh.jpg',
          name: 'Xiangdong Huang',
          role: 'PMC, Committer',
          id: 'hxd',
          date: '2018/11/18'
        }, 
        {
          avatar: '/img/contributor-avatar/yt.jpg',
          name: 'Yuan Tian',
          role: 'Committer',
          id: 'jackietien',
          date: '2019/12/18'
        }, 
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Julian Feinauer',
          role: 'PMC, Committer',
          id: 'jfeinauer',
          date: '2019/07/15'
        },
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Tian Jiang',
          role: 'PMC, Committer',
          id: 'jiangtian',
          date: '2018/11/18'
        }, 
        {
          avatar: 'http://ise.thss.tsinghua.edu.cn/~wangjianmin/jimwang.jpg',
          name: 'Jianmin Wang',
          role: 'PMC, Committer',
          id: 'jimwang',
          date: '2018/11/18'
        }, 
        {
          avatar: 'https://avatars1.githubusercontent.com/u/22488084?s=400&u=2ef5ad52a7c4241bf88fb5e45682948b307a7d90&v=4',
          name: 'Jincheng Sun',
          role: 'PMC, Committer',
          id: 'jincheng',
          date: '2020/01/21'
        },
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Rong Kang',
          role: 'PMC, Committer',
          id: 'kangrong',
          date: '2018/11/18'
        }, 
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Lei Rui',
          role: 'PMC, Committer',
          id: 'leirui',
          date: '2019/06/20'
        }, 
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Dawei Liu',
          role: 'PMC, Committer',
          id: 'liudw',
          date: '2020/03/01'
        },
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Kun Liu',
          role: 'PMC, Committer',
          id: 'liukun',
          date: '2018/11/18'
        }, 
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Rui Liu',
          role: 'PMC, Committer',
          id: 'liurui',
          date: '2018/11/18'
        }, 
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Tianan Li',
          role: 'PMC, Committer',
          id: 'lta',
          date: '2018/11/18'
        }, 
        {
          avatar: '/img/contributor-avatar/jlq.png',
          name: 'Jialin Qiao',
          role: 'PMC, Committer',
          id: 'qiaojialin',
          date: '2018/11/18'
        }, 
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Shuo Zhang',
          role: 'PMC, Committer',
          id: 'shuozhang',
          date: '2018/11/18'
        }, 
        {
          avatar: '/img/contributor-avatar/zss.jpg',
          name: 'Zesong Sun',
          role: 'Committer',
          id: 'sunzesong',
          date: '2019/12/23'
        }, 
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Yue Su',
          role: 'PMC, Committer',
          id: 'suyue',
          date: '2018/11/18'
        }, 
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Zonghan Cai',
          role: 'Committer',
          id: 'tsaitsunghan',
          date: '2019/11/05'
        },
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Chen Wang',
          role: 'PMC, Committer',
          id: 'wangchen',
          date: '2018/11/18'
        }, 
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Jinrui Zhang',
          role: 'PMC, Committer',
          id: 'xingtanzjr',
          date: '2018/11/18'
        }, 
        {
          avatar: 'https://avatars0.githubusercontent.com/u/6711230?s=460&u=b8f03cad5891dbe4eab23ca75df3d97358b23b0c&v=4',
          name: 'Xin Wang',
          role: 'Committer',
          id: 'xinwang',
          date: '2020/05/18'
        },
        {
          avatar: '/img/contributor-avatar/kfx.jpeg',
          name: 'Kaifeng Xue',
          role: 'Committer',
          id: 'xuekaifeng',
          date: '2018/12/19'
        }, 
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Jun Yuan',
          role: 'PMC, Committer',
          id: 'yuanjun',
          date: '2018/11/18'
        }, 
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Stefanie Zhao',
          role: 'PMC, Committer',
          id: 'zhaoxinyi',
          date: '2018/11/18'
        },
        {
          avatar:'/img/contributor-avatar/cw.jpg',
          name: 'Chao Wang',
          role: 'PMC, Committer',
          id: 'chaow',
          date: '2020/09/04'
        },
        {
          avatar:'/img/contributor-avatar/qhl.jpg',
          name: 'Houliang Qi',
          role: 'PMC, Committer',
          id: 'neuyilan',
          date: '2020/12/18'
        },
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Xinyu Tan',
          role: 'Committer',
          id: 'LebronAl',
          date: '2020/12/21'
        },
        {
          avatar:'/img/contributor-avatar/xww.jpg',
          name: 'Xiangwei Wei',
          role: 'Committer',
          id: 'Alima777',
          date: '2021/02/20'
        },
        {
          avatar:'/img/contributor-avatar/yrzh.jpg',
          name: 'Jesse Zhou',
          role: 'Committer',
          id: 'JesseZhou-1',
          date: '2021/02/20'
        },
        {
          avatar:'https://avatars.githubusercontent.com/u/30497621',
          name: 'Steve Yurong Su',
          role: 'Committer',
          id: 'rong',
          date: '2021/05/12'
        },
        {
          avatar:'/img/contributor-avatar/user.svg',
          name: 'Chao Wang',
          role: 'Committer',
          id: 'wangchao316',
          date: '2021/06/30'
        },
        {
          avatar:'/img/contributor-avatar/ericpai.jpg',
          name: 'Jian Bai',
          role: 'Committer',
          id: 'ericpai',
          date: '2021/12/24'
        },
        ]
      }
    },
    created() {
    },
    methods: {
      parse(html) {
        return Global.isReadyForPrerender(html)
      }
    }
  }
</script>
<style scoped>

   .box{
      width:50%;
      height:200px;
      display: inline-block;
      vertical-align: top;
      text-align: center;
   }

   img{
    vertical-align: top;
    margin-right: 20px;
    display: inline-block;
    width: 100px;
    height: 120px;
   }

   .right{
    display: inline-block;
    font-size: 14px;
    height: 120px;
    width: 180px;
    padding: 0;
   }

  .con-detailed-info p {
    margin: 0;
    line-height: 10px;
  }

  .con-detailed-info {
    margin-bottom: 10px;
  }

  .con-apache-info p {
    margin: 0;
    color: #888;
  }

  .con-name {
    font-size: 16px;
    font-weight: 500;
  }

</style>