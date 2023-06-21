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
  <div style="background:linear-gradient(top,#A2A2A2,#fff);">
    <h2 class="h2" style="font-size:80px;">Apache IoTDB</h2>
    <p class="project-name">物联网数据库</p>
    <p style="font-size: 20px;margin: 50px 0 10px 0;text-align:justify!important;">
      Apache IoTDB（物联网数据库）是一体化收集、存储、管理与分析物联网时序数据的软件系统。
      Apache IoTDB 采用轻量式架构，具有高性能和丰富的功能，并与Apache Hadoop、Spark和Flink等进行了深度集成，可以满足工业物联网领域的海量数据存储、高速数据读取和复杂数据分析需求。
    </p>
    <div style="margin-top:40px; text-align: center">
      <el-button
        type="warning"
        round
        style="font-size: 18px;letter-spacing: 0.03em;font-family: 'Arimo', sans-serif;"
        @click="addRoutes1"
      >下载</el-button>
      <el-button
        type="info"
        round
        style="font-size: 18px;letter-spacing: 0.03em;font-family: 'Arimo', sans-serif;"
        @click="addRoutes2"
      >快速开始</el-button>
    </div>

    <p class="home-title" style="font-size: 50px;">应用场景</p>

    <div class="block">
      <el-carousel trigger="click" height="100vh" indicator-position="none">
        <el-carousel-item v-for="(item, index) in imgBlock" :key="index" style="text-align:center;height: auto;">
          <img :src="item.src" alt="" style="width: 80%; height: auto;">
          <h3 style="font-size: 30px;color: #fcac45;text-align: center;line-height: normal;">{{item.des}}</h3>
          <p style="font-size: 18px;padding:15px;line-height: 22px;text-align:justify!important;font-weight:bold;">{{item.detail}}</p>
        </el-carousel-item>
      </el-carousel>
    </div>

    <p class="home-title" style="font-size: 50px;">主要特点</p>

    <el-row style="margin-top:50px;" :gutter="20">
      <el-col :xs="24" :sm="24" :md="12" :lg="8" style="text-align:center;margin-bottom:10px;">
        <el-popover
          placement="top-start"
          title="高吞吐量读写"
          width="325"
          trigger="hover"
          content="Apache IoTDB中可以支持数百万个低功耗和智能联网设备的高速写访问。 它还提供数据快速读取访问以查询。"
        >
          <template #reference>
            <div class="feature-item">
              <img src="/img/home-icon1.png" alt="" style="width:40px;height:40px;">
              <p>高吞吐量读写</p>
            </div>
          </template>
        </el-popover>
      </el-col>
      <el-col :xs="24" :sm="24" :md="12" :lg="8" style="text-align:center;margin-bottom:10px;">
        <el-popover
          placement="top-start"
          title="高效的目录结构"
          width="325"
          trigger="hover"
          content="Apache IoTDB可以对拥有复杂组织关系的物联网设备进行树形结构管理，并使用通配符对这些元数据进行模糊匹配。"
        >
          <template #reference>
            <div class="feature-item">
              <img src="/img/home-icon2.png" alt="" style="width:40px;height:40px;">
              <p> 高效的树形元数据结构</p></div>
          </template>

        </el-popover>
      </el-col>
      <el-col :xs="24" :sm="24" :md="12" :lg="8" style="text-align:center;margin-bottom:10px;">
        <el-popover
          placement="top-start"
          title="丰富的查询语义"
          width="325"
          trigger="hover"
          content="Apache IoTDB可以支持跨设备和传感器的时间对齐查询，在时间维度上的聚合（降采样）等。"
        >
          <template #reference>
            <div class="feature-item">
              <img src="/img/home-icon3.png" alt="" style="width:40px;height:40px;">
              <p>丰富的查询语义</p></div>
          </template>
        </el-popover>
      </el-col>
      <el-col :xs="24" :sm="24" :md="12" :lg="8" style="text-align:center;margin-bottom:10px;">
        <el-popover
          placement="top-start"
          title="硬件成本低"
          width="325"
          trigger="hover"
          content="Apache IoTDB可以实现磁盘存储的高压缩率。"
        >
          <template #reference>
            <div class="feature-item">
              <img src="/img/home-icon4.png" alt="" style="width:40px;height:40px;">
              <p>硬件成本低</p></div>
          </template>
        </el-popover>
      </el-col>
      <el-col :xs="24" :sm="24" :md="12" :lg="8" style="text-align:center;margin-bottom:10px;">
        <el-popover
          placement="top-start"
          title="灵活的部署"
          width="325"
          trigger="hover"
          content="Apache IoTDB可以为用户提供云上的一键式安装、终端访问工具以及边-云之间的协同（数据同步工具）。"
        >
          <template #reference>
            <div class="feature-item">
              <img src="/img/home-icon5.png" alt="" style="width:40px;height:40px;">
              <p>灵活的部署</p></div>
          </template>
        </el-popover>
      </el-col>
      <el-col :xs="24" :sm="24" :md="12" :lg="8" style="text-align:center;margin-bottom:10px;">
        <el-popover
          placement="top-start"
          title="与开源生态系统的紧密集成"
          width="325"
          trigger="hover"
          content="Apache IoTDB支持许多大数据软件生态系统，例如Hadoop、Spark、Flink和Grafana（可视化工具）。"
        >
          <template #reference>
            <div class="feature-item">
              <img src="/img/home-icon6.png" alt="" style="width:40px;height:40px;">
              <p>与开源生态系统的紧密集成</p>
            </div>
          </template>
        </el-popover>
      </el-col>
    </el-row>
    <span style="margin: 10px"></span>
  </div>
</template>

<script lang="ts">
import { defaultVersion } from '../utils';

export default {
  name: 'IoTDBZH',
  data() {
    return {
      msg: 'Welcome to Home Page',
      imgBlock: [
        {
          src: '/img/home-Slide1.png',
          des: '高端设备',
          detail: '在高端制造业中，有很多设备配备有传感器来收集工作状态数据，例如气象站，风力涡轮机是常见的高端设备。这些设备如果支持Java或Go（正在开发中），则可以运行TsFile在本地存储数据。通过这种方式，TsFile可以提供具有高吞吐、高压缩率和毫秒级查询延迟的数据管理功能。结合TsFile-Sync工具，可以将TsFiles同步到数据中心。',
        },
        {
          src: '/img/home-Slide2.png',
          des: '本地控制器',
          detail: '在工厂现场，LAN网络下有数十台设备。 IoTDB可以安装在工厂的本地控制器服务器上，以从这些设备接收数据。 安装有IoTDB的本地服务器（普通PC或工作站）可以使用类SQL存储和查询数据。 此外，使用TsFile-Sync工具，可以将本地控制器上的TsFile文件传输到云上安装有IoTDB实例的数据中心。',
        },
        {
          src: '/img/home-Slide3.png',
          des: '云数据管理',
          detail: '在高速网络（车联网等）的场景中，安装有传感器的汽车可以以一定频率收集自身的监视信息（行驶状态等）。 通常，这些汽车设备的硬件配置有限，并且难以进行复杂的应用。 轻量级的IoTDB（IoTDB客户端）应运而生。 借助JDBC API（或MQTT），它可以使用窄带IoT或4G/5G发送数据，从而将设备和云连接在一起。',
        },
      ],
      isHover: false,
    };
  },
  methods: {
    addRoutes1() {
      window.location.href = '/zh/Download/';
    },
    addRoutes2() {
      window.location.href = `/zh/UserGuide/${defaultVersion}/QuickStart/QuickStart.html`;
    },
  },
};
</script>

<style lang="scss" scoped>
.project-name {
  font-size: 20px;line-height:23px;margin: 10px 0 20px 0;font-family: 'Arimo', sans-serif;
      text-transform: uppercase;
      font-weight: 700;
      letter-spacing: 0.03em;
      color: #777;
      text-align: center;
}
  .el-row {
    margin-bottom: 20px;
    &:last-child {
      margin-bottom: 0;
    }
  }
  .feature-item {
    border: 1px solid var(--note-border-color);
    text-align:center;
    margin: 0 10px;
    width:100%;
    box-sizing: border-box;
  }
  .el-row p{
    font-size: 18px;
  }
  .el-col {
    border-radius: 4px;

  }
  .bg-purple-dark {
    background: #99a9bf;
  }
  .bg-purple {
    background: #d3dce6;
  }
  .bg-purple-light {
    background: #e5e9f2;
  }
  .grid-content {
    border-radius: 4px;
    min-height: 36px;
  }
  .row-bg {
    padding: 10px 0;
    background-color: #f9fafc;
  }

  .el-carousel__item:nth-child(2n) {
     background-color: #99a9bf;
  }

  .el-carousel__item:nth-child(2n+1) {
     background-color: #d3dce6;
  }

  @media only screen and (min-width:320px)  {
    .block {
      margin-bottom: -300px;
    }
  }

  @media only screen and (min-width:481px)  {
    .block {
      margin-bottom: -550px;
    }
  }

  @media only screen and (min-width:769px)  {
    .block {
      margin-bottom: -500px;
    }
  }

  @media only screen and (min-width:1025px) {
    .block {
      margin-bottom: 0;
    }
  }

  @media only screen and (min-width:1201px) {
    .block {
      margin-bottom: 0;
    }
  }

.Scenarios {
  padding: 5px 10px;
  color: white;
  margin-left: 10px;
  margin-right: 10px;
}

h2 {
  margin: 0 auto;
  padding: 0 5%;
  opacity: 0.9;
  color: #3f3b3f;
  font-size: 108px;
  text-align: center;
  text-transform: none;
  font-weight: 700;
  letter-spacing: 0.03em;
}

  .home-title {
    color: #ffa500;
    font-size: x-large;
    font-weight: 600;
    text-align: center;
  }
</style>
