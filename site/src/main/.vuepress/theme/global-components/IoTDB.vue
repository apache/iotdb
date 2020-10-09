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

<template >
  <div style="background:linear-gradient(top,#A2A2A2,#fff);">
    
    <div style="width:100%;margin: 0 auto;position: relative;height:480px;text-align:center;">
      <h2 class="h2">Apache IoTDB</h2>
      <p
        style="font-size: 20px;line-height:23px;margin: 10px 0 20px 0;font-family: 'Arimo', sans-serif;
    text-transform: uppercase;
    font-weight: 700;
    letter-spacing: 0.03em;
    color: #777;
    text-align: center;"
      >Database for Internet of Things</p>
      <p style="font-size: 20px;margin: 50px 0 10px 0;text-align:justify!important;">
        Apache IoTDB (Database for Internet of Things) is an IoT native database with high performance
        for data management and analysis, deployable on the edge and the cloud. Due to its light-weight architecture,
        high performance and rich feature set together with its deep integration with Apache
        Hadoop, Spark and Flink, Apache IoTDB can meet the requirements of massive
        data storage, high-speed data ingestion and complex data analysis in the IoT
        industrial fields.
      </p>
      <el-row style="margin-top:40px;text-align: center;">
        <el-button
          type="warning"
          round
          style="width:220px;
    font-size: 18px;    letter-spacing: 0.03em;font-family: 'Arimo', sans-serif;"
          @click="addRoutes1"
        >Download</el-button>
        <el-button
          type="info"
          round
          style="width:220px;
    font-size: 18px;    letter-spacing: 0.03em;font-family: 'Arimo', sans-serif;"
          @click="addRoutes2"
        >QuickStart</el-button>
      </el-row>
    </div>



    <p class="home-title" style="font-size: 50px;">Scenarios</p>

    <div class="block">
        <el-carousel trigger="click" height="700px">
          <el-carousel-item v-for="(item,index) in imgBlock" :key="index" style="text-align:center;">
            <img :src="item.src" height="500px">
            <h3 style="font-size: 30px;color: #fcac45;text-align: center;line-height: 0px;">{{item.des}}</h3>
            <p style="font-size: 18px;padding:10px;line-height: 22px;text-align:justify!important;font-weight:bold;">{{item.detail}}</p>
          </el-carousel-item>
        </el-carousel>
    </div>

    <p class="home-title" style="font-size: 50px;">Main Features</p>

    <el-row style="margin-top:50px;" :gutter="20">
      <el-col :span="8" style="text-align:center;">
          <el-popover
            placement="top-start"
            title="High-throughput read and write"
            width="350"
            trigger="hover"
            content="Apache IoTDB can support high-speed write access for millions of low-power and intelligently networked devices. It also provides lightning read access for retrieving data."
            >
            <el-button slot="reference" style="text-align:center;width:100%;">
              <img src="/img/home-icon1.png" style="width:40px;height:40px;">
              <p>High-throughput read and write</p>
              </el-button>
          </el-popover>
      </el-col>
      <el-col :span="8" style="text-align:center;">
          <el-popover
            placement="top-start"
            title="Efficient directory structure"
            width="350"
            trigger="hover"
            content="Apache IoTDB can efficiently organize complex data structure from IoT devices and large size of timeseries data with fuzzy searching strategy for complex directory of timeseries data."
            >
            <el-button slot="reference" style="text-align:center;width:100%;">
              <img src="/img/home-icon2.png" style="width:40px;height:40px;">
             <p> Efficient directory structure</p></el-button>
          </el-popover>
      </el-col>
      <el-col :span="8" style="text-align:center;">
          <el-popover
            placement="top-start"
            title="Rich query semantics"
            width="400"
            trigger="hover"
            content="Apache IoTDB can support time alignment for timeseries data across devices and sensors, computation in timeseries field and abundant aggregation functions in time dimension."
            >
            <el-button slot="reference" style="text-align:center;width:100%;">
             <img src="/img/home-icon3.png" style="width:40px;height:40px;">
             <p>Rich query semantics</p></el-button>
          </el-popover>
      </el-col>
    </el-row>

    <el-row style="margin-top:100px;height:300px;" :gutter="20">
      <el-col :span="8" style="text-align:center;">
          <el-popover
            placement="top-start"
            title="Low cost on hardware"
            width="400"
            trigger="hover"
            content="Apache IoTDB can reach a high compression ratio of disk storage (it costs less than $0.23 to store 1GB of data on hard disk)."
            >
            <el-button slot="reference" style="text-align:center;width:100%;">
             <img src="/img/home-icon4.png" style="width:40px;height:40px;">
             <p>Low cost on hardware</p></el-button>
          </el-popover>
      </el-col>
      <el-col :span="8" style="text-align:center;">
          <el-popover
            placement="top-start"
            title="Flexible deployment"
            width="400"
            trigger="hover"
            content="Apache IoTDB can provide users one-click installation on the cloud, terminal tool on desktop and the bridge tool between cloud platform and on premise machine (Data Synchronization Tool)."
            >
            <el-button slot="reference" style="text-align:center;width:100%;">
             <img src="/img/home-icon5.png" style="width:40px;height:40px;">
             <p>Flexible deployment</p></el-button>
          </el-popover>
      </el-col>
      <el-col :span="8" style="text-align:center;">
          <el-popover
            placement="top-start"
            title="Intense integration with Open Source Ecosystem"
            width="400"
            trigger="hover"
            content="Apache IoTDB can support analysis ecosystems, for example, Hadoop, Spark, Flink and Grafana （visualization tool）."
            >
            <el-button slot="reference" style="text-align:center;width:100%;">
             <img src="/img/home-icon6.png" style="width:40px;height:40px;">
             <p style="line-height: 5px;">Intense integration with </p>
             <p style="line-height: 5px;">Open Source Ecosystem</p></el-button>
          </el-popover>
      </el-col>
    </el-row>

  </div>
</template>

<script>
export default {
  name: "IoTDB",
  data() {
    return {
      msg: "Welcome to Home Page",
      imgBlock:[
        {
          src: "/img/home-Slide1.png",
          des:"High-end equipment",
          detail:" In high-end manufacturing, there are many devices equipped with sensors collecting working status data.Meteorological stations, wind turbines are common high-end devices.These devices, if supports Java or Go (in development), can run TsFile to store data locally.In this way, TsFile can provide the ability to save data with high throughput, high compression ratio, and millisecond query.Together with TsFile-Sync tool, TsFiles can be transmitted to the data center."
        },
        {
          src: "/img/home-Slide2.png",
          des:"Local Controller",
          detail:"In the scene of factories, there are tens of devices under LAN network. IoTDB can be installed on a local controller server in the factory to receive data from those devices. The local controller server (normal PC or workstation) with IoTDB can provide the ability to persist data and query data with SQL-like interface. In addition, with TsFile-Sync tool, TsFiles on the local controller can be transmitted to the data center equipped with IoTDB instance in the cloud."
        },
        {
          src: "/img/home-Slide3.png",
          des:"Cloud Data Management",
          detail:"In the scene of high-speed networks (Internet of Vehicles, etc.), a car installed of sensors, can collect monitoring information (driving status, etc.) of itself at a certain frequency. Usually, these automotive devices have limited hardware configurations and are difficult to carry complex applications. Here, lightweight IoTDB(IoTDB Client) came into being. IoTDB's JDBC API can make data that can be sent through narrow-band IoT networks or 4G whichever is possible. In this way, devices and cloud are connected together."
        },
      ],
      isHover: false
    };
  },
  methods: {
    addRoutes1() {
      this.$router.push("/Download/");
    },
    addRoutes2() {
      this.$router.push("/UserGuide/V0.10.x/Get%20Started/QuickStart.html");
    }
  }
};
</script>

<style scoped>

  .el-row {
    margin-bottom: 20px;
    height: 50px;
    &:last-child {
      margin-bottom: 0;
    }
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


@media (min-width: 200px) {
  .carousel-inner {
    min-height: 530px;
  }
}

@media (min-width: 768px) {
  .feature-item {
    min-height: 220px;
  }
  .carousel-inner {
    min-height: 520px;
  }
}

@media (min-width: 992px) {
  .feature-item {
    min-height: 240px;
  }
  .carousel-inner {
    min-height: 580px;
  }
}

@media (min-width: 1200px) {
  .feature-item {
    min-height: 210px;
  }
  .carousel-inner {
    min-height: 650px;
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
