/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import React, {useEffect, useState} from "react";
import {Col, Layout, Row, PageHeader, Divider, Progress, Tooltip, Button, Affix} from "antd";
import './Metrics.css'
import mgURL from "../logo.png";
import {getData} from "../utils/getData";
import MyTable from "./MyTable";

const { Header, Footer, Content } = Layout;


const Metrics = ()  => {
    const [version, setVersion] = useState("");
    const [serverInformation, setServerInformation] = useState({
        cpu_ratio: 0,
        cores: 0,
        total_memory: 81,
        port: 8181,
        totalPhysical_memory: "8",
        host: "hello",
        free_memory: 54,
        freePhysical_memory: "0",
        usedPhysical_memory: "8",
        max_memory: 1820,
    });
    useEffect(() => {
        getData.getServerInformation().then(r => setServerInformation(r));
        getData.getVersion().then(r => setVersion(r));
    }, []);

    if(serverInformation.cpu_ratio >= 100) {
        serverInformation.cpu_ratio = 100;
    }

    const cpuTip = serverInformation.cores + " Total, " + serverInformation.cpu_ratio+ "% CPU Ratio";

    let usedJVMMemoryRatio = (serverInformation.total_memory - serverInformation.free_memory)
        / serverInformation.total_memory * 100;

    const jvmTip = serverInformation.max_memory + " " + serverInformation.total_memory + " " + serverInformation.free_memory
    + "(MAX/TOTAL/FREE) MB";

    let usedHostMemoryRatio = Number(serverInformation.usedPhysical_memory) / Number(serverInformation.totalPhysical_memory) * 100;

    let hostTip = serverInformation.totalPhysical_memory + " GB Total, "+ serverInformation.usedPhysical_memory + " GB Used";

    let serverURL = "Server URL: " + serverInformation.host + ":" + serverInformation.port;

    return (
        <Layout>
            <Header className="Header">
                <Row>
                    <Col span={1}>
                        <img src={mgURL} alt={"logo"}/>
                    </Col>
                    <Col span={2}/>
                    <Col >
                        <PageHeader
                            className="site-page-header"
                            title="IOTDB Metrics Server"
                            subTitle={version}
                        />
                    </Col>
                </Row>
                <Divider />
            </Header>
            <Content className="Content">
                <br/>
                <Row>
                    <Col span={1}/>
                    <Col span={1}>
                        <Tooltip title= {cpuTip}>
                            <Progress percent={serverInformation.cpu_ratio}
                                      type="circle"
                                      width={90}
                                      format={() => serverInformation.cores + " Cores"}/>
                        </Tooltip>
                    </Col>
                    <Col span={1}/>
                    <Col span={1}>
                        <Tooltip title={jvmTip}>
                            <Progress percent={usedJVMMemoryRatio}
                                      type="circle"
                                      width={90}
                                      format={() => "JVM Memory"}/>
                        </Tooltip>
                    </Col>
                    <Col span={1}/>
                    <Col span={1}>
                        <Tooltip title={hostTip}>
                            <Progress percent={usedHostMemoryRatio}
                                      type="circle"
                                      width={90}
                                      format={() => "Host Memory"}/>
                        </Tooltip>
                    </Col>
                    <Col span={1}/>
                    <Col span={1}>
                        <Affix offsetTop={150} onChange={affixed => console.log(affixed)}>
                            <Button>{serverURL}</Button>
                        </Affix>
                    </Col>
                    <Divider />
                </Row>
                <Row>
                    <Col span={1}/>
                    <Col>
                        <MyTable/>
                    </Col>
                </Row>
            </Content>
            <Footer className="Footer"/>
        </Layout>
    )
};

export default Metrics;
