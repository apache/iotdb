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
import React from 'react'
import './ServerInfo.css'

class ServerInfo extends React.Component{
    constructor(props) {
        super(props);
        this.state = {
            list: {
            }
        }
    }

    componentDidMount(){
        fetch('http://localhost:8181/rest/server_information',{
            method:'GET',
            headers:{
                Accept: 'application/json',
                'Content-Type':'application/json;charset=UTF-8'
            },
            cache:'default'
        }).then(response => {
            return response.json()
        }).then(data => this.setState({list : data}))
    }

    render(){
        return (
            <div className="body">
                <p className="one">Server {'   '}URL: <span>{this.state.list.host}:{this.state.list.port}</span></p>
                <p className="two">CPU {'     '} Cores: <span>{this.state.list.cores} Total, {this.state.list.cpu_ratio} CPU Ratio</span></p>
                <p className="three">JVM {'  '}Memory: <span>{this.state.list.max_memory} {this.state.list.total_memory} {this.state.list.free_memory} (MAX/TOTAl/Free)MB</span></p>
                <p className="four">Host {'  '}Memory: <span>{this.state.list.totalPhysical_memory}GB Total, {this.state.list.usedPhysical_memory}GB Used</span></p>
                <p className="five">Status: <span>ALIVE</span></p>
            </div>
        )
    }
}
export default ServerInfo
