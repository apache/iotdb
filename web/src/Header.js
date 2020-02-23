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
import './Header.css'
import logo from './iotdb-logo.png'

class Header extends React.Component{
    constructor(props) {
        super(props);
        this.state = {
            version : ""
        }
    }

    componentDidMount() {
        fetch('http://localhost:8181/version', {
            method:'GET',
            headers:{
                Accept: 'text/plain',
                'Content-Type':'text/plain;charset=UTF-8'
            },
            cache:'default'
        }).then(request => {
            const test = request.text();
            test.then(result => {
               this.setState({version : result})
            });
        })
    }

    render() {
        return(
            <div className="header">
                <div className="word">
                    <img src={logo} alt="logo"/>
                    <span className="version">{this.state.version}</span>
                    <span className="IoTDB">IoTDB Metrics Server</span>
                </div>
                <div className="line"/>
            </div>
        )
    }
}

export default Header;
