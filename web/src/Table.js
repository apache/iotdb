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
import './Table.css'

class Table extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            lists: []
        };
        this.getMachineAction = this.getMachineAction.bind(this);
    }

    async getMachineAction() {
        try {
            await fetch( 'http://localhost:8181/sql_arguments', {
                method:'GET',
                headers:{
                    Accept: 'application/json',
                    'Content-Type':'application/json;charset=UTF-8'
                },
                cache:'default'
            }).then(
                response => {
                    const json = response.json();
                    json.then(json => {
                        console.log(json);
                        this.setState({lists : json})
                    })
                }
            );
        } catch (err) {
            // catches errors both in fetch and response.json
            console.log(err);
        }
    };

    componentDidMount(){
        this.getMachineAction();
        this.timerID = setInterval(this.getMachineAction, 5000);
    }

    componentWillUnmount() {
        clearInterval(this.timerID);
    }

    render() {
        const rows = [];
        for (let i = 0; i < this.state.lists.length; i++) {
            // note: we add a key prop here to allow react to uniquely identify each
            // element in this array. see: https://reactjs.org/docs/lists-and-keys.html
            let json = this.state.lists[i];
            let detail = json.errMsg;
            if(detail === "") {
                detail = "== Parsed Physical Plan =="
            }
            rows.push(
                <tr className="tableHeader">
                    <td>{" "}{json.operatorType}</td>
                    <td>{" "}{json.startTime}</td>
                    <td>{" "}{json.endTime}</td>
                    <td>{" "}{json.time}</td>
                    <td>{" "}{json.sql}</td>
                    <td>{" "}{json.status}</td>
                    <td>{" "}{json.physicalPlan}</td>
                </tr>
            );
        }
        return (
            <div className="sql">
                <p>Execute sql</p>
                <table>
                <tbody>
                    <tr className="tableHeader">
                        <td>{" "}Operation Type</td>
                        <td>{" "}Start Time</td>
                        <td>{" "}Finish Time</td>
                        <td>{" "}Duration</td>
                        <td>{" "}Statement</td>
                        <td>{" "}State</td>
                        <td>{" "}Physical Plan</td>
                    </tr>
                    {rows}
                </tbody>
                </table>
            </div>
        )
    }

}

export default Table;
