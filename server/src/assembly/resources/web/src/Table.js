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
      json : []
    }
  }
  componentDidMount() {
    this.getMachineAction().catch(e => console.log(e))
  }

   async getMachineAction() {
     let data = {"sql": "show timeseries"};
     try {
       await fetch('http://localhost:8181/rest/sql', {
         method: 'POST',
         headers: {
           Accept: 'application/json',
           'Content-Type': 'application/json;charset=UTF-8',
           Authorization: 'Basic cm9vdDpyb290'
         },
         cache: 'default',
         body: JSON.stringify(data)
       }).then(
           response => {
             const json = response.json();
             console.log(json);
             this.setState({json : json})
           });
     } catch (err) {
       // catches errors both in fetch and response.json
       console.log(err);
     }
   };

  render() {
    return <div>{this.state.json}</div>
  }

}

export default Table;
