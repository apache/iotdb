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
export const getData = {
    getServerInformation,
    getVersion,
    getSqlArgument
};

async function getServerInformation() {
    return await fetch('http://localhost:8181/rest/server_information',{
        method:'GET',
        headers:{
            Accept: 'application/json',
            'Content-Type':'application/json;charset=UTF-8'
        },
        cache:'default'
    }).then(response => {
        return response.json()
    });
}

async function getVersion() {
    return await fetch('http://localhost:8181/rest/version',{
        method:'GET',
        headers:{
            Accept: 'text/plain',
            'Content-Type':'text/plain;charset=UTF-8'
        },
        cache:'default'
    }).then(response => {
        return response.text()
    });
}

async function getSqlArgument() {
    return await fetch('http://localhost:8181/rest/sql_arguments',{
        method:'GET',
        headers:{
            Accept: 'application/json',
            'Content-Type':'application/json;charset=UTF-8'
        },
        cache:'default'
    }).then(response => {
        return response.json()
    });
}
