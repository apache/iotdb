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
import {getData} from "../utils/getData";
import {Table} from "antd";

const MyTable = () => {
    const [sqlArgument, setSqlArgument] = useState([{
        path: "",
        physicalPlan: "",
        startTime: "",
        endTime: "",
        time: 0,
        operatorType: "",
        sql: "",
        status: ""
    }]);
    useEffect(() => {
        getData.getSqlArgument().then(r => setSqlArgument(r));
    }, []);
    console.log(sqlArgument);
    return (
        <Table
            columns={columns}
            expandable={{
                expandedRowRender: record =>
                    <div>
                        <p style={{ margin: 0 }}>PhysicalPlan: {record.physicalPlan}</p>
                        <p style={{ margin: 0 }}>Path: {record.path}</p>
                    </div>,
                rowExpandable: record => record.status !== 'Finished',
            }}
            dataSource={sqlArgument}
        />
    );

};

const columns = [
    {
        title: 'OperatorType',
        dataIndex: 'operatorType',
        key: 'operatorType'
    },
    {
        title: 'StartTime',
        dataIndex: 'startTime',
        key: 'startTime',
    },
    {
        title: 'EndTime',
        dataIndex: 'endTime',
        key: 'endTime',
    },
    {
        title: 'Duration',
        dataIndex: 'time',
        key: 'time'
    },
    {
        title: 'Statement',
        dataIndex: 'sql',
        key: 'sql'
    },
    {
        title: 'Status',
        dataIndex: 'status',
        key: 'status'
    }
];



export default MyTable;
