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

import {Client} from "./thrift/TSIService";
import thrift = require("thrift");
import {
    TSCloseSessionReq,
    TSExecuteStatementReq,
    TSInsertStringRecordReq,
    TSOpenSessionReq,
    TSProtocolVersion,
    TSSetTimeZoneReq
} from "./thrift/rpc_types";
import {TSDataType} from "./utils/IoTDBConstants";

export class Session {

    static SUCCESS_CODE: number = 200;
    static DEFAULT_FETCH_SIZE: number = 10000;
    static DEFAULT_USER: string = "root";
    static DEFAULT_PASSWORD: string = "root";
    static DEFAULT_ZONE_ID: string = Intl.DateTimeFormat().resolvedOptions().timeZone;

    private host: string;
    private port: number;
    private user: string;
    private password: string;
    private fetch_size: number;
    private is_close: boolean;
    private client: Client;
    private protocol_version = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;
    private session_id: string;
    private statement_id: string;
    private zone_id: string;

    constructor(
        host: string,
        port: number,
        user = Session.DEFAULT_USER,
        password = Session.DEFAULT_PASSWORD,
        fetch_size = Session.DEFAULT_FETCH_SIZE,
        zone_id = Session.DEFAULT_ZONE_ID
    ) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.fetch_size = fetch_size;
        this.is_close = true;
        this.client = null;
        this.protocol_version = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;
        this.session_id = null;
        this.statement_id = null;
        this.zone_id = zone_id;
    }

    public async open(enable_rpc_compression: boolean) {
        if (!this.is_close) {
            return;
        }

        const connection = thrift.createConnection('127.0.0.1', 6667, {
            transport: thrift.TFramedTransport,
            protocol: enable_rpc_compression ? thrift.TCompactProtocol : thrift.TBinaryProtocol
        });
        this.client = thrift.createClient(Client, connection);

        let open_req = new TSOpenSessionReq({
            client_protocol: this.protocol_version,
            username: this.user,
            password: this.password,
            zoneId: this.zone_id
        });

        try {
            let open_resp = await this.client.openSession(open_req);

            if (this.protocol_version != open_resp.serverProtocolVersion) {
                console.log(
                    "Protocol differ, Client version is" + this.protocol_version
                    + ", but Server version is " + open_resp.serverProtocolVersion
                );
                // version is less than 0.10
                if (open_resp.serverProtocolVersion == 0) {
                    throw thrift.TTransport.TException("Protocol not supported.");
                }
            }

            this.session_id = open_resp.sessionId;
            this.statement_id = this.client.requestStatementId(this.session_id);
        } catch (err) {
            console.log("session closed because: " + err.message);
        }

        if (this.zone_id !== null) {
            this.set_time_zone(this.zone_id);
        } else {
            this.zone_id = this.get_time_zone();
        }

        this.is_close = false;
    }

    public is_open(): boolean {
        return !this.is_close;
    }

    public close() {
        if (this.is_close) {
            return;
        }

        let req = new TSCloseSessionReq({
            sessionId: this.session_id
        });

        try {
            this.client.closeSession(req);
        } catch (e) {
            console.log("Error occurs when closing session at server. Maybe server is down. Error message: "
                + e);
        } finally {
            this.is_close = true;
        }
    }

    public insert_str_record(device_id, timestamp, measurements, string_values) {
        /* special case for inserting one row of String (TEXT) value */
        if ((typeof string_values) === "string") {
            string_values = [string_values];
        }
        if (typeof measurements === "string") {
            measurements = [measurements];
        }
        let data_types: number[] = new Array(string_values.length);
        for (let j in string_values) {
            data_types[j] = TSDataType.TEXT;
        }
        let request = this.gen_insert_str_record_req(
            device_id, timestamp, measurements, data_types, string_values
        );
        let status = this.client.insertStringRecord(request);
        console.log(
            "insert one record to device " + device_id
            + " message: " + status.message
        );

        return Session.verify_success(status);
    }

    public gen_insert_str_record_req(device_id, timestamp, measurements, data_types, values) {
        if ((values.length != data_types.length) || (values.length != measurements.length)) {
            throw "length of data types does not equal to length of values!";
        }
        return new TSInsertStringRecordReq({
                sessionId: this.session_id,
                deviceId: device_id,
                measurements: measurements,
                values: values,
                timestamp: timestamp
            }
        );
    }

    public execute_query_statement(sql, timeout = 0) {
        /*
        execute query sql statement and returns SessionDataSet
        :param sql: String, query sql statement
        :return: SessionDataSet, contains query results and relevant info (see SessionDataSet.py)
        */
        let request = new TSExecuteStatementReq({
            sessionId: this.session_id,
            statement: sql,
            statementId: this.statement_id,
            fetchSize: this.fetch_size,
            timeout: timeout
        });
        let resp = this.client.executeQueryStatement(request);
        return null;
        // return new SessionDataSet(
        //     sql,
        //     resp.columns,
        //     resp.dataTypeList,
        //     resp.columnNameIndexMap,
        //     resp.queryId,
        //     this.client,
        //     this.session_id,
        //     resp.queryDataSet,
        //     resp.ignoreTimeStamp
        // );
    }

    public get_time_zone() {
        if (this.zone_id !== null) {
            return this.zone_id;
        }
        let resp;
        try {
            resp = this.client.getTimeZone(this.session_id);
        } catch (e) {
            throw "Could not get time zone because: TTransport.TException";
        }
        return resp.timeZone;
    }

    public set_time_zone(zone_id) {
        let request = new TSSetTimeZoneReq({
            sessionId: this.session_id,
            timeZone: zone_id
        });
        try {
            let status = this.client.setTimeZone(request);
            console.log(
                "setting time zone_id as " + zone_id
                + ", message: " + status.message);
        } catch (e) {
            throw "Could not set time zone because: TTransport.TException";
        }
        this.zone_id = zone_id;
    }

    public static verify_success(status) {
        /*
        verify success of operation
        :param status: execution result status
        */
        if (status.code == Session.SUCCESS_CODE) {  // this?
            return 0;
        }

        console.log("error status is" + status);
        return -1;
    }

}
