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
import {
    TSCloseSessionReq,
    TSCreateMultiTimeseriesReq,
    TSCreateTimeseriesReq,
    TSDeleteDataReq,
    TSExecuteStatementReq,
    TSInsertRecordReq,
    TSInsertRecordsOfOneDeviceReq,
    TSInsertRecordsReq,
    TSInsertStringRecordReq,
    TSInsertTabletReq,
    TSInsertTabletsReq,
    TSOpenSessionReq,
    TSProtocolVersion,
    TSSetTimeZoneReq,
    TSCreateAlignedTimeseriesReq,
    TSRawDataQueryReq,
    TSLastDataQueryReq,
    TSInsertStringRecordsOfOneDeviceReq
} from "./thrift/rpc_types";
import {TSDataType} from "./utils/IoTDBConstants";
import {SessionDataSet} from './utils/SessionDataSet';
import thrift = require("thrift");

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
        connection.on("error", function (e) {
            console.log("++++++++++", e);
        });

        let open_req = new TSOpenSessionReq({
            client_protocol: this.protocol_version,
            username: this.user,
            password: this.password,
            zoneId: this.zone_id,
            configuration: {"version": "V_0_13"}
            //new Map<string, string>([["version", "V_0_13"]])
        });

        try {
            let open_resp = await this.client.openSession(open_req);

            if (this.protocol_version != open_resp.serverProtocolVersion) {
                console.log(
                    "Protocol differ, Client version is " + this.protocol_version
                    + ", but Server version is " + open_resp.serverProtocolVersion
                );
                // version is less than 0.10
                if (open_resp.serverProtocolVersion == 0) {
                    throw new Error("Protocol not supported.");
                }
            }

            this.session_id = open_resp.sessionId;
            this.statement_id = await this.client.requestStatementId(this.session_id);
        } catch (err) {
            console.error("session closed because: " + err.message);
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
        } catch (err) {
            console.error("Error occurs when closing session at server. Maybe server is down. Error message: ", err);
        } finally {
            this.is_close = true;
        }
    }

    public async set_storage_group(group_name) {
        /*
        set one storage group
        :param group_name: String, storage group name (starts from root)
        */
        let status = await this.client.setStorageGroup(this.session_id, group_name);
        console.log(
            "setting storage group " + group_name + ", message: " + status.message
        );

        return Session.verify_success(status);
    }

    public delete_storage_group(storage_group) {
        /*
        delete one storage group.
        :param storage_group: String, path of the target storage group.
        */
        let groups = [storage_group];
        return this.delete_storage_groups(groups);
    }

    public async delete_storage_groups(storage_group_lst) {
        /*
        delete multiple storage groups.
        :param storage_group_lst: List, paths of the target storage groups.
        */
        let status = await this.client.deleteStorageGroups(this.session_id, storage_group_lst);
        console.log(
            "delete storage group(s) " + storage_group_lst +
            ", message: " + status.message
        );

        return Session.verify_success(status);
    }

    public async create_time_series(
        ts_path, 
        data_type, 
        encoding, 
        compressor, 
        props=null,
        tags=null,
        attributes=null,
        alias=null) {
        /*
        create single time series
        :param ts_path: String, complete time series path (starts from root)
        :param data_type: TSDataType, data type for this time series
        :param encoding: TSEncoding, encoding for this time series
        :param compressor: Compressor, compressing type for this time series
        :param props: Dictionary, properties for time series
        :param tags: Dictionary, tag map for time series
        :param attributes: Dictionary, attribute map for time series
        :param alias: String, measurement alias for time series  
        */
        let request = new TSCreateTimeseriesReq({
            sessionId: this.session_id,
            path: ts_path,
            dataType: data_type,
            encoding: encoding,
            compressor: compressor,
            props: props,
            tags: tags,
            attributes: attributes,
            measurementAlias: alias
        });
        let status = await this.client.createTimeseries(request);
        console.log(
            "creating time series %s, message: %s", ts_path, status.message
        );

        return Session.verify_success(status);
    }

    public async create_multi_time_series(
        ts_path_lst, 
        data_type_lst, 
        encoding_lst, 
        compressor_lst,
        props_lst=null,
        tags_lst=null,
        attributes_lst=null,
        alias_lst=null) {
        /*
        create multiple time series
        :param ts_path_lst: List of String, complete time series paths (starts from root)
        :param data_type_lst: List of TSDataType, data types for time series
        :param encoding_lst: List of TSEncoding, encodings for time series
        :param compressor_lst: List of Compressor, compressing types for time series
        :param props_lst: List of Props Dictionary, properties for time series
        :param tags_lst: List of tag Dictionary, tag maps for time series
        :param attributes_lst: List of attribute Dictionary, attribute maps for time series
        :param alias_lst: List of alias, measurement alias for time series       
        */
        let request = new TSCreateMultiTimeseriesReq({
            sessionId: this.session_id,
            paths: ts_path_lst,
            dataTypes: data_type_lst,
            encodings: encoding_lst,
            compressors: compressor_lst,
            propsList: props_lst,
            tagsList: tags_lst,
            attributesList: attributes_lst,
            measurementAliasList: alias_lst
        });
        let status = await this.client.createMultiTimeseries(request);
        console.log(
            "creating multiple time series %s, message: %s",
            ts_path_lst, status.message
        );

        return Session.verify_success(status);
    }

    public async delete_time_series(paths_list) {
        /*
        delete multiple time series, including data and schema
        :param paths_list: List of time series path, which should be complete (starts from root)
        */
        let status = await this.client.deleteTimeseries(this.session_id, paths_list);
        console.log(
            "deleting multiple time series %s, message: %s",
            paths_list, status.message
        );

        return Session.verify_success(status);
    }

    public async check_time_series_exists(path) {
        /*
        check whether a specific time series exists
        :param path: String, complete path of time series for checking
        :return Boolean value indicates whether it exists.
        */
        let data_set = await this.execute_query_statement("SHOW TIMESERIES " + path);
        let result = data_set.has_next();
        data_set.close_operation_handle();
        return result;
    }

    public async delete_data(paths_list, timestamp) {
        /*
        delete all data <= time in multiple time series
        :param paths_list: time series list that the data in.
        :param timestamp: data with time stamp less than or equal to time will be deleted.
        */
        let request = new TSDeleteDataReq({
            sessionId: this.session_id,
            paths: paths_list,
            startTime: 0,
            endTime: timestamp
        });
        try {
            let status = this.client.deleteData(request);
            console.debug(
                "deleting data from %s, message: %s", paths_list, status.message
            );
        } catch (err) {
            console.error("data deletion fails because: ", err);
        }
    }

    public async insert_str_record(device_id, timestamp, measurements, string_values) {
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
        let status = await this.client.insertStringRecord(request);
        console.log(
            "insert one record to device " + device_id
            + ", message: " + status.message
        );

        return Session.verify_success(status);
    }

    public async insert_aligned_str_record(
        device_id, timestamp, measurements, string_values) {
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
            device_id, timestamp, measurements, data_types, string_values, true
        );
        let status = await this.client.insertStringRecord(request);
        console.log(
            "insert one record to device " + device_id
            + ", message: " + status.message
        );

        return Session.verify_success(status);
    }

    public async insert_record(device_id, timestamp, measurements, data_types, values) {
        /*
        insert one row of record into database, if you want improve your performance, please use insertTablet method
            for example a record at time=10086 with three measurements is:
                timestamp,     m1,    m2,     m3
                    10086,  125.3,  true,  text1
        :param device_id: String, time series path for device
        :param timestamp: Integer, indicate the timestamp of the row of data
        :param measurements: List of String, sensor names
        :param data_types: List of TSDataType, indicate the data type for each sensor
        :param values: List, values to be inserted, for each sensor
        */
        let request = this.gen_insert_record_req(
            device_id, timestamp, measurements, data_types, values
        );
        let status = await this.client.insertRecord(request);
        console.log(
            "insert one record to device %s, message: %s",
            device_id, status.message
        );

        return Session.verify_success(status);
    }

    public async insert_records(device_ids, times, measurements_lst, types_lst, values_lst) {
        /*
        insert multiple rows of data, records are independent to each other, in other words, there's no relationship
        between those records
        :param device_ids: List of String, time series paths for device
        :param times: List of Integer, timestamps for records
        :param measurements_lst: 2-D List of String, each element of outer list indicates measurements of a device
        :param types_lst: 2-D List of TSDataType, each element of outer list indicates sensor data types of a device
        :param values_lst: 2-D List, values to be inserted, for each device
        */
        let request = this.gen_insert_records_req(
            device_ids, times, measurements_lst, types_lst, values_lst
        );
        let status = await this.client.insertRecords(request);
        console.log(
            "insert multiple records to devices %s message: %s",
            device_ids, status.message
        );

        return Session.verify_success(status);
    }

    public async insert_aligned_record(device_id, timestamp, measurements, data_types, values) {
        /*
        insert one row of record into database, if you want improve your performance, please use insertTablet method
            for example a record at time=10086 with three measurements is:
                timestamp,     m1,    m2,     m3
                    10086,  125.3,  true,  text1
        :param device_id: String, time series path for device
        :param timestamp: Integer, indicate the timestamp of the row of data
        :param measurements: List of String, sensor names
        :param data_types: List of TSDataType, indicate the data type for each sensor
        :param values: List, values to be inserted, for each sensor
        */
        let request = await this.gen_insert_record_req(
            device_id, timestamp, measurements, data_types, values, true
        );
        let status = await this.client.insertRecord(request);
        console.log(
            "insert one record to device %s, message: %s",
            device_id, status.message
        );

        return Session.verify_success(status);
    }

    public async insert_aligned_records(device_ids, times, measurements_lst, types_lst, values_lst) {
        /*
        insert multiple rows of data, records are independent to each other, in other words, there's no relationship
        between those records
        :param device_ids: List of String, time series paths for device
        :param times: List of Integer, timestamps for records
        :param measurements_lst: 2-D List of String, each element of outer list indicates measurements of a device
        :param types_lst: 2-D List of TSDataType, each element of outer list indicates sensor data types of a device
        :param values_lst: 2-D List, values to be inserted, for each device
        */
        let request = this.gen_insert_records_req(
            device_ids, times, measurements_lst, types_lst, values_lst, true
        );
        let status = await this.client.insertRecords(request);
        console.log(
            "insert multiple records to devices %s message: %s",
            device_ids, status.message
        );

        return Session.verify_success(status);
    }    

    public async test_insert_record(
        device_id, timestamp, measurements, data_types, values
    ) {
        /*
        this method DOES NOT insert data into database and the server just return after accept the request, this method
        should be used to test other time cost in client
        :param device_id: String, time series path for device
        :param timestamp: Integer, indicate the timestamp of the row of data
        :param measurements: List of String, sensor names
        :param data_types: List of TSDataType, indicate the data type for each sensor
        :param values: List, values to be inserted, for each sensor
        */
        let request = this.gen_insert_record_req(
            device_id, timestamp, measurements, data_types, values, false
        );
        let status = this.client.testInsertRecord(request);
        console.debug(
            "testing! insert one record to device %s, message: %s",
            device_id, status.message
        );

        return Session.verify_success(status);
    }

    public async test_insert_records(
        device_ids, times, measurements_lst, types_lst, values_lst
    ) {
        /*
        this method DOES NOT insert data into database and the server just return after accept the request, this method
        should be used to test other time cost in client
        :param device_ids: List of String, time series paths for device
        :param times: List of Integer, timestamps for records
        :param measurements_lst: 2-D List of String, each element of outer list indicates measurements of a device
        :param types_lst: 2-D List of TSDataType, each element of outer list indicates sensor data types of a device
        :param values_lst: 2-D List, values to be inserted, for each device
        */
        let request = this.gen_insert_records_req(
            device_ids, times, measurements_lst, types_lst, values_lst
        );
        let status = this.client.testInsertRecords(request);
        console.debug(
            "testing! insert multiple records, message: %s", status.message
        );

        return Session.verify_success(status);
    }

    public async insert_records_of_one_device(device_id, times_list, measurements_list, types_list, values_list) {
        // sort by timestamp
        let tuple_unsorted = [];
        for (let j in times_list) {
            tuple_unsorted.push([times_list[j], measurements_list[j], types_list[j], values_list[j]]);
        }
        let tuple_sorted = tuple_unsorted.sort();
        for (let p in tuple_sorted) {
            times_list[p] = tuple_sorted[p][0];
            measurements_list[p] = tuple_sorted[p][1];
            types_list[p] = tuple_sorted[p][2];
            values_list[p] = tuple_sorted[p][3];
        }

        return this.insert_records_of_one_device_sorted(
            device_id, times_list, measurements_list, types_list, values_list
        );
    }

    public async insert_records_of_one_device_sorted(device_id, times_list, measurements_list, types_list, values_list) {
        /*
        Insert multiple rows, which can reduce the overhead of network. This method is just like jdbc
        executeBatch, we pack some insert request in batch and send them to server. If you want improve
        your performance, please see insertTablet method

            :param device_id: device id
            :param times_list: timestamps list
            :param measurements_list: measurements list
            :param types_list: types list
            :param values_list: values list
            :param have_sorted: have these list been sorted by timestamp
        */
        // check parameter
        let size = times_list.length;
        if (
            size != measurements_list.length
            || size != types_list.length
            || size != values_list.length
        ) {
            throw "insert records of one device error: types, times, measurementsList and valuesList's size should be equal";
        }

        // check sorted
        if (!Session.check_sorted(times_list)) {
            throw "insert records of one device error: timestamp not sorted";
        }

        let request = await this.gen_insert_records_of_one_device_request(
            device_id, times_list, measurements_list, values_list, types_list
        );

        // send request
        let status = await this.client.insertRecordsOfOneDevice(request);
        console.debug("insert records of one device, message: ", status.message);

        return Session.verify_success(status);
    }

    public async insert_aligned_records_of_one_device(device_id, times_list, measurements_list, types_list, values_list) {
        // sort by timestamp
        let tuple_unsorted = [];
        for (let j in times_list) {
            tuple_unsorted.push([times_list[j], measurements_list[j], types_list[j], values_list[j]]);
        }
        let tuple_sorted = tuple_unsorted.sort();
        for (let p in tuple_sorted) {
            times_list[p] = tuple_sorted[p][0];
            measurements_list[p] = tuple_sorted[p][1];
            types_list[p] = tuple_sorted[p][2];
            values_list[p] = tuple_sorted[p][3];
        }

        return this.insert_records_of_one_device_sorted(
            device_id, times_list, measurements_list, types_list, values_list
        )
    }

    public async insert_aligned_records_of_one_device_sorted(device_id, times_list, measurements_list, types_list, values_list) {
        /*
        Insert multiple aligned rows, which can reduce the overhead of network. This method is just like jdbc
        executeBatch, we pack some insert request in batch and send them to server. If you want to improve
        your performance, please see insertTablet method

        :param device_id: device id
        :param times_list: timestamps list
        :param measurements_list: measurements list
        :param types_list: types list
        :param values_list: values list
        */
        // check parameter
        let size = times_list.length;
        if (
            size != measurements_list.length
            || size != types_list.length
            || size != values_list.length
        ) {
            throw "insert records of one device error: types, times, measurementsList and valuesList's size should be equal";
        }

        // check sorted
        if (!Session.check_sorted(times_list)) {
            throw "insert records of one device error: timestamp not sorted";
        }

        let request = await this.gen_insert_records_of_one_device_request(
            device_id, times_list, measurements_list, values_list, types_list, true
        );

        // send request
        let status = await this.client.insertRecordsOfOneDevice(request);
        console.debug("insert records of one device, message: ", status.message);

        return Session.verify_success(status);
    }    

    public async insert_tablet(tablet) {
        /*
        insert one tablet, in a tablet, for each timestamp, the number of measurements is same
            for example three records in the same device can form a tablet:
                timestamps,     m1,    m2,     m3
                         1,  125.3,  True,  text1
                         2,  111.6, False,  text2
                         3,  688.6,  True,  text3
        Notice: From 0.13.0, the tablet can contain empty cell
                The tablet itself is sorted (see docs of Tablet.py)
        :param tablet: a tablet specified above
        */
        let status = this.client.insertTablet(this.gen_insert_tablet_req(tablet));
        console.debug(
            "insert one tablet to device %s, message: %s", tablet.get_device_id(), status.message
        );
    }

    public async insert_tablets(tablet_lst) {
        /*
        insert multiple tablets, tablets are independent to each other
        :param tablet_lst: List of tablets
        */
        let status = this.client.insertTablets(this.gen_insert_tablets_req(tablet_lst));
        console.debug("insert multiple tablets, message: %s", status.message);

        return Session.verify_success(status);
    }

    public async insert_aligned_tablet(tablet) {
        /*
        insert one tablet, in a tablet, for each timestamp, the number of measurements is same
            for example three records in the same device can form a tablet:
                timestamps,     m1,    m2,     m3
                         1,  125.3,  True,  text1
                         2,  111.6, False,  text2
                         3,  688.6,  True,  text3
        Notice: From 0.13.0, the tablet can contain empty cell
                The tablet itself is sorted (see docs of Tablet.py)
        :param tablet: a tablet specified above
        */
        let status = this.client.insertTablet(this.gen_insert_tablet_req(tablet, true));
        console.debug(
            "insert one tablet to device %s, message: %s", tablet.get_device_id(), status.message
        );
    }

    public async insert_aligned_tablets(tablet_lst) {
        /*
        insert multiple tablets, tablets are independent to each other
        :param tablet_lst: List of tablets
        */
        let status = this.client.insertTablets(this.gen_insert_tablets_req(tablet_lst, true));
        console.debug("insert multiple tablets, message: %s", status.message);

        return Session.verify_success(status);
    }

    public async test_insert_tablet(tablet) {
        /*
        this method DOES NOT insert data into database and the server just return after accept the request, this method
        should be used to test other time cost in client
        :param tablet: a tablet of data
        */
        let status = this.client.testInsertTablet(this.gen_insert_tablet_req(tablet));
        console.debug(
            "testing! insert one tablet to device %s, message: %s",
            tablet.get_device_id(), status.message
        );

        return Session.verify_success(status);
    }

    public async test_insert_tablets(tablet_list) {
        /*
        this method DOES NOT insert data into database and the server just return after accept the request, this method
        should be used to test other time cost in client
        :param tablet_list: List of tablets
        */
        let status = this.client.testInsertTablets(
            this.gen_insert_tablets_req(tablet_list)
        );
        console.debug(
            "testing! insert multiple tablets, message: %s", status.message
        );

        return Session.verify_success(status);
    }

    public gen_insert_tablet_req(tablet, is_aligned=false) {
        return new TSInsertTabletReq({
            sessionId: this.session_id,
            prefixPath: tablet.get_device_id(),
            measurements: tablet.get_measurements(),
            values: tablet.get_binary_values(),
            timestamps: tablet.get_binary_timestamps(),
            types: tablet.get_data_types(),
            size: tablet.get_row_number(),
            isAligned: is_aligned
        });
    }

    public gen_insert_tablets_req(tablet_lst, is_aligned=false) {
        let device_id_lst = [];
        let measurements_lst = [];
        let values_lst = [];
        let timestamps_lst = [];
        let type_lst = [];
        let size_lst = [];
        for (let i in tablet_lst) {
            device_id_lst.push(tablet_lst[i].get_device_id());
            measurements_lst.push(tablet_lst[i].get_measurements());
            values_lst.push(tablet_lst[i].get_binary_values());
            timestamps_lst.push(tablet_lst[i].get_binary_timestamps());
            type_lst.push(tablet_lst[i].get_data_types());
            size_lst.push(tablet_lst[i].get_row_number());
        }
        return new TSInsertTabletsReq({
            sessionId: this.session_id,
            prefixPaths: device_id_lst,
            measurementsList: measurements_lst,
            valuesList: values_lst,
            timestampsList: timestamps_lst,
            typesList: type_lst,
            sizeList: size_lst,
            isAligned: is_aligned
        });
    }

    public async gen_insert_records_of_one_device_request(
        device_id, 
        times_list, 
        measurements_list, 
        values_list, 
        types_list,
        is_aligned=false
    ) {
        let binary_value_list = [];
        for (let j in values_list) {
            if ((values_list[j].length != types_list[j].length) || (values_list[j].length != measurements_list[j].length)) {
                throw "typesList, measurementsList and valuesList's size should be equal";
            }
            let values_in_bytes = Session.value_to_bytes(types_list[j], values_list[j]);
            binary_value_list.push(values_in_bytes);
        }

        return new TSInsertRecordsOfOneDeviceReq({
            sessionId: this.session_id,
            prefixPath: device_id,
            measurementsList: measurements_list,
            valuesList: binary_value_list,
            timestamps: times_list,
            isAligned: is_aligned
        });
    }

    public gen_insert_record_req(device_id, timestamp, measurements, data_types, values, is_aligned=false) {
        if ((values.length != data_types.length) || (values.length != measurements.length)) {
            throw "length of data types does not equal to length of values!";
        }
        let values_in_bytes = Session.value_to_bytes(data_types, values);
        return new TSInsertRecordReq({
            sessionId: this.session_id,
            prefixPath: device_id,
            measurements: measurements,
            values: values_in_bytes,
            timestamp: timestamp,
            isAligned: is_aligned
        });
    }

    public gen_insert_str_record_req(device_id, timestamp, measurements, data_types, values, is_aligned=false) {
        if ((values.length != data_types.length) || (values.length != measurements.length)) {
            throw "length of data types does not equal to length of values!";
        }
        return new TSInsertStringRecordReq({
                sessionId: this.session_id,
                prefixPath: device_id,
                measurements: measurements,
                values: values,
                timestamp: timestamp,
                isAligned: is_aligned
            }
        );
    }

    public gen_insert_records_req(device_ids, times, measurements_lst, types_lst, values_lst, is_aligned=false) {
        if (
            (device_ids.length != measurements_lst.length)
            || (times.length != types_lst.length)
            || (device_ids.length != times.length)
            || (times.length != values_lst.length)
        ) {
            throw "deviceIds, times, measurementsList and valuesList's size should be equal";
        }

        let value_lst = [];
        for (let i in values_lst) {
            if ((values_lst[i].length != types_lst[i].length) || (values_lst[i].length != measurements_lst[i].length)) {
                throw "for each device, times, measurementsList and valuesList's size should be equal";
            }
            let values_in_bytes = Session.value_to_bytes(types_lst[i], values_lst[i]);
            value_lst.push(values_in_bytes);
        }

        return new TSInsertRecordsReq({
            sessionId: this.session_id,
            prefixPaths: device_ids,
            measurementsList: measurements_lst,
            valuesList: value_lst,
            timestamps: times,
            isAligned: is_aligned
        });
    }

    public async execute_query_statement(sql, timeout = 0) {
        /*
        execute query sql statement and returns SessionDataSet
        :param sql: String, query sql statement
        :return: SessionDataSet, contains query results and relevant info (see SessionDataSet.ts)
        */
        let request = new TSExecuteStatementReq({
            sessionId: this.session_id,
            statement: sql,
            statementId: this.statement_id,
            fetchSize: this.fetch_size,
            timeout: timeout
        });
        let resp = await this.client.executeQueryStatement(request);
        return new SessionDataSet(
            sql,
            resp.columns,
            resp.dataTypeList,
            resp.columnNameIndexMap,
            resp.queryId,
            this.client,
            this.statement_id,
            this.session_id,
            resp.queryDataSet,
            resp.ignoreTimeStamp
        );
    }

    public async execute_non_query_statement(sql) {
        /*
        execute non-query sql statement
        :param sql: String, non-query sql statement
        */
        let request = new TSExecuteStatementReq({
            sessionId: this.session_id,
            statement: sql,
            statementId: this.statement_id,
        });
        try {
            let resp = await this.client.executeUpdateStatement(request);
            let status = resp.status;
            console.log(
                "execute non-query statement %s, message: %s", sql, status.message
            );
            return Session.verify_success(status);
        } catch (err) {
            throw new Error("execution of non-query statement fails");
        }
    }

    public static value_to_bytes(data_types, values) {
        let values_tobe_packed = [];
        for (let i in data_types) {
            if (data_types[i] == TSDataType.BOOLEAN) {
                values_tobe_packed.push(TSDataType.BOOLEAN);
                values_tobe_packed.push(values[i]);
            } else if (data_types[i] == TSDataType.INT32) {
                values_tobe_packed.push(TSDataType.INT32);
                let int32 = new Int32Array([values[i]]);
                let uint8 = new Uint8Array(int32.buffer).reverse();
                for (let j = 0; j < 4; j++) {
                    values_tobe_packed.push(uint8[j]);
                }
            } else if (data_types[i] == TSDataType.INT64) {
                values_tobe_packed.push(TSDataType.INT64);
                let bigint64 = new BigInt64Array([values[i]]);
                let uint8 = new Uint8Array(bigint64.buffer).reverse();
                for (let j = 0; j < 8; j++) {
                    values_tobe_packed.push(uint8[j]);
                }
            } else if (data_types[i] == TSDataType.FLOAT) {
                values_tobe_packed.push(TSDataType.FLOAT);
                let float32 = new Float32Array([values[i]]);
                let uint8 = new Uint8Array(float32.buffer).reverse();
                for (let j = 0; j < 4; j++) {
                    values_tobe_packed.push(uint8[j]);
                }
            } else if (data_types[i] == TSDataType.DOUBLE) {
                values_tobe_packed.push(TSDataType.DOUBLE);
                let float64 = new Float64Array([values[i]]);
                let uint8 = new Uint8Array(float64.buffer).reverse();
                for (let j = 0; j < 8; j++) {
                    values_tobe_packed.push(uint8[j]);
                }
            } else if (data_types[i] == TSDataType.TEXT) {
                values_tobe_packed.push(TSDataType.TEXT);
                let utf8arr = Buffer.from(values[i]);
                let int32 = new Uint32Array([utf8arr.length]);
                let uint8 = new Uint8Array(int32.buffer).reverse();
                for (let j = 0; j < 4; j++) {
                    values_tobe_packed.push(uint8[j]);
                }
                // @ts-ignore
                for (let item of utf8arr) {
                    values_tobe_packed.push(item);
                }
            } else {
                throw "Unsupported data type";
            }
        }

        return Buffer.from(values_tobe_packed);
    }

    public get_time_zone() {
        if (this.zone_id !== null) {
            return this.zone_id;
        }
        let resp;
        try {
            resp = this.client.getTimeZone(this.session_id);
        } catch (err) {
            throw new Error("Could not get time zone");
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
            console.log("setting time zone_id as " + zone_id);
        } catch (err) {
            throw new Error("Could not set time zone");
        }
        this.zone_id = zone_id;
    }

    public static check_sorted(timestamps) {
        for (let i = 1; i < timestamps.length; i++) {
            if (timestamps[i] < timestamps[i - 1]) {
                return false;
            }
        }
        return true;
    }

    public static verify_success(status) {
        /*
        verify success of operation
        :param status: execution result status
        */
        if (status.code == Session.SUCCESS_CODE) {
            return 0;
        }

        console.error("status code: " + status.code);
        return -1;
    }

}
