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

import {TSDataType} from "./IoTDBConstants";
import {IoTDBRpcDataSet} from "./IoTDBRpcDataSet";

export class SessionDataSet {

    public iotdb_rpc_data_set;

    constructor(
        sql,
        column_name_list,
        column_type_list,
        column_name_index,
        query_id,
        client,
        statement_id,
        session_id,
        query_data_set,
        ignore_timestamp
    ) {
        this.iotdb_rpc_data_set = new IoTDBRpcDataSet(
            sql,
            column_name_list,
            column_type_list,
            column_name_index,
            ignore_timestamp,
            query_id,
            client,
            statement_id,
            session_id,
            query_data_set,
            1024
        );
    }

    public get_fetch_size() {
        return this.iotdb_rpc_data_set.get_fetch_size();
    }

    public set_fetch_size(fetch_size) {
        this.iotdb_rpc_data_set.set_fetch_size(fetch_size);
    }

    public get_column_names() {
        return this.iotdb_rpc_data_set.get_column_names();
    }

    public get_column_types() {
        return this.iotdb_rpc_data_set.get_column_types();
    }

    public has_next() {
        return this.iotdb_rpc_data_set.next();
    }

    public next() {
        if (!this.iotdb_rpc_data_set.get_has_cached_record()) {
            if (!this.has_next()) {
                return null;
            }
        }
        this.iotdb_rpc_data_set.has_cached_record = false;
        return this.construct_row_record_from_value_array();
    }

    public construct_row_record_from_value_array() {
        let target_time_bytes = new Uint8Array(this.iotdb_rpc_data_set.get_time_bytes().slice(0, 8).reverse(), 0, 8);
        let time64 = new BigInt64Array(target_time_bytes.buffer, 0, 1);
        let obj = {timestamp: time64[0]};

        for (let i = 0; i < this.iotdb_rpc_data_set.get_column_size(); i++) {
            let index = i + 1;
            let data_set_column_index = i + IoTDBRpcDataSet.START_INDEX;
            if (this.iotdb_rpc_data_set.get_ignore_timestamp()) {
                index -= 1;
                data_set_column_index -= 1;
            }
            let column_name = this.iotdb_rpc_data_set.get_column_names()[index];
            let location = (
                this.iotdb_rpc_data_set.get_column_ordinal_dict().get(column_name)
                - IoTDBRpcDataSet.START_INDEX
            );

            if (!this.iotdb_rpc_data_set.is_null_by_index(data_set_column_index)) {

                let value_bytes = this.iotdb_rpc_data_set.get_values()[location];
                let data_type = this.iotdb_rpc_data_set.get_column_type_deduplicated_list()[location];
                let value;
                let ts_name = this.iotdb_rpc_data_set.find_column_name_by_index(data_set_column_index);
                if (data_type == TSDataType.BOOLEAN) {
                    let target_value_bytes = new Uint8Array(value_bytes.slice(0, 1).reverse(), 0, 1)
                    let int8 = new Int8Array(target_value_bytes.buffer, 0, 1)
                    value = int8[0];
                    obj[ts_name] = value;
                } else if (data_type == TSDataType.INT32) {
                    let target_value_bytes = new Uint8Array(value_bytes.slice(0, 4).reverse(), 0, 4)
                    let int32 = new Int32Array(target_value_bytes.buffer, 0, 1)
                    value = int32[0];
                    obj[ts_name] = value;
                } else if (data_type == TSDataType.INT64) {
                    let target_value_bytes = new Uint8Array(value_bytes.slice(0, 8).reverse(), 0, 8)
                    let int64 = new BigInt64Array(target_value_bytes.buffer, 0, 1)
                    value = int64[0];
                    obj[ts_name] = value;
                } else if (data_type == TSDataType.FLOAT) {
                    let target_value_bytes = new Uint8Array(value_bytes.slice(0, 4).reverse(), 0, 4)
                    let float32 = new Float32Array(target_value_bytes.buffer, 0, 1)
                    value = float32[0];
                    obj[ts_name] = value;
                } else if (data_type == TSDataType.DOUBLE) {
                    let target_value_bytes = new Uint8Array(value_bytes.slice(0, 8).reverse(), 0, 8)
                    let float64 = new Float64Array(target_value_bytes.buffer, 0, 1)
                    value = float64[0];
                    obj[ts_name] = value;
                } else if (data_type == TSDataType.TEXT) {
                    obj[ts_name] = value_bytes.toString();
                } else {
                    throw new Error("unsupported data type.");
                }
            } else {
                let ts_name = this.iotdb_rpc_data_set.find_column_name_by_index(data_set_column_index);
                obj[ts_name] = null;
            }
        }

        return obj;
    }

    public close_operation_handle() {
        this.iotdb_rpc_data_set.close();
    }

}
