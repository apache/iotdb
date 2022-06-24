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

import {TSFetchResultsReq, TSCloseOperationReq} from "../thrift/rpc_types";
import {TSDataType} from "./IoTDBConstants";

export class IoTDBRpcDataSet {
    static TIMESTAMP_STR: string = "Time";
    static START_INDEX = 2;
    static FLAG = 0x80;

    private session_id;
    private ignore_timestamp;
    private sql;
    private query_id;
    private client;
    private fetch_size;
    private column_size;
    private default_time_out;
    private column_name_list;
    private column_type_list;
    private column_ordinal_dict;
    private column_type_deduplicated_list;
    private time_bytes;
    private current_bitmap;
    private value;
    private query_data_set;
    private is_closed;
    private empty_resultSet;
    private has_cached_record;
    private rows_index;
    private statement_id;

    constructor(
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
        fetch_size
    ) {
        this.session_id = session_id;
        this.statement_id = statement_id;
        this.ignore_timestamp = ignore_timestamp;
        this.sql = sql;
        this.query_id = query_id;
        this.client = client;
        this.fetch_size = fetch_size;
        this.column_size = column_name_list.length;
        this.default_time_out = 1000;

        this.column_name_list = [];
        this.column_type_list = [];
        // @ts-ignore
        this.column_ordinal_dict = new Map();
        if (!ignore_timestamp) {
            this.column_name_list.push(IoTDBRpcDataSet.TIMESTAMP_STR);
            this.column_type_list.push(TSDataType.INT64);
            this.column_ordinal_dict.set(IoTDBRpcDataSet.TIMESTAMP_STR, 1);
        }

        if (column_name_index !== null) {
            this.column_type_deduplicated_list = [];
            for (let j = 0; j < column_name_index.length; j++) {
                this.column_type_deduplicated_list.push(null);
            }

            for (let i = 0; i < column_name_list.length; i++) {
                let name = column_name_list[i];
                this.column_name_list.push(name);
                this.column_type_list.push(TSDataType[column_type_list[i]]);
                if (!this.column_ordinal_dict.has(name)) {
                    let index = column_name_index[name];
                    this.column_ordinal_dict.set(name, index + IoTDBRpcDataSet.START_INDEX);
                    this.column_type_deduplicated_list[index] = TSDataType[column_type_list[i]];
                }
            }
        } else {
            let index = IoTDBRpcDataSet.START_INDEX;
            this.column_type_deduplicated_list = [];
            for (let i = 0; i < column_name_list.length; i++) {
                let name = column_name_list[i];
                this.column_name_list.push(name);
                this.column_type_list.push(TSDataType[column_type_list[i]]);
                if (!this.column_ordinal_dict.has(name)) {
                    this.column_ordinal_dict.set(name, index);
                    index += 1;
                    this.column_type_deduplicated_list.push(TSDataType[column_type_list[i]]);
                }
            }
        }

        this.time_bytes = Buffer.alloc(0);
        this.current_bitmap = [];
        for (let i = 0; i < this.column_type_deduplicated_list.length; i++) {
            this.current_bitmap.push(Buffer.alloc(0));
        }
        this.value = [];
        for (let i = 0; i < this.column_type_deduplicated_list.length; i++) {
            this.value.push(null);
        }
        this.query_data_set = query_data_set;
        this.is_closed = false;
        this.empty_resultSet = false;
        this.has_cached_record = false;
        this.rows_index = 0;
    }

    public close() {
        if (this.is_closed) {
            return;
        }
        if (this.client !== null) {
            try {
                let status = this.client.closeOperation(
                    new TSCloseOperationReq({
                        sessionId: this.session_id, 
                        queryId: this.query_id,
                        statementId: this.statement_id
                    })
                );
                console.log("close session " + this.session_id + ", message: " + status.message);
            } catch (err) {
                throw new Error("close session failed");
            }

            this.is_closed = true;
            this.client = null;
        }
    }

    public next() {
        if (this.has_cached_result()) {
            this.construct_one_row();
            return true;
        }
        if (this.empty_resultSet) {
            return false;
        }
        if (this.fetch_results()) {
            this.construct_one_row();
            return true;
        }
        return false;
    }

    public has_cached_result() {
        return (this.query_data_set !== null) && (this.query_data_set.time.length != 0);
    }

    public construct_one_row() {
        // simulating buffer, read 8 bytes from data set and discard first 8 bytes which have been read.
        this.time_bytes = this.query_data_set.time.slice(0, 8);
        let len = this.query_data_set.time.length;
        this.query_data_set.time = this.query_data_set.time.slice(8, len);
        for (let i = 0; i < this.query_data_set.bitmapList.length; i++) {
            let bitmap_buffer = this.query_data_set.bitmapList[i];

            // another 8 new rows, should move the bitmap buffer position to next byte
            if (this.rows_index % 8 == 0) {
                this.current_bitmap[i] = bitmap_buffer[0];
                let len = bitmap_buffer.length;
                this.query_data_set.bitmapList[i] = bitmap_buffer.slice(1, len);
            }
            if (!this.is_null(i, this.rows_index)) {
                let value_buffer = this.query_data_set.valueList[i];
                let data_type = this.column_type_deduplicated_list[i];

                // simulating buffer
                let len = value_buffer.length;
                if (data_type == TSDataType.BOOLEAN) {
                    this.value[i] = value_buffer.slice(0, 1);
                    this.query_data_set.valueList[i] = value_buffer.slice(1, len);
                } else if (data_type == TSDataType.INT32) {
                    this.value[i] = value_buffer.slice(0, 4);
                    this.query_data_set.valueList[i] = value_buffer.slice(4, len);
                } else if (data_type == TSDataType.INT64) {
                    this.value[i] = value_buffer.slice(0, 8);
                    this.query_data_set.valueList[i] = value_buffer.slice(8, len);
                } else if (data_type == TSDataType.FLOAT) {
                    this.value[i] = value_buffer.slice(0, 4);
                    this.query_data_set.valueList[i] = value_buffer.slice(4, len);
                } else if (data_type == TSDataType.DOUBLE) {
                    this.value[i] = value_buffer.slice(0, 8);
                    this.query_data_set.valueList[i] = value_buffer.slice(8, len);
                } else if (data_type == TSDataType.TEXT) {
                    let length = value_buffer.readInt32BE(0);
                    this.value[i] = value_buffer.slice(4, 4 + length);
                    this.query_data_set.valueList[i] = value_buffer.slice(4 + length, len);
                } else {
                    throw new Error("unsupported data type.");
                }
            }
        }
        this.rows_index += 1;
        this.has_cached_record = true;
    }

    public fetch_results() {
        this.rows_index = 0;
        let request = new TSFetchResultsReq({
            sessionId: this.session_id,
            statement: this.sql,
            fetchSize: this.fetch_size,
            queryId: this.query_id,
            isAlign: true,
            timeout: this.default_time_out
        });
        try {
            let resp = this.client.fetchResults(request);
            if (!resp.hasResultSet) {
                this.empty_resultSet = true;
            } else {
                this.query_data_set = resp.queryDataSet;
            }
            return resp.hasResultSet;
        } catch (err) {
            throw new Error("Cannot fetch result from server");
        }
    }

    public is_null(index, row_num) {
        let bitmap = this.current_bitmap[index];
        let shift = row_num % 8;
        return ((IoTDBRpcDataSet.FLAG >> shift) & (bitmap & 0xFF)) == 0;
    }

    public is_null_by_index(column_index) {
        let index = (
            this.column_ordinal_dict.get(this.find_column_name_by_index(column_index))
            - IoTDBRpcDataSet.START_INDEX
        );
        // time column will never be null
        if (index < 0) {
            return true;
        }
        return this.is_null(index, this.rows_index - 1);
    }

    public is_null_by_name(column_name) {
        let index = this.column_ordinal_dict.get(column_name) - IoTDBRpcDataSet.START_INDEX;
        // time column will never be null
        if (index < 0) {
            return true;
        }
        return this.is_null(index, this.rows_index - 1);
    }

    public find_column_name_by_index(column_index) {
        if (column_index <= 0) {
            throw new Error("Column index should start from 1");
        }
        if (column_index > this.column_name_list.length) {
            throw new Error("column index out of range");
        }
        return this.column_name_list[column_index - 1];
    }

    public get_fetch_size() {
        return this.fetch_size;
    }

    public set_fetch_size(fetch_size) {
        this.fetch_size = fetch_size;
    }

    public get_column_names() {
        return this.column_name_list;
    }

    public get_column_types() {
        return this.column_type_list;
    }

    public get_column_size() {
        return this.column_size;
    }

    public get_ignore_timestamp() {
        return this.ignore_timestamp;
    }

    public get_column_ordinal_dict() {
        return this.column_ordinal_dict;
    }

    public get_column_type_deduplicated_list() {
        return this.column_type_deduplicated_list;
    }

    public get_values() {
        return this.value;
    }

    public get_time_bytes() {
        return this.time_bytes;
    }

    public get_has_cached_record() {
        return this.has_cached_record;
    }

}
