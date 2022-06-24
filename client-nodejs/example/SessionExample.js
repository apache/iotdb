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

const {TSDataType, TSEncoding, Compressor} = require("../dist/utils/IoTDBConstants");
const session = require("../dist/Session");
const {Tablet} = require("../dist/utils/Tablet");

async function sessionExample() {
    // create a new session
    const s = new session.Session("127.0.0.1", 6667);
    await s.open(false);

    // set and delete storage groups
    await s.set_storage_group("root.sg_test_01");
    await s.set_storage_group("root.sg_test_02");
    await s.set_storage_group("root.sg_test_03");
    await s.set_storage_group("root.sg_test_04");
    await s.delete_storage_group("root.sg_test_02");
    await s.delete_storage_groups(["root.sg_test_03", "root.sg_test_04"]);

    // set time series
    await s.create_time_series(
        "root.sg_test_01.d_01.s_01", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY
    );
    await s.create_time_series(
        "root.sg_test_01.d_01.s_02", TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY
    );
    await s.create_time_series(
        "root.sg_test_01.d_01.s_03", TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY
    );
    await s.create_time_series(
        "root.sg_test_01.d_02.s_01",
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        Compressor.SNAPPY,
        null,
        {"tag1": "v1"},
        {"description": "v1"},
        "temperature"
    )

    // set multiple time series
    let ts_path_lst_ = [
        "root.sg_test_01.d_01.s_04",
        "root.sg_test_01.d_01.s_05",
        "root.sg_test_01.d_01.s_06",
        "root.sg_test_01.d_01.s_07",
        "root.sg_test_01.d_01.s_08",
        "root.sg_test_01.d_01.s_09",
    ];
    let data_type_lst_ = [
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TEXT,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TEXT,
    ];
    let encoding_lst_ = [];
    for(let i in data_type_lst_){
        encoding_lst_.push(TSEncoding.PLAIN);
    }
    let compressor_lst_ = [];
    for(let i in data_type_lst_){
        compressor_lst_.push(Compressor.SNAPPY);
    }
    await s.create_multi_time_series(
        ts_path_lst_, data_type_lst_, encoding_lst_, compressor_lst_
    );

    ts_path_lst_ = [
        "root.sg_test_01.d_02.s_04",
        "root.sg_test_01.d_02.s_05",
        "root.sg_test_01.d_02.s_06",
        "root.sg_test_01.d_02.s_07",
        "root.sg_test_01.d_02.s_08",
        "root.sg_test_01.d_02.s_09",
    ];
    data_type_lst_ = [
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TEXT,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TEXT,
    ];
    encoding_lst_ = [];
    for(let i in data_type_lst_){
        encoding_lst_.push(TSEncoding.PLAIN);
    }
    compressor_lst_ = [];
    for(let i in data_type_lst_){
        compressor_lst_.push(Compressor.SNAPPY);
    }
    let tags_lst_ = [];
    for(let i in data_type_lst_){
        tags_lst_.push({"tag2": "v2"});
    }
    let attributes_lst_ = [];
    for(let i in data_type_lst_){
        attributes_lst_.push({"description": "v2"});
    }    
    await s.create_multi_time_series(
        ts_path_lst_,
        data_type_lst_,
        encoding_lst_,
        compressor_lst_,
        null,
        tags_lst_,
        attributes_lst_,
        null,
    );

    // delete time series
    await s.delete_time_series(
        [
            "root.sg_test_01.d_01.s_07",
            "root.sg_test_01.d_01.s_08",
            "root.sg_test_01.d_01.s_09",
        ]
    );

    // check time series
    console.log(
        "s_07 expecting false, checking result: ",
        await s.check_time_series_exists("root.sg_test_01.d_01.s_07")
    );
    console.log(
        "s_03 expecting true, checking result: ",
        await s.check_time_series_exists("root.sg_test_01.d_01.s_03")
    );
    console.log(
        "d_02.s_01 expecting true, checking result: ",
        await s.check_time_series_exists("root.sg_test_01.d_02.s_01")
    );
    console.log(
        "d_02.s_06 expecting true, checking result: ",
        await s.check_time_series_exists("root.sg_test_01.d_02.s_06")
    );
      
    // insert one record into the database
    let measurements_ = ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"];
    let data_types_ = [
        TSDataType.BOOLEAN,
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TEXT
    ];
    let values_ = [true, 188, 18888n, 188.88, 18888.8888, "test_record"];
    await s.insert_record("root.sg_test_01.d_01", 1, measurements_, data_types_, values_); 

    // insert multiple records into database
    let measurements_list_ = [
        ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"],
        ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"],
    ];
    let values_list_ = [
        [false, 22, 3333333n, 4.44, 55.111111111, "test_records01"],
        [true, 77, 8888888n, 1.25, 88.125555555, "test_records02"],
    ];
    let data_type_list_ = [data_types_, data_types_];
    let device_ids_ = ["root.sg_test_01.d_01", "root.sg_test_01.d_01"];
    await s.insert_records(device_ids_, [2, 3], measurements_list_, data_type_list_, values_list_);

    // insert records of one device
    let time_list = [6, 5, 4];
    let measurements_list = [
        ["s_01", "s_02", "s_03"],
        ["s_01", "s_02", "s_03"],
        ["s_01", "s_02", "s_03"],
    ];
    let data_types_list = [
        [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64],
        [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64],
        [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64],
    ];
    let values_list = [[false, 22, 3333333333n], [true, 1, 6666666666n], [false, 15, 99999999999n]];
    await s.insert_records_of_one_device("root.sg_test_01.d_01", time_list, measurements_list, data_types_list, values_list);

    // insert one tablet into the database
    values_ = [
        [false, 10, 11n, 1.1, 10011.1, "test01"],
        [true, 100, 11111n, 1.25, 101.0, "test02"],
        [false, 100, 1n, 188.1, 688.25, "test03"],
        [true, 0, 0n, 0, 6.25, "test04"],
    ];  // Non-ASCII text will cause error since bytes can only hold 0-128 nums.
    let timestamps_ = [10n, 8n, 9n, 7n];
    let tablet_ = new Tablet("root.sg_test_01.d_01", measurements_, data_types_, values_, timestamps_);
    await s.insert_tablet(tablet_);

    //insert multiple tablets into database
    let values_01 = [
        [false, 10, 11n, 1.1, 10011.1, "test01"],
        [true, 100, 11111n, 1.25, 101.0, "test02"],
        [false, 100, 1n, 188.1, 688.25, "test03"],
        [true, 0, 0n, 0, 6.25, "test04"],
    ];  // Non-ASCII text will cause error since bytes can only hold 0-128 nums.
    let timestamps_01 = [12n, 13n, 11n, 14n];
    let tablet_01 = new Tablet("root.sg_test_01.d_01", measurements_, data_types_, values_01, timestamps_01);

    let values_02 = [
        [false, 10, 11n, 1.1, 10011.1, "test01"],
        [true, 100, 11111n, 1.25, 101.0, "test02"],
        [false, 100, 1n, 188.1, 688.25, "test03"],
        [true, 0, 0n, 0, 6.25, "test04"],
    ];  // Non-ASCII text will cause error since bytes can only hold 0-128 nums.
    let timestamps_02 = [18n, 16n, 17n, 15n];
    let tablet_02 = new Tablet("root.sg_test_01.d_01", measurements_, data_types_, values_02, timestamps_02);
    await s.insert_tablets([tablet_01, tablet_02]);
    

    // insert one tablet with empty cells into the database.
    values_ = [
        [null, 10, 11n, 1.1, 10011.1, "test01"],
        [true, null, 11111n, 1.25, 101.0, "test02"],
        [false, 100, 1n, null, 688.25, "test03"],
        [true, 0, 0n, 0, 6.25, null],
    ];  // Non-ASCII text will cause error since bytes can only hold 0-128 nums.
    timestamps_ = [20n, 21n, 22n, 19n];
    tablet_ = new Tablet(
        "root.sg_test_01.d_01", measurements_, data_types_, values_, timestamps_
    );
    await s.insert_tablet(tablet_);  

    // execute non-query sql statement
    await s.execute_non_query_statement("insert into root.sg_test_01.d_01(timestamp, s_02) values(23, 188)"); 

    // execute sql query statement
    const session_data_set_d_01 = await s.execute_query_statement("SELECT * FROM root.sg_test_01.d_01");
    while (session_data_set_d_01.has_next()){
        console.log(session_data_set_d_01.next());
    }

    await s.delete_storage_group("root.sg_test_01");

    s.close();

    console.log("All executions done!!");
}

sessionExample();
