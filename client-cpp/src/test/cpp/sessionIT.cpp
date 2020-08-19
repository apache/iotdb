/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "catch.hpp"
#include "IOTDBSession.h"

using namespace std;

extern Session *session;

static vector<string> testTimeseries =
    {"root.test.d1.s1","root.test.d1.s2","root.test.d1.s3"};

void prepareTimeseries(){
    for (string ts : testTimeseries) {
        session->createTimeseries(ts, TSDataType::INT64, TSEncoding::RLE, CompressionType::SNAPPY);
    }
}

void clearTimeseries() {
    session->deleteTimeseries(testTimeseries);
}

TEST_CASE( "Create timeseries success", "[createTimeseries]" ) {
    if (!session->checkTimeseriesExists("root.test.d1.s1")) {
        session->createTimeseries("root.test.d1.s1", TSDataType::INT64, TSEncoding::RLE, CompressionType::SNAPPY);
    }
    REQUIRE( session->checkTimeseriesExists("root.test.d1.s1") == true );
    session->deleteTimeseries("root.test.d1.s1");
}

TEST_CASE( "Test insertRecord by string", "[testInsertRecord]") {
    prepareTimeseries();
    string deviceId = "root.test.d1";
    vector<string> measurements;
    measurements.push_back("s1");
    measurements.push_back("s2");
    measurements.push_back("s3");

    for (long time = 0; time < 100; time++) {
        vector<string> values;
        values.push_back("1");
        values.push_back("2");
        values.push_back("3");
        session->insertRecord(deviceId, time, measurements, values);
    }

    session->executeNonQueryStatement("insert into root.test.d1(timestamp,s1, s2, s3) values(100, 1,2,3)");

    SessionDataSet *sessionDataSet = session->executeQueryStatement("select * from root.test.d1");
    sessionDataSet->setBatchSize(1024);
    int count = 0;
    while (sessionDataSet->hasNext()) {
        long index = 1;
        count++;
        for (Field *f : sessionDataSet->next()->fields) {
            REQUIRE( f->longV == index );
            index++;
        }
    }
    REQUIRE( count == 101 );
    clearTimeseries();
}