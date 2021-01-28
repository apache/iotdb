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
#include "Session.h"

using namespace std;

extern Session *session;

static vector<string> testTimeseries = {"root.test.d1.s1","root.test.d1.s2","root.test.d1.s3"};

void prepareTimeseries() {
  for (string timeseries : testTimeseries) {
    if (session->checkTimeseriesExists(timeseries)) {
      session->deleteTimeseries(timeseries);
    }
    session->createTimeseries(timeseries, TSDataType::INT64, TSEncoding::RLE, CompressionType::SNAPPY);
  }
}

TEST_CASE( "Create timeseries success", "[createTimeseries]" ) {
  if (!session->checkTimeseriesExists("root.test.d1.s1")) {
    session->createTimeseries("root.test.d1.s1", TSDataType::INT64, TSEncoding::RLE, CompressionType::SNAPPY);
  }
  REQUIRE( session->checkTimeseriesExists("root.test.d1.s1") == true );
  session->deleteTimeseries("root.test.d1.s1");
}

TEST_CASE( "Delete timeseries success", "[deleteTimeseries]" ) {
  if (!session->checkTimeseriesExists("root.test.d1.s1")) {
    session->createTimeseries("root.test.d1.s1", TSDataType::INT64, TSEncoding::RLE, CompressionType::SNAPPY);
  }
  REQUIRE( session->checkTimeseriesExists("root.test.d1.s1") == true );
  session->deleteTimeseries("root.test.d1.s1");
  REQUIRE( session->checkTimeseriesExists("root.test.d1.s1") == false );
}

TEST_CASE( "Test insertRecord by string", "[testInsertRecord]") {
  prepareTimeseries();
  string deviceId = "root.test.d1";
  vector<string> measurements = { "s1", "s2", "s3" };

  for (long time = 0; time < 100; time++) {
    vector<string> values = { "1", "2", "3" };
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
}

TEST_CASE( "Test insertRecords ", "[testInsertRecords]") {
  prepareTimeseries();
  string deviceId = "root.test.d1";
  vector<string> measurements = { "s1", "s2", "s3" };
  vector<string> deviceIds;
  vector<vector<string>> measurementsList;
  vector<vector<string>> valuesList;
  vector<int64_t> timestamps;

  for (int64_t time = 0; time < 500; time++) {
    vector<string> values = { "1", "2", "3" };

    deviceIds.push_back(deviceId);
    measurementsList.push_back(measurements);
    valuesList.push_back(values);
    timestamps.push_back(time);
    if (time != 0 && time % 100 == 0) {
      session->insertRecords(deviceIds, timestamps, measurementsList, valuesList);
      deviceIds.clear();
      measurementsList.clear();
      valuesList.clear();
      timestamps.clear();
    }
  }

  session->insertRecords(deviceIds, timestamps, measurementsList, valuesList);

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
  REQUIRE( count == 500 );
}

TEST_CASE( "Test insertRecord with types ", "[testTypedInsertRecord]") {
  vector<string> timeseries = {"root.test.d1.s1","root.test.d1.s2","root.test.d1.s3"};
  vector<TSDataType::TSDataType> types = { TSDataType::INT32, TSDataType::DOUBLE, TSDataType::INT64 };

  for (int i = 0; i < timeseries.size(); i++) {
    if (session->checkTimeseriesExists(timeseries[i])) {
      session->deleteTimeseries(timeseries[i]);
    }
    session->createTimeseries(timeseries[i], types[i], TSEncoding::RLE, CompressionType::SNAPPY);
  }
  string deviceId = "root.test.d1";
  vector<string> measurements = { "s1", "s2", "s3" };
  vector<int32_t> value1(100, 1);
  vector<double> value2(100, 2.2);
  vector<int64_t> value3(100, 3);

  for (long time = 0; time < 100; time++) {
    vector<char *> values = { (char *)(&value1[time]), (char *)(&value2[time]), (char *)(&value3[time]) };
    session->insertRecord(deviceId, time, measurements, types, values);
  }

  SessionDataSet *sessionDataSet = session->executeQueryStatement("select s1,s2,s3 from root.test.d1");
  sessionDataSet->setBatchSize(1024);
  long count = 0;
  while (sessionDataSet->hasNext()) {
    sessionDataSet->next();
    count++;
  }
  REQUIRE( count == 100 );
}

TEST_CASE( "Test insertRecords with types ", "[testTypedInsertRecords]") {
  vector<string> timeseries = {"root.test.d1.s1","root.test.d1.s2","root.test.d1.s3"};
  vector<TSDataType::TSDataType> types = { TSDataType::INT32, TSDataType::DOUBLE, TSDataType::INT64 };

  for (int i = 0; i < timeseries.size(); i++) {
    if (session->checkTimeseriesExists(timeseries[i])) {
      session->deleteTimeseries(timeseries[i]);
    }
    session->createTimeseries(timeseries[i], types[i], TSEncoding::RLE, CompressionType::SNAPPY);
  }
  string deviceId = "root.test.d1";
  vector<string> measurements = { "s1", "s2", "s3" };
  vector<string> deviceIds;
  vector<vector<string>> measurementsList;
  vector<vector<TSDataType::TSDataType>> typesList;
  vector<vector<char*>> valuesList;
  vector<int64_t> timestamps;
  vector<int32_t> value1(100, 1);
  vector<double> value2(100, 2.2);
  vector<int64_t> value3(100, 3);

  for (int64_t time = 0; time < 100; time++) {
    vector<char *> values = { (char *)(&value1[time]), (char *)(&value2[time]), (char *)(&value3[time]) };
    deviceIds.push_back(deviceId);
    measurementsList.push_back(measurements);
    typesList.push_back(types);
    valuesList.push_back(values);
    timestamps.push_back(time);
  }

  session->insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);

  SessionDataSet *sessionDataSet = session->executeQueryStatement("select * from root.test.d1");
  sessionDataSet->setBatchSize(1024);
  int count = 0;
  while (sessionDataSet->hasNext()) {
    sessionDataSet->next();
    count++;
  }
  REQUIRE( count == 100 );
}

TEST_CASE( "Test insertRecordsOfOneDevice", "[testInsertRecordsOfOneDevice]") {
  vector<string> timeseries = {"root.test.d1.s1","root.test.d1.s2","root.test.d1.s3"};
  vector<TSDataType::TSDataType> types = { TSDataType::INT32, TSDataType::DOUBLE, TSDataType::INT64 };

  for (int i = 0; i < timeseries.size(); i++) {
    if (session->checkTimeseriesExists(timeseries[i])) {
      session->deleteTimeseries(timeseries[i]);
    }
    session->createTimeseries(timeseries[i], types[i], TSEncoding::RLE, CompressionType::SNAPPY);
  }
  string deviceId = "root.test.d1";
  vector<string> measurements = { "s1", "s2", "s3" };
  vector<vector<string>> measurementsList;
  vector<vector<TSDataType::TSDataType>> typesList;
  vector<vector<char*>> valuesList;
  vector<int64_t> timestamps;
  vector<int32_t> value1(100, 1);
  vector<double> value2(100, 2.2);
  vector<int64_t> value3(100, 3);

  for (int64_t time = 0; time < 100; time++) {
    vector<char *> values = { (char *)(&value1[time]), (char *)(&value2[time]), (char *)(&value3[time]) };
    measurementsList.push_back(measurements);
    typesList.push_back(types);
    valuesList.push_back(values);
    timestamps.push_back(time);
  }

  session->insertRecordsOfOneDevice(deviceId, timestamps, measurementsList, typesList, valuesList);

  SessionDataSet *sessionDataSet = session->executeQueryStatement("select * from root.test.d1");
  sessionDataSet->setBatchSize(1024);
  int count = 0;
  while (sessionDataSet->hasNext()) {
    sessionDataSet->next();
    count++;
  }
  REQUIRE( count == 100 );
}

TEST_CASE( "Test insertTablet ", "[testInsertTablet]") {
  prepareTimeseries();
  string deviceId = "root.test.d1";
  vector<pair<string, TSDataType::TSDataType>> schemaList;
  schemaList.push_back(make_pair("s1", TSDataType::INT64));
  schemaList.push_back(make_pair("s2", TSDataType::INT64));
  schemaList.push_back(make_pair("s3", TSDataType::INT64));

  Tablet tablet(deviceId, schemaList, 100);
  for (int64_t time = 0; time < 100; time++) {
    int row = tablet.rowSize++;
    tablet.timestamps[row] = time;
    for (int i = 0; i < 3; i++) {
      tablet.values[i][row] = to_string(i);
    }
    if (tablet.rowSize == tablet.maxRowNumber) {
      session->insertTablet(tablet);
      tablet.reset();
    }
  }

  if (tablet.rowSize != 0) {
    session->insertTablet(tablet);
    tablet.reset();
  }
  SessionDataSet *sessionDataSet = session->executeQueryStatement("select * from root.test.d1");
  sessionDataSet->setBatchSize(1024);
  int count = 0;
  while (sessionDataSet->hasNext()) {
    long index = 0;
    count++;
    for (Field *f : sessionDataSet->next()->fields) {
      REQUIRE( f->longV == index );
      index++;
    }
  }
  REQUIRE( count == 100 );
}

TEST_CASE( "Test Last query ", "[testLastQuery]") {
  prepareTimeseries();
  string deviceId = "root.test.d1";
  vector<string> measurements = { "s1", "s2", "s3" };

  for (long time = 0; time < 100; time++) {
    vector<string> values = { "1", "2", "3" };
    session->insertRecord(deviceId, time, measurements, values);
  }

  vector<string> measurementValues = { "1", "2", "3" };
  SessionDataSet *sessionDataSet = session->executeQueryStatement("select last s1,s2,s3 from root.test.d1");
  sessionDataSet->setBatchSize(1024);
  long index = 0;
  while (sessionDataSet->hasNext()) {
    vector<Field*> fields = sessionDataSet->next()->fields;
    REQUIRE( fields[0]->stringV == deviceId + "." + measurements[index] );
    REQUIRE( fields[1]->stringV == measurementValues[index] );
    index++;
  }
}
