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

static vector<string> testTimeseries = {"root.test.d1.s1", "root.test.d1.s2", "root.test.d1.s3"};

void prepareTimeseries() {
    for (const string &timeseries: testTimeseries) {
        if (session->checkTimeseriesExists(timeseries)) {
            session->deleteTimeseries(timeseries);
        }
        session->createTimeseries(timeseries, TSDataType::INT64, TSEncoding::RLE, CompressionType::SNAPPY);
    }
}

static int global_test_id = 0;
class CaseReporter
{
public:
    CaseReporter(const char *caseNameArg) : caseName(caseNameArg)
    {
        test_id = global_test_id++;
        std::cout << "Test " << test_id << ": " << caseName << std::endl;
    }
    ~CaseReporter()
    {
        std::cout << "Test " << test_id << ": " << caseName << " Done"<< std::endl << std::endl;
    }
private:
    const char *caseName;
    int test_id;
};

TEST_CASE("Create timeseries success", "[createTimeseries]") {
    CaseReporter cr("createTimeseries");
    if (!session->checkTimeseriesExists("root.test.d1.s1")) {
        session->createTimeseries("root.test.d1.s1", TSDataType::INT64, TSEncoding::RLE, CompressionType::SNAPPY);
    }
    REQUIRE(session->checkTimeseriesExists("root.test.d1.s1") == true);
    session->deleteTimeseries("root.test.d1.s1");
}

TEST_CASE("Delete timeseries success", "[deleteTimeseries]") {
    CaseReporter cr("deleteTimeseries");
    if (!session->checkTimeseriesExists("root.test.d1.s1")) {
        session->createTimeseries("root.test.d1.s1", TSDataType::INT64, TSEncoding::RLE, CompressionType::SNAPPY);
    }
    REQUIRE(session->checkTimeseriesExists("root.test.d1.s1") == true);
    session->deleteTimeseries("root.test.d1.s1");
    REQUIRE(session->checkTimeseriesExists("root.test.d1.s1") == false);
}

TEST_CASE("Test insertRecord by string", "[testInsertRecord]") {
    CaseReporter cr("testInsertRecord");
    prepareTimeseries();
    string deviceId = "root.test.d1";
    vector<string> measurements = {"s1", "s2", "s3"};

    for (long time = 0; time < 100; time++) {
        vector<string> values = {"1", "2", "3"};
        session->insertRecord(deviceId, time, measurements, values);
    }

    session->executeNonQueryStatement("insert into root.test.d1(timestamp,s1, s2, s3) values(100, 1,2,3)");

    unique_ptr<SessionDataSet> sessionDataSet = session->executeQueryStatement("select s1,s2,s3 from root.test.d1");
    sessionDataSet->setFetchSize(1024);
    int count = 0;
    while (sessionDataSet->hasNext()) {
        long index = 1;
        count++;
        for (const Field &f: sessionDataSet->next()->fields) {
            REQUIRE(f.longV == index);
            index++;
        }
    }
    REQUIRE(count == 101);
}

TEST_CASE("Test insertRecords ", "[testInsertRecords]") {
    CaseReporter cr("testInsertRecords");
    prepareTimeseries();
    string deviceId = "root.test.d1";
    vector<string> measurements = {"s1", "s2", "s3"};
    vector<string> deviceIds;
    vector<vector<string>> measurementsList;
    vector<vector<string>> valuesList;
    vector<int64_t> timestamps;

    int64_t COUNT = 500;
    for (int64_t time = 1; time <= COUNT; time++) {
        vector<string> values = {"1", "2", "3"};

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

    if (timestamps.size() > 0) {
        session->insertRecords(deviceIds, timestamps, measurementsList, valuesList);
    }

    unique_ptr<SessionDataSet> sessionDataSet = session->executeQueryStatement("select s1,s2,s3 from root.test.d1");
    sessionDataSet->setFetchSize(1024);
    int count = 0;
    while (sessionDataSet->hasNext()) {
        long index = 1;
        count++;
        for (const Field &f: sessionDataSet->next()->fields) {
            REQUIRE(f.longV == index);
            index++;
        }
    }
    REQUIRE(count == COUNT);
}

TEST_CASE("Test insertRecord with types ", "[testTypedInsertRecord]") {
    CaseReporter cr("testTypedInsertRecord");
    vector<string> timeseries = {"root.test.d1.s1", "root.test.d1.s2", "root.test.d1.s3"};
    vector<TSDataType::TSDataType> types = {TSDataType::INT32, TSDataType::DOUBLE, TSDataType::INT64};

    for (size_t i = 0; i < timeseries.size(); i++) {
        if (session->checkTimeseriesExists(timeseries[i])) {
            session->deleteTimeseries(timeseries[i]);
        }
        session->createTimeseries(timeseries[i], types[i], TSEncoding::RLE, CompressionType::SNAPPY);
    }
    string deviceId = "root.test.d1";
    vector<string> measurements = {"s1", "s2", "s3"};
    vector<int32_t> value1(100, 1);
    vector<double> value2(100, 2.2);
    vector<int64_t> value3(100, 3);

    for (long time = 0; time < 100; time++) {
        vector<char *> values = {(char *) (&value1[time]), (char *) (&value2[time]), (char *) (&value3[time])};
        session->insertRecord(deviceId, time, measurements, types, values);
    }

    unique_ptr<SessionDataSet> sessionDataSet = session->executeQueryStatement("select s1,s2,s3 from root.test.d1");
    sessionDataSet->setFetchSize(1024);
    long count = 0;
    while (sessionDataSet->hasNext()) {
        sessionDataSet->next();
        count++;
    }
    REQUIRE(count == 100);
}

TEST_CASE("Test insertRecords with types ", "[testTypedInsertRecords]") {
    CaseReporter cr("testTypedInsertRecords");
    vector<string> timeseries = {"root.test.d1.s1", "root.test.d1.s2", "root.test.d1.s3"};
    vector<TSDataType::TSDataType> types = {TSDataType::INT32, TSDataType::DOUBLE, TSDataType::INT64};

    for (size_t i = 0; i < timeseries.size(); i++) {
        if (session->checkTimeseriesExists(timeseries[i])) {
            session->deleteTimeseries(timeseries[i]);
        }
        session->createTimeseries(timeseries[i], types[i], TSEncoding::RLE, CompressionType::SNAPPY);
    }
    string deviceId = "root.test.d1";
    vector<string> measurements = {"s1", "s2", "s3"};
    vector<string> deviceIds;
    vector<vector<string>> measurementsList;
    vector<vector<TSDataType::TSDataType>> typesList;
    vector<vector<char *>> valuesList;
    vector<int64_t> timestamps;
    vector<int32_t> value1(100, 1);
    vector<double> value2(100, 2.2);
    vector<int64_t> value3(100, 3);

    for (int64_t time = 0; time < 100; time++) {
        vector<char *> values = {(char *) (&value1[time]), (char *) (&value2[time]), (char *) (&value3[time])};
        deviceIds.push_back(deviceId);
        measurementsList.push_back(measurements);
        typesList.push_back(types);
        valuesList.push_back(values);
        timestamps.push_back(time);
    }

    session->insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);

    unique_ptr<SessionDataSet> sessionDataSet = session->executeQueryStatement("select s1,s2,s3 from root.test.d1");
    sessionDataSet->setFetchSize(1024);
    int count = 0;
    while (sessionDataSet->hasNext()) {
        sessionDataSet->next();
        count++;
    }
    REQUIRE(count == 100);
}

TEST_CASE("Test insertRecordsOfOneDevice", "[testInsertRecordsOfOneDevice]") {
    CaseReporter cr("testInsertRecordsOfOneDevice");
    vector<string> timeseries = {"root.test.d1.s1", "root.test.d1.s2", "root.test.d1.s3"};
    vector<TSDataType::TSDataType> types = {TSDataType::INT32, TSDataType::DOUBLE, TSDataType::INT64};

    for (size_t i = 0; i < timeseries.size(); i++) {
        if (session->checkTimeseriesExists(timeseries[i])) {
            session->deleteTimeseries(timeseries[i]);
        }
        session->createTimeseries(timeseries[i], types[i], TSEncoding::RLE, CompressionType::SNAPPY);
    }
    string deviceId = "root.test.d1";
    vector<string> measurements = {"s1", "s2", "s3"};
    vector<vector<string>> measurementsList;
    vector<vector<TSDataType::TSDataType>> typesList;
    vector<vector<char *>> valuesList;
    vector<int64_t> timestamps;
    vector<int32_t> value1(100, 1);
    vector<double> value2(100, 2.2);
    vector<int64_t> value3(100, 3);

    for (int64_t time = 0; time < 100; time++) {
        vector<char *> values = {(char *) (&value1[time]), (char *) (&value2[time]), (char *) (&value3[time])};
        measurementsList.push_back(measurements);
        typesList.push_back(types);
        valuesList.push_back(values);
        timestamps.push_back(time);
    }

    session->insertRecordsOfOneDevice(deviceId, timestamps, measurementsList, typesList, valuesList);

    unique_ptr<SessionDataSet> sessionDataSet = session->executeQueryStatement("select * from root.test.d1");
    sessionDataSet->setFetchSize(1024);
    int count = 0;
    while (sessionDataSet->hasNext()) {
        sessionDataSet->next();
        count++;
    }
    REQUIRE(count == 100);
}

TEST_CASE("Test insertTablet ", "[testInsertTablet]") {
    CaseReporter cr("testInsertTablet");
    prepareTimeseries();
    string deviceId = "root.test.d1";
    vector<pair<string, TSDataType::TSDataType>> schemaList;
    schemaList.emplace_back("s1", TSDataType::INT64);
    schemaList.emplace_back("s2", TSDataType::INT64);
    schemaList.emplace_back("s3", TSDataType::INT64);

    Tablet tablet(deviceId, schemaList, 100);
    for (int64_t time = 0; time < 100; time++) {
        int row = tablet.rowSize++;
        tablet.timestamps[row] = time;
        for (int64_t i = 0; i < 3; i++) {
            tablet.addValue(i, row, i);
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
    unique_ptr<SessionDataSet> sessionDataSet = session->executeQueryStatement("select s1,s2,s3 from root.test.d1");
    sessionDataSet->setFetchSize(1024);
    int count = 0;
    while (sessionDataSet->hasNext()) {
        long index = 0;
        count++;
        for (const Field& f: sessionDataSet->next()->fields) {
            REQUIRE(f.longV == index);
            index++;
        }
    }
    REQUIRE(count == 100);
}

TEST_CASE("Test Last query ", "[testLastQuery]") {
    CaseReporter cr("testLastQuery");
    prepareTimeseries();
    string deviceId = "root.test.d1";
    vector<string> measurements = {"s1", "s2", "s3"};

    for (long time = 0; time < 100; time++) {
        vector<string> values = {"1", "2", "3"};
        session->insertRecord(deviceId, time, measurements, values);
    }

    vector<string> measurementValues = {"1", "2", "3"};
    unique_ptr<SessionDataSet> sessionDataSet = session->executeQueryStatement(
            "select last s1,s2,s3 from root.test.d1");
    sessionDataSet->setFetchSize(1024);
    long index = 0;
    while (sessionDataSet->hasNext()) {
        vector<Field> fields = sessionDataSet->next()->fields;
        REQUIRE("1" <= fields[1].stringV);
        REQUIRE(fields[1].stringV <= "3");
        index++;
    }
}

TEST_CASE("Test Huge query ", "[testHugeQuery]") {
    CaseReporter cr("testHugeQuery");
    prepareTimeseries();
    string deviceId = "root.test.d1";
    vector<string> measurements = {"s1", "s2", "s3"};
    vector<TSDataType::TSDataType> types = {TSDataType::INT64, TSDataType::INT64, TSDataType::INT64};
    int64_t value1 = 1, value2 = 2, value3 = 3;
    vector<char*> values = {(char*)&value1, (char*)&value2, (char*)&value3};

    long total_count = 500000;
    int print_count = 0;
    std::cout.width(7);
    std::cout << "inserting " << total_count << " rows:" << std::endl;
    for (long time = 0; time < total_count; time++) {
        session->insertRecord(deviceId, time, measurements, types, values);
        if (time != 0 && time % 1000 == 0) {
            std::cout << time << "\t" << std::flush;
            if (++print_count % 20 == 0) {
                std::cout << std::endl;
            }
        }
    }

    unique_ptr<SessionDataSet> sessionDataSet = session->executeQueryStatement("select s1,s2,s3 from root.test.d1");
    sessionDataSet->setFetchSize(1024);
    RowRecord* rowRecord;
    int count = 0;
    print_count = 0;
    std::cout << "\n\niterating " << total_count << " rows:" << std::endl;
    while (sessionDataSet->hasNext()) {
        rowRecord = sessionDataSet->next();
        REQUIRE(rowRecord->timestamp == count);
        REQUIRE(rowRecord->fields[0].longV== 1);
        REQUIRE(rowRecord->fields[1].longV == 2);
        REQUIRE(rowRecord->fields[2].longV == 3);
        count++;
        if (count % 1000 == 0) {
            std::cout << count << "\t" << std::flush;
            if (++print_count % 20 == 0) {
                std::cout << std::endl;
            }
        }
    }

    REQUIRE(count == total_count);
}


TEST_CASE("Test executeRawDataQuery ", "[executeRawDataQuery]") {
    CaseReporter cr("executeRawDataQuery");
    prepareTimeseries();

    string deviceId = "root.test.d1";
    vector<string> measurements = {"s1", "s2", "s3"};
    vector<TSDataType::TSDataType> types = {TSDataType::INT64, TSDataType::INT64, TSDataType::INT64};

    long total_count = 5000;
    vector<char*> values;
    int64_t valueArray[3];
    for (long time = -total_count; time < total_count; time++) {
        valueArray[0] = time;
        valueArray[1] = time * 2;
        valueArray[2] = time * 3;
        values.clear();
        values.push_back((char*)&valueArray[0]);
        values.push_back((char*)&valueArray[1]);
        values.push_back((char*)&valueArray[2]);
        session->insertRecord(deviceId, time, measurements, types, values);
        if (time == 100) {   //insert 1 big timestamp data for generate un-seq data.
            valueArray[0] = 9;
            valueArray[2] = 999;
            values.clear();
            values.push_back((char*)&valueArray[0]);
            values.push_back((char*)&valueArray[2]);
            vector<string> measurements2 = {"s1", "s3"};
            vector<TSDataType::TSDataType> types2 = {TSDataType::INT64, TSDataType::INT64};
            session->insertRecord(deviceId, 99999, measurements2, types2, values);
        }
    }

    vector<string> paths;
    paths.push_back("root.test.d1.s1");
    paths.push_back("root.test.d1.s2");
    paths.push_back("root.test.d1.s3");

    //== Test executeRawDataQuery() with negative timestamp
    int startTs = -total_count, endTs = total_count;
    unique_ptr<SessionDataSet> sessionDataSet = session->executeRawDataQuery(paths, startTs, endTs);
    sessionDataSet->setFetchSize(10);
    vector<string> columns = sessionDataSet->getColumnNames();
    columns = sessionDataSet->getColumnNames();
    for (const string &column : columns) {
        cout << column << " " ;
    }
    cout << endl;
    REQUIRE(columns[0] == "Time");
    REQUIRE(columns[1] == paths[0]);
    REQUIRE(columns[2] == paths[1]);
    REQUIRE(columns[3] == paths[2]);

    int ts = startTs;
    while (sessionDataSet->hasNext()) {
        RowRecord *rowRecordPtr = sessionDataSet->next();
        //cout << rowRecordPtr->toString();

        vector<Field> fields = rowRecordPtr->fields;
        REQUIRE(rowRecordPtr->timestamp == ts);
        REQUIRE(fields[0].dataType == TSDataType::INT64);
        REQUIRE(fields[0].longV == ts);
        REQUIRE(fields[1].dataType == TSDataType::INT64);
        REQUIRE(fields[1].longV == ts * 2);
        REQUIRE(fields[2].dataType == TSDataType::INT64);
        REQUIRE(fields[2].longV == ts *3);
        ts++;
    }


    //== Test executeRawDataQuery() with null field
    startTs = 99999;
    endTs = 99999 + 10;
    sessionDataSet = session->executeRawDataQuery(paths, startTs, endTs);

    sessionDataSet->setFetchSize(10);
    columns = sessionDataSet->getColumnNames();
    for (const string &column : columns) {
        cout << column << " " ;
    }
    cout << endl;
    REQUIRE(columns[0] == "Time");
    REQUIRE(columns[1] == paths[0]);
    REQUIRE(columns[2] == paths[1]);
    REQUIRE(columns[3] == paths[2]);
    ts = startTs;
    while (sessionDataSet->hasNext()) {
        RowRecord *rowRecordPtr = sessionDataSet->next();
        cout << rowRecordPtr->toString();

        vector<Field> fields = rowRecordPtr->fields;
        REQUIRE(rowRecordPtr->timestamp == ts);
        REQUIRE(fields[0].dataType == TSDataType::INT64);
        REQUIRE(fields[0].longV == 9);
        REQUIRE(fields[1].dataType == TSDataType::NULLTYPE);
        REQUIRE(fields[2].dataType == TSDataType::INT64);
        REQUIRE(fields[2].longV == 999);
    }

    //== Test executeRawDataQuery() with empty data
    sessionDataSet = session->executeRawDataQuery(paths, 100000, 110000);
    sessionDataSet->setFetchSize(1);
    REQUIRE(sessionDataSet->hasNext() == false);
}

TEST_CASE("Test executeLastDataQuery ", "[testExecuteLastDataQuery]") {
    CaseReporter cr("testExecuteLastDataQuery");
    prepareTimeseries();

    string deviceId = "root.test.d1";
    vector<string> measurements = {"s1", "s2", "s3"};
    vector<TSDataType::TSDataType> types = {TSDataType::INT64, TSDataType::INT64, TSDataType::INT64};

    long total_count = 5000;
    vector<char*> values;
    int64_t valueArray[3];
    for (long time = -total_count; time < total_count; time++) {
        valueArray[0] = time;
        valueArray[1] = time * 2;
        valueArray[2] = time * 3;
        values.clear();
        values.push_back((char*)&valueArray[0]);
        values.push_back((char*)&valueArray[1]);
        values.push_back((char*)&valueArray[2]);
        session->insertRecord(deviceId, time, measurements, types, values);
        if (time == 100) {    //insert 1 big timestamp data for gen unseq data.
            valueArray[0] = 9;
            valueArray[2] = 999;
            values.clear();
            values.push_back((char*)&valueArray[0]);
            values.push_back((char*)&valueArray[2]);
            vector<string> measurements2 = {"s1", "s3"};
            vector<TSDataType::TSDataType> types2 = {TSDataType::INT64, TSDataType::INT64};
            session->insertRecord(deviceId, 99999, measurements2, types2, values);
        }
    }

    int64_t tsCheck[3] = {99999, 4999, 99999};
    std::vector<std::string> valueCheck = {"9", "9998", "999"};

    vector<string> paths;
    paths.push_back("root.test.d1.s1");
    paths.push_back("root.test.d1.s2");
    paths.push_back("root.test.d1.s3");

    //== Test executeLastDataQuery() without lastTime
    unique_ptr<SessionDataSet> sessionDataSet = session->executeLastDataQuery(paths);
    sessionDataSet->setFetchSize(1);

    vector<string> columns = sessionDataSet->getColumnNames();
    for (const string &column : columns) {
        cout << column << " " ;
    }
    cout << endl;

    int index = 0;
    while (sessionDataSet->hasNext()) {
        RowRecord *rowRecordPtr = sessionDataSet->next();
        cout << rowRecordPtr->toString();

        vector<Field> fields = rowRecordPtr->fields;
        REQUIRE(rowRecordPtr->timestamp == tsCheck[index]);
        REQUIRE(fields[0].stringV == paths[index]);
        REQUIRE(fields[1].stringV == valueCheck[index]);
        REQUIRE(fields[2].stringV == "INT64");
        index++;
    }

    //== Test executeLastDataQuery() with negative lastTime
    sessionDataSet = session->executeLastDataQuery(paths, -200);
    sessionDataSet->setFetchSize(1);
    columns = sessionDataSet->getColumnNames();
    for (const string &column : columns) {
        cout << column << " " ;
    }
    cout << endl;

    index = 0;
    while (sessionDataSet->hasNext()) {
        RowRecord *rowRecordPtr = sessionDataSet->next();
        cout << rowRecordPtr->toString();

        vector<Field> fields = rowRecordPtr->fields;
        REQUIRE(rowRecordPtr->timestamp == tsCheck[index]);
        REQUIRE(fields[0].stringV == paths[index]);
        REQUIRE(fields[1].stringV == valueCheck[index]);
        REQUIRE(fields[2].stringV == "INT64");
        index++;
    }

    //== Test executeLastDataQuery() with the lastTime that is > largest timestamp.
    sessionDataSet = session->executeLastDataQuery(paths, 100000);
    sessionDataSet->setFetchSize(1024);
    REQUIRE(sessionDataSet->hasNext() == false);
}
