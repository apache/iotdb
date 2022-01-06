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

#include "Session.h"

using namespace std;

Session *session;

void createAlignedTimeseries() {
    string alignedDeviceId = "root.sg1.d1";
    vector<string> measurements = {"s1", "s2", "s3"};
    vector<string> alignedTimeseries = {"root.sg1.d1.s1","root.sg1.d1.s2","root.sg1.d1.s3"};
    vector<TSDataType::TSDataType> dataTypes = {TSDataType::INT32, TSDataType::DOUBLE, TSDataType::BOOLEAN};
    vector<TSEncoding::TSEncoding> encodings = {TSEncoding::PLAIN, TSEncoding::GORILLA, TSEncoding::RLE};
    vector<CompressionType::CompressionType> compressors = {
        CompressionType::SNAPPY, CompressionType::UNCOMPRESSED, CompressionType::SNAPPY};
    for (string timeseries : alignedTimeseries) {
        if (session->checkTimeseriesExists(timeseries)) {
            session->deleteTimeseries(timeseries);
        }
    }
    session->createAlignedTimeseries(alignedDeviceId, measurements, dataTypes, encodings, compressors);
}

void insertAlignedRecord() {
    string deviceId = "root.sg1.d1";
    vector<string> measurements;
    measurements.push_back("s1");
    measurements.push_back("s2");
    measurements.push_back("s3");

    for (int64_t time = 0; time < 100; time++) {
        vector<string> values;
        values.push_back("1");
        values.push_back("1.0");
        values.push_back("true");
        session->insertAlignedRecord(deviceId, time, measurements, values);
    }
}

void insertAlignedRecords() {
    string deviceId = "root.sg1.d1";
    vector<string> measurements;
    measurements.push_back("s1");
    measurements.push_back("s2");
    measurements.push_back("s3");

    vector<string> deviceIds;
    vector<vector<string>> measurementsList;
    vector<vector<string>> valuesList;
    vector<int64_t> timestamps;

    for (int64_t time = 100; time < 200; time++) {
        vector<string> values;
        values.push_back("1");
        values.push_back("1.0");
        values.push_back("true");

        deviceIds.push_back(deviceId);
        measurementsList.push_back(measurements);
        valuesList.push_back(values);
        timestamps.push_back(time);
        if (time != 100 && time % 100 == 0) {
            session->insertAlignedRecords(deviceIds, timestamps, measurementsList, valuesList);
            deviceIds.clear();
            measurementsList.clear();
            valuesList.clear();
            timestamps.clear();
        }
    }

    session->insertAlignedRecords(deviceIds, timestamps, measurementsList, valuesList);
}

void insertAlignedTablet() {
    pair<string, TSDataType::TSDataType> pairA("s1", TSDataType::INT32);
    pair<string, TSDataType::TSDataType> pairB("s2", TSDataType::DOUBLE);
    pair<string, TSDataType::TSDataType> pairC("s3", TSDataType::BOOLEAN);
    vector<pair<string, TSDataType::TSDataType>> schemas;
    schemas.push_back(pairA);
    schemas.push_back(pairB);
    schemas.push_back(pairC);

    Tablet tablet("root.sg1.d1", schemas, 100);
    tablet.setAligned(true);

    for (int64_t time = 200; time < 300; time++) {
        int row = tablet.rowSize++;
        tablet.timestamps[row] = time;
        tablet.values[0][row] = "1";
        tablet.values[1][row] = "1.0";
        tablet.values[2][row] = "true";
        if (tablet.rowSize == tablet.maxRowNumber) {
            session->insertTablet(tablet, true);
            tablet.reset();
        }
    }

    if (tablet.rowSize != 0) {
        session->insertTablet(tablet);
        tablet.reset();
    }
}

void insertAlignedTablets() {
    pair<string, TSDataType::TSDataType> pairA("s1", TSDataType::INT32);
    pair<string, TSDataType::TSDataType> pairB("s2", TSDataType::DOUBLE);
    pair<string, TSDataType::TSDataType> pairC("s3", TSDataType::BOOLEAN);
    vector<pair<string, TSDataType::TSDataType>> schemas;
    schemas.push_back(pairA);
    schemas.push_back(pairB);
    schemas.push_back(pairC);

    Tablet tablet1("root.sg1.d1", schemas, 100);
    Tablet tablet2("root.sg1.d2", schemas, 100);
    Tablet tablet3("root.sg1.d3", schemas, 100);

    map<string, Tablet*> tabletMap;
    tabletMap["root.sg1.d1"] = &tablet1;
    tabletMap["root.sg1.d2"] = &tablet2;
    tabletMap["root.sg1.d3"] = &tablet3;

    for (int64_t time = 300; time < 400; time++) {
        int row1 = tablet1.rowSize++;
        int row2 = tablet2.rowSize++;
        int row3 = tablet3.rowSize++;
        tablet1.timestamps[row1] = time;
        tablet2.timestamps[row2] = time;
        tablet3.timestamps[row3] = time;

        tablet1.values[0][row1] = "1";
        tablet2.values[0][row2] = "2";
        tablet3.values[0][row3] = "3";

        tablet1.values[1][row1] = "1.0";
        tablet2.values[1][row2] = "2.0";
        tablet3.values[1][row3] = "3.0";

        tablet1.values[2][row1] = "true";
        tablet2.values[2][row2] = "false";
        tablet3.values[2][row3] = "true";

        if (tablet1.rowSize == tablet1.maxRowNumber) {
            session->insertAlignedTablets(tabletMap, true);

            tablet1.reset();
            tablet2.reset();
            tablet3.reset();
        }
    }

    if (tablet1.rowSize != 0) {
        session->insertAlignedTablets(tabletMap, true);
        tablet1.reset();
        tablet2.reset();
        tablet3.reset();
    }
}

int main() {
    session = new Session("127.0.0.1", 6667, "root", "root");

    cout << "session open\n" << endl;
    session->open(false);

    cout << "setStorageGroup\n" << endl;
    try {
        session->setStorageGroup("root.sg1");
    }
    catch (IoTDBConnectionException e){
        string errorMessage(e.what());
        if (errorMessage.find("StorageGroupAlreadySetException") == string::npos) {
            cout << errorMessage << endl;
            throw e;
        }
    }

    cout << "createAlignedTimeseries\n" << endl;
    createAlignedTimeseries();

    cout << "insertAlignedRecord\n" << endl;
    insertAlignedRecord();

    cout << "insertAlignedRecords\n" << endl;
    insertAlignedRecords();

    cout << "insertAlignedTablet\n" << endl;
    insertAlignedTablet();

    cout << "insertAlignedTablets\n" << endl;
    insertAlignedTablets();

    cout << "session close\n" << endl;
    session->close();

    delete session;

    cout << "finished\n" << endl;
    return 0;
}
