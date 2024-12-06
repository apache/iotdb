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

#define DEFAULT_ROW_NUMBER 1000000

void createAlignedTimeseries() {
    string alignedDeviceId = "root.sg1.d1";
    vector<string> measurements = {"s1", "s2", "s3"};
    vector<string> alignedTimeseries = {"root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s3"};
    vector<TSDataType::TSDataType> dataTypes = {TSDataType::INT32, TSDataType::DOUBLE, TSDataType::BOOLEAN};
    vector<TSEncoding::TSEncoding> encodings = {TSEncoding::PLAIN, TSEncoding::GORILLA, TSEncoding::RLE};
    vector<CompressionType::CompressionType> compressors = {
            CompressionType::SNAPPY, CompressionType::UNCOMPRESSED, CompressionType::SNAPPY};
    for (const string &timeseries: alignedTimeseries) {
        if (session->checkTimeseriesExists(timeseries)) {
            session->deleteTimeseries(timeseries);
        }
    }
    session->createAlignedTimeseries(alignedDeviceId, measurements, dataTypes, encodings, compressors);
}

void createSchemaTemplate() {
    if (!session->checkTemplateExists("template1")) {
        Template temp("template1", false);

        InternalNode iNodeD99("d99", true);

        MeasurementNode mNodeS1("s1", TSDataType::INT32, TSEncoding::RLE, CompressionType::SNAPPY);
        MeasurementNode mNodeS2("s2", TSDataType::INT64, TSEncoding::RLE, CompressionType::SNAPPY);
        MeasurementNode mNodeD99S1("s1", TSDataType::DOUBLE, TSEncoding::RLE, CompressionType::SNAPPY);
        MeasurementNode mNodeD99S2("s2", TSDataType::BOOLEAN, TSEncoding::RLE, CompressionType::SNAPPY);

        iNodeD99.addChild(mNodeD99S1);
        iNodeD99.addChild(mNodeD99S2);

        temp.addToTemplate(iNodeD99);
        temp.addToTemplate(mNodeS1);
        temp.addToTemplate(mNodeS2);

        session->createSchemaTemplate(temp);
        session->setSchemaTemplate("template1", "root.sg3.d1");
    }
}

void ActivateTemplate() {
    session->executeNonQueryStatement("insert into root.sg3.d1(timestamp,s1, s2) values(200, 1, 1);");
}

void showDevices() {
    unique_ptr<SessionDataSet> dataSet = session->executeQueryStatement("show devices with database");
    for (const string &name: dataSet->getColumnNames()) {
        cout << name << "  ";
    }
    cout << endl;

    dataSet->setFetchSize(1024);
    while (dataSet->hasNext()) {
        cout << dataSet->next()->toString();
    }
    cout << endl;

    dataSet->closeOperationHandle();
}

void showTimeseries() {
    unique_ptr<SessionDataSet> dataSet = session->executeQueryStatement("show timeseries");
    for (const string &name: dataSet->getColumnNames()) {
        cout << name << "  ";
    }
    cout << endl;

    dataSet->setFetchSize(1024);
    while (dataSet->hasNext()) {
        cout << dataSet->next()->toString();
    }
    cout << endl;

    dataSet->closeOperationHandle();
}

void insertAlignedRecord() {
    string deviceId = "root.sg1.d1";
    vector<string> measurements;
    measurements.emplace_back("s1");
    measurements.emplace_back("s2");
    measurements.emplace_back("s3");

    for (int64_t time = 0; time < 10; time++) {
        vector<string> values;
        values.emplace_back("1");
        values.emplace_back("1.0");
        values.emplace_back("true");
        session->insertAlignedRecord(deviceId, time, measurements, values);
    }
}

void insertAlignedRecords() {
    string deviceId = "root.sg1.d1";
    vector<string> measurements;
    measurements.emplace_back("s1");
    measurements.emplace_back("s2");
    measurements.emplace_back("s3");

    vector<string> deviceIds;
    vector<vector<string>> measurementsList;
    vector<vector<string>> valuesList;
    vector<int64_t> timestamps;

    for (int64_t time = 10; time < 20; time++) {
        vector<string> values;
        values.emplace_back("1");
        values.emplace_back("1.0");
        values.emplace_back("true");

        deviceIds.push_back(deviceId);
        measurementsList.push_back(measurements);
        valuesList.push_back(values);
        timestamps.push_back(time);
        if (time != 10 && time % 10 == 0) {
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
    pair<string, TSDataType::TSDataType> pairC("s3", TSDataType::DOUBLE);
    vector<pair<string, TSDataType::TSDataType>> schemas;
    schemas.push_back(pairA);
    schemas.push_back(pairB);
    schemas.push_back(pairC);

    Tablet tablet("root.sg2.d2", schemas, 100000);
    tablet.setAligned(true);

    for (int64_t time = 0; time < DEFAULT_ROW_NUMBER; time++) {
        size_t row = tablet.rowSize++;
        tablet.timestamps[row] = time;
        int randVal1 = 123456;
        double randVal2 = 123456.1234;
        double randVal3 = 123456.1234;
        tablet.addValue(0, row, randVal1);
        tablet.addValue(1, row, randVal2);
        tablet.addValue(2, row, randVal3);
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

    unordered_map<string, Tablet *> tabletMap;
    tabletMap["root.sg1.d1"] = &tablet1;
    tabletMap["root.sg1.d2"] = &tablet2;
    tabletMap["root.sg1.d3"] = &tablet3;

    for (int64_t time = 0; time < 20; time++) {
        size_t row1 = tablet1.rowSize++;
        size_t row2 = tablet2.rowSize++;
        size_t row3 = tablet3.rowSize++;
        tablet1.timestamps[row1] = time;
        tablet2.timestamps[row2] = time;
        tablet3.timestamps[row3] = time;

        int randVal11 = rand();
        int randVal12 = rand();
        int randVal13 = rand();
        tablet1.addValue(0, row1, randVal11);
        tablet2.addValue(0, row2, randVal12);
        tablet3.addValue(0, row3, randVal13);

        double randVal21 = rand() / 99.9;
        double randVal22 = rand() / 99.9;
        double randVal23 = rand() / 99.9;
        tablet1.addValue(1, row1, randVal21);
        tablet2.addValue(1, row2, randVal22);
        tablet3.addValue(1, row3, randVal23);

        bool randVal31 = (bool)(rand() % 2);
        bool randVal32 = (bool)(rand() % 2);
        bool randVal33 = (bool)(rand() % 2);
        tablet1.addValue(2, row1, randVal31);
        tablet2.addValue(2, row2, randVal32);
        tablet3.addValue(2, row3, randVal33);

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

void insertNullableTabletWithAlignedTimeseries() {
    pair<string, TSDataType::TSDataType> pairA("s1", TSDataType::INT32);
    pair<string, TSDataType::TSDataType> pairB("s2", TSDataType::INT64);
    pair<string, TSDataType::TSDataType> pairC("s3", TSDataType::BOOLEAN);
    vector<pair<string, TSDataType::TSDataType>> schemas;
    schemas.push_back(pairA);
    schemas.push_back(pairB);
    schemas.push_back(pairC);

    Tablet tablet("root.sg1.d4", schemas, 20);
    tablet.setAligned(true);

    for (int64_t time = 0; time < 20; time++) {
        size_t row = tablet.rowSize++;
        tablet.timestamps[row] = time;
        for (int i = 0; i < 3; i++) {
            int randVal1 = rand();
            int64_t randVal2 = rand();
            bool randVal3 = (bool)(rand() % 2);
            if (i == 0) {
                tablet.addValue(i, row, randVal1);
            } else if (i == 1) {
                tablet.addValue(i, row, randVal2);
            } else {
                tablet.addValue(i, row, randVal3);
            }
            // mark null value
            if ((row % 3) == (unsigned int) i) {
                tablet.bitMaps[i].mark(row);
            }
        }
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

void query() {
    unique_ptr<SessionDataSet> dataSet = session->executeQueryStatement("select * from root.sg1.**");
    cout << "timestamp" << "  ";
    for (const string &name: dataSet->getColumnNames()) {
        cout << name << "  ";
    }
    cout << endl;

    dataSet->setFetchSize(1024);
    while (dataSet->hasNext()) {
        cout << dataSet->next()->toString();
    }
    cout << endl;

    dataSet->closeOperationHandle();
}

void deleteData() {
    string path = "root.**";
    int64_t deleteTime = 49;
    session->deleteData(path, deleteTime);
}

void deleteTimeseries() {
    vector<string> paths;
    vector<string> alignedTimeseries = {"root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s3", "root.sg1.d1.s4",
                                        "root.sg1.d2.s1", "root.sg1.d2.s2", "root.sg1.d2.s3",
                                        "root.sg1.d3.s1", "root.sg1.d3.s2", "root.sg1.d3.s3",
                                        "root.sg1.d4.s1", "root.sg1.d4.s2", "root.sg1.d4.s3",
                                        "root.sg2.d2.s1", "root.sg2.d2.s2", "root.sg2.d2.s3", };
    for (const string &timeseries: alignedTimeseries) {
        if (session->checkTimeseriesExists(timeseries)) {
            paths.push_back(timeseries);
        }
    }
    session->deleteTimeseries(paths);
}

void deleteStorageGroups() {
    vector<string> storageGroups;
    storageGroups.emplace_back("root.sg1");
    storageGroups.emplace_back("root.sg2");
    session->deleteStorageGroups(storageGroups);
}


int main() {
    LOG_LEVEL = LEVEL_DEBUG;

    session = new Session("127.0.0.1", 6667, "root", "root");

    cout << "session open\n" << endl;
    session->open(false);

    cout << "setStorageGroup\n" << endl;
    try {
        session->setStorageGroup("root.sg1");
    }
    catch (IoTDBException &e) {
        string errorMessage(e.what());
        if (errorMessage.find("StorageGroupAlreadySetException") == string::npos) {
            cout << errorMessage << endl;
            //throw e;
        }
    }

    cout << "createAlignedTimeseries\n" << endl;
    createAlignedTimeseries();

    cout << "createSchemaTemplate\n" << endl;
    createSchemaTemplate();

    cout << "ActivateTemplate\n" << endl;
    ActivateTemplate();

    cout << "showDevices\n" << endl;
    showDevices();

    cout << "showTimeseries\n" << endl;
    showTimeseries();

    cout << "insertAlignedRecord\n" << endl;
    insertAlignedRecord();

    cout << "insertAlignedRecords\n" << endl;
    insertAlignedRecords();

    cout << "insertAlignedTablet" << endl;
    cout << "Insert " << DEFAULT_ROW_NUMBER << " records." << endl;
    time_t now1 = time(0);
    insertAlignedTablet();
    time_t now2 = time(0);
    time_t useTime = now2 - now1;
    cout << "Use time: " << useTime << "s.\n" << endl;

    cout << "insertAlignedTablets\n" << endl;
    insertAlignedTablets();

    cout << "insertNullableTabletWithAlignedTimeseries\n" << endl;
    insertNullableTabletWithAlignedTimeseries();

    cout << "query\n" << endl;
    query();

    cout << "deleteData\n" << endl;
    deleteData();

    cout << "deleteTimeseries\n" << endl;
    deleteTimeseries();

    cout << "deleteStorageGroups\n" << endl;
    deleteStorageGroups();

    cout << "session close\n" << endl;
    session->close();

    delete session;

    cout << "finished\n" << endl;
    return 0;
}
