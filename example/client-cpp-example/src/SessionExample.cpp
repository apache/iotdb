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

void createTimeseries() {
    if (!session->checkTimeseriesExists("root.sg1.d1.s1")) {
        session->createTimeseries("root.sg1.d1.s1", TSDataType::BOOLEAN, TSEncoding::RLE,
                                  CompressionType::SNAPPY);
    }
    if (!session->checkTimeseriesExists("root.sg1.d1.s2")) {
        session->createTimeseries("root.sg1.d1.s2", TSDataType::INT32, TSEncoding::RLE,
                                  CompressionType::SNAPPY);
    }
    if (!session->checkTimeseriesExists("root.sg1.d1.s3")) {
        session->createTimeseries("root.sg1.d1.s3", TSDataType::FLOAT, TSEncoding::RLE,
                                  CompressionType::SNAPPY);
    }

    // create timeseries with tags and attributes
    if (!session->checkTimeseriesExists("root.sg1.d1.s4")) {
        map<string, string> tags;
        tags["tag1"] = "v1";
        map<string, string> attributes;
        attributes["description"] = "v1";
        session->createTimeseries("root.sg1.d1.s4", TSDataType::INT64, TSEncoding::RLE,
                                  CompressionType::SNAPPY, nullptr, &tags, &attributes, "temperature");
    }
}

void createMultiTimeseries() {
    if (!session->checkTimeseriesExists("root.sg1.d2.s1") && !session->checkTimeseriesExists("root.sg1.d2.s2")) {
        vector<string> paths;
        paths.emplace_back("root.sg1.d2.s1");
        paths.emplace_back("root.sg1.d2.s2");
        vector<TSDataType::TSDataType> tsDataTypes;
        tsDataTypes.push_back(TSDataType::INT64);
        tsDataTypes.push_back(TSDataType::DOUBLE);
        vector<TSEncoding::TSEncoding> tsEncodings;
        tsEncodings.push_back(TSEncoding::RLE);
        tsEncodings.push_back(TSEncoding::RLE);
        vector<CompressionType::CompressionType> compressionTypes;
        compressionTypes.push_back(CompressionType::SNAPPY);
        compressionTypes.push_back(CompressionType::SNAPPY);

        vector<map<string, string>> tagsList;
        map<string, string> tags;
        tags["unit"] = "kg";
        tagsList.push_back(tags);
        tagsList.push_back(tags);

        vector<map<string, string>> attributesList;
        map<string, string> attributes;
        attributes["minValue"] = "1";
        attributes["maxValue"] = "100";
        attributesList.push_back(attributes);
        attributesList.push_back(attributes);

        vector<string> alias;
        alias.emplace_back("weight1");
        alias.emplace_back("weight2");

        session->createMultiTimeseries(paths, tsDataTypes, tsEncodings, compressionTypes, nullptr, &tagsList,
                                       &attributesList, &alias);
    }
}

void createSchemaTemplate() {
    if (!session->checkTemplateExists("template1")) {
        Template temp("template1", false);

        InternalNode iNodeD99("d99", false);

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

void showTimeseries() {
    unique_ptr<SessionDataSet> dataSet = session->executeQueryStatement("show timeseries");
    for (const string &name: dataSet->getColumnNames()) {
        cout << name << "  ";
    }
    cout << endl;

    dataSet->setBatchSize(1024);
    while (dataSet->hasNext()) {
        cout << dataSet->next()->toString();
    }
    cout << endl;

    dataSet->closeOperationHandle();
}

void insertRecord() {
    string deviceId = "root.sg2.d1";
    vector<string> measurements;
    measurements.emplace_back("s1");
    measurements.emplace_back("s2");
    measurements.emplace_back("s3");
    for (int64_t time = 0; time < 10; time++) {
        vector<string> values;
        values.emplace_back("1");
        values.emplace_back("2");
        values.emplace_back("3");
        session->insertRecord(deviceId, time, measurements, values);
    }
}

void insertTablet() {
    pair<string, TSDataType::TSDataType> pairA("s1", TSDataType::BOOLEAN);
    pair<string, TSDataType::TSDataType> pairB("s2", TSDataType::INT32);
    pair<string, TSDataType::TSDataType> pairC("s3", TSDataType::FLOAT);
    vector<pair<string, TSDataType::TSDataType>> schemas;
    schemas.push_back(pairA);
    schemas.push_back(pairB);
    schemas.push_back(pairC);

    Tablet tablet("root.sg1.d1", schemas, 100);

    for (int64_t time = 0; time < 30; time++) {
        size_t row = tablet.rowSize++;
        tablet.timestamps[row] = time;

        bool randVal1 = rand() % 2;
        tablet.addValue(0, row, &randVal1);

        int randVal2 = rand();
        tablet.addValue(1, row, &randVal2);

        float randVal3 = (float)(rand() / 99.9);
        tablet.addValue(2, row, &randVal3);

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

void insertRecords() {
    string deviceId = "root.sg2.d1";
    vector<string> measurements;
    measurements.emplace_back("s1");
    measurements.emplace_back("s2");
    measurements.emplace_back("s3");

    vector<string> deviceIds;
    vector<vector<string>> measurementsList;
    vector<vector<string>> valuesList;
    vector<int64_t> timestamps;

    for (int64_t time = 10; time < 30; time++) {
        vector<string> values;
        values.emplace_back("1");
        values.emplace_back("2");
        values.emplace_back("3");

        deviceIds.push_back(deviceId);
        measurementsList.push_back(measurements);
        valuesList.push_back(values);
        timestamps.push_back(time);
        if (time != 20 && time % 10 == 0) {
            session->insertRecords(deviceIds, timestamps, measurementsList, valuesList);
            deviceIds.clear();
            measurementsList.clear();
            valuesList.clear();
            timestamps.clear();
        }
    }

    session->insertRecords(deviceIds, timestamps, measurementsList, valuesList);
}

void insertTablets() {
    pair<string, TSDataType::TSDataType> pairA("s1", TSDataType::INT64);
    pair<string, TSDataType::TSDataType> pairB("s2", TSDataType::DOUBLE);
    pair<string, TSDataType::TSDataType> pairC("s3", TSDataType::TEXT);
    vector<pair<string, TSDataType::TSDataType>> schemas;
    schemas.push_back(pairA);
    schemas.push_back(pairB);
    schemas.push_back(pairC);

    Tablet tablet1("root.sg1.d2", schemas, 100);
    Tablet tablet2("root.sg1.d3", schemas, 100);

    unordered_map<string, Tablet *> tabletMap;
    tabletMap["root.sg1.d2"] = &tablet1;
    tabletMap["root.sg1.d3"] = &tablet2;

    for (int64_t time = 0; time < 30; time++) {
        size_t row1 = tablet1.rowSize++;
        size_t row2 = tablet2.rowSize++;
        tablet1.timestamps[row1] = time;
        tablet2.timestamps[row2] = time;

        int64_t randVal11 = rand();
        tablet1.addValue(0, row1, &randVal11);

        double randVal12 = rand() / 99.9;
        tablet1.addValue(1, row1, &randVal12);

        string randVal13 = "string" + to_string(rand());
        tablet1.addValue(2, row1, &randVal13);

        int64_t randVal21 = rand();
        tablet2.addValue(0, row2, &randVal21);

        double randVal22 = rand() / 99.9;
        tablet2.addValue(1, row2, &randVal22);

        string randVal23 = "string" + to_string(rand());
        tablet2.addValue(2, row2, &randVal23);

        if (tablet1.rowSize == tablet1.maxRowNumber) {
            session->insertTablets(tabletMap, true);

            tablet1.reset();
            tablet2.reset();
        }
    }

    if (tablet1.rowSize != 0) {
        session->insertTablets(tabletMap, true);
        tablet1.reset();
        tablet2.reset();
    }
}

void insertTabletWithNullValues() {
    /*
     * A Tablet example:
     *      device1
     * time s1,   s2,   s3
     * 1,   null, 1,    1
     * 2,   2,    null, 2
     * 3,   3,    3,    null
     */
    pair<string, TSDataType::TSDataType> pairA("s1", TSDataType::INT64);
    pair<string, TSDataType::TSDataType> pairB("s2", TSDataType::INT64);
    pair<string, TSDataType::TSDataType> pairC("s3", TSDataType::INT64);
    vector<pair<string, TSDataType::TSDataType>> schemas;
    schemas.push_back(pairA);
    schemas.push_back(pairB);
    schemas.push_back(pairC);

    Tablet tablet("root.sg1.d4", schemas, 30);

    for (int64_t time = 0; time < 30; time++) {
        size_t row = tablet.rowSize++;
        tablet.timestamps[row] = time;
        for (int i = 0; i < 3; i++) {
            int64_t randVal = rand();
            tablet.addValue(i, row, &randVal);
            // mark null value
            if (row % 3 == (unsigned int) i) {
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

void nonQuery() {
    session->executeNonQueryStatement("insert into root.sg1.d1(timestamp,s1) values(100, 1);");
}

void query() {
    unique_ptr<SessionDataSet> dataSet = session->executeQueryStatement("select s1, s2, s3 from root.**");
    cout << "timestamp" << "  ";
    for (const string &name: dataSet->getColumnNames()) {
        cout << name << "  ";
    }
    cout << endl;

    dataSet->setBatchSize(1024);
    while (dataSet->hasNext()) {
        cout << dataSet->next()->toString();
    }
    cout << endl;

    dataSet->closeOperationHandle();
}

void deleteData() {
    string path = "root.sg1.d1.s1";
    int64_t deleteTime = 99;
    session->deleteData(path, deleteTime);
}

void deleteTimeseries() {
    vector<string> paths;
    vector<string> timeseriesGrp = { "root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s3", 
                                    "root.sg1.d2.s1", "root.sg1.d2.s2", "root.sg1.d2.s3", 
                                    "root.sg1.d3.s1", "root.sg1.d3.s2", "root.sg1.d3.s3",
                                    "root.sg1.d4.s1", "root.sg1.d4.s2", "root.sg1.d4.s3",
                                    "root.sg2.d1.s1", "root.sg2.d1.s2", "root.sg2.d1.s3" };
    for (const string& timeseries : timeseriesGrp) {
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

void queryLast() {
    unique_ptr<SessionDataSet> dataSet = session->executeQueryStatement("select last s1,s2,s3 from root.**");
    for (const string &name: dataSet->getColumnNames()) {
        cout << name << "  ";
    }
    cout << endl;

    while (dataSet->hasNext()) {
        cout << dataSet->next()->toString();
    }
    cout << endl;

    dataSet->closeOperationHandle();
}

int main() {
    LOG_LEVEL = LEVEL_DEBUG;

    session = new Session("127.0.0.1", 6667, "root", "root");
    session->open(false);

    cout << "setStorageGroup: root.sg1\n" << endl;
    try {
        session->setStorageGroup("root.sg1");
    }
    catch (IoTDBException &e) {
        string errorMessage(e.what());
        if (errorMessage.find("StorageGroupAlreadySetException") == string::npos) {
            cout << errorMessage << endl;
        }
        throw e;
    }

    cout << "setStorageGroup: root.sg2\n" << endl;
    try {
        session->setStorageGroup("root.sg2");
    }
    catch (IoTDBException &e) {
        string errorMessage(e.what());
        if (errorMessage.find("StorageGroupAlreadySetException") == string::npos) {
            cout << errorMessage << endl;
        }
        throw e;
    }

    cout << "createTimeseries\n" << endl;
    createTimeseries();

    cout << "createMultiTimeseries\n" << endl;
    createMultiTimeseries();

    cout << "createSchemaTemplate\n" << endl;
    createSchemaTemplate();

    cout << "ActivateTemplate\n" << endl;
    ActivateTemplate();

    cout << "showTimeseries\n" << endl;
    showTimeseries();

    cout << "insertRecord\n" << endl;
    insertRecord();

    cout << "insertTablet\n" << endl;
    insertTablet();

    cout << "insertRecords\n" << endl;
    insertRecords();

    cout << "insertTablets\n" << endl;
    insertTablets();

    cout << "insertTabletWithNullValues\n" << endl;
    insertTabletWithNullValues();

    cout << "nonQuery\n" << endl;
    nonQuery();

    cout << "queryLast\n" << endl;
    queryLast();

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

    cout << "finished!\n" << endl;
    return 0;
}
