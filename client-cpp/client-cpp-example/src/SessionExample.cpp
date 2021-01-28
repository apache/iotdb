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
        session->createTimeseries("root.sg1.d1.s1", TSDataType::INT64, TSEncoding::RLE,
        CompressionType::SNAPPY);
    }
    if (!session->checkTimeseriesExists("root.sg1.d1.s2")) {
        session->createTimeseries("root.sg1.d1.s2", TSDataType::INT64, TSEncoding::RLE,
        CompressionType::SNAPPY);
    }
    if (!session->checkTimeseriesExists("root.sg1.d1.s3")) {
        session->createTimeseries("root.sg1.d1.s3", TSDataType::INT64, TSEncoding::RLE,
        CompressionType::SNAPPY);
    }

    // create timeseries with tags and attributes
    if (!session->checkTimeseriesExists("root.sg1.d1.s4")) {
      map<string, string> tags;
      tags["tag1"] = "v1";
      map<string, string> attributes;
      attributes["description"] = "v1";
      session->createTimeseries("root.sg1.d1.s4", TSDataType::INT64, TSEncoding::RLE,
          CompressionType::SNAPPY, NULL, &tags, &attributes, "temperature");
    }
}

void createMultiTimeseries() {
  if (!session->checkTimeseriesExists("root.sg1.d2.s1") && !session->checkTimeseriesExists("root.sg1.d2.s1")) {
    vector<string> paths;
    paths.push_back("root.sg1.d2.s1");
    paths.push_back("root.sg1.d2.s2");
    vector<TSDataType::TSDataType> tsDataTypes;
    tsDataTypes.push_back(TSDataType::INT64);
    tsDataTypes.push_back(TSDataType::INT64);
    vector<TSEncoding::TSEncoding> tsEncodings;
    tsEncodings.push_back(TSEncoding::RLE);
    tsEncodings.push_back(TSEncoding::RLE);
    vector<CompressionType::CompressionType> compressionTypes;
    compressionTypes.push_back(CompressionType::SNAPPY);
    compressionTypes.push_back(CompressionType::SNAPPY);

    vector<map<string,string>> tagsList;
    map<string,string> tags;
    tags["unit"] = "kg";
    tagsList.push_back(tags);
    tagsList.push_back(tags);

    vector<map<string,string>> attributesList;
    map<string,string> attributes;
    attributes["minValue"] = "1";
    attributes["maxValue"] = "100";
    attributesList.push_back(attributes);
    attributesList.push_back(attributes);

    vector<string> alias;
    alias.push_back("weight1");
    alias.push_back("weight2");

    session->createMultiTimeseries(paths, tsDataTypes, tsEncodings, compressionTypes, NULL, &tagsList, &attributesList, &alias);
  }
}

void insertRecord() {
  string deviceId = "root.sg1.d1";
  vector<string> measurements;
  measurements.push_back("s1");
  measurements.push_back("s2");
  measurements.push_back("s3");
  for (int64_t time = 0; time < 100; time++) {
    vector<string> values;
    values.push_back("11");
    values.push_back("22");
    values.push_back("33");
    session->insertRecord(deviceId, time, measurements, values);
  }
}

void insertTablet() {
  pair<string, TSDataType::TSDataType> pairA("s1", TSDataType::INT64);
  pair<string, TSDataType::TSDataType> pairB("s2", TSDataType::INT64);
  pair<string, TSDataType::TSDataType> pairC("s3", TSDataType::INT64);
  vector<pair<string, TSDataType::TSDataType>> schemas;
  schemas.push_back(pairA);
  schemas.push_back(pairB);
  schemas.push_back(pairC);

  Tablet tablet("root.sg1.d1", schemas, 100);

  for (int64_t time = 0; time < 100; time++) {
    int row = tablet.rowSize++;
    tablet.timestamps[row] = time;
    for (int i = 0; i < 3; i++) {
      tablet.values[i][row] = to_string(i);
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

void insertTablets() {
  pair<string, TSDataType::TSDataType> pairA("s1", TSDataType::INT64);
  pair<string, TSDataType::TSDataType> pairB("s2", TSDataType::INT64);
  pair<string, TSDataType::TSDataType> pairC("s3", TSDataType::INT64);
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

  for (int64_t time = 0; time < 100; time++) {
    int row1 = tablet1.rowSize++;
    int row2 = tablet2.rowSize++;
    int row3 = tablet3.rowSize++;
    tablet1.timestamps[row1] = time;
    tablet2.timestamps[row2] = time;
    tablet3.timestamps[row3] = time;

    for (int i = 0; i < 3; i++) {
      tablet1.values[i][row1] = to_string(i);
      tablet2.values[i][row1] = to_string(i);
      tablet3.values[i][row1] = to_string(i);
    }
    if (tablet1.rowSize == tablet1.maxRowNumber) {
      session->insertTablets(tabletMap, true);

      tablet1.reset();
      tablet2.reset();
      tablet3.reset();
    }
  }

  if (tablet1.rowSize != 0) {
    session->insertTablets(tabletMap, true);
    tablet1.reset();
    tablet2.reset();
    tablet3.reset();
  }
}

void insertRecords() {
  string deviceId = "root.sg1.d1";
  vector<string> measurements;
  measurements.push_back("s1");
  measurements.push_back("s2");
  measurements.push_back("s3");

  vector<string> deviceIds;
  vector<vector<string>> measurementsList;
  vector<vector<string>> valuesList;
  vector<int64_t> timestamps;
  vector<string>* values;

  for (int64_t time = 0; time < 500; time++) {
      values = new vector<string>();
      values->push_back("1");
      values->push_back("2");
      values->push_back("3");

      deviceIds.push_back(deviceId);
      measurementsList.push_back(measurements);
      valuesList.push_back(*values);
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
}

void nonQuery() {
  session->executeNonQueryStatement("insert into root.sg1.d1(timestamp,s1) values(200, 1);");
}

void query() {
  SessionDataSet* dataSet;
  dataSet = session->executeQueryStatement("select * from root.sg1.d1");
  cout << "timestamp" << "  ";
  for (string name : dataSet->getColumnNames()) {
    cout << name << "  ";
  }
  cout << endl;
  dataSet->setBatchSize(1024);
  while (dataSet->hasNext())
  {
    cout << dataSet->next()->toString();
  }
  
  dataSet->closeOperationHandle();
}

void deleteData() {
  string path = "root.sg1.d1.s1";
  int64_t deleteTime = 99;
  session->deleteData(path, deleteTime);
}

void deleteTimeseries() {
    vector<string> paths;
    if (session->checkTimeseriesExists("root.sg1.d1.s1")) {
      paths.push_back("root.sg1.d1.s1");
    }
    if (session->checkTimeseriesExists("root.sg1.d1.s2")) {
      paths.push_back("root.sg1.d1.s2");
    }
    if (session->checkTimeseriesExists("root.sg1.d1.s3")) {
      paths.push_back("root.sg1.d1.s3");
    }
    if (session->checkTimeseriesExists("root.sg1.d1.s4")) {
      paths.push_back("root.sg1.d1.s4");
    }
    session->deleteTimeseries(paths);
}

void queryLast() {
    SessionDataSet *dataSet = session->executeQueryStatement("select last s1,s2,s3 from root.sg1.d1");
    for (string name: dataSet->getColumnNames()) {
        cout << name << "  ";
    }
    cout << endl;
    while (dataSet->hasNext()) {
        cout << dataSet->next()->toString();
    }
}

int main() {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session->open(false);

    cout << "setStorageGroup\n";
    try {
      session->setStorageGroup("root.sg1");
    }
    catch (IoTDBConnectionException e){
      string errorMessage(e.what());
      if (errorMessage.find("StorageGroupAlreadySetException") == string::npos) {
        throw e;
      }
    }

    createTimeseries();

    createMultiTimeseries();

    insertRecord();

    queryLast();

    insertTablet();

    insertRecords();

    insertTablets();

    nonQuery();

    query();

    deleteData();

    deleteTimeseries();

    session->close();
    
    return 0;
}
