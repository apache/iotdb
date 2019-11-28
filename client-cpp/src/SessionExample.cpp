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
 
#include "IOTDBSession.h"

Session * session;

void insert()
{
    session->createTimeseries("root.sg1.d1.s1", TSDataType::INT64, TSEncoding::RLE, CompressionType::SNAPPY);
    session->createTimeseries("root.sg1.d1.s2", TSDataType::DOUBLE, TSEncoding::RLE, CompressionType::SNAPPY);
    session->createTimeseries("root.sg1.d1.s3", TSDataType::TEXT, TSEncoding::PLAIN, CompressionType::SNAPPY);
    string deviceId = "root.sg1.d1";
    vector<string> measurements;
    measurements.push_back("s1");
    measurements.push_back("s2");
    measurements.push_back("s3");
    for (int time = 0; time < 100; time++)
    {
        vector<string> values;
        values.push_back("1");
        values.push_back("2.0");
        values.push_back("\"three\"");
        session->insert(deviceId, time, measurements, values);
    }
}

void insertRowBatch()
{
    session->createTimeseries("root.sg1.d2.s1", TSDataType::INT64, TSEncoding::RLE, CompressionType::SNAPPY);
    session->createTimeseries("root.sg1.d2.s2", TSDataType::DOUBLE, TSEncoding::RLE, CompressionType::SNAPPY);
    session->createTimeseries("root.sg1.d2.s3", TSDataType::TEXT, TSEncoding::PLAIN, CompressionType::SNAPPY);
    string deviceId = "root.sg1.d2";

    int rowCount = 3;

    vector<string> measurements;
    measurements.push_back("s1");
    measurements.push_back("s2");
    measurements.push_back("s3");

    vector<TSDataType::TSDataType> types;
    types.push_back(TSDataType::INT64);
    types.push_back(TSDataType::DOUBLE);
    types.push_back(TSDataType::TEXT);

    vector<long long> timestamps;
    timestamps.push_back(101);
    timestamps.push_back(102);
    timestamps.push_back(103);

    vector<vector<string> > values;
    vector<string> tmp1;
    tmp1.push_back("1");
    tmp1.push_back("2.0");
    tmp1.push_back("three");
    vector<string> tmp2;
    tmp2.push_back("4");
    tmp2.push_back("5.0");
    tmp2.push_back("six");
    vector<string> tmp3;
    tmp3.push_back("7");
    tmp3.push_back("8.0");
    tmp3.push_back("nine");
    values.push_back(tmp1);
    values.push_back(tmp2);
    values.push_back(tmp3);

    session->insertBatch(deviceId, rowCount, measurements, types, timestamps, values);
}

void nonQuery()
{
    session->executeNonQueryStatement("insert into root.sg1.d1(timestamp,s1) values(200, 1)");
}

void query()
{
    SessionDataSet* dataSet = session->executeQueryStatement("select * from root.sg1.*");
    dataSet->setBatchSize(1024); // default is 512
    printf("%s",dataSet->columntoString().c_str());
    while (dataSet->hasNext())
    {
        printf("%s",dataSet->next().toString().c_str());
    }
    dataSet->closeOperationHandle();
    delete dataSet;
}

void deleteData()
{
    vector<string> del;
    del.push_back("root.sg1.d1.s1");
    session->deleteData(del, 99);
}

void deleteTimeseries()
{
    vector<string> paths;
    paths.push_back("root.sg1.d1.s1");
    paths.push_back("root.sg1.d1.s2");
    paths.push_back("root.sg1.d1.s3");
    session->deleteTimeseries(paths);
}

int main()
{
    session = new Session("127.0.0.1", 6667, "root", "root");
    session->open();
    session->setStorageGroup("root.sg1");
    insert();
    insertRowBatch();
    nonQuery();
    query();
    deleteData();
    deleteTimeseries();
    session->deleteStorageGroup("root.sg1");
    session->close();
 //   getchar();
}



