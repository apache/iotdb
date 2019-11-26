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

using namespace std;
int main()
{
    Session *session = new Session("127.0.0.1", 6667, "root", "root");
    
    session->open();
    
    session->setStorageGroup("root.sg1");
    
    session->createTimeseries("root.sg1.d1.s1", INT64, RLE, SNAPPY);
    session->createTimeseries("root.sg1.d1.s2", INT64, RLE, SNAPPY);
    session->createTimeseries("root.sg1.d1.s3", INT64, RLE, SNAPPY);
    
    string deviceId = "root.sg1.d1";
    vector<string> measurements;
    measurements.push_back("s1");
    measurements.push_back("s2");
    measurements.push_back("s3");
    for (int time = 0; time < 100; time++) 
    {

        vector<string> values;
        values.push_back("1");
        values.push_back("2");
        values.push_back("3");
        session->insert(deviceId, time, measurements, values);
    }

    SessionDataSet* dataSet = session->executeQueryStatement("select * from root.sg1.d1");
    dataSet->setBatchSize(1024); // default is 512
    while (dataSet->hasNext())
    {
        printf("%s",dataSet->next().toString().c_str());
    }

    dataSet->closeOperationHandle();

    vector<string> del;
    del.push_back("root.sg1.d1.s1");
    session->deleteData(del, 99);
    
    
    vector<string> paths;
    paths.push_back("root.sg1.d1.s1");
    paths.push_back("root.sg1.d1.s2");
    paths.push_back("root.sg1.d1.s3");
  //  session->deleteTimeseries(paths);
    
    session->deleteStorageGroup("root.sg1");
    
    session->close();
}



