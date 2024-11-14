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

void insertRelationalTablet() {
    vector<pair<string, TSDataType::TSDataType>> schemaList;
    schemaList.push_back(make_pair("id1", TSDataType::TEXT));
    schemaList.push_back(make_pair("attr1", TSDataType::TEXT));
    schemaList.push_back(make_pair("m1", TSDataType::DOUBLE));
    vector<ColumnCategory> columnTypes = {ColumnCategory::ID, ColumnCategory::ATTRIBUTE, ColumnCategory::MEASUREMENT};

    int64_t timestamp = 0;
    Tablet tablet("table1", schemaList, columnTypes, 15);

    for (int row = 0; row < 15; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.timestamps[rowIndex] = timestamp + row;
        string data = "id:"; data += to_string(row);
        tablet.addValue(0, rowIndex, &data);
        data = "attr:"; data += to_string(row);
        tablet.addValue(1, rowIndex, &data);
        double value = row * 1.1;
        tablet.addValue(2, rowIndex, &value);
        if (tablet.rowSize == tablet.maxRowNumber) {
            session->insertRelationalTablet(tablet, true);
            tablet.reset();
        }
    }

    if (tablet.rowSize != 0) {
        session->insertRelationalTablet(tablet, true);
        tablet.reset();
    }
}

int main() {
    
    session = new Session("127.0.0.1", 6667, "root", "root");
    session->setSqlDialect("table");
    session->open(false);

    cout << "Create Database db1" << endl;
    try {
        session->createDatabase("db1");
    } catch (IoTDBException &e) {
        cout << e.what() << endl;
    }

    cout << "Use db1 as database" << endl;
    session->executeNonQueryStatement("USE db1");

    cout << "Create Table table1" << endl;
    try {
        session->executeNonQueryStatement("CREATE TABLE table1 ("
            "id1 string id,"
            "attr1 string attribute,"
            "m1 double measurement)");
    } catch (IoTDBException &e) {
        cout << e.what() << endl;
    }

    cout << "InsertRelationalTablet" << endl;
    insertRelationalTablet();

    cout << "Drop Database db1" << endl;
    try {
        session->deleteDatabase("db1");
    } catch (IoTDBException &e) {
        cout << e.what() << endl;
    }

    cout << "session close\n" << endl;
    session->close();

    delete session;

    cout << "finished!\n" << endl;
    return 0;
}