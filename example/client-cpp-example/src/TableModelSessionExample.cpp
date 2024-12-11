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

#include "TableSession.h"
#include "TableSessionBuilder.h"

using namespace std;

TableSession *session;

void insertRelationalTablet() {

    vector<pair<string, TSDataType::TSDataType>> schemaList {
        make_pair("region_id", TSDataType::TEXT),
        make_pair("plant_id", TSDataType::TEXT),
        make_pair("device_id", TSDataType::TEXT),
        make_pair("model", TSDataType::TEXT),
        make_pair("temperature", TSDataType::FLOAT),
        make_pair("humidity", TSDataType::DOUBLE)
    };

    vector<ColumnCategory> columnTypes = {
        ColumnCategory::ID,
        ColumnCategory::ID,
        ColumnCategory::ID,
        ColumnCategory::ATTRIBUTE,
        ColumnCategory::MEASUREMENT,
        ColumnCategory::MEASUREMENT
    };

    Tablet tablet("table1", schemaList, columnTypes, 100);

    for (int row = 0; row < 100; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.timestamps[rowIndex] = row;
        tablet.addValue("region_id", rowIndex, "1");
        tablet.addValue("plant_id", rowIndex, "5");
        tablet.addValue("device_id", rowIndex, "3");
        tablet.addValue("model", rowIndex, "A");
        tablet.addValue("temperature", rowIndex, 37.6F);
        tablet.addValue("humidity", rowIndex, 111.1);
        if (tablet.rowSize == tablet.maxRowNumber) {
            session->insert(tablet);
            tablet.reset();
        }
    }

    if (tablet.rowSize != 0) {
        session->insert(tablet);
        tablet.reset();
    }
}

template<typename T>
inline void Output(vector<T> &columnNames) {
    for (auto &name: columnNames) {
        cout << name << "\t";
    }
    cout << endl;
}

int main() {
    try {
        session = new TableSessionBuilder()
            .host("127.0.0.1")
            .rpcPort(6667)
            .username("root")
            .password("root")
            .build();

        cout << "Create Database db1,db2" << endl;
        try {
            session->executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS db1");
            session->executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS db2");
        } catch (IoTDBException &e) {
            cout << e.what() << endl;
        }

        cout << "Use db1 as database" << endl;
        try {
            session->executeNonQueryStatement("USE db1");
        } catch (IoTDBException &e) {
            cout << e.what() << endl;
        }

        cout << "Create Table table1,table2" << endl;
        try {
            session->executeNonQueryStatement("create table db1.table1(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT) with (TTL=3600000)");
            session->executeNonQueryStatement("create table db2.table2(region_id STRING ID, plant_id STRING ID, color STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, speed DOUBLE MEASUREMENT) with (TTL=6600000)");
        } catch (IoTDBException &e) {
            cout << e.what() << endl;
        }

        cout << "Show Tables" << endl;
        try {
            SessionDataSet dataSet = session->executeQueryStatement("SHOW TABLES");
            Output(dataSet.getColumnNames());
            while(dataSet.hasNext()) {
                Output(dataSet.next());
            }
        } catch (IoTDBException &e) {
            cout << e.what() << endl;
        }

        cout << "Show tables from specific database" << endl;
        try {
            SessionDataSet dataSet = session->executeQueryStatement("SHOW TABLES FROM db1");
            Output(dataSet.getColumnNames());
            while(dataSet.hasNext()) {
                Output(dataSet.next());
            }
        } catch (IoTDBException &e) {
            cout << e.what() << endl;
        }

        cout << "InsertTablet" << endl;
        try {
            insertRelationalTablet();
        } catch (IoTDBException &e) {
            cout << e.what() << endl;
        }

        cout << "Query Table Data" << endl;
        try {
            SessionDataSet dataSet = session->executeQueryStatement("SELECT * FROM table1"
                " where region_id = '1' and plant_id in ('3', '5') and device_id = '3'");
            Output(dataSet.getColumnNames());
            Output(dataSet.getColumnTypeList());
            while(dataSet.hasNext()) {
                Output(dataSet.next());
            }
        } catch (IoTDBException &e) {
            cout << e.what() << endl;
        }

        session->close();

        // specify database in constructor
        session = new TableSessionBuilder()
            .host("127.0.0.1")
            .rpcPort(6667)
            .username("root")
            .password("root")
            .database("db1")
            .build();

        cout << "Show tables from current database(db1)" << endl;
        try {
            SessionDataSet dataSet = session->executeQueryStatement("SHOW TABLES");
            Output(dataSet.getColumnNames());
            while(dataSet.hasNext()) {
                Output(dataSet.next());
            }
        } catch (IoTDBException &e) {
            cout << e.what() << endl;
        }

        cout << "Change database to db2" << endl;
        try {
            session->executeNonQueryStatement("USE db2");
        } catch (IoTDBException &e) {
            cout << e.what() << endl;
        }

        cout << "Show tables from current database(db2)" << endl;
        try {
            SessionDataSet dataSet = session->executeQueryStatement("SHOW TABLES");
            Output(dataSet.getColumnNames());
            while(dataSet.hasNext()) {
                Output(dataSet.next());
            }
        } catch (IoTDBException &e) {
            cout << e.what() << endl;
        }

        cout << "Drop Database db1,db2" << endl;
        try {
            session->executeNonQueryStatement("DROP DATABASE db1");
            session->executeNonQueryStatement("DROP DATABASE db2");
        } catch (IoTDBException &e) {
            cout << e.what() << endl;
        }

        cout << "session close\n" << endl;
        session->close();

        delete session;

        cout << "finished!\n" << endl;
    } catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
    } catch (IoTDBException &e) {
        cout << e.what() << endl;
    }
    return 0;
}