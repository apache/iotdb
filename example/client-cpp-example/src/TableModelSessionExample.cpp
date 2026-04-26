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

#include "PreparedParameterBinary.h"
#include "TableSession.h"
#include "TableSessionBuilder.h"

using namespace std;

shared_ptr<TableSession> session;

void insertRelationalTablet() {

  vector<pair<string, TSDataType::TSDataType>> schemaList{
      make_pair("region_id", TSDataType::TEXT),    make_pair("plant_id", TSDataType::TEXT),
      make_pair("device_id", TSDataType::TEXT),    make_pair("model", TSDataType::TEXT),
      make_pair("temperature", TSDataType::FLOAT), make_pair("humidity", TSDataType::DOUBLE)};

  vector<ColumnCategory> columnTypes = {ColumnCategory::TAG,   ColumnCategory::TAG,
                                        ColumnCategory::TAG,   ColumnCategory::ATTRIBUTE,
                                        ColumnCategory::FIELD, ColumnCategory::FIELD};

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

void Output(unique_ptr<SessionDataSet>& dataSet) {
  for (const string& name : dataSet->getColumnNames()) {
    cout << name << "  ";
  }
  cout << endl;
  while (dataSet->hasNext()) {
    cout << dataSet->next()->toString();
  }
  cout << endl;
}

static bool preparedStatementUnsupported(const string& msg) {
  return msg.find("prepareStatement") != string::npos ||
         msg.find("Invalid method name") != string::npos;
}

/** Table-model prepared statement: prepare → ParamSlot bind → execute → deallocate. */
void tableModelPreparedStatementExample() {
  cout << "[Table prepared statement]\n" << endl;
  try {
    session->executeNonQueryStatement("DROP DATABASE IF EXISTS db_cpp_prep_ex");
    session->executeNonQueryStatement("CREATE DATABASE db_cpp_prep_ex");
    session->executeNonQueryStatement("USE \"db_cpp_prep_ex\"");
    session->executeNonQueryStatement("CREATE TABLE t_ps_ex (tag1 string tag, m1 double field)");

    vector<pair<string, TSDataType::TSDataType>> schemaList;
    schemaList.push_back(make_pair("tag1", TSDataType::STRING));
    schemaList.push_back(make_pair("m1", TSDataType::DOUBLE));
    vector<ColumnCategory> columnTypes = {ColumnCategory::TAG, ColumnCategory::FIELD};
    Tablet tablet("t_ps_ex", schemaList, columnTypes, 10);
    for (int row = 0; row < 3; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.timestamps[rowIndex] = row;
      tablet.addValue(0, rowIndex, string("A"));
      tablet.addValue(1, rowIndex, static_cast<double>(row));
    }
    session->insert(tablet, true);

    const string sql = "SELECT m1 FROM db_cpp_prep_ex.t_ps_ex WHERE tag1 = ?";
    const string stmtName = "cpp_ex_ps_1";
    const int32_t pc = session->prepareStatement(sql, stmtName);
    vector<iotdb::prepared::ParamSlot> params(static_cast<size_t>(pc));
    params[0].kind = iotdb::prepared::ParamKind::kString;
    params[0].stringOrBlob = "A";
    unique_ptr<SessionDataSet> ds = session->executePreparedStatement(sql, stmtName, params, -1);
    int n = 0;
    while (ds->hasNext()) {
      (void)ds->next();
      n++;
    }
    cout << "prepared statement result rows: " << n << endl;
    session->deallocatePreparedStatement(stmtName);
    session->executeNonQueryStatement("DROP DATABASE IF EXISTS db_cpp_prep_ex");
  } catch (IoTDBException& e) {
    if (preparedStatementUnsupported(string(e.what()))) {
      cout << "Server has no table-model prepared statement RPC; skip example.\n" << endl;
    } else {
      throw;
    }
  }
}

void OutputWithType(unique_ptr<SessionDataSet>& dataSet) {
  for (const string& name : dataSet->getColumnNames()) {
    cout << name << "  ";
  }
  cout << endl;
  for (const string& type : dataSet->getColumnTypeList()) {
    cout << type << "  ";
  }
  cout << endl;
  while (dataSet->hasNext()) {
    cout << dataSet->next()->toString();
  }
  cout << endl;
}

int main() {
  try {
    session = (new TableSessionBuilder())
                  ->host("127.0.0.1")
                  ->rpcPort(6667)
                  ->username("root")
                  ->password("TimechoDB@2021")
                  ->build();

    cout << "[Create Database db1,db2]\n" << endl;
    try {
      session->executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS db1");
      session->executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS db2");
    } catch (IoTDBException& e) {
      cout << e.what() << endl;
    }

    cout << "[Use db1 as database]\n" << endl;
    try {
      session->executeNonQueryStatement("USE db1");
    } catch (IoTDBException& e) {
      cout << e.what() << endl;
    }

    cout << "[Create Table table1,table2]\n" << endl;
    try {
      session->executeNonQueryStatement(
          "create table db1.table1(region_id STRING TAG, plant_id STRING TAG, device_id STRING "
          "TAG, model STRING ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE FIELD) with "
          "(TTL=3600000)");
      session->executeNonQueryStatement(
          "create table db2.table2(region_id STRING TAG, plant_id STRING TAG, color STRING "
          "ATTRIBUTE, temperature FLOAT FIELD, speed DOUBLE FIELD) with (TTL=6600000)");
    } catch (IoTDBException& e) {
      cout << e.what() << endl;
    }

    cout << "[Show Tables]\n" << endl;
    try {
      unique_ptr<SessionDataSet> dataSet = session->executeQueryStatement("SHOW TABLES");
      Output(dataSet);
    } catch (IoTDBException& e) {
      cout << e.what() << endl;
    }

    cout << "[Show tables from specific database]\n" << endl;
    try {
      unique_ptr<SessionDataSet> dataSet = session->executeQueryStatement("SHOW TABLES FROM db1");
      Output(dataSet);
    } catch (IoTDBException& e) {
      cout << e.what() << endl;
    }

    cout << "[InsertTablet]\n" << endl;
    try {
      insertRelationalTablet();
    } catch (IoTDBException& e) {
      cout << e.what() << endl;
    }

    cout << "[Query Table Data]\n" << endl;
    try {
      unique_ptr<SessionDataSet> dataSet = session->executeQueryStatement(
          "SELECT * FROM table1"
          " where region_id = '1' and plant_id in ('3', '5') and device_id = '3'");
      OutputWithType(dataSet);
    } catch (IoTDBException& e) {
      cout << e.what() << endl;
    }

    try {
      tableModelPreparedStatementExample();
    } catch (IoTDBException& e) {
      cout << e.what() << endl;
    }

    session->close();

    // specify database in constructor
    session = (new TableSessionBuilder())
                  ->host("127.0.0.1")
                  ->rpcPort(6667)
                  ->username("root")
                  ->password("TimechoDB@2021")
                  ->database("db1")
                  ->build();

    cout << "[Show tables from current database(db1)]\n" << endl;
    try {
      unique_ptr<SessionDataSet> dataSet = session->executeQueryStatement("SHOW TABLES");
      Output(dataSet);
    } catch (IoTDBException& e) {
      cout << e.what() << endl;
    }

    cout << "[Change database to db2]\n" << endl;
    try {
      session->executeNonQueryStatement("USE db2");
    } catch (IoTDBException& e) {
      cout << e.what() << endl;
    }

    cout << "[Show tables from current database(db2)]\n" << endl;
    try {
      unique_ptr<SessionDataSet> dataSet = session->executeQueryStatement("SHOW TABLES");
      Output(dataSet);
    } catch (IoTDBException& e) {
      cout << e.what() << endl;
    }

    cout << "[Drop Database db1,db2]\n" << endl;
    try {
      session->executeNonQueryStatement("DROP DATABASE db1");
      session->executeNonQueryStatement("DROP DATABASE db2");
    } catch (IoTDBException& e) {
      cout << e.what() << endl;
    }

    cout << "session close\n" << endl;
    session->close();

    cout << "finished!\n" << endl;
  } catch (IoTDBConnectionException& e) {
    cout << e.what() << endl;
  } catch (IoTDBException& e) {
    cout << e.what() << endl;
  }
  return 0;
}