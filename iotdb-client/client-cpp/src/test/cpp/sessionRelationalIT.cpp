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
#include "TableSession.h"
#include <math.h>

using namespace std;

extern TableSession *session;

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

TEST_CASE("Create table success", "[createTable]") {
    // REQUIRE(true);
    CaseReporter cr("createTable");
    session->executeNonQueryStatement("CREATE DATABASE db1");
    session->executeNonQueryStatement("CREATE DATABASE db2");
    session->executeNonQueryStatement("USE \"db1\"");
    REQUIRE(session->getDatabase() == "db1");
    session->executeNonQueryStatement("CREATE TABLE table0 ("
            "id1 string id,"
            "attr1 string attribute,"
            "m1 double measurement)");
    unique_ptr<SessionDataSet> sessionDataSet = session->executeQueryStatement("SHOW TABLES");
    sessionDataSet->setFetchSize(1024);
    bool tableExist = false;
    while (sessionDataSet->hasNext()) {
        if (sessionDataSet->next()->fields[0].stringV == "table0") {
            tableExist = true;
            break;
        }
    }
    REQUIRE(tableExist == true);
}

TEST_CASE("Test insertRelationalTablet", "[testInsertRelationalTablet]") {
    CaseReporter cr("testInsertRelationalTablet");
    session->executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS db1");
    session->executeNonQueryStatement("USE db1");
    session->executeNonQueryStatement("CREATE TABLE table1 ("
        "id1 string id,"
        "attr1 string attribute,"
        "m1 double measurement)");
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
        tablet.addValue(0, rowIndex, data);
        data = "attr:"; data += to_string(row);
        tablet.addValue(1, rowIndex, data);
        double value = row * 1.1;
        tablet.addValue(2, rowIndex, value);
        if (tablet.rowSize == tablet.maxRowNumber) {
            session->insert(tablet, true);
            tablet.reset();
        }
    }

    if (tablet.rowSize != 0) {
        session->insert(tablet, true);
        tablet.reset();
    }
    session->executeNonQueryStatement("FLUSH");

    int cnt = 0;
    unique_ptr<SessionDataSet> sessionDataSet = session->executeQueryStatement("SELECT * FROM table1 order by time");
    while (sessionDataSet->hasNext()) {
        RowRecord *rowRecord = sessionDataSet->next();
        REQUIRE(rowRecord->fields[1].stringV == string("id:") + to_string(cnt));
        REQUIRE(rowRecord->fields[2].stringV == string("attr:") + to_string(cnt));
        REQUIRE(fabs(rowRecord->fields[3].doubleV - cnt * 1.1) < 0.0001);
        cnt++;
    }
    REQUIRE(cnt == 15);
}
