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

extern std::shared_ptr<TableSession> session;

static int global_test_tag = 0;

class CaseReporter {
public:
    CaseReporter(const char* caseNameArg) : caseName(caseNameArg) {
        test_tag = global_test_tag++;
        std::cout << "Test " << test_tag << ": " << caseName << std::endl;
    }

    ~CaseReporter() {
        std::cout << "Test " << test_tag << ": " << caseName << " Done" << std::endl << std::endl;
    }

private:
    const char* caseName;
    int test_tag;
};

TEST_CASE("Create table success", "[createTable]") {
    // REQUIRE(true);
    CaseReporter cr("createTable");
    session->executeNonQueryStatement("DROP DATABASE IF EXISTS db1");
    session->executeNonQueryStatement("CREATE DATABASE db1");
    session->executeNonQueryStatement("DROP DATABASE IF EXISTS db2");
    session->executeNonQueryStatement("CREATE DATABASE db2");
    session->executeNonQueryStatement("USE \"db1\"");
    session->executeNonQueryStatement("CREATE TABLE table0 ("
        "tag1 string tag,"
        "attr1 string attribute,"
        "m1 double field)");
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
    session->executeNonQueryStatement("DROP TABLE IF EXISTS table1");
    session->executeNonQueryStatement("CREATE TABLE table1 ("
        "tag1 string tag,"
        "attr1 string attribute,"
        "m1 double field)");
    vector<pair<string, TSDataType::TSDataType>> schemaList;
    schemaList.push_back(make_pair("tag1", TSDataType::TEXT));
    schemaList.push_back(make_pair("attr1", TSDataType::TEXT));
    schemaList.push_back(make_pair("m1", TSDataType::DOUBLE));
    vector<ColumnCategory> columnTypes = {ColumnCategory::TAG, ColumnCategory::ATTRIBUTE, ColumnCategory::FIELD};

    int64_t timestamp = 0;
    Tablet tablet("table1", schemaList, columnTypes, 15);

    for (int row = 0; row < 15; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.timestamps[rowIndex] = timestamp + row;
        string data = "tag:";
        data += to_string(row);
        tablet.addValue(0, rowIndex, data);
        data = "attr:";
        data += to_string(row);
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
        auto rowRecord = sessionDataSet->next();
        REQUIRE(rowRecord->fields[1].stringV == string("tag:") + to_string(cnt));
        REQUIRE(rowRecord->fields[2].stringV == string("attr:") + to_string(cnt));
        REQUIRE(fabs(rowRecord->fields[3].doubleV - cnt * 1.1) < 0.0001);
        cnt++;
    }
    REQUIRE(cnt == 15);
}

TEST_CASE("Test RelationalTabletTsblockRead", "[testRelationalTabletTsblockRead]") {
    CaseReporter cr("testRelationalTabletTsblockRead");
    session->executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS db1");
    session->executeNonQueryStatement("USE db1");
    session->executeNonQueryStatement("DROP TABLE IF EXISTS table1");
    session->executeNonQueryStatement("CREATE TABLE table1 ("
        "field1 BOOLEAN field,"
        "field2 INT32 field,"
        "field3 INT64 field,"
        "field4 FLOAT field,"
        "field5 DOUBLE field,"
        "field6 TEXT field,"
        "field7 TIMESTAMP field,"
        "field8 DATE field,"
        "field9 BLOB field,"
        "field10 STRING field)");

    vector<pair<string, TSDataType::TSDataType>> schemaList;
    schemaList.push_back(make_pair("field1", TSDataType::BOOLEAN));
    schemaList.push_back(make_pair("field2", TSDataType::INT32));
    schemaList.push_back(make_pair("field3", TSDataType::INT64));
    schemaList.push_back(make_pair("field4", TSDataType::FLOAT));
    schemaList.push_back(make_pair("field5", TSDataType::DOUBLE));
    schemaList.push_back(make_pair("field6", TSDataType::TEXT));
    schemaList.push_back(make_pair("field7", TSDataType::TIMESTAMP));
    schemaList.push_back(make_pair("field8", TSDataType::DATE));
    schemaList.push_back(make_pair("field9", TSDataType::BLOB));
    schemaList.push_back(make_pair("field10", TSDataType::STRING));

    vector<ColumnCategory> columnTypes(10, ColumnCategory::FIELD);

    int64_t timestamp = 0;
    int maxRowNumber = 50000;
    Tablet tablet("table1", schemaList, columnTypes, maxRowNumber);

    for (int row = 0; row < maxRowNumber; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.timestamps[rowIndex] = timestamp + row;
        tablet.addValue(0, rowIndex, row % 2 == 0);
        tablet.addValue(1, rowIndex, static_cast<int32_t>(row));
        tablet.addValue(2, rowIndex, static_cast<int64_t>(timestamp));
        tablet.addValue(3, rowIndex, static_cast<float>(row * 1.1f));
        tablet.addValue(4, rowIndex, static_cast<double>(row * 1.1f));
        tablet.addValue(5, rowIndex, "text_" + to_string(row));
        tablet.addValue(6, rowIndex, static_cast<int64_t>(timestamp));
        tablet.addValue(7, rowIndex, boost::gregorian::date(2025, 5, 15));
        tablet.addValue(8, rowIndex, "blob_" + to_string(row));
        tablet.addValue(9, rowIndex, "string_" + to_string(row));

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

    unique_ptr<SessionDataSet> sessionDataSet = session->executeQueryStatement("SELECT * FROM table1 order by time");
    auto dataIter = sessionDataSet->getIterator();
    int rowNum = 0;
    timestamp = 0;
    while (dataIter.next()) {
        REQUIRE(dataIter.getLongByIndex(1) == timestamp + rowNum);
        REQUIRE(dataIter.getBooleanByIndex(2) == (rowNum % 2 == 0));
        REQUIRE(dataIter.getIntByIndex(3) == static_cast<int32_t>(rowNum));
        REQUIRE(dataIter.getLongByIndex(4) == static_cast<int64_t>(timestamp));
        REQUIRE(fabs(dataIter.getFloatByIndex(5) - rowNum * 1.1f) < 0.0001f);
        REQUIRE(fabs(dataIter.getDoubleByIndex(6) - rowNum * 1.1f) < 0.0001);
        REQUIRE(dataIter.getStringByIndex(7) == "text_" + to_string(rowNum));
        REQUIRE(dataIter.getLongByIndex(8) == static_cast<int64_t>(timestamp));
        REQUIRE(dataIter.getDateByIndex(9) == boost::gregorian::date(2025, 5, 15));
        REQUIRE(dataIter.getStringByIndex(10) == "blob_" + to_string(rowNum));
        REQUIRE(dataIter.getStringByIndex(11) == "string_" + to_string(rowNum));
        rowNum++;
    }
    REQUIRE(rowNum == maxRowNumber);
}
