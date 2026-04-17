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
#include "TableSessionBuilder.h"
#include "TestCredentials.h"
#include <math.h>

using namespace std;

extern std::shared_ptr<TableSession> session;

/** IoTDB builds without the prepareStatement Thrift RPC return an "Invalid method name" error. */
static bool isPreparedStatementUnsupportedByServer(const std::exception& e) {
    std::string m(e.what());
    return m.find("prepareStatement") != std::string::npos || m.find("Invalid method name") != std::string::npos;
}

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
        if (sessionDataSet->next()->fields[0].stringV.value() == "table0") {
            tableExist = true;
            break;
        }
    }
    REQUIRE(tableExist == true);
}

TEST_CASE("Test TableSession builder with nodeUrls", "[SessionBuilderInit]") {
    CaseReporter cr("TableSessionInitWithNodeUrls");

    std::vector<std::string> nodeUrls = {"127.0.0.1:6667"};
    auto builder = std::unique_ptr<TableSessionBuilder>(new TableSessionBuilder());
    std::shared_ptr<TableSession> session =
        std::shared_ptr<TableSession>(
            builder
            ->username(iotdb::integration_test::kUsername)
            ->password(iotdb::integration_test::kPassword)
            ->nodeUrls(nodeUrls)
            ->build()
        );
    session->open();

    session->executeNonQueryStatement("DROP DATABASE IF EXISTS db1");
    session->executeNonQueryStatement("CREATE DATABASE db1");
    session->executeNonQueryStatement("DROP DATABASE IF EXISTS db2");
    session->executeNonQueryStatement("CREATE DATABASE db2");

    session->close();
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
        REQUIRE(rowRecord->fields[1].stringV.value() == string("tag:") + to_string(cnt));
        REQUIRE(rowRecord->fields[2].stringV.value() == string("attr:") + to_string(cnt));
        REQUIRE(fabs(rowRecord->fields[3].doubleV.value() - cnt * 1.1) < 0.0001);
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

        if (row % 2 == 0) {
            for (int col = 0; col <= 9; col++) {
                tablet.bitMaps[col].mark(row);
            }
        }

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
        if (rowNum % 2 == 0) {
            REQUIRE_FALSE(dataIter.getBooleanByIndex(2).is_initialized());
            REQUIRE_FALSE(dataIter.getIntByIndex(3).is_initialized());
            REQUIRE_FALSE(dataIter.getLongByIndex(4).is_initialized());
            REQUIRE_FALSE(dataIter.getFloatByIndex(5).is_initialized());
            REQUIRE_FALSE(dataIter.getDoubleByIndex(6).is_initialized());
            REQUIRE_FALSE(dataIter.getStringByIndex(7).is_initialized());
            REQUIRE_FALSE(dataIter.getTimestampByIndex(8).is_initialized());
            REQUIRE_FALSE(dataIter.getDateByIndex(9).is_initialized());
            REQUIRE_FALSE(dataIter.getStringByIndex(10).is_initialized());
            REQUIRE_FALSE(dataIter.getStringByIndex(11).is_initialized());
        } else {
            REQUIRE(dataIter.getLongByIndex(1).value() == timestamp + rowNum);
            REQUIRE(dataIter.getBooleanByIndex(2).value() == (rowNum % 2 == 0));
            REQUIRE(dataIter.getIntByIndex(3).value() == static_cast<int32_t>(rowNum));
            REQUIRE(dataIter.getLongByIndex(4).value() == static_cast<int64_t>(timestamp));
            REQUIRE(fabs(dataIter.getFloatByIndex(5).value() - rowNum * 1.1f) < 0.1f);
            REQUIRE(fabs(dataIter.getDoubleByIndex(6).value() - rowNum * 1.1f) < 0.1);
            REQUIRE(dataIter.getStringByIndex(7).value() == "text_" + to_string(rowNum));
            REQUIRE(dataIter.getTimestampByIndex(8).value() == static_cast<int64_t>(timestamp));
            REQUIRE(dataIter.getDateByIndex(9).value() == boost::gregorian::date(2025, 5, 15));
            REQUIRE(dataIter.getStringByIndex(10).value() == "blob_" + to_string(rowNum));
            REQUIRE(dataIter.getStringByIndex(11).value() == "string_" + to_string(rowNum));
        }
        rowNum++;
    }
    REQUIRE(rowNum == maxRowNumber);

    sessionDataSet = session->executeQueryStatement("SELECT * FROM table1 order by time");
    rowNum = 0;
    timestamp = 0;
    while (sessionDataSet->hasNext()) {
        auto record = sessionDataSet->next();
        if (rowNum % 2 == 0) {
            REQUIRE_FALSE(record->fields[1].boolV.is_initialized());
            REQUIRE_FALSE(record->fields[2].intV.is_initialized());
            REQUIRE_FALSE(record->fields[3].longV.is_initialized());
            REQUIRE_FALSE(record->fields[4].floatV.is_initialized());
            REQUIRE_FALSE(record->fields[5].doubleV.is_initialized());
            REQUIRE_FALSE(record->fields[6].stringV.is_initialized());
            REQUIRE_FALSE(record->fields[7].longV.is_initialized());
            REQUIRE_FALSE(record->fields[8].dateV.is_initialized());
            REQUIRE_FALSE(record->fields[9].stringV.is_initialized());
            REQUIRE_FALSE(record->fields[10].stringV.is_initialized());
        } else {
            REQUIRE(record->fields[1].boolV.value() == (rowNum % 2 == 0));
            REQUIRE(record->fields[2].intV.value() == static_cast<int32_t>(rowNum));
            REQUIRE(record->fields[3].longV.value() == static_cast<int64_t>(timestamp));
            REQUIRE(fabs(record->fields[4].floatV.value() - rowNum * 1.1f) < 0.1f);
            REQUIRE(fabs(record->fields[5].doubleV.value() - rowNum * 1.1f) < 0.1);
            REQUIRE(record->fields[6].stringV.value() == "text_" + to_string(rowNum));
            REQUIRE(record->fields[7].longV.value() == static_cast<int64_t>(timestamp));
            REQUIRE(record->fields[8].dateV.value() == boost::gregorian::date(2025, 5, 15));
            REQUIRE(record->fields[9].stringV.value() == "blob_" + to_string(rowNum));
            REQUIRE(record->fields[10].stringV.value() == "string_" + to_string(rowNum));
        }
        rowNum++;
    }
    REQUIRE(rowNum == maxRowNumber);
}

TEST_CASE("TableSession prepared statement all supported parameter types", "[tablePreparedStatement][tablePreparedAllTypes]") {
    CaseReporter cr("tablePreparedStatementAllTypes");
    session->executeNonQueryStatement("DROP DATABASE IF EXISTS db_prep_cpp_types");
    session->executeNonQueryStatement("CREATE DATABASE db_prep_cpp_types");
    session->executeNonQueryStatement("USE \"db_prep_cpp_types\"");
    session->executeNonQueryStatement(
        "CREATE TABLE t_ps_types ("
        "tag1 string tag,"
        "b boolean field,"
        "i32 int32 field,"
        "i64 int64 field,"
        "f float field,"
        "d double field,"
        "s string field)");

    vector<pair<string, TSDataType::TSDataType>> schemaList;
    schemaList.push_back(make_pair("tag1", TSDataType::STRING));
    schemaList.push_back(make_pair("b", TSDataType::BOOLEAN));
    schemaList.push_back(make_pair("i32", TSDataType::INT32));
    schemaList.push_back(make_pair("i64", TSDataType::INT64));
    schemaList.push_back(make_pair("f", TSDataType::FLOAT));
    schemaList.push_back(make_pair("d", TSDataType::DOUBLE));
    schemaList.push_back(make_pair("s", TSDataType::STRING));
    vector<ColumnCategory> columnTypes = {
        ColumnCategory::TAG, ColumnCategory::FIELD, ColumnCategory::FIELD, ColumnCategory::FIELD,
        ColumnCategory::FIELD, ColumnCategory::FIELD, ColumnCategory::FIELD
    };
    Tablet tablet("t_ps_types", schemaList, columnTypes, 10);

    int rowIndex = tablet.rowSize++;
    tablet.timestamps[rowIndex] = 1;
    tablet.addValue(0, rowIndex, string("dev1"));
    tablet.addValue(1, rowIndex, true);
    tablet.addValue(2, rowIndex, static_cast<int32_t>(123));
    tablet.addValue(3, rowIndex, static_cast<int64_t>(123456789LL));
    tablet.addValue(4, rowIndex, static_cast<float>(1.5f));
    tablet.addValue(5, rowIndex, static_cast<double>(2.5));
    tablet.addValue(6, rowIndex, string("alpha"));

    rowIndex = tablet.rowSize++;
    tablet.timestamps[rowIndex] = 2;
    tablet.addValue(0, rowIndex, string("dev2"));
    tablet.addValue(1, rowIndex, false);
    tablet.addValue(2, rowIndex, static_cast<int32_t>(456));
    tablet.addValue(3, rowIndex, static_cast<int64_t>(987654321LL));
    tablet.addValue(4, rowIndex, static_cast<float>(3.5f));
    tablet.addValue(5, rowIndex, static_cast<double>(4.5));
    tablet.addValue(6, rowIndex, string("beta"));
    session->insert(tablet, true);

    try {
        const std::string sql =
            "SELECT i64 FROM db_prep_cpp_types.t_ps_types "
            "WHERE tag1 = ? AND b = ? AND i32 = ? AND i64 = ? AND f = ? AND d = ? AND s = ?";
        const std::string statementName = "cpp_ps_all_types";
        const int32_t paramCount = session->prepareStatement(sql, statementName);
        REQUIRE(paramCount == 7);

        std::vector<iotdb::prepared::ParamSlot> params(static_cast<size_t>(paramCount));
        params[0].kind = iotdb::prepared::ParamKind::kString;
        params[0].stringOrBlob = "dev1";
        params[1].kind = iotdb::prepared::ParamKind::kBool;
        params[1].boolVal = true;
        params[2].kind = iotdb::prepared::ParamKind::kInt32;
        params[2].int32Val = 123;
        params[3].kind = iotdb::prepared::ParamKind::kInt64;
        params[3].int64Val = 123456789LL;
        params[4].kind = iotdb::prepared::ParamKind::kFloat;
        params[4].floatVal = 1.5f;
        params[5].kind = iotdb::prepared::ParamKind::kDouble;
        params[5].doubleVal = 2.5;
        params[6].kind = iotdb::prepared::ParamKind::kString;
        params[6].stringOrBlob = "alpha";

        unique_ptr<SessionDataSet> sessionDataSet = session->executePreparedStatement(sql, statementName, params, -1);
        int cnt = 0;
        while (sessionDataSet->hasNext()) {
            auto row = sessionDataSet->next();
            REQUIRE(row->fields[0].longV.value() == 123456789LL);
            cnt++;
        }
        REQUIRE(cnt == 1);

        session->deallocatePreparedStatement(statementName);

        // Smoke-check NULL parameter encoding path on client side.
        std::vector<iotdb::prepared::ParamSlot> nullParams(1);
        nullParams[0].kind = iotdb::prepared::ParamKind::kNull;
        REQUIRE_FALSE(iotdb::prepared::serializeParameters(nullParams).empty());
    } catch (const std::exception& e) {
        if (isPreparedStatementUnsupportedByServer(e)) {
            WARN("Skipping: server does not expose prepareStatement RPC (use a build that includes table-model "
                 "prepared statements)");
            return;
        }
        throw;
    }
}
