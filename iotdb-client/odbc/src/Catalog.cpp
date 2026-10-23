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
#include "driver.h"
#include <codecvt>
#include <map>
#include <tuple>

namespace {
using Row = std::vector<ODBCField>;
using Rows = std::vector<Row>;
const std::vector<std::string> tableNames = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE",
                                             "REMARKS"};
const std::vector<std::string> columnNames = {
    "TABLE_CAT",         "TABLE_SCHEM",      "TABLE_NAME",    "COLUMN_NAME",    "DATA_TYPE",
    "TYPE_NAME",         "COLUMN_SIZE",      "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
    "NULLABLE",          "REMARKS",          "COLUMN_DEF",    "SQL_DATA_TYPE",  "SQL_DATETIME_SUB",
    "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE"};
const std::vector<std::string> columnTypes = {
    "STRING", "STRING", "STRING", "STRING", "INT16", "STRING", "INT32", "INT32", "INT16",
    "INT16",  "INT16",  "STRING", "STRING", "INT16", "INT16",  "INT32", "INT32", "STRING"};

std::string text(SQLCHAR* value, SQLSMALLINT length) {
  return !value ? ""
                : std::string(reinterpret_cast<char*>(value),
                              length == SQL_NTS ? std::strlen(reinterpret_cast<char*>(value))
                                                : static_cast<size_t>(length));
}

bool validLength(SQLCHAR* value, SQLSMALLINT length) {
  return !value || length >= 0 || length == SQL_NTS;
}

size_t lastPathSeparator(const std::string& path) {
  bool quoted = false;
  size_t separator = std::string::npos;
  for (size_t i = 0; i < path.size(); ++i) {
    if (path[i] == '`') {
      if (quoted && i + 1 < path.size() && path[i + 1] == '`')
        ++i;
      else
        quoted = !quoted;
    } else if (path[i] == '.' && !quoted)
      separator = i;
  }
  return separator;
}

bool matches(const std::string& value, SQLCHAR* pattern, SQLSMALLINT length, bool identifier) {
  if (!pattern)
    return true;
  std::string requested = text(pattern, length);
  if (identifier) {
    auto lower = [](std::string s) {
      std::transform(s.begin(), s.end(), s.begin(),
                     [](unsigned char c) { return std::tolower(c); });
      return s;
    };
    return lower(value) == lower(requested);
  }
  // Match ODBC patterns locally: never interpolate catalog arguments into SQL.
  // Decode UTF-8 so '_' matches one Unicode character rather than one byte.
  std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> codec;
  auto input = codec.from_bytes(value), filter = codec.from_bytes(requested);
  std::vector<bool> previous(input.size() + 1), next(input.size() + 1);
  previous[0] = true;
  for (size_t i = 0; i < filter.size(); ++i) {
    char32_t token = filter[i];
    bool escaped = token == U'\\' && i + 1 < filter.size();
    if (escaped)
      token = filter[++i];
    std::fill(next.begin(), next.end(), false);
    if (token == U'%' && !escaped) {
      next[0] = previous[0];
      for (size_t j = 1; j <= input.size(); ++j)
        next[j] = previous[j] || next[j - 1];
    } else {
      for (size_t j = 1; j <= input.size(); ++j)
        next[j] = previous[j - 1] && ((token == U'_' && !escaped) || token == input[j - 1]);
    }
    previous.swap(next);
  }
  return previous.back();
}

SQLRETURN query(StatementHandle* stmt, const std::string& sql, Rows& rows) {
  SQLRETURN rc = IoTDB_ExecDirect(stmt, sql);
  if (!SQL_SUCCEEDED(rc))
    return rc;
  if (!stmt->resultSetPtr)
    return SQL_ERROR;
  rows = stmt->resultSetPtr->data;
  if (!stmt->getConnection()->useRestful) {
    auto* result = static_cast<ODBCSessionResultSet*>(stmt->resultSetPtr.get());
    // Convert every source batch before exposing the fixed catalog schema.
    while (result->getNumRows() == stmt->getConnection()->batchSize && result->loadDataNextBatch())
      rows.insert(rows.end(), result->data.begin(), result->data.end());
  }
  return SQL_SUCCESS;
}

SQLRETURN result(StatementHandle* stmt, const std::vector<std::string>& names,
                 const std::vector<std::string>& types, Rows rows) {
  stmt->ClearResultSet();
  stmt->AllocateSessionResultSet();
  stmt->resultSetPtr->columnNames = names;
  stmt->resultSetPtr->columnTypes = types;
  stmt->resultSetPtr->data = std::move(rows);
  stmt->resultSetPtr->numRows = static_cast<int>(stmt->resultSetPtr->data.size());
  stmt->resultSetPtr->numColumns = static_cast<int>(names.size());
  stmt->resultSetPtr->isMetaData = true;
  stmt->curRow = -1;
  stmt->rowsReturned = 0;
  stmt->isQuery = true;
  stmt->isStream = false;
  stmt->lastGetDataRow = -1;
  stmt->getDataOffsets.clear();
  SQLSMALLINT count;
  return SQLNumResultCols(stmt, &count);
}

bool includesTables(std::string types) {
  if (types.empty() || types == "%")
    return true;
  std::stringstream stream(types);
  std::string type;
  while (std::getline(stream, type, ',')) {
    const size_t first = type.find_first_not_of(" '\t\r\n");
    const size_t last = type.find_last_not_of(" '\t\r\n");
    if (first != std::string::npos && type.substr(first, last - first + 1) == "TABLE")
      return true;
  }
  return false;
}

Row column(const std::string& catalog, const std::string& table, const std::string& name,
           const std::string& type, bool time, const ODBCField& remark, int position) {
  SQLSMALLINT sqlType = SQL_WVARCHAR;
  int size = 2147483647, bytes = 2147483647;
  ODBCField scale = ODBCField::null(), radix = ODBCField::null(), sub = ODBCField::null();
  ODBCField octets = ODBCField::null();
  if (type == "BOOLEAN") {
    sqlType = SQL_BIT;
    size = bytes = 1;
  } else if (type == "INT32") {
    sqlType = SQL_INTEGER;
    size = 10;
    bytes = 4;
    scale = ODBCField(0);
    radix = ODBCField(10);
  } else if (type == "INT64" || type == "TIMESTAMP") {
    // Session query metadata exposes timestamps as epoch integers.
    sqlType = SQL_BIGINT;
    size = 19;
    bytes = 8;
    scale = ODBCField(0);
    radix = ODBCField(10);
  } else if (type == "FLOAT" || type == "DOUBLE") {
    sqlType = type == "FLOAT" ? SQL_REAL : SQL_DOUBLE;
    size = type == "FLOAT" ? 7 : 15;
    bytes = type == "FLOAT" ? 4 : 8;
    radix = ODBCField(2);
  } else if (type == "DATE") {
    sqlType = SQL_TYPE_DATE;
    size = 10;
    bytes = sizeof(SQL_DATE_STRUCT);
    sub = ODBCField(SQL_CODE_DATE);
  } else {
    sqlType = type == "BLOB" ? SQL_LONGVARBINARY : type == "TEXT" ? SQL_WLONGVARCHAR : SQL_WVARCHAR;
    octets = ODBCField(bytes);
  }
  return {ODBCField(catalog),
          ODBCField::null(),
          ODBCField(table),
          ODBCField(name),
          ODBCField(sqlType),
          ODBCField(type),
          ODBCField(size),
          ODBCField(bytes),
          scale,
          radix,
          ODBCField(time ? SQL_NO_NULLS : SQL_NULLABLE),
          remark,
          ODBCField::null(),
          ODBCField(type == "DATE" ? SQL_DATETIME : sqlType),
          sub,
          octets,
          ODBCField(position),
          ODBCField(time ? "NO" : "YES")};
}
} // namespace

SQLRETURN SQL_API SQLTables(SQLHSTMT handle, SQLCHAR* catalog, SQLSMALLINT catalogLength,
                            SQLCHAR* schema, SQLSMALLINT schemaLength, SQLCHAR* table,
                            SQLSMALLINT tableLength, SQLCHAR* types, SQLSMALLINT typesLength) {
  if (!handle)
    return SQL_INVALID_HANDLE;
  auto* stmt = static_cast<StatementHandle*>(handle);
  stmt->clearDiagnostics();
  if (!validLength(catalog, catalogLength) || !validLength(schema, schemaLength) ||
      !validLength(table, tableLength) || !validLength(types, typesLength)) {
    stmt->addDiagnostic("HY090", "Invalid string or buffer length");
    return SQL_ERROR;
  }
  try {
    const std::string cat = text(catalog, catalogLength), sch = text(schema, schemaLength),
                      tbl = text(table, tableLength), typ = text(types, typesLength);
    const bool emptyCatalog = catalog && cat.empty(), emptySchema = schema && sch.empty(),
               emptyTable = table && tbl.empty();
    const std::vector<std::string> tableTypes(5, "STRING");
    Rows rows, output;
    if (cat == SQL_ALL_CATALOGS && emptySchema && emptyTable) {
      auto rc = query(stmt, "SHOW DATABASES", rows);
      if (!SQL_SUCCEEDED(rc))
        return rc;
      for (const auto& row : rows)
        output.push_back({row.at(0), ODBCField::null(), ODBCField::null(), ODBCField::null(),
                          ODBCField::null()});
    } else if (sch == SQL_ALL_SCHEMAS && emptyCatalog && emptyTable) {
      // IoTDB has catalogs but no schemas; return the standard empty schema.
    } else if (typ == SQL_ALL_TABLE_TYPES && emptyCatalog && emptySchema && emptyTable) {
      output.push_back({ODBCField::null(), ODBCField::null(), ODBCField::null(), ODBCField("TABLE"),
                        ODBCField::null()});
    } else if (matches("", schema, schemaLength, stmt->metadataId) && includesTables(typ)) {
      bool relational = stmt->getConnection()->isTableModel;
      auto rc =
          query(stmt,
                relational ? "SELECT database, table_name, comment FROM information_schema.tables"
                           : "SHOW DEVICES WITH DATABASE",
                rows);
      if (!SQL_SUCCEEDED(rc))
        return rc;
      for (const auto& row : rows) {
        std::string database = row.at(relational ? 0 : 1).toString();
        std::string name = row.at(relational ? 1 : 0).toString();
        if (!relational && name.compare(0, database.size() + 1, database + ".") == 0)
          name.erase(0, database.size() + 1);
        if (matches(database, catalog, catalogLength, stmt->metadataId) &&
            matches(name, table, tableLength, stmt->metadataId))
          output.push_back({ODBCField(database), ODBCField::null(), ODBCField(name),
                            ODBCField("TABLE"), relational ? row.at(2) : ODBCField::null()});
      }
    }
    std::sort(output.begin(), output.end(), [](const Row& a, const Row& b) {
      return std::make_tuple(a[3].toString(), a[0].toString(), a[2].toString()) <
             std::make_tuple(b[3].toString(), b[0].toString(), b[2].toString());
    });
    return result(stmt, tableNames, tableTypes, std::move(output));
  } catch (const std::exception& error) {
    stmt->addDiagnostic("HY000", error.what());
    return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLColumns(SQLHSTMT handle, SQLCHAR* catalog, SQLSMALLINT catalogLength,
                             SQLCHAR* schema, SQLSMALLINT schemaLength, SQLCHAR* table,
                             SQLSMALLINT tableLength, SQLCHAR* name, SQLSMALLINT nameLength) {
  if (!handle)
    return SQL_INVALID_HANDLE;
  auto* stmt = static_cast<StatementHandle*>(handle);
  stmt->clearDiagnostics();
  if (!validLength(catalog, catalogLength) || !validLength(schema, schemaLength) ||
      !validLength(table, tableLength) || !validLength(name, nameLength)) {
    stmt->addDiagnostic("HY090", "Invalid string or buffer length");
    return SQL_ERROR;
  }
  try {
    Rows rows, output;
    if (!matches("", schema, schemaLength, stmt->metadataId))
      return result(stmt, columnNames, columnTypes, {});
    bool relational = stmt->getConnection()->isTableModel;
    auto rc = query(stmt,
                    relational ? "SELECT database, table_name, column_name, datatype, category, "
                                 "comment FROM information_schema.columns"
                               : "SHOW TIMESERIES",
                    rows);
    if (!SQL_SUCCEEDED(rc))
      return rc;
    auto source = stmt->resultSetPtr;
    int timeseries = relational ? -1 : source->findColumnIndex("Timeseries");
    int databaseIndex = relational ? 0 : source->findColumnIndex("Database");
    int typeIndex = relational ? 3 : source->findColumnIndex("DataType");
    if (databaseIndex < 0 || typeIndex < 0 || (!relational && timeseries < 0))
      throw std::runtime_error("Missing catalog source columns");
    std::map<std::pair<std::string, std::string>, int> positions;
    for (const auto& row : rows) {
      std::string database = row.at(databaseIndex).toString(), device, field;
      bool time = false;
      if (relational) {
        device = row.at(1).toString();
        field = row.at(2).toString();
        time = row.at(4).toString() == "TIME";
      } else {
        std::string path = row.at(timeseries).toString();
        size_t split = lastPathSeparator(path);
        device = path.substr(0, split);
        field = path.substr(split + 1);
        if (device.compare(0, database.size() + 1, database + ".") == 0)
          device.erase(0, database.size() + 1);
      }
      auto key = std::make_pair(database, device);
      int& position = positions[key];
      if (!relational && position == 0) {
        position = 1;
        output.push_back(column(database, device, "Time", "INT64", true, ODBCField::null(), 1));
      }
      output.push_back(column(database, device, field, row.at(typeIndex).toString(), time,
                              relational ? row.at(5) : ODBCField::null(), ++position));
    }
    output.erase(
        std::remove_if(output.begin(), output.end(),
                       [&](const Row& row) {
                         return !matches(row[0].toString(), catalog, catalogLength,
                                         stmt->metadataId) ||
                                !matches(row[2].toString(), table, tableLength, stmt->metadataId) ||
                                !matches(row[3].toString(), name, nameLength, stmt->metadataId);
                       }),
        output.end());
    std::sort(output.begin(), output.end(), [](const Row& a, const Row& b) {
      SQLINTEGER pa = 0, pb = 0;
      a[16].toSLong(&pa);
      b[16].toSLong(&pb);
      return std::make_tuple(a[0].toString(), a[2].toString(), pa) <
             std::make_tuple(b[0].toString(), b[2].toString(), pb);
    });
    return result(stmt, columnNames, columnTypes, std::move(output));
  } catch (const std::exception& error) {
    stmt->addDiagnostic("HY000", error.what());
    return SQL_ERROR;
  }
}
