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

namespace {
std::string utf8(const SQLWCHAR* value, SQLINTEGER length) {
  if (length < 0 && length != SQL_NTS)
    throw std::invalid_argument("Invalid string or buffer length");
  if (!value)
    return "";
  size_t count = length == SQL_NTS ? std::char_traits<SQLWCHAR>::length(value) : length;
  std::u16string input(count, u'\0');
  if (count)
    std::memcpy(&input[0], value, count * sizeof(SQLWCHAR));
  return std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t>().to_bytes(input);
}

SQLCHAR* ansi(const SQLWCHAR* original, std::string& converted) {
  return original ? reinterpret_cast<SQLCHAR*>(&converted[0]) : nullptr;
}

template <class F> SQLRETURN invoke(SQLHANDLE handle, F call) {
  if (!handle)
    return SQL_INVALID_HANDLE;
  try {
    return call();
  } catch (const std::invalid_argument& error) {
    auto* h = static_cast<ODBCHandle*>(handle);
    h->clearDiagnostics();
    h->addDiagnostic("HY090", error.what());
  } catch (const std::exception& error) {
    auto* h = static_cast<ODBCHandle*>(handle);
    h->clearDiagnostics();
    h->addDiagnostic("22018", error.what());
  }
  return SQL_ERROR;
}

template <class Length>
SQLRETURN output(SQLHANDLE handle, const std::string& value, SQLWCHAR* buffer, SQLINTEGER capacity,
                 Length* length, bool bytes, bool diagnostic = false) {
  if (capacity < 0)
    throw std::invalid_argument("Invalid string or buffer length");
  auto wide = std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t>().from_bytes(value);
  const size_t units = bytes ? sizeof(SQLWCHAR) : 1;
  if (length)
    *length = static_cast<Length>(
        std::min(wide.size() * units, static_cast<size_t>(std::numeric_limits<Length>::max())));
  if (!buffer)
    return SQL_SUCCESS;
  size_t slots = static_cast<size_t>(capacity) / units;
  size_t copied = slots ? std::min(wide.size(), slots - 1) : 0;
  if (copied)
    std::memcpy(buffer, wide.data(), copied * sizeof(SQLWCHAR));
  if (slots)
    buffer[copied] = 0;
  if (copied < wide.size()) {
    if (!diagnostic)
      static_cast<ODBCHandle*>(handle)->addDiagnostic("01004", "String data, right truncated");
    return SQL_SUCCESS_WITH_INFO;
  }
  return SQL_SUCCESS;
}

bool stringInfo(SQLUSMALLINT info) {
  switch (info) {
  case SQL_DRIVER_NAME:
  case SQL_DRIVER_ODBC_VER:
  case SQL_DRIVER_VER:
  case SQL_DATABASE_NAME:
  case SQL_DESCRIBE_PARAMETER:
  case SQL_DBMS_NAME:
  case SQL_DBMS_VER:
  case SQL_IDENTIFIER_QUOTE_CHAR:
  case SQL_CATALOG_NAME:
  case SQL_CATALOG_NAME_SEPARATOR:
  case SQL_ORDER_BY_COLUMNS_IN_SELECT:
  case SQL_COLUMN_ALIAS:
  case SQL_CATALOG_TERM:
  case SQL_SCHEMA_TERM:
  case SQL_SEARCH_PATTERN_ESCAPE:
  case SQL_SPECIAL_CHARACTERS:
  case SQL_NEED_LONG_DATA_LEN:
  case SQL_DATA_SOURCE_NAME:
  case SQL_DATA_SOURCE_READ_ONLY:
  case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
  case SQL_PROCEDURE_TERM:
  case SQL_INTEGRITY:
  case SQL_TABLE_TERM:
  case SQL_USER_NAME:
  case SQL_MULT_RESULT_SETS:
  case SQL_SERVER_NAME:
  case SQL_ACCESSIBLE_PROCEDURES:
  case SQL_ACCESSIBLE_TABLES:
  case SQL_PROCEDURES:
  case SQL_LIKE_ESCAPE_CLAUSE:
  case SQL_OUTER_JOINS:
    return true;
  default:
    return false;
  }
}
} // namespace

SQLRETURN SQL_API SQLExecDirectW(SQLHSTMT h, SQLWCHAR* sql, SQLINTEGER length) {
  return invoke(h, [&] {
    auto value = utf8(sql, length);
    return SQLExecDirect(h, ansi(sql, value), static_cast<SQLINTEGER>(value.size()));
  });
}
SQLRETURN SQL_API SQLPrepareW(SQLHSTMT h, SQLWCHAR* sql, SQLINTEGER length) {
  return invoke(h, [&] {
    auto value = utf8(sql, length);
    return SQLPrepare(h, ansi(sql, value), static_cast<SQLINTEGER>(value.size()));
  });
}
SQLRETURN SQL_API SQLDriverConnectW(SQLHDBC h, SQLHWND window, SQLWCHAR* input, SQLSMALLINT size,
                                    SQLWCHAR* buffer, SQLSMALLINT capacity, SQLSMALLINT* length,
                                    SQLUSMALLINT completion) {
  return invoke(h, [&] {
    if (capacity < 0)
      throw std::invalid_argument("Invalid string or buffer length");
    auto value = utf8(input, size);
    SQLRETURN rc =
        SQLDriverConnect(h, window, ansi(input, value), SQL_NTS, nullptr, 0, nullptr, completion);
    if (!SQL_SUCCEEDED(rc))
      return rc;
    SQLRETURN copied = output(h, value, buffer, capacity, length, false);
    return copied == SQL_SUCCESS ? rc : copied;
  });
}
SQLRETURN SQL_API SQLConnectW(SQLHDBC h, SQLWCHAR* dsn, SQLSMALLINT ds, SQLWCHAR* user,
                              SQLSMALLINT us, SQLWCHAR* password, SQLSMALLINT ps) {
  return invoke(h, [&] {
    auto d = utf8(dsn, ds), u = utf8(user, us), p = utf8(password, ps);
    return SQLConnect(h, ansi(dsn, d), SQL_NTS, ansi(user, u), SQL_NTS, ansi(password, p), SQL_NTS);
  });
}
SQLRETURN SQL_API SQLTablesW(SQLHSTMT h, SQLWCHAR* cat, SQLSMALLINT cs, SQLWCHAR* schema,
                             SQLSMALLINT ss, SQLWCHAR* table, SQLSMALLINT ts, SQLWCHAR* type,
                             SQLSMALLINT ys) {
  return invoke(h, [&] {
    auto c = utf8(cat, cs), s = utf8(schema, ss), t = utf8(table, ts), y = utf8(type, ys);
    return SQLTables(h, ansi(cat, c), SQL_NTS, ansi(schema, s), SQL_NTS, ansi(table, t), SQL_NTS,
                     ansi(type, y), SQL_NTS);
  });
}
SQLRETURN SQL_API SQLColumnsW(SQLHSTMT h, SQLWCHAR* cat, SQLSMALLINT cs, SQLWCHAR* schema,
                              SQLSMALLINT ss, SQLWCHAR* table, SQLSMALLINT ts, SQLWCHAR* name,
                              SQLSMALLINT ns) {
  return invoke(h, [&] {
    auto c = utf8(cat, cs), s = utf8(schema, ss), t = utf8(table, ts), n = utf8(name, ns);
    return SQLColumns(h, ansi(cat, c), SQL_NTS, ansi(schema, s), SQL_NTS, ansi(table, t), SQL_NTS,
                      ansi(name, n), SQL_NTS);
  });
}
SQLRETURN SQL_API SQLDescribeColW(SQLHSTMT h, SQLUSMALLINT col, SQLWCHAR* name,
                                  SQLSMALLINT capacity, SQLSMALLINT* length, SQLSMALLINT* type,
                                  SQLULEN* size, SQLSMALLINT* digits, SQLSMALLINT* nullable) {
  return invoke(h, [&] {
    std::vector<SQLCHAR> value(32767);
    SQLRETURN rc =
        SQLDescribeCol(h, col, value.data(), 32767, nullptr, type, size, digits, nullable);
    if (!SQL_SUCCEEDED(rc))
      return rc;
    return output(h, reinterpret_cast<char*>(value.data()), name, capacity, length, false);
  });
}
SQLRETURN SQL_API SQLColAttributeW(SQLHSTMT h, SQLUSMALLINT col, SQLUSMALLINT field,
                                   SQLPOINTER buffer, SQLSMALLINT capacity, SQLSMALLINT* length,
                                   SQLLEN* numeric) {
  switch (field) {
  case SQL_COLUMN_NAME:
  case SQL_DESC_NAME:
  case SQL_DESC_LABEL:
  case SQL_DESC_TYPE_NAME:
  case SQL_DESC_BASE_COLUMN_NAME:
  case SQL_DESC_BASE_TABLE_NAME:
  case SQL_DESC_CATALOG_NAME:
  case SQL_DESC_SCHEMA_NAME:
  case SQL_DESC_TABLE_NAME:
  case SQL_DESC_LITERAL_PREFIX:
  case SQL_DESC_LITERAL_SUFFIX:
  case SQL_DESC_LOCAL_TYPE_NAME:
    return invoke(h, [&] {
      std::vector<SQLCHAR> value(32767);
      SQLRETURN rc = SQLColAttribute(h, col, field, value.data(), 32767, nullptr, numeric);
      if (!SQL_SUCCEEDED(rc))
        return rc;
      return output(h, reinterpret_cast<char*>(value.data()), static_cast<SQLWCHAR*>(buffer),
                    capacity, length, true);
    });
  default:
    return SQLColAttribute(h, col, field, buffer, capacity, length, numeric);
  }
}
SQLRETURN SQL_API SQLGetInfoW(SQLHDBC h, SQLUSMALLINT info, SQLPOINTER buffer, SQLSMALLINT capacity,
                              SQLSMALLINT* length) {
  if (!stringInfo(info))
    return SQLGetInfo(h, info, buffer, capacity, length);
  return invoke(h, [&] {
    std::vector<SQLCHAR> value(32767);
    SQLRETURN rc = SQLGetInfo(h, info, value.data(), 32767, nullptr);
    if (!SQL_SUCCEEDED(rc))
      return rc;
    return output(h, reinterpret_cast<char*>(value.data()), static_cast<SQLWCHAR*>(buffer),
                  capacity, length, true);
  });
}
SQLRETURN SQL_API SQLGetDiagRecW(SQLSMALLINT type, SQLHANDLE h, SQLSMALLINT record, SQLWCHAR* state,
                                 SQLINTEGER* native, SQLWCHAR* message, SQLSMALLINT capacity,
                                 SQLSMALLINT* length) {
  if (!h || static_cast<ODBCHandle*>(h)->getHandleType() != type)
    return SQL_INVALID_HANDLE;
  if (record <= 0 || capacity < 0)
    return SQL_ERROR;
  // Diagnostic reads must not change the records they are reading.
  try {
    std::string s, m;
    int n;
    if (!static_cast<ODBCHandle*>(h)->getDiagnostic(record, s, m, n))
      return SQL_NO_DATA;
    if (state)
      output<SQLSMALLINT>(h, s, state, 6, nullptr, false, true);
    if (native)
      *native = n;
    return output(h, m, message, capacity, length, false, true);
  } catch (...) {
    return SQL_ERROR;
  }
}
SQLRETURN SQL_API SQLGetDiagFieldW(SQLSMALLINT type, SQLHANDLE h, SQLSMALLINT record,
                                   SQLSMALLINT field, SQLPOINTER buffer, SQLSMALLINT capacity,
                                   SQLSMALLINT* length) {
  switch (field) {
  case SQL_DIAG_SQLSTATE:
  case SQL_DIAG_MESSAGE_TEXT:
  case SQL_DIAG_CLASS_ORIGIN:
  case SQL_DIAG_SUBCLASS_ORIGIN:
  case SQL_DIAG_CONNECTION_NAME:
  case SQL_DIAG_SERVER_NAME: {
    // Do not use invoke: reading diagnostics must preserve the diagnostic stack.
    try {
      std::vector<SQLCHAR> value(32767);
      SQLRETURN rc = SQLGetDiagField(type, h, record, field, value.data(), 32767, nullptr);
      if (!SQL_SUCCEEDED(rc))
        return rc;
      return output(h, reinterpret_cast<char*>(value.data()), static_cast<SQLWCHAR*>(buffer),
                    capacity, length, true, true);
    } catch (...) {
      return SQL_ERROR;
    }
  }
  default:
    return SQLGetDiagField(type, h, record, field, buffer, capacity, length);
  }
}
SQLRETURN SQL_API SQLGetTypeInfoW(SQLHSTMT h, SQLSMALLINT type) {
  return SQLGetTypeInfo(h, type);
}
SQLRETURN SQL_API SQLGetStmtAttrW(SQLHSTMT h, SQLINTEGER attr, SQLPOINTER value, SQLINTEGER size,
                                  SQLINTEGER* length) {
  return SQLGetStmtAttr(h, attr, value, size, length);
}
SQLRETURN SQL_API SQLSetStmtAttrW(SQLHSTMT h, SQLINTEGER attr, SQLPOINTER value, SQLINTEGER size) {
  return SQLSetStmtAttr(h, attr, value, size);
}
SQLRETURN SQL_API SQLGetConnectAttrW(SQLHDBC h, SQLINTEGER attr, SQLPOINTER value, SQLINTEGER size,
                                     SQLINTEGER* length) {
  if (attr != SQL_ATTR_CURRENT_CATALOG)
    return SQLGetConnectAttr(h, attr, value, size, length);
  return invoke(h, [&] {
    auto* connection = static_cast<ConnectionHandle*>(h);
    connection->clearDiagnostics();
    return output(h, connection->database, static_cast<SQLWCHAR*>(value), size, length, true);
  });
}
SQLRETURN SQL_API SQLSetConnectAttrW(SQLHDBC h, SQLINTEGER attr, SQLPOINTER value,
                                     SQLINTEGER size) {
  if (attr != SQL_ATTR_CURRENT_CATALOG)
    return SQLSetConnectAttr(h, attr, value, size);
  return invoke(h, [&] {
    if ((size < 0 && size != SQL_NTS) || (size >= 0 && size % sizeof(SQLWCHAR)))
      throw std::invalid_argument("Invalid string or buffer length");
    auto converted = utf8(static_cast<SQLWCHAR*>(value), size == SQL_NTS ? size : size / 2);
    return SQLSetConnectAttr(h, attr, value ? &converted[0] : nullptr,
                             static_cast<SQLINTEGER>(converted.size()));
  });
}
SQLRETURN SQL_API SQLNativeSqlW(SQLHDBC h, SQLWCHAR* input, SQLINTEGER size, SQLWCHAR* buffer,
                                SQLINTEGER capacity, SQLINTEGER* length) {
  return invoke(h, [&] {
    auto converted = utf8(input, size);
    SQLRETURN rc = SQLNativeSql(h, ansi(input, converted),
                                static_cast<SQLINTEGER>(converted.size()), nullptr, 0, nullptr);
    if (!SQL_SUCCEEDED(rc))
      return rc;
    return output(h, converted, buffer, capacity, length, false);
  });
}
SQLRETURN SQL_API SQLGetDescFieldW(SQLHDESC h, SQLSMALLINT record, SQLSMALLINT field,
                                   SQLPOINTER value, SQLINTEGER size, SQLINTEGER* length) {
  if (field != SQL_DESC_NAME)
    return SQLGetDescField(h, record, field, value, size, length);
  return invoke(h, [&] {
    std::vector<SQLCHAR> converted(32767);
    SQLRETURN rc = SQLGetDescField(h, record, field, converted.data(), 32767, nullptr);
    if (!SQL_SUCCEEDED(rc))
      return rc;
    return output(h, reinterpret_cast<char*>(converted.data()), static_cast<SQLWCHAR*>(value), size,
                  length, true);
  });
}
SQLRETURN SQL_API SQLGetDescRecW(SQLHDESC h, SQLSMALLINT record, SQLWCHAR* name,
                                 SQLSMALLINT capacity, SQLSMALLINT* length, SQLSMALLINT* type,
                                 SQLSMALLINT* sub, SQLLEN* size, SQLSMALLINT* precision,
                                 SQLSMALLINT* scale, SQLSMALLINT* nullable) {
  return invoke(h, [&] {
    std::vector<SQLCHAR> converted(32767);
    SQLRETURN rc = SQLGetDescRec(h, record, converted.data(), 32767, nullptr, type, sub, size,
                                 precision, scale, nullable);
    if (!SQL_SUCCEEDED(rc))
      return rc;
    return output(h, reinterpret_cast<char*>(converted.data()), name, capacity, length, false);
  });
}
SQLRETURN SQL_API SQLSetDescFieldW(SQLHDESC h, SQLSMALLINT record, SQLSMALLINT field,
                                   SQLPOINTER value, SQLINTEGER size) {
  if (field != SQL_DESC_NAME)
    return SQLSetDescField(h, record, field, value, size);
  return invoke(h, [&] {
    if ((size < 0 && size != SQL_NTS) || (size >= 0 && size % sizeof(SQLWCHAR)))
      throw std::invalid_argument("Invalid string or buffer length");
    auto converted = utf8(static_cast<SQLWCHAR*>(value), size == SQL_NTS ? size : size / 2);
    return SQLSetDescField(h, record, field, value ? &converted[0] : nullptr,
                           static_cast<SQLINTEGER>(converted.size()));
  });
}
