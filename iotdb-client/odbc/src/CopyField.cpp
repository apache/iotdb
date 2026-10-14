/**
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
#include <string>
#include <stdexcept>
#include <codecvt>
#include <locale>
#ifdef _WIN32
#include <windows.h>
#endif

#include "ODBCField.h"
#include "Pch.h"
#include "driver.h"
#include "Log.h"
#include "ConnectionHandle.h"
#include "StatementHandle.h"
#include "DiagnosticManager.h"

// UTF-8 to UTF-16/WCHAR conversion function
std::wstring utf8ToWide(const std::string& utf8Str) {
// Windows implementation (using MultiByteToWideChar)
#ifdef _WIN32
  if (utf8Str.empty())
    return L"";

  int size = MultiByteToWideChar(CP_UTF8, 0, utf8Str.c_str(), -1, nullptr, 0);

  if (size == 0) {
    throw std::runtime_error("UTF-8 to WideChar conversion failed");
  }

  std::wstring result(size, 0);
  MultiByteToWideChar(CP_UTF8, 0, utf8Str.c_str(), -1, &result[0], size);

  result.resize(size - 1); // Remove extra null terminator
  return result;

// Linux/macOS implementation (using standard library)
#else
  std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
  return converter.from_bytes(utf8Str);
#endif
}

static bool BuildCharStringForField(const ODBCField& value, std::string& out) {
  SQLLEN len = 0;
  if (!value.toChar(nullptr, 0, &len)) {
    return false;
  }
  if (len == SQL_NULL_DATA) {
    out.clear();
    return true;
  }
  std::vector<SQLCHAR> buffer(static_cast<size_t>(len) + 1);
  if (!value.toChar(buffer.data(), static_cast<SQLLEN>(buffer.size()), &len)) {
    return false;
  }
  out.assign(reinterpret_cast<const char*>(buffer.data()), static_cast<size_t>(len));
  return true;
}

static bool BuildWCharStringForField(const ODBCField& value, std::basic_string<SQLWCHAR>& out) {
  SQLLEN lenBytes = 0;
  if (!value.toWChar(nullptr, 0, &lenBytes)) {
    return false;
  }
  if (lenBytes == SQL_NULL_DATA) {
    out.clear();
    return true;
  }
  size_t wcharCount = static_cast<size_t>(lenBytes / sizeof(SQLWCHAR));
  std::vector<SQLWCHAR> buffer(wcharCount + 1);
  if (!value.toWChar(buffer.data(), static_cast<SQLLEN>((wcharCount + 1) * sizeof(SQLWCHAR)),
                     &lenBytes)) {
    return false;
  }
  out.assign(buffer.data(), buffer.data() + wcharCount);
  return true;
}

static bool BuildBinaryForField(const ODBCField& value, std::vector<SQLCHAR>& out) {
  SQLLEN len = 0;
  if (!value.toBinary(nullptr, 0, &len)) {
    return false;
  }
  if (len == SQL_NULL_DATA) {
    out.clear();
    return true;
  }
  out.resize(static_cast<size_t>(len));
  if (!value.toBinary(out.data(), len, &len)) {
    return false;
  }
  return true;
}

SQLRETURN CopyFieldToTargetVariable(StatementHandle* stmt, const ODBCField& value,
                                    SQLSMALLINT targetType, SQLPOINTER targetValuePtr,
                                    SQLLEN bufferLength, SQLLEN* strLen_or_IndPtr,
                                    SQLUSMALLINT col) {
  ConnectionHandle* cnct = stmt ? stmt->getConnection() : nullptr;

  if (bufferLength < 0) {
    stmt->addDiagnostic("HY090", "Invalid string or buffer length");
    return SQL_ERROR;
  }

  if (stmt->getDataOffsets.size() <= col) {
    const int numCols = stmt->resultSetPtr ? stmt->resultSetPtr->getNumColumns() : 0;
    stmt->getDataOffsets.assign(static_cast<size_t>(numCols + 1), 0);
  }

  SQLLEN& offset = stmt->getDataOffsets[col];

  SQLLEN totalBytes = 0;
  bool maxLengthApplied = false;

  std::string charValue;
  std::basic_string<SQLWCHAR> wcharValue;
  std::vector<SQLCHAR> binaryValue;

  if (targetType == SQL_C_CHAR) {
    if (!BuildCharStringForField(value, charValue)) {
      stmt->addDiagnostic("22005", "Error converting to SQL_C_CHAR");
      return SQL_ERROR;
    }
    totalBytes = static_cast<SQLLEN>(charValue.size());
  } else if (targetType == SQL_C_WCHAR) {
    if (!BuildWCharStringForField(value, wcharValue)) {
      stmt->addDiagnostic("22005", "Error converting to SQL_C_WCHAR");
      return SQL_ERROR;
    }
    totalBytes = static_cast<SQLLEN>(wcharValue.size() * sizeof(SQLWCHAR));
  } else if (targetType == SQL_C_BINARY) {
    if (!BuildBinaryForField(value, binaryValue)) {
      stmt->addDiagnostic("22005", "Error converting to SQL_C_BINARY");
      return SQL_ERROR;
    }
    totalBytes = static_cast<SQLLEN>(binaryValue.size());
  } else {
    stmt->addDiagnostic("HY003", "Invalid application buffer type");
    return SQL_ERROR;
  }

  if (stmt->maxLength > 0 && totalBytes > static_cast<SQLLEN>(stmt->maxLength)) {
    totalBytes = static_cast<SQLLEN>(stmt->maxLength);
    maxLengthApplied = true;
  }

  SQLLEN remaining = totalBytes - offset;
  if (totalBytes == 0 && offset == 0) {
    if (strLen_or_IndPtr)
      *strLen_or_IndPtr = 0;
    if (targetValuePtr) {
      if (targetType == SQL_C_CHAR && bufferLength > 0)
        *static_cast<SQLCHAR*>(targetValuePtr) = 0;
      if (targetType == SQL_C_WCHAR && bufferLength >= static_cast<SQLLEN>(sizeof(SQLWCHAR)))
        *static_cast<SQLWCHAR*>(targetValuePtr) = 0;
      offset = 1;
    }
    return SQL_SUCCESS;
  }
  if (remaining <= 0) {
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = 0;
    }
    return SQL_NO_DATA;
  }

  if (strLen_or_IndPtr) {
    *strLen_or_IndPtr = remaining;
  }

  SQLLEN copyLen = 0;
  if (targetType == SQL_C_CHAR) {
    SQLLEN maxPayload = (bufferLength > 1) ? (bufferLength - 1) : 0;
    copyLen = (remaining < maxPayload) ? remaining : maxPayload;
    if (targetValuePtr && bufferLength > 0) {
      if (copyLen > 0) {
        std::memcpy(targetValuePtr, charValue.data() + offset, static_cast<size_t>(copyLen));
      }
      static_cast<char*>(targetValuePtr)[copyLen] = '\0';
    }
  } else if (targetType == SQL_C_WCHAR) {
    SQLLEN maxPayload = (bufferLength >= static_cast<SQLLEN>(sizeof(SQLWCHAR)))
                            ? (bufferLength - static_cast<SQLLEN>(sizeof(SQLWCHAR)))
                            : 0;
    copyLen = (remaining < maxPayload) ? remaining : maxPayload;
    copyLen -= (copyLen % static_cast<SQLLEN>(sizeof(SQLWCHAR)));
    if (targetValuePtr && bufferLength >= static_cast<SQLLEN>(sizeof(SQLWCHAR))) {
      if (copyLen > 0) {
        std::memcpy(targetValuePtr, reinterpret_cast<const SQLCHAR*>(wcharValue.data()) + offset,
                    static_cast<size_t>(copyLen));
      }
      const SQLLEN wcharIndex = copyLen / static_cast<SQLLEN>(sizeof(SQLWCHAR));
      static_cast<SQLWCHAR*>(targetValuePtr)[wcharIndex] = L'\0';
    }
  } else if (targetType == SQL_C_BINARY) {
    SQLLEN maxPayload = (bufferLength > 0) ? bufferLength : 0;
    copyLen = (remaining < maxPayload) ? remaining : maxPayload;
    if (targetValuePtr && copyLen > 0) {
      std::memcpy(targetValuePtr, binaryValue.data() + offset, static_cast<size_t>(copyLen));
    }
  }

  if (copyLen > 0) {
    offset += copyLen;
  }

  if (remaining > copyLen) {
    stmt->addDiagnostic("01004", "String data, right truncated");
    return SQL_SUCCESS_WITH_INFO;
  }

  if (maxLengthApplied) {
    return SQL_SUCCESS;
  }

  return SQL_SUCCESS;
}

SQLRETURN CopyFieldToTarget(StatementHandle* stmt, const ODBCField& value, SQLSMALLINT targetType,
                            SQLPOINTER targetValuePtr, SQLLEN bufferLength,
                            SQLLEN* strLen_or_IndPtr, bool reportTruncation, SQLUSMALLINT col) {
  ConnectionHandle* cnct = stmt ? stmt->getConnection() : nullptr;
  logMessage(cnct, "CopyFieldToTarget: Copying field to target type: " + CDataTypeName(targetType),
             LOG_LEVEL_TRACE);
  logMessage(cnct,
             "CopyFieldToTarget: Field Type: " + TSDataTypeName(value.getDataType()) +
                 ", Field value: " + value.toString(),
             LOG_LEVEL_TRACE);

  auto reportTruncationIfNeeded = [&](SQLLEN fullLenBytes) -> SQLRETURN {
    if (!reportTruncation) {
      return SQL_SUCCESS;
    }
    if (bufferLength > 0 && fullLenBytes != SQL_NULL_DATA && bufferLength <= fullLenBytes) {
      stmt->addDiagnostic("01004", "String data, right truncated");
      logMessage(cnct, "CopyFieldToTarget: Data truncated for column " + std::to_string(col),
                 LOG_LEVEL_INFO);
      return SQL_SUCCESS_WITH_INFO;
    }
    return SQL_SUCCESS;
  };

  switch (targetType) {
  case SQL_C_CHAR: {
    if (!value.toChar(static_cast<SQLCHAR*>(targetValuePtr), bufferLength, strLen_or_IndPtr)) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_CHAR", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_CHAR");
      return SQL_ERROR;
    }
    SQLLEN len = strLen_or_IndPtr ? *strLen_or_IndPtr : 0;
    return reportTruncationIfNeeded(len);
  }
  case SQL_C_WCHAR: {
    if (!value.toWChar(static_cast<SQLWCHAR*>(targetValuePtr), bufferLength, strLen_or_IndPtr)) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_WCHAR", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_WCHAR");
      return SQL_ERROR;
    }
    SQLLEN len = strLen_or_IndPtr ? *strLen_or_IndPtr : 0;
    return reportTruncationIfNeeded(len);
  }
  case SQL_C_SHORT:
  case SQL_C_SSHORT: {
    if (!value.toSShort(static_cast<SQLSMALLINT*>(targetValuePtr))) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_SSHORT", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_SSHORT");
      return SQL_ERROR;
    }
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = sizeof(SQLSMALLINT);
    }
    break;
  }
  case SQL_C_USHORT: {
    if (!value.toUShort(static_cast<SQLUSMALLINT*>(targetValuePtr))) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_USHORT", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_USHORT");
      return SQL_ERROR;
    }
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = sizeof(SQLUSMALLINT);
    }
    break;
  }
  case SQL_C_LONG:
  case SQL_C_SLONG: {
    if (!value.toSLong(static_cast<SQLINTEGER*>(targetValuePtr))) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_SLONG", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_SLONG");
      return SQL_ERROR;
    }
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = sizeof(SQLINTEGER);
    }
    break;
  }
  case SQL_C_ULONG: {
    if (!value.toULong(static_cast<SQLUINTEGER*>(targetValuePtr))) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_ULONG", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_ULONG");
      return SQL_ERROR;
    }
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = sizeof(SQLUINTEGER);
    }
    break;
  }
  case SQL_C_FLOAT: {
    if (!value.toFloat(static_cast<SQLREAL*>(targetValuePtr))) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_FLOAT", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_FLOAT");
      return SQL_ERROR;
    }
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = sizeof(SQLREAL);
    }
    break;
  }
  case SQL_C_DOUBLE: {
    if (!value.toDouble(static_cast<SQLDOUBLE*>(targetValuePtr))) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_DOUBLE", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_DOUBLE");
      return SQL_ERROR;
    }
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = sizeof(SQLDOUBLE);
    }
    break;
  }
  case SQL_C_BIT: {
    if (!value.toBit(static_cast<SQLCHAR*>(targetValuePtr))) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_BIT", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_BIT");
      return SQL_ERROR;
    }
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = sizeof(SQLCHAR);
    }
    break;
  }
  case SQL_C_TINYINT:
  case SQL_C_STINYINT: {
    if (!value.toSTinyInt(static_cast<SQLSCHAR*>(targetValuePtr))) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_STINYINT", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_STINYINT");
      return SQL_ERROR;
    }
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = sizeof(SQLSCHAR);
    }
    break;
  }
  case SQL_C_UTINYINT: {
    if (!value.toUTinyInt(static_cast<SQLCHAR*>(targetValuePtr))) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_UTINYINT", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_UTINYINT");
      return SQL_ERROR;
    }
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = sizeof(SQLCHAR);
    }
    break;
  }
  case SQL_BIGINT:
  case SQL_C_SBIGINT: {
    if (!value.toSBigInt(static_cast<SQLBIGINT*>(targetValuePtr))) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_SBIGINT", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_SBIGINT");
      return SQL_ERROR;
    }
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = sizeof(SQLBIGINT);
    }
    break;
  }
  case SQL_C_UBIGINT: {
    if (!value.toUBigInt(static_cast<SQLUBIGINT*>(targetValuePtr))) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_UBIGINT", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_UBIGINT");
      return SQL_ERROR;
    }
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = sizeof(SQLUBIGINT);
    }
    break;
  }
  case SQL_C_BINARY: {
    if (!value.toBinary(static_cast<SQLCHAR*>(targetValuePtr), bufferLength, strLen_or_IndPtr)) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_BINARY", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_BINARY");
      return SQL_ERROR;
    }
    SQLLEN len = strLen_or_IndPtr ? *strLen_or_IndPtr : 0;
    return reportTruncationIfNeeded(len);
  }
  case SQL_C_TYPE_DATE: {
    if (!value.toDate(static_cast<SQL_DATE_STRUCT*>(targetValuePtr))) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_TYPE_DATE", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_TYPE_DATE");
      return SQL_ERROR;
    }
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = sizeof(SQL_DATE_STRUCT);
    }
    break;
  }
  case SQL_C_TYPE_TIME: {
    if (!value.toTime(static_cast<SQL_TIME_STRUCT*>(targetValuePtr))) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_TYPE_TIME", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_TYPE_TIME");
      return SQL_ERROR;
    }
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = sizeof(SQL_TIME_STRUCT);
    }
    break;
  }
  case SQL_C_TYPE_TIMESTAMP: {
    if (!value.toTimestamp(static_cast<SQL_TIMESTAMP_STRUCT*>(targetValuePtr))) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_TYPE_TIMESTAMP",
                 LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_TYPE_TIMESTAMP");
      return SQL_ERROR;
    }
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = sizeof(SQL_TIMESTAMP_STRUCT);
    }
    break;
  }
  case SQL_C_NUMERIC: {
    if (!value.toNumeric(static_cast<SQL_NUMERIC_STRUCT*>(targetValuePtr))) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_NUMERIC", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_NUMERIC");
      return SQL_ERROR;
    }
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = sizeof(SQL_NUMERIC_STRUCT);
    }
    break;
  }
  case SQL_C_GUID: {
    if (!value.toGUID(static_cast<SQLGUID*>(targetValuePtr))) {
      logMessage(cnct, "CopyFieldToTarget: Error converting to SQL_C_GUID", LOG_LEVEL_ERROR);
      stmt->addDiagnostic("22005", "Error converting to SQL_C_GUID");
      return SQL_ERROR;
    }
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = sizeof(SQLGUID);
    }
    break;
  }
  default:
    logMessage(cnct,
               "CopyFieldToTarget: Invalid application buffer type: " + CDataTypeName(targetType),
               LOG_LEVEL_ERROR);
    stmt->addDiagnostic("HY003", "Invalid application buffer type");
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}
