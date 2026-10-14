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
#include "ODBCField.h"
#include "Pch.h"
#include <string>
#include <cstring>
#include <cwchar>
#include <cctype>
#include "Log.h"
#include <unordered_map>
#include <unordered_set>

ODBCField::ODBCField() {
  field_.dataType = TSDataType::UNKNOWN;
  // Constructor implementation
}

ODBCField::ODBCField(bool value) {
  setValue(value);
}

ODBCField::ODBCField(int value) {
  setValue(value);
}

ODBCField::ODBCField(const IoTDBDate& value) {
  setValue(value);
}

ODBCField::ODBCField(int64_t value) {
  setValue(value);
}

ODBCField::ODBCField(float value) {
  setValue(value);
}

ODBCField::ODBCField(double value) {
  setValue(value);
}

ODBCField::ODBCField(const std::string& value) {
  setValue(value);
}

ODBCField::ODBCField(const Field& field) : field_(field) {
  // Constructor with field implementation
}

ODBCField::~ODBCField() {
  // Destructor implementation
}

ODBCField ODBCField::null() {
  return ODBCField();
}

void ODBCField::setField(const Field& field) {
  field_ = field;
}

TSDataType::TSDataType ODBCField::getDataType() const {
  return field_.dataType;
}

bool ODBCField::isNull() const {
  // Check if field is null based on dataType and optional values
  switch (field_.dataType) {
  case TSDataType::BOOLEAN:
    return !field_.boolV.is_initialized();
  case TSDataType::INT32:
    return !field_.intV.is_initialized();
  case TSDataType::INT64:
  case TSDataType::TIMESTAMP:
    return !field_.longV.is_initialized();
  case TSDataType::FLOAT:
    return !field_.floatV.is_initialized();
  case TSDataType::DOUBLE:
    return !field_.doubleV.is_initialized();
  case TSDataType::TEXT:
  case TSDataType::STRING:
    return !field_.stringV.is_initialized();
  case TSDataType::DATE:
    return !field_.dateV.is_initialized();
  case TSDataType::BLOB:
    // BLOB is represented as string in this implementation
    return !field_.stringV.is_initialized();
  default:
    return true; // Unknown type, treat as null
  }
}

bool ODBCField::toChar(SQLCHAR* target, SQLLEN bufferLength, SQLLEN* strLen_or_IndPtr) const {
  // Convert to SQL_C_CHAR (null-terminated string)
  if (isNull()) {
    if (strLen_or_IndPtr)
      *strLen_or_IndPtr = SQL_NULL_DATA;
    return true;
  }

  std::string strValue;
  switch (field_.dataType) {
  case TSDataType::BOOLEAN:
    strValue = field_.boolV.value() ? "1" : "0";
    break;
  case TSDataType::INT32:
    strValue = std::to_string(field_.intV.value());
    break;
  case TSDataType::INT64:
  case TSDataType::TIMESTAMP:
    strValue = std::to_string(field_.longV.value());
    break;
  case TSDataType::FLOAT:
    strValue = std::to_string(field_.floatV.value());
    break;
  case TSDataType::DOUBLE:
    strValue = std::to_string(field_.doubleV.value());
    break;
  case TSDataType::TEXT:
  case TSDataType::STRING:
  case TSDataType::BLOB:
    strValue = field_.stringV.value();
    break;
  case TSDataType::DATE:
    strValue = field_.dateV.value().toIsoExtendedString();
    break;
  default:
    return false; // Unsupported conversion
  }

  SQLLEN requiredLength = strValue.length() + 1; // +1 for null terminator

  if (strLen_or_IndPtr) {
    *strLen_or_IndPtr = strValue.length(); // Length without null terminator
  }

  if (target && bufferLength >= requiredLength) {
    std::memcpy(target, strValue.c_str(), requiredLength);
  } else if (target && bufferLength > 0) {
    // Truncate if buffer is too small
    std::memcpy(target, strValue.c_str(), bufferLength - 1);
    target[bufferLength - 1] = '\0';
  }

  return true;
}

bool ODBCField::toWChar(SQLWCHAR* target, SQLLEN bufferLength, SQLLEN* strLen_or_IndPtr) const {
  // Convert to SQL_C_WCHAR (wide character string)
  if (isNull()) {
    if (strLen_or_IndPtr)
      *strLen_or_IndPtr = SQL_NULL_DATA;
    return true;
  }

  std::string strValue;
  switch (field_.dataType) {
  case TSDataType::BOOLEAN:
    strValue = field_.boolV.value() ? "1" : "0";
    break;
  case TSDataType::INT32:
    strValue = std::to_string(field_.intV.value());
    break;
  case TSDataType::INT64:
  case TSDataType::TIMESTAMP:
    strValue = std::to_string(field_.longV.value());
    break;
  case TSDataType::FLOAT:
    strValue = std::to_string(field_.floatV.value());
    break;
  case TSDataType::DOUBLE:
    strValue = std::to_string(field_.doubleV.value());
    break;
  case TSDataType::TEXT:
  case TSDataType::STRING:
  case TSDataType::BLOB:
    strValue = field_.stringV.value();
    break;
  case TSDataType::DATE:
    strValue = field_.dateV.value().toIsoExtendedString();
    break;
  default:
    return false; // Unsupported conversion
  }

  // Convert to wide string
  std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> converter;
  std::u16string wstrValue = converter.from_bytes(strValue);
  SQLLEN requiredLength = (wstrValue.length() + 1) * sizeof(SQLWCHAR); // +1 for null terminator

  if (strLen_or_IndPtr) {
    *strLen_or_IndPtr =
        wstrValue.length() * sizeof(SQLWCHAR); // Length in bytes without null terminator
  }

  if (target && bufferLength >= requiredLength) {
    std::memcpy(target, wstrValue.c_str(), requiredLength);
  } else if (target && bufferLength >= sizeof(SQLWCHAR)) {
    // Truncate if buffer is too small (at least room for null terminator)
    SQLLEN maxChars = (bufferLength / sizeof(SQLWCHAR)) - 1;
    std::memcpy(target, wstrValue.c_str(), maxChars * sizeof(SQLWCHAR));
    target[maxChars] = L'\0';
  }

  return true;
}

bool ODBCField::toSTinyInt(SQLSCHAR* target) const {
  // Convert to SQL_C_STINYINT (signed char, -128 to 127)
  if (!target || isNull())
    return false;

  switch (field_.dataType) {
  case TSDataType::BOOLEAN:
    *target = field_.boolV.value() ? 1 : 0;
    return true;
  case TSDataType::INT32:
    if (field_.intV.value() >= -128 && field_.intV.value() <= 127) {
      *target = static_cast<SQLSCHAR>(field_.intV.value());
      return true;
    }
    break;
  case TSDataType::INT64:
  case TSDataType::TIMESTAMP:
    if (field_.longV.value() >= -128 && field_.longV.value() <= 127) {
      *target = static_cast<SQLSCHAR>(field_.longV.value());
      return true;
    }
    break;
  default:
    break;
  }
  return false;
}

bool ODBCField::toUTinyInt(SQLCHAR* target) const {
  // Convert to SQL_C_UTINYINT (unsigned char, 0 to 255)
  if (!target || isNull())
    return false;

  switch (field_.dataType) {
  case TSDataType::BOOLEAN:
    *target = field_.boolV.value() ? 1 : 0;
    return true;
  case TSDataType::INT32:
    if (field_.intV.value() >= 0 && field_.intV.value() <= 255) {
      *target = static_cast<SQLCHAR>(field_.intV.value());
      return true;
    }
    break;
  case TSDataType::INT64:
  case TSDataType::TIMESTAMP:
    if (field_.longV.value() >= 0 && field_.longV.value() <= 255) {
      *target = static_cast<SQLCHAR>(field_.longV.value());
      return true;
    }
    break;
  default:
    break;
  }
  return false;
}

bool ODBCField::toSShort(SQLSMALLINT* target) const {
  // Convert to SQL_C_SSHORT (signed short int, -32,768 to 32,767)
  if (!target || isNull())
    return false;

  switch (field_.dataType) {
  case TSDataType::BOOLEAN:
    *target = field_.boolV.value() ? 1 : 0;
    return true;
  case TSDataType::INT32:
    if (field_.intV.value() >= -32768 && field_.intV.value() <= 32767) {
      *target = static_cast<SQLSMALLINT>(field_.intV.value());
      return true;
    }
    break;
  case TSDataType::INT64:
  case TSDataType::TIMESTAMP:
    if (field_.longV.value() >= -32768 && field_.longV.value() <= 32767) {
      *target = static_cast<SQLSMALLINT>(field_.longV.value());
      return true;
    }
    break;
  default:
    break;
  }
  return false;
}

bool ODBCField::toUShort(SQLUSMALLINT* target) const {
  // Convert to SQL_C_USHORT (unsigned short int, 0 to 65,535)
  if (!target || isNull())
    return false;

  switch (field_.dataType) {
  case TSDataType::BOOLEAN:
    *target = field_.boolV.value() ? 1 : 0;
    return true;
  case TSDataType::INT32:
    if (field_.intV.value() >= 0 && field_.intV.value() <= 65535) {
      *target = static_cast<SQLUSMALLINT>(field_.intV.value());
      return true;
    }
    break;
  case TSDataType::INT64:
  case TSDataType::TIMESTAMP:
    if (field_.longV.value() >= 0 && field_.longV.value() <= 65535) {
      *target = static_cast<SQLUSMALLINT>(field_.longV.value());
      return true;
    }
    break;
  default:
    break;
  }
  return false;
}

bool ODBCField::toSLong(SQLINTEGER* target) const {
  // Convert to SQL_C_SLONG (signed long int)
  if (!target || isNull())
    return false;

  switch (field_.dataType) {
  case TSDataType::BOOLEAN:
    *target = field_.boolV.value() ? 1 : 0;
    return true;
  case TSDataType::INT32:
    *target = static_cast<SQLINTEGER>(field_.intV.value());
    return true;
  case TSDataType::INT64:
    // Check for overflow/underflow
    if (field_.longV.value() >= INT_MIN && field_.longV.value() <= INT_MAX) {
      *target = static_cast<SQLINTEGER>(field_.longV.value());
      return true;
    }
    break;
  case TSDataType::TIMESTAMP:
    // Timestamp can be converted to long (seconds since epoch)
    *target = static_cast<SQLINTEGER>(field_.longV.value() / 1000); // Convert ms to seconds
    return true;
  default:
    break;
  }
  return false;
}

bool ODBCField::toULong(SQLUINTEGER* target) const {
  // Convert to SQL_C_ULONG (unsigned long int)
  if (!target || isNull())
    return false;

  switch (field_.dataType) {
  case TSDataType::BOOLEAN:
    *target = field_.boolV.value() ? 1 : 0;
    return true;
  case TSDataType::INT32:
    if (field_.intV.value() >= 0) {
      *target = static_cast<SQLUINTEGER>(field_.intV.value());
      return true;
    }
    break;
  case TSDataType::INT64:
    if (field_.longV.value() >= 0 && field_.longV.value() <= UINT_MAX) {
      *target = static_cast<SQLUINTEGER>(field_.longV.value());
      return true;
    }
    break;
  case TSDataType::TIMESTAMP:
    // Timestamp can be converted to unsigned long (seconds since epoch)
    if (field_.longV.value() >= 0) {
      *target = static_cast<SQLUINTEGER>(field_.longV.value() / 1000); // Convert ms to seconds
      return true;
    }
    break;
  default:
    break;
  }
  return false;
}

bool ODBCField::toSBigInt(SQLBIGINT* target) const {
  // Convert to SQL_C_SBIGINT (signed 64-bit integer)
  if (!target || isNull())
    return false;

  switch (field_.dataType) {
  case TSDataType::BOOLEAN:
    *target = field_.boolV.value() ? 1 : 0;
    return true;
  case TSDataType::INT32:
    *target = static_cast<SQLBIGINT>(field_.intV.value());
    return true;
  case TSDataType::INT64:
  case TSDataType::TIMESTAMP:
    *target = static_cast<SQLBIGINT>(field_.longV.value());
    return true;
  default:
    break;
  }
  return false;
}

bool ODBCField::toUBigInt(SQLUBIGINT* target) const {
  // Convert to SQL_C_UBIGINT (unsigned 64-bit integer)
  if (!target || isNull())
    return false;

  switch (field_.dataType) {
  case TSDataType::BOOLEAN:
    *target = field_.boolV.value() ? 1 : 0;
    return true;
  case TSDataType::INT32:
    if (field_.intV.value() >= 0) {
      *target = static_cast<SQLUBIGINT>(field_.intV.value());
      return true;
    }
    break;
  case TSDataType::INT64:
    if (field_.longV.value() >= 0) {
      *target = static_cast<SQLUBIGINT>(field_.longV.value());
      return true;
    }
    break;
  case TSDataType::TIMESTAMP:
    if (field_.longV.value() >= 0) {
      *target = static_cast<SQLUBIGINT>(field_.longV.value());
      return true;
    }
    break;
  default:
    break;
  }
  return false;
}

bool ODBCField::toFloat(SQLREAL* target) const {
  // Convert to SQL_C_FLOAT (float)
  if (!target || isNull())
    return false;

  switch (field_.dataType) {
  case TSDataType::BOOLEAN:
    *target = field_.boolV.value() ? 1.0f : 0.0f;
    return true;
  case TSDataType::INT32:
    *target = static_cast<SQLREAL>(field_.intV.value());
    return true;
  case TSDataType::INT64:
    *target = static_cast<SQLREAL>(field_.longV.value());
    return true;
  case TSDataType::FLOAT:
    *target = static_cast<SQLREAL>(field_.floatV.value());
    return true;
  case TSDataType::DOUBLE:
    // Check for precision loss
    {
      double val = field_.doubleV.value();
      if (val >= -std::numeric_limits<float>::max() && val <= std::numeric_limits<float>::max()) {
        *target = static_cast<SQLREAL>(val);
        return true;
      }
    }
    break;
  case TSDataType::TIMESTAMP:
    *target = static_cast<SQLREAL>(field_.longV.value());
    return true;
  default:
    break;
  }
  return false;
}

bool ODBCField::toDouble(SQLDOUBLE* target) const {
  // Convert to SQL_C_DOUBLE (double)
  if (!target || isNull())
    return false;

  switch (field_.dataType) {
  case TSDataType::BOOLEAN:
    *target = field_.boolV.value() ? 1.0 : 0.0;
    return true;
  case TSDataType::INT32:
    *target = static_cast<SQLDOUBLE>(field_.intV.value());
    return true;
  case TSDataType::INT64:
    *target = static_cast<SQLDOUBLE>(field_.longV.value());
    return true;
  case TSDataType::FLOAT:
    *target = static_cast<SQLDOUBLE>(field_.floatV.value());
    return true;
  case TSDataType::DOUBLE:
    *target = static_cast<SQLDOUBLE>(field_.doubleV.value());
    return true;
  case TSDataType::TIMESTAMP:
    *target = static_cast<SQLDOUBLE>(field_.longV.value());
    return true;
  default:
    break;
  }
  return false;
}

bool ODBCField::toBit(SQLCHAR* target) const {
  // Convert to SQL_C_BIT (unsigned char, 0 or 1)
  if (!target || isNull())
    return false;

  switch (field_.dataType) {
  case TSDataType::BOOLEAN:
    *target = field_.boolV.value() ? 1 : 0;
    return true;
  case TSDataType::INT32:
    *target = (field_.intV.value() != 0) ? 1 : 0;
    return true;
  case TSDataType::INT64:
    *target = (field_.longV.value() != 0) ? 1 : 0;
    return true;
  case TSDataType::FLOAT:
    *target = (field_.floatV.value() != 0.0f) ? 1 : 0;
    return true;
  case TSDataType::DOUBLE:
    *target = (field_.doubleV.value() != 0.0) ? 1 : 0;
    return true;
  case TSDataType::TEXT:
  case TSDataType::STRING: {
    std::string str = field_.stringV.value();
    // Convert string to boolean (common conventions)
    if (str == "1" || str == "true" || str == "TRUE" || str == "yes" || str == "YES") {
      *target = 1;
      return true;
    } else if (str == "0" || str == "false" || str == "FALSE" || str == "no" || str == "NO") {
      *target = 0;
      return true;
    }
  } break;
  default:
    break;
  }
  return false;
}

bool ODBCField::toBinary(SQLCHAR* target, SQLLEN bufferLength, SQLLEN* strLen_or_IndPtr) const {
  // Convert to SQL_C_BINARY (binary data as unsigned char array)
  if (isNull()) {
    if (strLen_or_IndPtr)
      *strLen_or_IndPtr = SQL_NULL_DATA;
    return true;
  }
  // For BLOB data, decode the hex string (e.g. "0x506C...") into raw bytes
  if (field_.dataType == TSDataType::BLOB && field_.stringV.is_initialized()) {
    const std::string& raw = field_.stringV.value();
    // Strip optional "0x"/"0X" prefix
    std::string hex = raw;
    if (hex.size() >= 2 && hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X')) {
      hex = hex.substr(2);
    }

    if (hex.size() % 2 != 0) {
      return false;
    }

    auto hexNibble = [](unsigned char c) -> int {
      if (c >= '0' && c <= '9')
        return c - '0';
      if (c >= 'a' && c <= 'f')
        return 10 + (c - 'a');
      if (c >= 'A' && c <= 'F')
        return 10 + (c - 'A');
      return -1;
    };

    std::vector<SQLCHAR> decoded;
    decoded.reserve(hex.size() / 2);
    for (size_t i = 0; i < hex.size(); i += 2) {
      int high = hexNibble(static_cast<unsigned char>(hex[i]));
      int low = hexNibble(static_cast<unsigned char>(hex[i + 1]));
      if (high < 0 || low < 0) {
        return false;
      }
      decoded.push_back(static_cast<SQLCHAR>((high << 4) | low));
    }

    SQLLEN dataLength = static_cast<SQLLEN>(decoded.size());
    if (strLen_or_IndPtr) {
      *strLen_or_IndPtr = dataLength; // total byte length
    }

    if (!target || bufferLength <= 0) {
      return true;
    }

    SQLLEN copyLength = (bufferLength < dataLength) ? bufferLength : dataLength;
    std::memcpy(target, decoded.data(), static_cast<size_t>(copyLength));
    return true;
  }

  // For other types, this conversion is not supported
  return false;
}

bool ODBCField::toTimestamp(SQL_TIMESTAMP_STRUCT* target) const {
  // Convert to SQL_C_TYPE_TIMESTAMP
  if (!target || isNull())
    return false;

  if (field_.dataType == TSDataType::DATE && field_.dateV.is_initialized()) {
    target->year = field_.dateV.value().year();
    target->month = field_.dateV.value().month();
    target->day = field_.dateV.value().day();
    target->hour = 0;
    target->minute = 0;
    target->second = 0;
    target->fraction = 0;
    return true;
  }

  if (field_.dataType == TSDataType::TIMESTAMP && field_.longV.is_initialized()) {
    int64_t ms = field_.longV.value();
    // Floor division preserves the fractional part before the Unix epoch.
    int64_t seconds = ms / 1000 - (ms % 1000 < 0);
    int64_t days = seconds / 86400 - (seconds % 86400 < 0);
    int64_t secondsInDay = seconds - days * 86400;
    auto leap = [](int year) { return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0); };
    int year = 1970;
    // Windows gmtime rejects negative timestamps; use the Gregorian calendar
    // directly so pre-1970 data has identical behavior on both platforms.
    while (days < 0 && year > 1)
      days += leap(--year) ? 366 : 365;
    while (year <= 9999 && days >= (leap(year) ? 366 : 365)) {
      days -= leap(year) ? 366 : 365;
      ++year;
    }
    if (days < 0 || year > 9999)
      return false;
    int monthDays[] = {31, leap(year) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int month = 0;
    while (days >= monthDays[month])
      days -= monthDays[month++];
    target->year = static_cast<SQLSMALLINT>(year);
    target->month = static_cast<SQLUSMALLINT>(month + 1);
    target->day = static_cast<SQLUSMALLINT>(days + 1);
    target->hour = static_cast<SQLUSMALLINT>(secondsInDay / 3600);
    target->minute = static_cast<SQLUSMALLINT>((secondsInDay / 60) % 60);
    target->second = static_cast<SQLUSMALLINT>(secondsInDay % 60);
    // fraction: 纳秒 (十亿分之一秒) = 毫秒余数 * 1000000
    int64_t ms_part = ms % 1000;
    if (ms_part < 0)
      ms_part += 1000;
    target->fraction = static_cast<SQLUINTEGER>(ms_part * 1000000);
    return true;
  }

  return false;
}

bool ODBCField::toDate(SQL_DATE_STRUCT* target) const {
  // Convert to SQL_C_TYPE_DATE
  if (!target || isNull())
    return false;

  if (field_.dataType == TSDataType::DATE && field_.dateV.is_initialized()) {
    IoTDBDate date = field_.dateV.value();
    target->year = date.year();
    target->month = date.month();
    target->day = date.day();
    return true;
  }

  return false;
}

bool ODBCField::toTime(SQL_TIME_STRUCT* target) const {
  // Convert to SQL_C_TYPE_TIME
  if (!target || isNull())
    return false;

  // For timestamp, extract time part (simplified implementation)
  if (field_.dataType == TSDataType::TIMESTAMP && field_.longV.is_initialized()) {
    // This is a simplified implementation - real implementation would need proper timestamp parsing
    // For now, just return false for timestamp to time conversion
    return false;
  }

  return false;
}

bool ODBCField::toNumeric(SQL_NUMERIC_STRUCT* target) const {
  // Convert to SQL_C_NUMERIC
  if (!target || isNull())
    return false;

  // Initialize the numeric struct
  target->precision = 0;
  target->scale = 0;
  target->sign = 1; // Positive by default
  std::memset(target->val, 0, SQL_MAX_NUMERIC_LEN);

  bool success = false;
  switch (field_.dataType) {
  case TSDataType::INT32: {
    int32_t value = field_.intV.value();
    target->sign = (value < 0) ? 0 : 1;
    int64_t absValue = std::abs(static_cast<int64_t>(value));

    // Convert to scaled integer in little-endian format
    target->precision = 10; // Default precision for 32-bit int
    target->scale = 0;

    for (int i = 0; i < SQL_MAX_NUMERIC_LEN && absValue > 0; ++i) {
      target->val[i] = absValue % 256;
      absValue /= 256;
    }
    success = true;
    break;
  }
  case TSDataType::INT64:
  case TSDataType::TIMESTAMP: {
    int64_t value = field_.longV.value();
    target->sign = (value < 0) ? 0 : 1;
    int64_t absValue = std::abs(value);

    // Convert to scaled integer in little-endian format
    target->precision = 19; // Default precision for 64-bit int
    target->scale = 0;

    for (int i = 0; i < SQL_MAX_NUMERIC_LEN && absValue > 0; ++i) {
      target->val[i] = absValue % 256;
      absValue /= 256;
    }
    success = true;
    break;
  }
  case TSDataType::FLOAT:
  case TSDataType::DOUBLE:
    // For simplicity, convert floating point to string then to numeric
    // This is a basic implementation - real implementation would need proper decimal handling
    {
      double value =
          (field_.dataType == TSDataType::FLOAT) ? field_.floatV.value() : field_.doubleV.value();
      target->sign = (value < 0) ? 0 : 1;
      double absValue = std::abs(value);

      // Very basic conversion - just store as integer part
      target->precision = 15;
      target->scale = 0;

      int64_t intPart = static_cast<int64_t>(absValue);
      for (int i = 0; i < SQL_MAX_NUMERIC_LEN && intPart > 0; ++i) {
        target->val[i] = intPart % 256;
        intPart /= 256;
      }
      success = true;
      break;
    }
  default:
    break;
  }
  return success;
}

bool ODBCField::toGUID(SQLGUID* target) const {
  // Convert to SQL_C_GUID
  // GUID conversion is only supported for string representations of GUIDs
  if (!target || isNull())
    return false;

  if (field_.dataType == TSDataType::TEXT || field_.dataType == TSDataType::STRING) {
    const std::string& guidStr = field_.stringV.value();

    // Basic GUID parsing - expects format like "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    // This is a simplified implementation
    if (guidStr.length() == 36) {
      // Parse the GUID string (this is a very basic implementation)
      // Real implementation would need proper GUID parsing
      try {
        // For simplicity, we'll just return false for now
        // A full implementation would parse the string and set the Data1-Data4 fields
        return false;
      } catch (...) {
        return false;
      }
    }
  }

  return false;
}

// Set value methods - overloaded for different data types
void ODBCField::setValue(bool value) {
  field_.dataType = TSDataType::BOOLEAN;
  field_.boolV = value;
  // Clear other optional values
  field_.intV = {};
  field_.dateV = {};
  field_.longV = {};
  field_.floatV = {};
  field_.doubleV = {};
  field_.stringV = {};
}

void ODBCField::setValue(int value) {
  field_.dataType = TSDataType::INT32;
  field_.intV = value;
  // Clear other optional values
  field_.boolV = {};
  field_.dateV = {};
  field_.longV = {};
  field_.floatV = {};
  field_.doubleV = {};
  field_.stringV = {};
}

void ODBCField::setValue(const IoTDBDate& value) {
  field_.dataType = TSDataType::DATE;
  field_.dateV = value;
  // Clear other optional values
  field_.boolV = {};
  field_.intV = {};
  field_.longV = {};
  field_.floatV = {};
  field_.doubleV = {};
  field_.stringV = {};
}

void ODBCField::setValue(int64_t value) {
  field_.dataType = TSDataType::INT64;
  field_.longV = value;
  // Clear other optional values
  field_.boolV = {};
  field_.intV = {};
  field_.dateV = {};
  field_.floatV = {};
  field_.doubleV = {};
  field_.stringV = {};
}

void ODBCField::setValue(float value) {
  field_.dataType = TSDataType::FLOAT;
  field_.floatV = value;
  // Clear other optional values
  field_.boolV = {};
  field_.intV = {};
  field_.dateV = {};
  field_.longV = {};
  field_.doubleV = {};
  field_.stringV = {};
}

void ODBCField::setValue(double value) {
  field_.dataType = TSDataType::DOUBLE;
  field_.doubleV = value;
  // Clear other optional values
  field_.boolV = {};
  field_.intV = {};
  field_.dateV = {};
  field_.longV = {};
  field_.floatV = {};
  field_.stringV = {};
}

void ODBCField::setValue(const std::string& value) {
  field_.dataType = TSDataType::STRING;
  field_.stringV = value;
  // Clear other optional values
  field_.boolV = {};
  field_.intV = {};
  field_.dateV = {};
  field_.longV = {};
  field_.floatV = {};
  field_.doubleV = {};
}

void ODBCField::setDataType(TSDataType::TSDataType dataType) {
  // get the only existing value in field_
  TSDataType::TSDataType existingDataType = TSDataType::UNKNOWN;
  int existingDataTypeCount = 0;
  if (field_.boolV.is_initialized()) {
    existingDataType = TSDataType::BOOLEAN;
    existingDataTypeCount++;
  } else if (field_.intV.is_initialized()) { // INT32
    existingDataType = TSDataType::INT32;
    existingDataTypeCount++;
  } else if (field_.longV.is_initialized()) { // INT64, TIMESTAMP
    existingDataType = TSDataType::INT64;
    existingDataTypeCount++;
  } else if (field_.floatV.is_initialized()) { // FLOAT
    existingDataType = TSDataType::FLOAT;
    existingDataTypeCount++;
  } else if (field_.doubleV.is_initialized()) { // DOUBLE
    existingDataType = TSDataType::DOUBLE;
    existingDataTypeCount++;
  } else if (field_.stringV.is_initialized()) { // TEXT, STRING, BLOB
    existingDataType = TSDataType::STRING;
    existingDataTypeCount++;
  } else if (field_.dateV.is_initialized()) { // DATE
    existingDataType = TSDataType::DATE;
    existingDataTypeCount++;
  }
  if (existingDataTypeCount > 1) {
    throw std::runtime_error("ODBCField::setDataType: Multiple data types exist");
  }
  if (existingDataTypeCount == 0) {
    field_.dataType = dataType;
    return;
  }

  // Down below is the case that existingDataTypeCount is 1
  if (existingDataType == dataType || field_.dataType == dataType) {
    return;
  }

  switch (dataType) {
  case TSDataType::TIMESTAMP:
    if (existingDataType == TSDataType::INT64) {
      field_.dataType = TSDataType::TIMESTAMP;
    }
    break;
  case TSDataType::TEXT:
    if (existingDataType == TSDataType::STRING) {
      field_.dataType = TSDataType::TEXT;
    }
    break;
  case TSDataType::BLOB:
    if (existingDataType == TSDataType::STRING) {
      field_.dataType = TSDataType::BLOB;
    }
    break;
  default:
    throw std::runtime_error("ODBCField::setDataType: Unsupported data type. Existing data type: " +
                             std::to_string(existingDataType) +
                             ", New data type: " + std::to_string(dataType));
  }
}

std::string ODBCField::toString() const {
  // Convert field value to string representation for debugging
  if (isNull()) {
    return "NULL";
  }

  switch (field_.dataType) {
  case TSDataType::BOOLEAN:
    return field_.boolV.value() ? "true" : "false";
  case TSDataType::INT32:
    return std::to_string(field_.intV.value());
  case TSDataType::INT64:
    return std::to_string(field_.longV.value());
  case TSDataType::TIMESTAMP:
    return "timestamp(" + std::to_string(field_.longV.value()) + ")";
  case TSDataType::FLOAT:
    return std::to_string(field_.floatV.value());
  case TSDataType::DOUBLE:
    return std::to_string(field_.doubleV.value());
  case TSDataType::TEXT:
  case TSDataType::STRING:
    return field_.stringV.value();
  case TSDataType::DATE:
    return "date(" + field_.dateV.value().toIsoExtendedString() + ")";
  case TSDataType::BLOB:
    return "blob(" + std::to_string((field_.stringV.value()).length()) + " bytes)";
  default:
    return "unknown(" + std::to_string(static_cast<int>(field_.dataType)) + ")";
  }
}

bool ODBCField::canConvert(TSDataType::TSDataType tsDataType, SQLSMALLINT cDataType) {
  static const std::unordered_map<TSDataType::TSDataType, std::unordered_set<SQLSMALLINT>>
      typeConversionMap = []() {
        std::unordered_map<TSDataType::TSDataType, std::unordered_set<SQLSMALLINT>> map;
        // 初始化映射表
        std::unordered_set<SQLSMALLINT> boolSet = {
            SQL_C_CHAR,   SQL_C_WCHAR,  SQL_C_STINYINT, SQL_C_UTINYINT, SQL_C_SSHORT,
            SQL_C_USHORT, SQL_C_SLONG,  SQL_C_ULONG,    SQL_C_SBIGINT,  SQL_C_UBIGINT,
            SQL_C_FLOAT,  SQL_C_DOUBLE, SQL_C_BIT};
        map[TSDataType::BOOLEAN] = boolSet;

        std::unordered_set<SQLSMALLINT> int32Set = {
            SQL_C_CHAR,   SQL_C_WCHAR,  SQL_C_STINYINT, SQL_C_UTINYINT, SQL_C_SSHORT,
            SQL_C_USHORT, SQL_C_SLONG,  SQL_C_ULONG,    SQL_C_SBIGINT,  SQL_C_UBIGINT,
            SQL_C_FLOAT,  SQL_C_DOUBLE, SQL_C_BIT,      SQL_C_NUMERIC};
        map[TSDataType::INT32] = int32Set;

        std::unordered_set<SQLSMALLINT> int64Set = {
            SQL_C_CHAR,   SQL_C_WCHAR,  SQL_C_STINYINT, SQL_C_UTINYINT, SQL_C_SSHORT,
            SQL_C_USHORT, SQL_C_SLONG,  SQL_C_ULONG,    SQL_C_SBIGINT,  SQL_C_UBIGINT,
            SQL_C_FLOAT,  SQL_C_DOUBLE, SQL_C_BIT,      SQL_C_NUMERIC};
        map[TSDataType::INT64] = int64Set;

        std::unordered_set<SQLSMALLINT> timestampSet = int64Set;
        map[TSDataType::TIMESTAMP] = timestampSet;

        std::unordered_set<SQLSMALLINT> floatSet = {SQL_C_CHAR,   SQL_C_WCHAR, SQL_C_FLOAT,
                                                    SQL_C_DOUBLE, SQL_C_BIT,   SQL_C_NUMERIC};
        map[TSDataType::FLOAT] = floatSet;

        std::unordered_set<SQLSMALLINT> doubleSet = {SQL_C_CHAR,   SQL_C_WCHAR, SQL_C_FLOAT,
                                                     SQL_C_DOUBLE, SQL_C_BIT,   SQL_C_NUMERIC};
        map[TSDataType::DOUBLE] = doubleSet;

        std::unordered_set<SQLSMALLINT> textSet = {SQL_C_CHAR, SQL_C_WCHAR, SQL_C_BIT};
        map[TSDataType::TEXT] = textSet;
        map[TSDataType::STRING] = textSet;

        std::unordered_set<SQLSMALLINT> blobSet = {SQL_C_BINARY};
        map[TSDataType::BLOB] = blobSet;

        std::unordered_set<SQLSMALLINT> dateSet = {SQL_C_CHAR, SQL_C_WCHAR, SQL_C_TYPE_DATE,
                                                   SQL_C_TYPE_TIMESTAMP};
        map[TSDataType::DATE] = dateSet;

        return map;
      }();

  SQLSMALLINT effectiveCType = cDataType;
  // The deprecated unsuffixed numeric C types are still used by MSDASQL.
  if (effectiveCType == SQL_C_LONG)
    effectiveCType = SQL_C_SLONG;
  else if (effectiveCType == SQL_C_SHORT)
    effectiveCType = SQL_C_SSHORT;
  else if (effectiveCType == SQL_C_TINYINT)
    effectiveCType = SQL_C_STINYINT;
  if (effectiveCType == SQL_C_DEFAULT) {
    effectiveCType = resolveDefaultCType(tsDataType);
  }

  auto it = typeConversionMap.find(tsDataType);
  if (it == typeConversionMap.end()) {
    return false;
  }
  return it->second.find(effectiveCType) != it->second.end();
}

SQLSMALLINT ODBCField::resolveDefaultCType(TSDataType::TSDataType tsDataType) {
  switch (tsDataType) {
  case TSDataType::BOOLEAN:
    return SQL_C_BIT;
  case TSDataType::INT32:
    return SQL_C_SLONG;
  case TSDataType::INT64:
  case TSDataType::TIMESTAMP:
    return SQL_C_SBIGINT;
  case TSDataType::FLOAT:
    return SQL_C_FLOAT;
  case TSDataType::DOUBLE:
    return SQL_C_DOUBLE;
  case TSDataType::TEXT:
  case TSDataType::STRING:
    return SQL_C_WCHAR;
  case TSDataType::BLOB:
    return SQL_C_BINARY;
  case TSDataType::DATE:
    return SQL_C_TYPE_DATE;
  default:
    return SQL_C_CHAR;
  }
}
