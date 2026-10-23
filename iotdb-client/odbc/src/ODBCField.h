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
#ifndef ODBCFIELD_H
#define ODBCFIELD_H

#include "Pch.h"
#include <Common.h>
#include <sqltypes.h>
#include <string>

class ODBCField {
public:
  // Construct a null field
  ODBCField();
  // Constructors for different data types
  ODBCField(bool value);
  ODBCField(int value);
  ODBCField(const IoTDBDate& value);
  ODBCField(int64_t value);
  ODBCField(float value);
  ODBCField(double value);
  ODBCField(const std::string& value);
  ODBCField(const char* value) : ODBCField(std::string(value)) {}
  // Construct with a Field
  explicit ODBCField(const Field& field);
  ~ODBCField();

  // Get a null field, same as ODBCField() constructor
  static ODBCField null();

  // Set the underlying Field
  void setField(const Field& field);
  void setDataType(TSDataType::TSDataType dataType);

  // Get the data type
  TSDataType::TSDataType getDataType() const;

  // Check if the field is null
  bool isNull() const;

  // Convert to various ODBC C data types
  // These methods return true if conversion is successful, false otherwise

  // Character string types
  bool toChar(SQLCHAR* target, SQLLEN bufferLength, SQLLEN* strLen_or_IndPtr) const; // SQL_C_CHAR
  bool toWChar(SQLWCHAR* target, SQLLEN bufferLength,
               SQLLEN* strLen_or_IndPtr) const; // SQL_C_WCHAR

  // Exact numeric types
  bool toSTinyInt(SQLSCHAR* target) const;   // SQL_C_STINYINT
  bool toUTinyInt(SQLCHAR* target) const;    // SQL_C_UTINYINT
  bool toSShort(SQLSMALLINT* target) const;  // SQL_C_SSHORT
  bool toUShort(SQLUSMALLINT* target) const; // SQL_C_USHORT
  bool toSLong(SQLINTEGER* target) const;    // SQL_C_SLONG
  bool toULong(SQLUINTEGER* target) const;   // SQL_C_ULONG
  bool toSBigInt(SQLBIGINT* target) const;   // SQL_C_SBIGINT
  bool toUBigInt(SQLUBIGINT* target) const;  // SQL_C_UBIGINT

  // Approximate numeric types
  bool toFloat(SQLREAL* target) const;    // SQL_C_FLOAT
  bool toDouble(SQLDOUBLE* target) const; // SQL_C_DOUBLE

  // Boolean type
  bool toBit(SQLCHAR* target) const; // SQL_C_BIT

  // Character string representing binary data
  bool toBinary(SQLCHAR* target, SQLLEN bufferLength,
                SQLLEN* strLen_or_IndPtr) const; // SQL_C_BINARY

  // Datetime types
  bool toDate(SQL_DATE_STRUCT* target) const;           // SQL_C_TYPE_DATE
  bool toTime(SQL_TIME_STRUCT* target) const;           // SQL_C_TYPE_TIME
  bool toTimestamp(SQL_TIMESTAMP_STRUCT* target) const; // SQL_C_TYPE_TIMESTAMP

  // Numeric type
  bool toNumeric(SQL_NUMERIC_STRUCT* target, SQLSMALLINT precision = 38, SQLSMALLINT scale = 0,
                 bool* truncated = nullptr) const; // SQL_C_NUMERIC

  // GUID type
  bool toGUID(SQLGUID* target) const; // SQL_C_GUID

  // Set value methods - overloaded for different data types
  void setValue(bool value);
  void setValue(int value);
  void setValue(const IoTDBDate& value);
  void setValue(int64_t value);
  void setValue(float value);
  void setValue(double value);
  void setValue(const std::string& value);

  // Get string representation for debugging
  std::string toString() const;

  static bool canConvert(TSDataType::TSDataType tsDataType, SQLSMALLINT cDataType);

  /// Resolve SQL_C_DEFAULT to the default C type for the given IoTDB/TSDataType.
  static SQLSMALLINT resolveDefaultCType(TSDataType::TSDataType tsDataType);

private:
  Field field_;
};

#endif // ODBCFIELD_H
