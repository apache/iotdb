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
//
// Created by christoferdutz on 05.11.2024.
//

#ifndef DRIVER_H
#define DRIVER_H

#include "Pch.h"
#include "DiagnosticManager.h"
#include "DriverConfig.h"

// IoTDB Session API includes
#include <TableSession.h>

// Forward declarations to avoid circular dependencies
struct BindColInfo;

#include "ODBCHandle.h"
#include "EnvironmentHandle.h"
#include "ConnectionHandle.h"
#include "StatementHandle.h"

extern "C" {
SQLRETURN SQL_API SQLAllocConnect(SQLHENV environmentHandle, SQLHDBC* connectionHandle);
SQLRETURN SQL_API SQLAllocEnv(SQLHENV* environmentHandle);
SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT handleType, SQLHANDLE inputHandle,
                                 SQLHANDLE* outputHandle);
SQLRETURN SQL_API SQLAllocStmt(SQLHDBC ConnectionHandle, SQLHSTMT* StatementHandle);
SQLRETURN SQL_API SQLBindCol(SQLHSTMT StatementHandle, SQLUSMALLINT columnNumber,
                             SQLSMALLINT TargetType, SQLPOINTER TargetValue, SQLLEN BufferLength,
                             SQLLEN* StrLen_or_Ind);
SQLRETURN SQL_API SQLBindParam(SQLHSTMT StatementHandle, SQLUSMALLINT ParameterNumber,
                               SQLSMALLINT ValueType, SQLSMALLINT ParameterType,
                               SQLULEN LengthPrecision, SQLSMALLINT ParameterScale,
                               SQLPOINTER ParameterValue, SQLLEN* StrLen_or_Ind);
SQLRETURN SQL_API SQLBindParameter(SQLHSTMT StatementHandle, SQLUSMALLINT ParameterNumber,
                                   SQLSMALLINT InputOutputType, SQLSMALLINT ValueType,
                                   SQLSMALLINT ParameterType, SQLULEN ColumnSize,
                                   SQLSMALLINT DecimalDigits, SQLPOINTER ParameterValuePtr,
                                   SQLLEN BufferLength, SQLLEN* StrLen_or_IndPtr);
SQLRETURN SQL_API SQLCancel(SQLHSTMT StatementHandle);
SQLRETURN SQL_API SQLCancelHandle(SQLSMALLINT HandleType, SQLHANDLE InputHandle);
SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT StatementHandle);
SQLRETURN SQL_API SQLColAttribute(SQLHSTMT StatementHandle, SQLUSMALLINT columnNumber,
                                  SQLUSMALLINT fieldIdentifier, SQLPOINTER characterAttribute,
                                  SQLSMALLINT bufferLength, SQLSMALLINT* stringLength,
                                  SQLLEN* numericAttribute);
SQLRETURN SQL_API SQLColumns(SQLHSTMT StatementHandle, SQLCHAR* CatalogName,
                             SQLSMALLINT NameLength1, SQLCHAR* SchemaName, SQLSMALLINT NameLength2,
                             SQLCHAR* TableName, SQLSMALLINT NameLength3, SQLCHAR* ColumnName,
                             SQLSMALLINT NameLength4);
SQLRETURN SQL_API SQLConnect(SQLHDBC ConnectionHandle, SQLCHAR* ServerName, SQLSMALLINT NameLength1,
                             SQLCHAR* UserName, SQLSMALLINT NameLength2, SQLCHAR* Password,
                             SQLSMALLINT NameLength3);
SQLRETURN SQL_API SQLCopyDesc(SQLHDESC SourceDescHandle, SQLHDESC TargetDescHandle);
SQLRETURN SQL_API SQLDataSources(SQLHENV EnvironmentHandle, SQLUSMALLINT Direction,
                                 SQLCHAR* ServerName, SQLSMALLINT BufferLength1,
                                 SQLSMALLINT* NameLength1Ptr, SQLCHAR* Description,
                                 SQLSMALLINT BufferLength2, SQLSMALLINT* NameLength2Ptr);
SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber,
                                 SQLCHAR* ColumnName, SQLSMALLINT BufferLength,
                                 SQLSMALLINT* NameLength, SQLSMALLINT* DataType,
                                 SQLULEN* ColumnSize, SQLSMALLINT* DecimalDigits,
                                 SQLSMALLINT* Nullable);
SQLRETURN SQL_API SQLDisconnect(SQLHDBC ConnectionHandle);
SQLRETURN SQL_API SQLEndTran(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT CompletionType);
SQLRETURN SQL_API SQLError(SQLHENV EnvironmentHandle, SQLHDBC ConnectionHandle,
                           SQLHSTMT StatementHandle, SQLCHAR* Sqlstate, SQLINTEGER* NativeError,
                           SQLCHAR* MessageText, SQLSMALLINT BufferLength, SQLSMALLINT* TextLength);
SQLRETURN SQL_API SQLExecDirect(SQLHSTMT StatementHandle, SQLCHAR* StatementText,
                                SQLINTEGER TextLength);
SQLRETURN SQL_API SQLExecute(SQLHSTMT StatementHandle);
SQLRETURN SQL_API SQLFetch(SQLHSTMT StatementHandle);
SQLRETURN SQL_API SQLFetchScroll(SQLHSTMT StatementHandle, SQLSMALLINT FetchOrientation,
                                 SQLLEN FetchOffset);
SQLRETURN SQL_API SQLFreeConnect(SQLHDBC ConnectionHandle);
SQLRETURN SQL_API SQLFreeEnv(SQLHENV EnvironmentHandle);
SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT handleType, SQLHANDLE handle);
SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT StatementHandle, SQLUSMALLINT Option);
SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC ConnectionHandle, SQLINTEGER Attribute,
                                    SQLPOINTER Value, SQLINTEGER BufferLength,
                                    SQLINTEGER* StringLengthPtr);
SQLRETURN SQL_API SQLGetConnectOption(SQLHDBC ConnectionHandle, SQLUSMALLINT Option,
                                      SQLPOINTER Value);
SQLRETURN SQL_API SQLGetCursorName(SQLHSTMT StatementHandle, SQLCHAR* CursorName,
                                   SQLSMALLINT BufferLength, SQLSMALLINT* NameLengthPtr);
SQLRETURN SQL_API SQLGetData(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber,
                             SQLSMALLINT TargetType, SQLPOINTER TargetValue, SQLLEN BufferLength,
                             SQLLEN* StrLen_or_IndPtr);
SQLRETURN SQL_API SQLGetDescField(SQLHDESC DescriptorHandle, SQLSMALLINT RecNumber,
                                  SQLSMALLINT FieldIdentifier, SQLPOINTER Value,
                                  SQLINTEGER BufferLength, SQLINTEGER* StringLength);
SQLRETURN SQL_API SQLGetDescRec(SQLHDESC DescriptorHandle, SQLSMALLINT RecNumber, SQLCHAR* Name,
                                SQLSMALLINT BufferLength, SQLSMALLINT* StringLengthPtr,
                                SQLSMALLINT* TypePtr, SQLSMALLINT* SubTypePtr, SQLLEN* LengthPtr,
                                SQLSMALLINT* PrecisionPtr, SQLSMALLINT* ScalePtr,
                                SQLSMALLINT* NullablePtr);
SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber,
                                  SQLSMALLINT DiagIdentifier, SQLPOINTER DiagInfoPtr,
                                  SQLSMALLINT BufferLength, SQLSMALLINT* StringLengthPtr);
SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber,
                                SQLCHAR* Sqlstate, SQLINTEGER* NativeError, SQLCHAR* MessageText,
                                SQLSMALLINT BufferLength, SQLSMALLINT* TextLength);
SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute, SQLPOINTER Value,
                                SQLINTEGER BufferLength, SQLINTEGER* StringLength);
SQLRETURN SQL_API SQLGetFunctions(SQLHDBC ConnectionHandle, SQLUSMALLINT FunctionId,
                                  SQLUSMALLINT* Supported);
SQLRETURN SQL_API SQLGetInfo(SQLHDBC ConnectionHandle, SQLUSMALLINT InfoType, SQLPOINTER InfoValue,
                             SQLSMALLINT BufferLength, SQLSMALLINT* StringLengthPtr);
SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT StatementHandle, SQLINTEGER Attribute, SQLPOINTER Value,
                                 SQLINTEGER BufferLength, SQLINTEGER* StringLength);
SQLRETURN SQL_API SQLGetStmtOption(SQLHSTMT StatementHandle, SQLUSMALLINT Option, SQLPOINTER Value);
SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT StatementHandle, SQLSMALLINT DataType);
SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT StatementHandle, SQLSMALLINT* ColumnCount);
SQLRETURN SQL_API SQLNumParams(SQLHSTMT StatementHandle, SQLSMALLINT* ParameterCountPtr);
SQLRETURN SQL_API SQLNativeSql(SQLHDBC ConnectionHandle, SQLCHAR* InStatementText,
                               SQLINTEGER TextLength1, SQLCHAR* OutStatementText,
                               SQLINTEGER BufferLength, SQLINTEGER* TextLength2Ptr);
SQLRETURN SQL_API SQLParamData(SQLHSTMT StatementHandle, SQLPOINTER* Value);
SQLRETURN SQL_API SQLPrepare(SQLHSTMT StatementHandle, SQLCHAR* StatementText,
                             SQLINTEGER TextLength);
SQLRETURN SQL_API SQLPutData(SQLHSTMT StatementHandle, SQLPOINTER Data, SQLLEN StrLen_or_Ind);
SQLRETURN SQL_API SQLRowCount(SQLHSTMT StatementHandle, SQLLEN* RowCount);
SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC ConnectionHandle, SQLINTEGER Attribute,
                                    SQLPOINTER Value, SQLINTEGER StringLength);
SQLRETURN SQL_API SQLSetConnectOption(SQLHDBC ConnectionHandle, SQLUSMALLINT Option, SQLULEN Value);
SQLRETURN SQL_API SQLSetCursorName(SQLHSTMT StatementHandle, SQLCHAR* CursorName,
                                   SQLSMALLINT NameLength);
SQLRETURN SQL_API SQLSetDescField(SQLHDESC DescriptorHandle, SQLSMALLINT RecNumber,
                                  SQLSMALLINT FieldIdentifier, SQLPOINTER Value,
                                  SQLINTEGER BufferLength);
SQLRETURN SQL_API SQLSetDescRec(SQLHDESC DescriptorHandle, SQLSMALLINT RecNumber, SQLSMALLINT Type,
                                SQLSMALLINT SubType, SQLLEN Length, SQLSMALLINT Precision,
                                SQLSMALLINT Scale, SQLPOINTER Data, SQLLEN* StringLength,
                                SQLLEN* Indicator);
SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute, SQLPOINTER Value,
                                SQLINTEGER StringLength);
SQLRETURN SQL_API SQLSetParam(SQLHSTMT StatementHandle, SQLUSMALLINT ParameterNumber,
                              SQLSMALLINT ValueType, SQLSMALLINT ParameterType,
                              SQLULEN LengthPrecision, SQLSMALLINT ParameterScale,
                              SQLPOINTER ParameterValue, SQLLEN* StrLen_or_Ind);
SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT StatementHandle, SQLINTEGER Attribute, SQLPOINTER Value,
                                 SQLINTEGER StringLength);
SQLRETURN SQL_API SQLSetStmtOption(SQLHSTMT StatementHandle, SQLUSMALLINT Option, SQLULEN Value);
SQLRETURN SQL_API SQLSpecialColumns(SQLHSTMT StatementHandle, SQLUSMALLINT IdentifierType,
                                    SQLCHAR* CatalogName, SQLSMALLINT NameLength1,
                                    SQLCHAR* SchemaName, SQLSMALLINT NameLength2,
                                    SQLCHAR* TableName, SQLSMALLINT NameLength3, SQLUSMALLINT Scope,
                                    SQLUSMALLINT Nullable);
SQLRETURN SQL_API SQLStatistics(SQLHSTMT StatementHandle, SQLCHAR* CatalogName,
                                SQLSMALLINT NameLength1, SQLCHAR* SchemaName,
                                SQLSMALLINT NameLength2, SQLCHAR* TableName,
                                SQLSMALLINT NameLength3, SQLUSMALLINT Unique,
                                SQLUSMALLINT Reserved);
SQLRETURN SQL_API SQLTables(SQLHSTMT StatementHandle, SQLCHAR* CatalogName, SQLSMALLINT NameLength1,
                            SQLCHAR* SchemaName, SQLSMALLINT NameLength2, SQLCHAR* TableName,
                            SQLSMALLINT NameLength3, SQLCHAR* TableType, SQLSMALLINT NameLength4);
SQLRETURN SQL_API SQLTransact(SQLHENV EnvironmentHandle, SQLHDBC ConnectionHandle,
                              SQLUSMALLINT CompletionType);

// IoTDB functions
}
SQLRETURN IoTDB_ExecDirect(StatementHandle* const stmt, const std::string& statementText,
                           bool saveStatementText = true);

#endif //DRIVER_H
