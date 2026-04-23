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

#include "SessionC.h"
#include "Session.h"
#include "TableSession.h"
#include "TableSessionBuilder.h"
#include "SessionBuilder.h"
#include "SessionDataSet.h"

#include <cstring>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <memory>

/* ============================================================
 *  Internal wrapper structs — the opaque handles point to these
 * ============================================================ */

struct CSession_ {
    std::shared_ptr<Session> cpp;
};

struct CTableSession_ {
    std::shared_ptr<TableSession> cpp;
};

struct CTablet_ {
    Tablet cpp;
};

struct CSessionDataSet_ {
    std::unique_ptr<SessionDataSet> cpp;
};

struct CRowRecord_ {
    std::shared_ptr<RowRecord> cpp;
};

/* ============================================================
 *  Thread-local error message buffer
 * ============================================================ */

static thread_local std::string g_lastError;

static void clearError() {
    g_lastError.clear();
}

static TsStatus setError(TsStatus code, const std::string& msg) {
    g_lastError = msg;
    return code;
}

static TsStatus setError(TsStatus code, const std::exception& e) {
    g_lastError = e.what();
    return code;
}

static TsStatus handleException(const std::exception& e) {
#if defined(_CPPRTTI) || defined(__GXX_RTTI)
    // Try to classify exception type (requires RTTI enabled).
    if (dynamic_cast<const IoTDBConnectionException*>(&e)) {
        return setError(TS_ERR_CONNECTION, e);
    }
    if (dynamic_cast<const ExecutionException*>(&e) ||
        dynamic_cast<const StatementExecutionException*>(&e) ||
        dynamic_cast<const BatchExecutionException*>(&e)) {
        return setError(TS_ERR_EXECUTION, e);
    }
#endif
    return setError(TS_ERR_UNKNOWN, e);
}

extern "C" {

const char* ts_get_last_error(void) {
    return g_lastError.c_str();
}

}  /* extern "C" */

/* ============================================================
 *  Helpers — convert C arrays to C++ vectors
 * ============================================================ */

static std::vector<std::string> toStringVec(const char* const* arr, int count) {
    std::vector<std::string> v;
    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.emplace_back(arr[i]);
    }
    return v;
}

static std::vector<TSDataType::TSDataType> toTypeVec(const TSDataType_C* arr, int count) {
    std::vector<TSDataType::TSDataType> v;
    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.push_back(static_cast<TSDataType::TSDataType>(arr[i]));
    }
    return v;
}

static std::vector<TSEncoding::TSEncoding> toEncodingVec(const TSEncoding_C* arr, int count) {
    std::vector<TSEncoding::TSEncoding> v;
    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.push_back(static_cast<TSEncoding::TSEncoding>(arr[i]));
    }
    return v;
}

static std::vector<CompressionType::CompressionType> toCompressionVec(const TSCompressionType_C* arr, int count) {
    std::vector<CompressionType::CompressionType> v;
    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.push_back(static_cast<CompressionType::CompressionType>(arr[i]));
    }
    return v;
}

static std::map<std::string, std::string> toStringMap(int count, const char* const* keys, const char* const* values) {
    std::map<std::string, std::string> m;
    for (int i = 0; i < count; i++) {
        m[keys[i]] = values[i];
    }
    return m;
}

/**
 * Convert C typed values (void* const* values, TSDataType_C* types, int count)
 * to C++ vector<char*> that Session expects.
 * The caller must free the returned char* pointers using freeCharPtrVec().
 */
static std::vector<char*> toCharPtrVec(const TSDataType_C* types, const void* const* values, int count) {
    std::vector<char*> result(count);
    for (int i = 0; i < count; i++) {
        switch (types[i]) {
            case TS_TYPE_BOOLEAN: {
                bool* p = new bool(*static_cast<const bool*>(values[i]));
                result[i] = reinterpret_cast<char*>(p);
                break;
            }
            case TS_TYPE_INT32: {
                int32_t* p = new int32_t(*static_cast<const int32_t*>(values[i]));
                result[i] = reinterpret_cast<char*>(p);
                break;
            }
            case TS_TYPE_INT64:
            case TS_TYPE_TIMESTAMP: {
                int64_t* p = new int64_t(*static_cast<const int64_t*>(values[i]));
                result[i] = reinterpret_cast<char*>(p);
                break;
            }
            case TS_TYPE_FLOAT: {
                float* p = new float(*static_cast<const float*>(values[i]));
                result[i] = reinterpret_cast<char*>(p);
                break;
            }
            case TS_TYPE_DOUBLE: {
                double* p = new double(*static_cast<const double*>(values[i]));
                result[i] = reinterpret_cast<char*>(p);
                break;
            }
            case TS_TYPE_TEXT:
            case TS_TYPE_STRING:
            case TS_TYPE_BLOB:
            default: {
                const char* src = static_cast<const char*>(values[i]);
                size_t len = strlen(src) + 1;
                char* p = new char[len];
                memcpy(p, src, len);
                result[i] = p;
                break;
            }
        }
    }
    return result;
}

static void freeCharPtrVec(std::vector<char*>& vec, const TSDataType_C* types, int count) {
    for (int i = 0; i < count; i++) {
        switch (types[i]) {
            case TS_TYPE_BOOLEAN:   delete reinterpret_cast<bool*>(vec[i]); break;
            case TS_TYPE_INT32:     delete reinterpret_cast<int32_t*>(vec[i]); break;
            case TS_TYPE_INT64:
            case TS_TYPE_TIMESTAMP: delete reinterpret_cast<int64_t*>(vec[i]); break;
            case TS_TYPE_FLOAT:     delete reinterpret_cast<float*>(vec[i]); break;
            case TS_TYPE_DOUBLE:    delete reinterpret_cast<double*>(vec[i]); break;
            default:                delete[] vec[i]; break;
        }
    }
}

/* ============================================================
 *  Session Lifecycle  —  Tree Model
 * ============================================================ */

extern "C" {

CSession* ts_session_new(const char* host, int rpcPort,
                          const char* username, const char* password) {
    clearError();
    try {
        auto cpp = std::make_shared<Session>(
            std::string(host), rpcPort,
            std::string(username), std::string(password));
        auto* cs = new CSession_();
        cs->cpp = std::move(cpp);
        return cs;
    } catch (const std::exception& e) {
        handleException(e);
        return nullptr;
    }
}

CSession* ts_session_new_with_zone(const char* host, int rpcPort,
                                    const char* username, const char* password,
                                    const char* zoneId, int fetchSize) {
    clearError();
    try {
        auto cpp = std::make_shared<Session>(
            std::string(host), rpcPort,
            std::string(username), std::string(password),
            std::string(zoneId), fetchSize);
        auto* cs = new CSession_();
        cs->cpp = std::move(cpp);
        return cs;
    } catch (const std::exception& e) {
        handleException(e);
        return nullptr;
    }
}

CSession* ts_session_new_multi_node(const char* const* nodeUrls, int urlCount,
                                     const char* username, const char* password) {
    clearError();
    try {
        auto urls = toStringVec(nodeUrls, urlCount);
        auto cpp = std::make_shared<Session>(urls, std::string(username), std::string(password));
        auto* cs = new CSession_();
        cs->cpp = std::move(cpp);
        return cs;
    } catch (const std::exception& e) {
        handleException(e);
        return nullptr;
    }
}

void ts_session_destroy(CSession* session) {
    delete session;
}

TsStatus ts_session_open(CSession* session) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        session->cpp->open();
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_open_with_compression(CSession* session, bool enableRPCCompression) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        session->cpp->open(enableRPCCompression);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_close(CSession* session) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        session->cpp->close();
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

/* ============================================================
 *  Session Lifecycle  —  Table Model
 * ============================================================ */

CTableSession* ts_table_session_new(const char* host, int rpcPort,
                                     const char* username, const char* password,
                                     const char* database) {
    clearError();
    try {
        std::unique_ptr<TableSessionBuilder> builder(new TableSessionBuilder());
        auto tableSession = builder->host(std::string(host))
            ->rpcPort(rpcPort)
            ->username(std::string(username))
            ->password(std::string(password))
            ->database(std::string(database ? database : ""))
            ->build();
        CTableSession_ tmp{};
        tmp.cpp = std::move(tableSession);
        auto* cts = new CTableSession_();
        cts->cpp = std::move(tmp.cpp);
        return cts;
    } catch (const std::exception& e) {
        handleException(e);
        return nullptr;
    }
}

CTableSession* ts_table_session_new_multi_node(const char* const* nodeUrls, int urlCount,
                                                const char* username, const char* password,
                                                const char* database) {
    clearError();
    try {
        auto urls = toStringVec(nodeUrls, urlCount);
        std::unique_ptr<TableSessionBuilder> builder(new TableSessionBuilder());
        auto tableSession = builder->nodeUrls(urls)
            ->username(std::string(username))
            ->password(std::string(password))
            ->database(std::string(database ? database : ""))
            ->build();
        CTableSession_ tmp{};
        tmp.cpp = std::move(tableSession);
        auto* cts = new CTableSession_();
        cts->cpp = std::move(tmp.cpp);
        return cts;
    } catch (const std::exception& e) {
        handleException(e);
        return nullptr;
    }
}

void ts_table_session_destroy(CTableSession* session) {
    delete session;
}

TsStatus ts_table_session_open(CTableSession* session) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        session->cpp->open();
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_table_session_close(CTableSession* session) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        session->cpp->close();
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

/* ============================================================
 *  Timezone
 * ============================================================ */

TsStatus ts_session_set_timezone(CSession* session, const char* zoneId) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        session->cpp->setTimeZone(std::string(zoneId));
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_get_timezone(CSession* session, char* buf, int bufLen) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    if (!buf || bufLen <= 0) return setError(TS_ERR_INVALID_PARAM, "invalid buffer");
    try {
        std::string tz = session->cpp->getTimeZone();
        if ((int)tz.size() >= bufLen) {
            return setError(TS_ERR_INVALID_PARAM, "buffer too small");
        }
        strncpy(buf, tz.c_str(), bufLen);
        buf[bufLen - 1] = '\0';
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

/* ============================================================
 *  Database Management  (Tree Model)
 * ============================================================ */

TsStatus ts_session_create_database(CSession* session, const char* database) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        session->cpp->createDatabase(std::string(database));
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_delete_database(CSession* session, const char* database) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        session->cpp->deleteDatabase(std::string(database));
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_delete_databases(CSession* session, const char* const* databases, int count) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        auto dbs = toStringVec(databases, count);
        session->cpp->deleteDatabases(dbs);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

/* ============================================================
 *  Timeseries Management  (Tree Model)
 * ============================================================ */

TsStatus ts_session_create_timeseries(CSession* session, const char* path,
                                       TSDataType_C dataType, TSEncoding_C encoding,
                                       TSCompressionType_C compressor) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        session->cpp->createTimeseries(
            std::string(path),
            static_cast<TSDataType::TSDataType>(dataType),
            static_cast<TSEncoding::TSEncoding>(encoding),
            static_cast<CompressionType::CompressionType>(compressor));
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_create_timeseries_ex(CSession* session, const char* path,
                                          TSDataType_C dataType, TSEncoding_C encoding,
                                          TSCompressionType_C compressor,
                                          int propsCount,
                                          const char* const* propKeys,
                                          const char* const* propValues,
                                          int tagsCount,
                                          const char* const* tagKeys,
                                          const char* const* tagValues,
                                          int attrsCount,
                                          const char* const* attrKeys,
                                          const char* const* attrValues,
                                          const char* measurementAlias) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        std::map<std::string, std::string> props = propsCount > 0 ? toStringMap(propsCount, propKeys, propValues) : std::map<std::string, std::string>();
        std::map<std::string, std::string> tags = tagsCount > 0 ? toStringMap(tagsCount, tagKeys, tagValues) : std::map<std::string, std::string>();
        std::map<std::string, std::string> attrs = attrsCount > 0 ? toStringMap(attrsCount, attrKeys, attrValues) : std::map<std::string, std::string>();
        session->cpp->createTimeseries(
            std::string(path),
            static_cast<TSDataType::TSDataType>(dataType),
            static_cast<TSEncoding::TSEncoding>(encoding),
            static_cast<CompressionType::CompressionType>(compressor),
            &props, &tags, &attrs,
            std::string(measurementAlias ? measurementAlias : ""));
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_create_multi_timeseries(CSession* session, int count,
                                             const char* const* paths,
                                             const TSDataType_C* dataTypes,
                                             const TSEncoding_C* encodings,
                                             const TSCompressionType_C* compressors) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        auto pathsVec = toStringVec(paths, count);
        auto typesVec = toTypeVec(dataTypes, count);
        auto encVec = toEncodingVec(encodings, count);
        auto compVec = toCompressionVec(compressors, count);
        session->cpp->createMultiTimeseries(
            pathsVec, typesVec, encVec, compVec,
            nullptr, nullptr, nullptr, nullptr);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_create_aligned_timeseries(CSession* session, const char* deviceId,
                                               int count,
                                               const char* const* measurements,
                                               const TSDataType_C* dataTypes,
                                               const TSEncoding_C* encodings,
                                               const TSCompressionType_C* compressors) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        auto measurementsVec = toStringVec(measurements, count);
        auto typesVec = toTypeVec(dataTypes, count);
        auto encVec = toEncodingVec(encodings, count);
        auto compVec = toCompressionVec(compressors, count);
        session->cpp->createAlignedTimeseries(
            std::string(deviceId), measurementsVec, typesVec, encVec, compVec);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_check_timeseries_exists(CSession* session, const char* path, bool* exists) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    if (!exists) return setError(TS_ERR_INVALID_PARAM, "exists pointer is null");
    try {
        *exists = session->cpp->checkTimeseriesExists(std::string(path));
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_delete_timeseries(CSession* session, const char* path) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        session->cpp->deleteTimeseries(std::string(path));
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_delete_timeseries_batch(CSession* session, const char* const* paths, int count) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        auto pathsVec = toStringVec(paths, count);
        session->cpp->deleteTimeseries(pathsVec);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

/* ============================================================
 *  Tablet Operations
 * ============================================================ */

CTablet* ts_tablet_new(const char* deviceId, int columnCount,
                        const char* const* columnNames,
                        const TSDataType_C* dataTypes,
                        int maxRowNumber) {
    try {
        std::vector<std::pair<std::string, TSDataType::TSDataType>> schemas;
        schemas.reserve(columnCount);
        for (int i = 0; i < columnCount; i++) {
            schemas.emplace_back(std::string(columnNames[i]),
                                 static_cast<TSDataType::TSDataType>(dataTypes[i]));
        }
        Tablet tablet(std::string(deviceId), schemas, maxRowNumber);
        auto* ct = new CTablet_();
        ct->cpp = std::move(tablet);
        return ct;
    } catch (const std::exception& e) {
        handleException(e);
        return nullptr;
    }
}

CTablet* ts_tablet_new_with_category(const char* deviceId, int columnCount,
                                      const char* const* columnNames,
                                      const TSDataType_C* dataTypes,
                                      const TSColumnCategory_C* columnCategories,
                                      int maxRowNumber) {
    try {
        std::vector<std::pair<std::string, TSDataType::TSDataType>> schemas;
        std::vector<ColumnCategory> colTypes;
        schemas.reserve(columnCount);
        colTypes.reserve(columnCount);
        for (int i = 0; i < columnCount; i++) {
            schemas.emplace_back(std::string(columnNames[i]),
                                 static_cast<TSDataType::TSDataType>(dataTypes[i]));
            colTypes.push_back(static_cast<ColumnCategory>(columnCategories[i]));
        }
        Tablet tablet(std::string(deviceId), schemas, colTypes, maxRowNumber);
        auto* ct = new CTablet_();
        ct->cpp = std::move(tablet);
        return ct;
    } catch (const std::exception& e) {
        handleException(e);
        return nullptr;
    }
}

void ts_tablet_destroy(CTablet* tablet) {
    delete tablet;
}

void ts_tablet_reset(CTablet* tablet) {
    if (tablet) {
        tablet->cpp.reset();
    }
}

int ts_tablet_get_row_count(CTablet* tablet) {
    if (!tablet) return 0;
    return static_cast<int>(tablet->cpp.rowSize);
}

TsStatus ts_tablet_set_row_count(CTablet* tablet, int rowCount) {
    clearError();
    if (!tablet) return setError(TS_ERR_NULL_PTR, "tablet is null");
    tablet->cpp.rowSize = rowCount;
    return TS_OK;
}

TsStatus ts_tablet_add_timestamp(CTablet* tablet, int rowIndex, int64_t timestamp) {
    clearError();
    if (!tablet) return setError(TS_ERR_NULL_PTR, "tablet is null");
    try {
        tablet->cpp.addTimestamp(static_cast<size_t>(rowIndex), timestamp);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_tablet_add_value_bool(CTablet* tablet, int colIndex, int rowIndex, bool value) {
    clearError();
    if (!tablet) return setError(TS_ERR_NULL_PTR, "tablet is null");
    try {
        tablet->cpp.addValue(static_cast<size_t>(colIndex), static_cast<size_t>(rowIndex), value);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_tablet_add_value_int32(CTablet* tablet, int colIndex, int rowIndex, int32_t value) {
    clearError();
    if (!tablet) return setError(TS_ERR_NULL_PTR, "tablet is null");
    try {
        tablet->cpp.addValue(static_cast<size_t>(colIndex), static_cast<size_t>(rowIndex), value);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_tablet_add_value_int64(CTablet* tablet, int colIndex, int rowIndex, int64_t value) {
    clearError();
    if (!tablet) return setError(TS_ERR_NULL_PTR, "tablet is null");
    try {
        tablet->cpp.addValue(static_cast<size_t>(colIndex), static_cast<size_t>(rowIndex), value);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_tablet_add_value_float(CTablet* tablet, int colIndex, int rowIndex, float value) {
    clearError();
    if (!tablet) return setError(TS_ERR_NULL_PTR, "tablet is null");
    try {
        tablet->cpp.addValue(static_cast<size_t>(colIndex), static_cast<size_t>(rowIndex), value);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_tablet_add_value_double(CTablet* tablet, int colIndex, int rowIndex, double value) {
    clearError();
    if (!tablet) return setError(TS_ERR_NULL_PTR, "tablet is null");
    try {
        tablet->cpp.addValue(static_cast<size_t>(colIndex), static_cast<size_t>(rowIndex), value);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_tablet_add_value_string(CTablet* tablet, int colIndex, int rowIndex, const char* value) {
    clearError();
    if (!tablet) return setError(TS_ERR_NULL_PTR, "tablet is null");
    try {
        std::string str(value);
        tablet->cpp.addValue(static_cast<size_t>(colIndex), static_cast<size_t>(rowIndex), str);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

/* ============================================================
 *  Data Insertion  —  Tree Model  (Record, string values)
 * ============================================================ */

TsStatus ts_session_insert_record_str(CSession* session, const char* deviceId,
                                       int64_t time, int count,
                                       const char* const* measurements,
                                       const char* const* values) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        auto measurementsVec = toStringVec(measurements, count);
        auto valuesVec = toStringVec(values, count);
        session->cpp->insertRecord(std::string(deviceId), time, measurementsVec, valuesVec);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_insert_record(CSession* session, const char* deviceId,
                                   int64_t time, int count,
                                   const char* const* measurements,
                                   const TSDataType_C* types,
                                   const void* const* values) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        auto measurementsVec = toStringVec(measurements, count);
        auto typesVec = toTypeVec(types, count);
        auto charVec = toCharPtrVec(types, values, count);
        session->cpp->insertRecord(std::string(deviceId), time, measurementsVec, typesVec, charVec);
        freeCharPtrVec(charVec, types, count);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_insert_aligned_record_str(CSession* session, const char* deviceId,
                                               int64_t time, int count,
                                               const char* const* measurements,
                                               const char* const* values) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        auto measurementsVec = toStringVec(measurements, count);
        auto valuesVec = toStringVec(values, count);
        session->cpp->insertAlignedRecord(std::string(deviceId), time, measurementsVec, valuesVec);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_insert_aligned_record(CSession* session, const char* deviceId,
                                           int64_t time, int count,
                                           const char* const* measurements,
                                           const TSDataType_C* types,
                                           const void* const* values) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        auto measurementsVec = toStringVec(measurements, count);
        auto typesVec = toTypeVec(types, count);
        auto charVec = toCharPtrVec(types, values, count);
        session->cpp->insertAlignedRecord(std::string(deviceId), time, measurementsVec, typesVec, charVec);
        freeCharPtrVec(charVec, types, count);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

/* ============================================================
 *  Data Insertion  —  Batch, multiple devices (string values)
 * ============================================================ */

TsStatus ts_session_insert_records_str(CSession* session, int deviceCount,
                                        const char* const* deviceIds,
                                        const int64_t* times,
                                        const int* measurementCounts,
                                        const char* const* const* measurementsList,
                                        const char* const* const* valuesList) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        auto devVec = toStringVec(deviceIds, deviceCount);
        std::vector<int64_t> timesVec(times, times + deviceCount);
        std::vector<std::vector<std::string>> mList, vList;
        mList.reserve(deviceCount);
        vList.reserve(deviceCount);
        for (int i = 0; i < deviceCount; i++) {
            mList.push_back(toStringVec(measurementsList[i], measurementCounts[i]));
            vList.push_back(toStringVec(valuesList[i], measurementCounts[i]));
        }
        session->cpp->insertRecords(devVec, timesVec, mList, vList);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_insert_aligned_records_str(CSession* session, int deviceCount,
                                                const char* const* deviceIds,
                                                const int64_t* times,
                                                const int* measurementCounts,
                                                const char* const* const* measurementsList,
                                                const char* const* const* valuesList) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        auto devVec = toStringVec(deviceIds, deviceCount);
        std::vector<int64_t> timesVec(times, times + deviceCount);
        std::vector<std::vector<std::string>> mList, vList;
        mList.reserve(deviceCount);
        vList.reserve(deviceCount);
        for (int i = 0; i < deviceCount; i++) {
            mList.push_back(toStringVec(measurementsList[i], measurementCounts[i]));
            vList.push_back(toStringVec(valuesList[i], measurementCounts[i]));
        }
        session->cpp->insertAlignedRecords(devVec, timesVec, mList, vList);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

/* ============================================================
 *  Data Insertion  —  Batch, multiple devices (typed values)
 * ============================================================ */

TsStatus ts_session_insert_records(CSession* session, int deviceCount,
                                    const char* const* deviceIds,
                                    const int64_t* times,
                                    const int* measurementCounts,
                                    const char* const* const* measurementsList,
                                    const TSDataType_C* const* typesList,
                                    const void* const* const* valuesList) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        auto devVec = toStringVec(deviceIds, deviceCount);
        std::vector<int64_t> timesVec(times, times + deviceCount);
        std::vector<std::vector<std::string>> mList;
        std::vector<std::vector<TSDataType::TSDataType>> tList;
        std::vector<std::vector<char*>> vList;
        mList.reserve(deviceCount);
        tList.reserve(deviceCount);
        vList.reserve(deviceCount);
        for (int i = 0; i < deviceCount; i++) {
            mList.push_back(toStringVec(measurementsList[i], measurementCounts[i]));
            tList.push_back(toTypeVec(typesList[i], measurementCounts[i]));
            vList.push_back(toCharPtrVec(typesList[i], valuesList[i], measurementCounts[i]));
        }
        session->cpp->insertRecords(devVec, timesVec, mList, tList, vList);
        for (int i = 0; i < deviceCount; i++) {
            freeCharPtrVec(vList[i], typesList[i], measurementCounts[i]);
        }
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_insert_aligned_records(CSession* session, int deviceCount,
                                            const char* const* deviceIds,
                                            const int64_t* times,
                                            const int* measurementCounts,
                                            const char* const* const* measurementsList,
                                            const TSDataType_C* const* typesList,
                                            const void* const* const* valuesList) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        auto devVec = toStringVec(deviceIds, deviceCount);
        std::vector<int64_t> timesVec(times, times + deviceCount);
        std::vector<std::vector<std::string>> mList;
        std::vector<std::vector<TSDataType::TSDataType>> tList;
        std::vector<std::vector<char*>> vList;
        mList.reserve(deviceCount);
        tList.reserve(deviceCount);
        vList.reserve(deviceCount);
        for (int i = 0; i < deviceCount; i++) {
            mList.push_back(toStringVec(measurementsList[i], measurementCounts[i]));
            tList.push_back(toTypeVec(typesList[i], measurementCounts[i]));
            vList.push_back(toCharPtrVec(typesList[i], valuesList[i], measurementCounts[i]));
        }
        session->cpp->insertAlignedRecords(devVec, timesVec, mList, tList, vList);
        for (int i = 0; i < deviceCount; i++) {
            freeCharPtrVec(vList[i], typesList[i], measurementCounts[i]);
        }
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

/* ============================================================
 *  Data Insertion  —  Batch, single device (typed values)
 * ============================================================ */

TsStatus ts_session_insert_records_of_one_device(CSession* session, const char* deviceId,
                                                  int rowCount, const int64_t* times,
                                                  const int* measurementCounts,
                                                  const char* const* const* measurementsList,
                                                  const TSDataType_C* const* typesList,
                                                  const void* const* const* valuesList,
                                                  bool sorted) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        std::vector<int64_t> timesVec(times, times + rowCount);
        std::vector<std::vector<std::string>> mList;
        std::vector<std::vector<TSDataType::TSDataType>> tList;
        std::vector<std::vector<char*>> vList;
        mList.reserve(rowCount);
        tList.reserve(rowCount);
        vList.reserve(rowCount);
        for (int i = 0; i < rowCount; i++) {
            mList.push_back(toStringVec(measurementsList[i], measurementCounts[i]));
            tList.push_back(toTypeVec(typesList[i], measurementCounts[i]));
            vList.push_back(toCharPtrVec(typesList[i], valuesList[i], measurementCounts[i]));
        }
        session->cpp->insertRecordsOfOneDevice(std::string(deviceId), timesVec, mList, tList, vList, sorted);
        for (int i = 0; i < rowCount; i++) {
            freeCharPtrVec(vList[i], typesList[i], measurementCounts[i]);
        }
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_insert_aligned_records_of_one_device(CSession* session, const char* deviceId,
                                                          int rowCount, const int64_t* times,
                                                          const int* measurementCounts,
                                                          const char* const* const* measurementsList,
                                                          const TSDataType_C* const* typesList,
                                                          const void* const* const* valuesList,
                                                          bool sorted) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        std::vector<int64_t> timesVec(times, times + rowCount);
        std::vector<std::vector<std::string>> mList;
        std::vector<std::vector<TSDataType::TSDataType>> tList;
        std::vector<std::vector<char*>> vList;
        mList.reserve(rowCount);
        tList.reserve(rowCount);
        vList.reserve(rowCount);
        for (int i = 0; i < rowCount; i++) {
            mList.push_back(toStringVec(measurementsList[i], measurementCounts[i]));
            tList.push_back(toTypeVec(typesList[i], measurementCounts[i]));
            vList.push_back(toCharPtrVec(typesList[i], valuesList[i], measurementCounts[i]));
        }
        session->cpp->insertAlignedRecordsOfOneDevice(std::string(deviceId), timesVec, mList, tList, vList, sorted);
        for (int i = 0; i < rowCount; i++) {
            freeCharPtrVec(vList[i], typesList[i], measurementCounts[i]);
        }
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

/* ============================================================
 *  Data Insertion  —  Tree Model  (Tablet)
 * ============================================================ */

TsStatus ts_session_insert_tablet(CSession* session, CTablet* tablet, bool sorted) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    if (!tablet) return setError(TS_ERR_NULL_PTR, "tablet is null");
    try {
        session->cpp->insertTablet(tablet->cpp, sorted);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_insert_aligned_tablet(CSession* session, CTablet* tablet, bool sorted) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    if (!tablet) return setError(TS_ERR_NULL_PTR, "tablet is null");
    try {
        session->cpp->insertAlignedTablet(tablet->cpp, sorted);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_insert_tablets(CSession* session, int tabletCount,
                                    const char* const* deviceIds,
                                    CTablet** tablets, bool sorted) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        std::unordered_map<std::string, Tablet*> tabletMap;
        for (int i = 0; i < tabletCount; i++) {
            tabletMap[std::string(deviceIds[i])] = &(tablets[i]->cpp);
        }
        session->cpp->insertTablets(tabletMap, sorted);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_insert_aligned_tablets(CSession* session, int tabletCount,
                                            const char* const* deviceIds,
                                            CTablet** tablets, bool sorted) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        std::unordered_map<std::string, Tablet*> tabletMap;
        for (int i = 0; i < tabletCount; i++) {
            tabletMap[std::string(deviceIds[i])] = &(tablets[i]->cpp);
        }
        session->cpp->insertAlignedTablets(tabletMap, sorted);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

/* ============================================================
 *  Data Insertion  —  Table Model  (Tablet)
 * ============================================================ */

TsStatus ts_table_session_insert(CTableSession* session, CTablet* tablet) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    if (!tablet) return setError(TS_ERR_NULL_PTR, "tablet is null");
    try {
        session->cpp->insert(tablet->cpp);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

/* ============================================================
 *  Query  —  Tree Model
 * ============================================================ */

TsStatus ts_session_execute_query(CSession* session, const char* sql,
                                   CSessionDataSet** dataSet) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    if (!dataSet) return setError(TS_ERR_INVALID_PARAM, "dataSet pointer is null");
    try {
        auto ds = session->cpp->executeQueryStatement(std::string(sql));
        CSessionDataSet_ tmp{};
        tmp.cpp = std::move(ds);
        auto* cds = new CSessionDataSet_();
        cds->cpp = std::move(tmp.cpp);
        *dataSet = cds;
        return TS_OK;
    } catch (const std::exception& e) {
        *dataSet = nullptr;
        return handleException(e);
    }
}

TsStatus ts_session_execute_query_with_timeout(CSession* session, const char* sql,
                                                int64_t timeoutInMs,
                                                CSessionDataSet** dataSet) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    if (!dataSet) return setError(TS_ERR_INVALID_PARAM, "dataSet pointer is null");
    try {
        auto ds = session->cpp->executeQueryStatement(std::string(sql), timeoutInMs);
        CSessionDataSet_ tmp{};
        tmp.cpp = std::move(ds);
        auto* cds = new CSessionDataSet_();
        cds->cpp = std::move(tmp.cpp);
        *dataSet = cds;
        return TS_OK;
    } catch (const std::exception& e) {
        *dataSet = nullptr;
        return handleException(e);
    }
}

TsStatus ts_session_execute_non_query(CSession* session, const char* sql) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        session->cpp->executeNonQueryStatement(std::string(sql));
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_execute_raw_data_query(CSession* session,
                                            int pathCount, const char* const* paths,
                                            int64_t startTime, int64_t endTime,
                                            CSessionDataSet** dataSet) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    if (!dataSet) return setError(TS_ERR_INVALID_PARAM, "dataSet pointer is null");
    try {
        auto pathsVec = toStringVec(paths, pathCount);
        auto ds = session->cpp->executeRawDataQuery(pathsVec, startTime, endTime);
        CSessionDataSet_ tmp{};
        tmp.cpp = std::move(ds);
        auto* cds = new CSessionDataSet_();
        cds->cpp = std::move(tmp.cpp);
        *dataSet = cds;
        return TS_OK;
    } catch (const std::exception& e) {
        *dataSet = nullptr;
        return handleException(e);
    }
}

TsStatus ts_session_execute_last_data_query(CSession* session,
                                             int pathCount, const char* const* paths,
                                             CSessionDataSet** dataSet) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    if (!dataSet) return setError(TS_ERR_INVALID_PARAM, "dataSet pointer is null");
    try {
        auto pathsVec = toStringVec(paths, pathCount);
        auto ds = session->cpp->executeLastDataQuery(pathsVec);
        CSessionDataSet_ tmp{};
        tmp.cpp = std::move(ds);
        auto* cds = new CSessionDataSet_();
        cds->cpp = std::move(tmp.cpp);
        *dataSet = cds;
        return TS_OK;
    } catch (const std::exception& e) {
        *dataSet = nullptr;
        return handleException(e);
    }
}

TsStatus ts_session_execute_last_data_query_with_time(CSession* session,
                                                       int pathCount, const char* const* paths,
                                                       int64_t lastTime,
                                                       CSessionDataSet** dataSet) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    if (!dataSet) return setError(TS_ERR_INVALID_PARAM, "dataSet pointer is null");
    try {
        auto pathsVec = toStringVec(paths, pathCount);
        auto ds = session->cpp->executeLastDataQuery(pathsVec, lastTime);
        CSessionDataSet_ tmp{};
        tmp.cpp = std::move(ds);
        auto* cds = new CSessionDataSet_();
        cds->cpp = std::move(tmp.cpp);
        *dataSet = cds;
        return TS_OK;
    } catch (const std::exception& e) {
        *dataSet = nullptr;
        return handleException(e);
    }
}

/* ============================================================
 *  Query  —  Table Model
 * ============================================================ */

TsStatus ts_table_session_execute_query(CTableSession* session, const char* sql,
                                         CSessionDataSet** dataSet) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    if (!dataSet) return setError(TS_ERR_INVALID_PARAM, "dataSet pointer is null");
    try {
        auto ds = session->cpp->executeQueryStatement(std::string(sql));
        CSessionDataSet_ tmp{};
        tmp.cpp = std::move(ds);
        auto* cds = new CSessionDataSet_();
        cds->cpp = std::move(tmp.cpp);
        *dataSet = cds;
        return TS_OK;
    } catch (const std::exception& e) {
        *dataSet = nullptr;
        return handleException(e);
    }
}

TsStatus ts_table_session_execute_query_with_timeout(CTableSession* session, const char* sql,
                                                      int64_t timeoutInMs,
                                                      CSessionDataSet** dataSet) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    if (!dataSet) return setError(TS_ERR_INVALID_PARAM, "dataSet pointer is null");
    try {
        auto ds = session->cpp->executeQueryStatement(std::string(sql), timeoutInMs);
        CSessionDataSet_ tmp{};
        tmp.cpp = std::move(ds);
        auto* cds = new CSessionDataSet_();
        cds->cpp = std::move(tmp.cpp);
        *dataSet = cds;
        return TS_OK;
    } catch (const std::exception& e) {
        *dataSet = nullptr;
        return handleException(e);
    }
}

TsStatus ts_table_session_execute_non_query(CTableSession* session, const char* sql) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        session->cpp->executeNonQueryStatement(std::string(sql));
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

/* ============================================================
 *  SessionDataSet & RowRecord  —  Result Iteration
 * ============================================================ */

void ts_dataset_destroy(CSessionDataSet* dataSet) {
    if (dataSet) {
        if (dataSet->cpp) {
            dataSet->cpp->closeOperationHandle();
        }
        delete dataSet;
    }
}

bool ts_dataset_has_next(CSessionDataSet* dataSet) {
    clearError();
    if (!dataSet) {
        (void)setError(TS_ERR_NULL_PTR, "dataSet is null");
        return false;
    }
    if (!dataSet->cpp) {
        (void)setError(TS_ERR_NULL_PTR, "dataSet is not initialized");
        return false;
    }
    try {
        return dataSet->cpp->hasNext();
    } catch (const std::exception& e) {
        (void)handleException(e);
        return false;
    } catch (...) {
        (void)setError(TS_ERR_UNKNOWN, "non-standard exception");
        return false;
    }
}

CRowRecord* ts_dataset_next(CSessionDataSet* dataSet) {
    clearError();
    if (!dataSet) {
        (void)setError(TS_ERR_NULL_PTR, "dataSet is null");
        return nullptr;
    }
    if (!dataSet->cpp) {
        (void)setError(TS_ERR_NULL_PTR, "dataSet is not initialized");
        return nullptr;
    }
    try {
        auto row = dataSet->cpp->next();
        if (!row) return nullptr;
        CRowRecord_ tmp{};
        tmp.cpp = std::move(row);
        auto* crr = new CRowRecord_();
        crr->cpp = std::move(tmp.cpp);
        return crr;
    } catch (const std::exception& e) {
        (void)handleException(e);
        return nullptr;
    } catch (...) {
        (void)setError(TS_ERR_UNKNOWN, "non-standard exception");
        return nullptr;
    }
}

int ts_dataset_get_column_count(CSessionDataSet* dataSet) {
    if (!dataSet || !dataSet->cpp) return 0;
    return static_cast<int>(dataSet->cpp->getColumnNames().size());
}

static thread_local std::string g_colNameBuf;

const char* ts_dataset_get_column_name(CSessionDataSet* dataSet, int index) {
    if (!dataSet || !dataSet->cpp) return "";
    const auto& names = dataSet->cpp->getColumnNames();
    if (index < 0 || index >= (int)names.size()) return "";
    g_colNameBuf = names[index];
    return g_colNameBuf.c_str();
}

static thread_local std::string g_colTypeBuf;

const char* ts_dataset_get_column_type(CSessionDataSet* dataSet, int index) {
    if (!dataSet || !dataSet->cpp) return "";
    const auto& types = dataSet->cpp->getColumnTypeList();
    if (index < 0 || index >= (int)types.size()) return "";
    g_colTypeBuf = types[index];
    return g_colTypeBuf.c_str();
}

void ts_dataset_set_fetch_size(CSessionDataSet* dataSet, int fetchSize) {
    if (dataSet && dataSet->cpp) {
        dataSet->cpp->setFetchSize(fetchSize);
    }
}

void ts_row_record_destroy(CRowRecord* record) {
    delete record;
}

int64_t ts_row_record_get_timestamp(CRowRecord* record) {
    if (!record || !record->cpp) return -1;
    return record->cpp->timestamp;
}

int ts_row_record_get_field_count(CRowRecord* record) {
    if (!record || !record->cpp) return 0;
    return static_cast<int>(record->cpp->fields.size());
}

bool ts_row_record_is_null(CRowRecord* record, int index) {
    if (!record || !record->cpp) return true;
    if (index < 0 || index >= (int)record->cpp->fields.size()) return true;
    return record->cpp->fields[index].isNull();
}

bool ts_row_record_get_bool(CRowRecord* record, int index) {
    if (!record || !record->cpp) return false;
    if (index < 0 || index >= (int)record->cpp->fields.size()) return false;
    const Field& f = record->cpp->fields[index];
    return f.boolV.is_initialized() ? f.boolV.value() : false;
}

int32_t ts_row_record_get_int32(CRowRecord* record, int index) {
    if (!record || !record->cpp) return 0;
    if (index < 0 || index >= (int)record->cpp->fields.size()) return 0;
    const Field& f = record->cpp->fields[index];
    return f.intV.is_initialized() ? f.intV.value() : 0;
}

int64_t ts_row_record_get_int64(CRowRecord* record, int index) {
    if (!record || !record->cpp) return 0;
    if (index < 0 || index >= (int)record->cpp->fields.size()) return 0;
    const Field& f = record->cpp->fields[index];
    return f.longV.is_initialized() ? f.longV.value() : 0;
}

float ts_row_record_get_float(CRowRecord* record, int index) {
    if (!record || !record->cpp) return 0.0f;
    if (index < 0 || index >= (int)record->cpp->fields.size()) return 0.0f;
    const Field& f = record->cpp->fields[index];
    return f.floatV.is_initialized() ? f.floatV.value() : 0.0f;
}

double ts_row_record_get_double(CRowRecord* record, int index) {
    if (!record || !record->cpp) return 0.0;
    if (index < 0 || index >= (int)record->cpp->fields.size()) return 0.0;
    const Field& f = record->cpp->fields[index];
    return f.doubleV.is_initialized() ? f.doubleV.value() : 0.0;
}

static thread_local std::string g_stringBuf;

const char* ts_row_record_get_string(CRowRecord* record, int index) {
    if (!record || !record->cpp) return "";
    if (index < 0 || index >= (int)record->cpp->fields.size()) return "";
    const Field& f = record->cpp->fields[index];
    if (f.stringV.is_initialized()) {
        g_stringBuf = f.stringV.value();
        return g_stringBuf.c_str();
    }
    return "";
}

TSDataType_C ts_row_record_get_data_type(CRowRecord* record, int index) {
    if (!record || !record->cpp) return TS_TYPE_INVALID;
    if (index < 0 || index >= (int)record->cpp->fields.size()) return TS_TYPE_INVALID;
    return static_cast<TSDataType_C>(record->cpp->fields[index].dataType);
}

/* ============================================================
 *  Data Deletion  (Tree Model)
 * ============================================================ */

TsStatus ts_session_delete_data(CSession* session, const char* path, int64_t endTime) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        session->cpp->deleteData(std::string(path), endTime);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_delete_data_batch(CSession* session, int pathCount,
                                       const char* const* paths, int64_t endTime) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        auto pathsVec = toStringVec(paths, pathCount);
        session->cpp->deleteData(pathsVec, endTime);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

TsStatus ts_session_delete_data_range(CSession* session, int pathCount,
                                       const char* const* paths,
                                       int64_t startTime, int64_t endTime) {
    clearError();
    if (!session) return setError(TS_ERR_NULL_PTR, "session is null");
    try {
        auto pathsVec = toStringVec(paths, pathCount);
        session->cpp->deleteData(pathsVec, startTime, endTime);
        return TS_OK;
    } catch (const std::exception& e) {
        return handleException(e);
    }
}

}  /* extern "C" */
