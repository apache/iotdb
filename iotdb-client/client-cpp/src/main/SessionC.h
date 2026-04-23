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

#ifndef IOTDB_SESSION_C_H
#define IOTDB_SESSION_C_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 *  Error Handling
 * ============================================================ */

typedef int64_t TsStatus;

#define TS_OK                   0
#define TS_ERR_CONNECTION      -1
#define TS_ERR_EXECUTION       -2
#define TS_ERR_INVALID_PARAM   -3
#define TS_ERR_NULL_PTR        -4
#define TS_ERR_UNKNOWN         -99

/**
 * Returns the error message from the last failed C API call on the current thread.
 * The returned pointer is valid until the next C API call on the same thread.
 */
const char* ts_get_last_error(void);

/* ============================================================
 *  Opaque Handle Types
 * ============================================================ */

typedef struct CSession_        CSession;
typedef struct CTableSession_   CTableSession;
typedef struct CTablet_         CTablet;
typedef struct CSessionDataSet_ CSessionDataSet;
typedef struct CRowRecord_      CRowRecord;

/* ============================================================
 *  Enums  (values match C++ TSDataType / TSEncoding / CompressionType)
 * ============================================================ */

typedef enum {
    TS_TYPE_BOOLEAN   = 0,
    TS_TYPE_INT32     = 1,
    TS_TYPE_INT64     = 2,
    TS_TYPE_FLOAT     = 3,
    TS_TYPE_DOUBLE    = 4,
    TS_TYPE_TEXT      = 5,
    TS_TYPE_TIMESTAMP = 8,
    TS_TYPE_DATE      = 9,
    TS_TYPE_BLOB      = 10,
    TS_TYPE_STRING    = 11,
    /** Not a server type; used for invalid arguments / error paths in the C API. */
    TS_TYPE_INVALID   = 255
} TSDataType_C;

typedef enum {
    TS_ENCODING_PLAIN      = 0,
    TS_ENCODING_DICTIONARY = 1,
    TS_ENCODING_RLE        = 2,
    TS_ENCODING_DIFF       = 3,
    TS_ENCODING_TS_2DIFF   = 4,
    TS_ENCODING_BITMAP     = 5,
    TS_ENCODING_GORILLA_V1 = 6,
    TS_ENCODING_REGULAR    = 7,
    TS_ENCODING_GORILLA    = 8,
    TS_ENCODING_ZIGZAG     = 9,
    TS_ENCODING_FREQ       = 10
} TSEncoding_C;

typedef enum {
    TS_COMPRESSION_UNCOMPRESSED = 0,
    TS_COMPRESSION_SNAPPY       = 1,
    TS_COMPRESSION_GZIP         = 2,
    TS_COMPRESSION_LZO          = 3,
    TS_COMPRESSION_SDT          = 4,
    TS_COMPRESSION_PAA          = 5,
    TS_COMPRESSION_PLA          = 6,
    TS_COMPRESSION_LZ4          = 7,
    TS_COMPRESSION_ZSTD         = 8,
    TS_COMPRESSION_LZMA2        = 9
} TSCompressionType_C;

typedef enum {
    TS_COL_TAG       = 0,
    TS_COL_FIELD     = 1,
    TS_COL_ATTRIBUTE = 2
} TSColumnCategory_C;

/* ============================================================
 *  Session Lifecycle  —  Tree Model
 * ============================================================ */

CSession* ts_session_new(const char* host, int rpcPort,
                          const char* username, const char* password);

CSession* ts_session_new_with_zone(const char* host, int rpcPort,
                                    const char* username, const char* password,
                                    const char* zoneId, int fetchSize);

CSession* ts_session_new_multi_node(const char* const* nodeUrls, int urlCount,
                                     const char* username, const char* password);

void ts_session_destroy(CSession* session);

TsStatus ts_session_open(CSession* session);

TsStatus ts_session_open_with_compression(CSession* session, bool enableRPCCompression);

TsStatus ts_session_close(CSession* session);

/* ============================================================
 *  Session Lifecycle  —  Table Model
 * ============================================================ */

CTableSession* ts_table_session_new(const char* host, int rpcPort,
                                     const char* username, const char* password,
                                     const char* database);

CTableSession* ts_table_session_new_multi_node(const char* const* nodeUrls, int urlCount,
                                                const char* username, const char* password,
                                                const char* database);

void ts_table_session_destroy(CTableSession* session);

TsStatus ts_table_session_open(CTableSession* session);

TsStatus ts_table_session_close(CTableSession* session);

/* ============================================================
 *  Timezone
 * ============================================================ */

TsStatus ts_session_set_timezone(CSession* session, const char* zoneId);

TsStatus ts_session_get_timezone(CSession* session, char* buf, int bufLen);

/* ============================================================
 *  Database Management  (Tree Model)
 * ============================================================ */

TsStatus ts_session_create_database(CSession* session, const char* database);

TsStatus ts_session_delete_database(CSession* session, const char* database);

TsStatus ts_session_delete_databases(CSession* session, const char* const* databases, int count);

/* ============================================================
 *  Timeseries Management  (Tree Model)
 * ============================================================ */

TsStatus ts_session_create_timeseries(CSession* session, const char* path,
                                       TSDataType_C dataType, TSEncoding_C encoding,
                                       TSCompressionType_C compressor);

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
                                          const char* measurementAlias);

TsStatus ts_session_create_multi_timeseries(CSession* session, int count,
                                             const char* const* paths,
                                             const TSDataType_C* dataTypes,
                                             const TSEncoding_C* encodings,
                                             const TSCompressionType_C* compressors);

TsStatus ts_session_create_aligned_timeseries(CSession* session, const char* deviceId,
                                               int count,
                                               const char* const* measurements,
                                               const TSDataType_C* dataTypes,
                                               const TSEncoding_C* encodings,
                                               const TSCompressionType_C* compressors);

TsStatus ts_session_check_timeseries_exists(CSession* session, const char* path, bool* exists);

TsStatus ts_session_delete_timeseries(CSession* session, const char* path);

TsStatus ts_session_delete_timeseries_batch(CSession* session, const char* const* paths, int count);

/* ============================================================
 *  Tablet Operations
 * ============================================================ */

CTablet* ts_tablet_new(const char* deviceId, int columnCount,
                        const char* const* columnNames,
                        const TSDataType_C* dataTypes,
                        int maxRowNumber);

CTablet* ts_tablet_new_with_category(const char* deviceId, int columnCount,
                                      const char* const* columnNames,
                                      const TSDataType_C* dataTypes,
                                      const TSColumnCategory_C* columnCategories,
                                      int maxRowNumber);

void ts_tablet_destroy(CTablet* tablet);

void ts_tablet_reset(CTablet* tablet);

int ts_tablet_get_row_count(CTablet* tablet);

TsStatus ts_tablet_set_row_count(CTablet* tablet, int rowCount);

TsStatus ts_tablet_add_timestamp(CTablet* tablet, int rowIndex, int64_t timestamp);

TsStatus ts_tablet_add_value_bool(CTablet* tablet, int colIndex, int rowIndex, bool value);

TsStatus ts_tablet_add_value_int32(CTablet* tablet, int colIndex, int rowIndex, int32_t value);

TsStatus ts_tablet_add_value_int64(CTablet* tablet, int colIndex, int rowIndex, int64_t value);

TsStatus ts_tablet_add_value_float(CTablet* tablet, int colIndex, int rowIndex, float value);

TsStatus ts_tablet_add_value_double(CTablet* tablet, int colIndex, int rowIndex, double value);

TsStatus ts_tablet_add_value_string(CTablet* tablet, int colIndex, int rowIndex, const char* value);

/* ============================================================
 *  Data Insertion  —  Tree Model  (Record)
 * ============================================================ */

TsStatus ts_session_insert_record_str(CSession* session, const char* deviceId,
                                       int64_t time, int count,
                                       const char* const* measurements,
                                       const char* const* values);

TsStatus ts_session_insert_record(CSession* session, const char* deviceId,
                                   int64_t time, int count,
                                   const char* const* measurements,
                                   const TSDataType_C* types,
                                   const void* const* values);

TsStatus ts_session_insert_aligned_record_str(CSession* session, const char* deviceId,
                                               int64_t time, int count,
                                               const char* const* measurements,
                                               const char* const* values);

TsStatus ts_session_insert_aligned_record(CSession* session, const char* deviceId,
                                           int64_t time, int count,
                                           const char* const* measurements,
                                           const TSDataType_C* types,
                                           const void* const* values);

/* Batch insert — multiple devices (string values) */
TsStatus ts_session_insert_records_str(CSession* session, int deviceCount,
                                        const char* const* deviceIds,
                                        const int64_t* times,
                                        const int* measurementCounts,
                                        const char* const* const* measurementsList,
                                        const char* const* const* valuesList);

TsStatus ts_session_insert_aligned_records_str(CSession* session, int deviceCount,
                                                const char* const* deviceIds,
                                                const int64_t* times,
                                                const int* measurementCounts,
                                                const char* const* const* measurementsList,
                                                const char* const* const* valuesList);

/* Batch insert — multiple devices (typed values) */
TsStatus ts_session_insert_records(CSession* session, int deviceCount,
                                    const char* const* deviceIds,
                                    const int64_t* times,
                                    const int* measurementCounts,
                                    const char* const* const* measurementsList,
                                    const TSDataType_C* const* typesList,
                                    const void* const* const* valuesList);

TsStatus ts_session_insert_aligned_records(CSession* session, int deviceCount,
                                            const char* const* deviceIds,
                                            const int64_t* times,
                                            const int* measurementCounts,
                                            const char* const* const* measurementsList,
                                            const TSDataType_C* const* typesList,
                                            const void* const* const* valuesList);

/* Batch insert — single device (typed values) */
TsStatus ts_session_insert_records_of_one_device(CSession* session, const char* deviceId,
                                                  int rowCount, const int64_t* times,
                                                  const int* measurementCounts,
                                                  const char* const* const* measurementsList,
                                                  const TSDataType_C* const* typesList,
                                                  const void* const* const* valuesList,
                                                  bool sorted);

TsStatus ts_session_insert_aligned_records_of_one_device(CSession* session, const char* deviceId,
                                                          int rowCount, const int64_t* times,
                                                          const int* measurementCounts,
                                                          const char* const* const* measurementsList,
                                                          const TSDataType_C* const* typesList,
                                                          const void* const* const* valuesList,
                                                          bool sorted);

/* ============================================================
 *  Data Insertion  —  Tree Model  (Tablet)
 * ============================================================ */

TsStatus ts_session_insert_tablet(CSession* session, CTablet* tablet, bool sorted);

TsStatus ts_session_insert_aligned_tablet(CSession* session, CTablet* tablet, bool sorted);

TsStatus ts_session_insert_tablets(CSession* session, int tabletCount,
                                    const char* const* deviceIds,
                                    CTablet** tablets, bool sorted);

TsStatus ts_session_insert_aligned_tablets(CSession* session, int tabletCount,
                                            const char* const* deviceIds,
                                            CTablet** tablets, bool sorted);

/* ============================================================
 *  Data Insertion  —  Table Model  (Tablet)
 * ============================================================ */

TsStatus ts_table_session_insert(CTableSession* session, CTablet* tablet);

/* ============================================================
 *  Query  —  Tree Model
 * ============================================================ */

TsStatus ts_session_execute_query(CSession* session, const char* sql,
                                   CSessionDataSet** dataSet);

TsStatus ts_session_execute_query_with_timeout(CSession* session, const char* sql,
                                                int64_t timeoutInMs,
                                                CSessionDataSet** dataSet);

TsStatus ts_session_execute_non_query(CSession* session, const char* sql);

TsStatus ts_session_execute_raw_data_query(CSession* session,
                                            int pathCount, const char* const* paths,
                                            int64_t startTime, int64_t endTime,
                                            CSessionDataSet** dataSet);

TsStatus ts_session_execute_last_data_query(CSession* session,
                                             int pathCount, const char* const* paths,
                                             CSessionDataSet** dataSet);

TsStatus ts_session_execute_last_data_query_with_time(CSession* session,
                                                       int pathCount, const char* const* paths,
                                                       int64_t lastTime,
                                                       CSessionDataSet** dataSet);

/* ============================================================
 *  Query  —  Table Model
 * ============================================================ */

TsStatus ts_table_session_execute_query(CTableSession* session, const char* sql,
                                         CSessionDataSet** dataSet);

TsStatus ts_table_session_execute_query_with_timeout(CTableSession* session, const char* sql,
                                                      int64_t timeoutInMs,
                                                      CSessionDataSet** dataSet);

TsStatus ts_table_session_execute_non_query(CTableSession* session, const char* sql);

/* ============================================================
 *  SessionDataSet & RowRecord  —  Result Iteration
 * ============================================================ */

void ts_dataset_destroy(CSessionDataSet* dataSet);

/** On failure (null handle, exception), see ts_get_last_error(). */
bool ts_dataset_has_next(CSessionDataSet* dataSet);

/** On failure (null handle, exception), see ts_get_last_error(); nullptr may also mean end of rows. */
CRowRecord* ts_dataset_next(CSessionDataSet* dataSet);

int ts_dataset_get_column_count(CSessionDataSet* dataSet);

const char* ts_dataset_get_column_name(CSessionDataSet* dataSet, int index);

const char* ts_dataset_get_column_type(CSessionDataSet* dataSet, int index);

void ts_dataset_set_fetch_size(CSessionDataSet* dataSet, int fetchSize);

void ts_row_record_destroy(CRowRecord* record);

int64_t ts_row_record_get_timestamp(CRowRecord* record);

int ts_row_record_get_field_count(CRowRecord* record);

bool ts_row_record_is_null(CRowRecord* record, int index);

bool ts_row_record_get_bool(CRowRecord* record, int index);

int32_t ts_row_record_get_int32(CRowRecord* record, int index);

int64_t ts_row_record_get_int64(CRowRecord* record, int index);

float ts_row_record_get_float(CRowRecord* record, int index);

double ts_row_record_get_double(CRowRecord* record, int index);

const char* ts_row_record_get_string(CRowRecord* record, int index);

/** Returns TS_TYPE_INVALID if record is null or index is out of range. */
TSDataType_C ts_row_record_get_data_type(CRowRecord* record, int index);

/* ============================================================
 *  Data Deletion  (Tree Model)
 * ============================================================ */

TsStatus ts_session_delete_data(CSession* session, const char* path, int64_t endTime);

TsStatus ts_session_delete_data_batch(CSession* session, int pathCount,
                                       const char* const* paths, int64_t endTime);

TsStatus ts_session_delete_data_range(CSession* session, int pathCount,
                                       const char* const* paths,
                                       int64_t startTime, int64_t endTime);

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif /* IOTDB_SESSION_C_H */
