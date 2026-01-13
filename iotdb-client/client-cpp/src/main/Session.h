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
#ifndef IOTDB_SESSION_H
#define IOTDB_SESSION_H

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <exception>
#include <iostream>
#include <algorithm>
#include <map>
#include <unordered_map>
#include <thread>
#include <stdexcept>
#include <cstdlib>
#include <future>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportException.h>
#include <thrift/transport/TBufferTransports.h>
#include "IClientRPCService.h"
#include "NodesSupplier.h"
#include "AbstractSessionBuilder.h"
#include "SessionConnection.h"
#include "SessionDataSet.h"
#include "DeviceID.h"
#include "Common.h"

//== For compatible with Windows OS ==
#ifndef LONG_LONG_MIN
#define LONG_LONG_MIN 0x8000000000000000
#endif

using namespace std;

using ::apache::thrift::protocol::TBinaryProtocol;
using ::apache::thrift::protocol::TCompactProtocol;
using ::apache::thrift::transport::TSocket;
using ::apache::thrift::transport::TTransport;
using ::apache::thrift::transport::TTransportException;
using ::apache::thrift::transport::TBufferedTransport;
using ::apache::thrift::transport::TFramedTransport;
using ::apache::thrift::TException;


template <typename T, typename Target>
void safe_cast(const T& value, Target& target) {
    /*
        Target	Allowed Source Types
        BOOLEAN	BOOLEAN
        INT32	INT32
        INT64	INT32 INT64
        FLOAT	INT32 FLOAT
        DOUBLE	INT32 INT64 FLOAT DOUBLE
        TEXT	TEXT
    */
    if (std::is_same<Target, T>::value) {
        target = *(Target*)&value;
    }
    else if (std::is_same<Target, string>::value && std::is_array<T>::value && std::is_same<
        char, typename std::remove_extent<T>::type>::value) {
        string tmp((const char*)&value);
        target = *(Target*)&tmp;
    }
    else if (std::is_same<Target, int64_t>::value && std::is_same<T, int32_t>::value) {
        int64_t tmp = *(int32_t*)&value;
        target = *(Target*)&tmp;
    }
    else if (std::is_same<Target, float>::value && std::is_same<T, int32_t>::value) {
        float tmp = *(int32_t*)&value;
        target = *(Target*)&tmp;
    }
    else if (std::is_same<Target, double>::value && std::is_same<T, int32_t>::value) {
        double tmp = *(int32_t*)&value;
        target = *(Target*)&tmp;
    }
    else if (std::is_same<Target, double>::value && std::is_same<T, int64_t>::value) {
        double tmp = *(int64_t*)&value;
        target = *(Target*)&tmp;
    }
    else if (std::is_same<Target, double>::value && std::is_same<T, float>::value) {
        double tmp = *(float*)&value;
        target = *(Target*)&tmp;
    }
    else {
        throw UnSupportedDataTypeException("Error: Parameter type " +
            std::string(typeid(T).name()) + " cannot be converted to DataType" +
            std::string(typeid(Target).name()));
    }
}

/*
 * A tablet data of one device, the tablet contains multiple measurements of this device that share
 * the same time column.
 *
 * for example:  device root.sg1.d1
 *
 * time, m1, m2, m3
 *    1,  1,  2,  3
 *    2,  1,  2,  3
 *    3,  1,  2,  3
 *
 * Notice: The tablet should not have empty cell
 *
 */
class Tablet {
private:
    static const int DEFAULT_ROW_SIZE = 1024;

    void createColumns();
    void deleteColumns();

public:
    std::string deviceId; // deviceId of this tablet
    std::vector<std::pair<std::string, TSDataType::TSDataType>> schemas;
    // the list of measurement schemas for creating the tablet
    std::map<std::string, size_t> schemaNameIndex; // the map of schema name to index
    std::vector<ColumnCategory> columnTypes; // the list of column types (used in table model)
    std::vector<int64_t> timestamps; // timestamps in this tablet
    std::vector<void*> values; // each object is a primitive type array, which represents values of one measurement
    std::vector<BitMap> bitMaps; // each bitmap represents the existence of each value in the current column
    size_t rowSize; //the number of rows to include in this tablet
    size_t maxRowNumber; // the maximum number of rows for this tablet
    bool isAligned; // whether this tablet store data of aligned timeseries or not
    std::vector<int> idColumnIndexes;

    Tablet() = default;

    /**
    * Return a tablet with default specified row number. This is the standard
    * constructor (all Tablet should be the same size).
    *
    * @param deviceId   the name of the device specified to be written in
    * @param timeseries the list of measurement schemas for creating the tablet
    */
    Tablet(const std::string& deviceId,
           const std::vector<std::pair<std::string, TSDataType::TSDataType>>& timeseries)
        : Tablet(deviceId, timeseries, DEFAULT_ROW_SIZE) {
    }

    Tablet(const std::string& deviceId,
           const std::vector<std::pair<std::string, TSDataType::TSDataType>>& timeseries,
           const std::vector<ColumnCategory>& columnTypes)
        : Tablet(deviceId, timeseries, columnTypes, DEFAULT_ROW_SIZE) {
    }

    /**
     * Return a tablet with the specified number of rows (maxBatchSize). Only
     * call this constructor directly for testing purposes. Tablet should normally
     * always be default size.
     *
     * @param deviceId     the name of the device specified to be written in
     * @param schemas   the list of measurement schemas for creating the row
     *                     batch
     * @param columnTypes the list of column types (used in table model)
     * @param maxRowNumber the maximum number of rows for this tablet
     */
    Tablet(const std::string& deviceId,
           const std::vector<std::pair<std::string, TSDataType::TSDataType>>& schemas,
           int maxRowNumber)
        : Tablet(deviceId, schemas, std::vector<ColumnCategory>(schemas.size(), ColumnCategory::FIELD), maxRowNumber) {
    }

    Tablet(const std::string& deviceId, const std::vector<std::pair<std::string, TSDataType::TSDataType>>& schemas,
           const std::vector<ColumnCategory> columnTypes,
           size_t maxRowNumber, bool _isAligned = false) : deviceId(deviceId), schemas(schemas),
                                                           columnTypes(columnTypes),
                                                           maxRowNumber(maxRowNumber), isAligned(_isAligned) {
        // create timestamp column
        timestamps.resize(maxRowNumber);
        // create value columns
        values.resize(schemas.size());
        createColumns();
        // init idColumnIndexs
        for (size_t i = 0; i < this->columnTypes.size(); i++) {
            if (this->columnTypes[i] == ColumnCategory::TAG) {
                idColumnIndexes.push_back(i);
            }
        }
        // create bitMaps
        bitMaps.resize(schemas.size());
        for (size_t i = 0; i < schemas.size(); i++) {
            bitMaps[i].resize(maxRowNumber);
        }
        // create schemaNameIndex
        for (size_t i = 0; i < schemas.size(); i++) {
            schemaNameIndex[schemas[i].first] = i;
        }
        this->rowSize = 0;
    }

    Tablet(const Tablet& other)
        : deviceId(other.deviceId),
          schemas(other.schemas),
          schemaNameIndex(other.schemaNameIndex),
          columnTypes(other.columnTypes),
          timestamps(other.timestamps),
          maxRowNumber(other.maxRowNumber),
          bitMaps(other.bitMaps),
          rowSize(other.rowSize),
          isAligned(other.isAligned),
          idColumnIndexes(other.idColumnIndexes) {
        values.resize(other.values.size());
        for (size_t i = 0; i < other.values.size(); ++i) {
            if (!other.values[i]) continue;
            TSDataType::TSDataType type = schemas[i].second;
            deepCopyTabletColValue(&(other.values[i]), &values[i], type, maxRowNumber);
        }
    }

    Tablet& operator=(const Tablet& other) {
        if (this != &other) {
            deleteColumns();
            deviceId = other.deviceId;
            schemas = other.schemas;
            schemaNameIndex = other.schemaNameIndex;
            columnTypes = other.columnTypes;
            timestamps = other.timestamps;
            maxRowNumber = other.maxRowNumber;
            rowSize = other.rowSize;
            isAligned = other.isAligned;
            idColumnIndexes = other.idColumnIndexes;
            bitMaps = other.bitMaps;
            values.resize(other.values.size());
            for (size_t i = 0; i < other.values.size(); ++i) {
                if (!other.values[i]) continue;
                TSDataType::TSDataType type = schemas[i].second;
                deepCopyTabletColValue(&(other.values[i]), &values[i], type, maxRowNumber);
            }
        }
        return *this;
    }

    ~Tablet() {
        try {
            deleteColumns();
        }
        catch (exception& e) {
            log_debug(string("Tablet::~Tablet(), ") + e.what());
        }
    }

    void addTimestamp(size_t rowIndex, int64_t timestamp) {
        timestamps[rowIndex] = timestamp;
        rowSize = max(rowSize, rowIndex + 1);
    }

    static void deepCopyTabletColValue(void* const* srcPtr, void** destPtr,
        TSDataType::TSDataType type, int maxRowNumber);

    template <typename T>
    void addValue(size_t schemaId, size_t rowIndex, const T& value) {
        if (schemaId >= schemas.size()) {
            char tmpStr[100];
            sprintf(tmpStr, "Tablet::addValue(), schemaId >= schemas.size(). schemaId=%ld, schemas.size()=%ld.",
                    schemaId, schemas.size());
            throw std::out_of_range(tmpStr);
        }

        if (rowIndex >= rowSize) {
            char tmpStr[100];
            sprintf(tmpStr, "Tablet::addValue(), rowIndex >= rowSize. rowIndex=%ld, rowSize.size()=%ld.", rowIndex,
                    rowSize);
            throw std::out_of_range(tmpStr);
        }

        TSDataType::TSDataType dataType = schemas[schemaId].second;
        switch (dataType) {
        case TSDataType::BOOLEAN: {
            safe_cast<T, bool>(value, ((bool*)values[schemaId])[rowIndex]);
            break;
        }
        case TSDataType::INT32: {
            safe_cast<T, int32_t>(value, ((int*)values[schemaId])[rowIndex]);
            break;
        }
        case TSDataType::DATE: {
            safe_cast<T, boost::gregorian::date>(value, ((boost::gregorian::date*)values[schemaId])[rowIndex]);
            break;
        }
        case TSDataType::TIMESTAMP:
        case TSDataType::INT64: {
            safe_cast<T, int64_t>(value, ((int64_t*)values[schemaId])[rowIndex]);
            break;
        }
        case TSDataType::FLOAT: {
            safe_cast<T, float>(value, ((float*)values[schemaId])[rowIndex]);
            break;
        }
        case TSDataType::DOUBLE: {
            safe_cast<T, double>(value, ((double*)values[schemaId])[rowIndex]);
            break;
        }
        case TSDataType::BLOB:
        case TSDataType::STRING:
        case TSDataType::TEXT: {
            safe_cast<T, string>(value, ((string*)values[schemaId])[rowIndex]);
            break;
        }
        default:
            throw UnSupportedDataTypeException(string("Data type ") + to_string(dataType) + " is not supported.");
        }
    }

    // Add a Binary value with extra metadata: [isEOF (1 byte)] + [offset (8 bytes)] + [actual content]
    void addValue(size_t schemaId, size_t rowIndex, bool isEOF, int64_t offset, const std::vector<uint8_t>& content) {
        // Check schemaId bounds
        if (schemaId >= schemas.size()) {
            char tmpStr[100];
            sprintf(tmpStr,
                    "Tablet::addBinaryValueWithMeta(), schemaId >= schemas.size(). schemaId=%ld, schemas.size()=%ld.",
                    schemaId, schemas.size());
            throw std::out_of_range(tmpStr);
        }

        // Check rowIndex bounds
        if (rowIndex >= rowSize) {
            char tmpStr[100];
            sprintf(tmpStr, "Tablet::addBinaryValueWithMeta(), rowIndex >= rowSize. rowIndex=%ld, rowSize=%ld.",
                    rowIndex, rowSize);
            throw std::out_of_range(tmpStr);
        }

        TSDataType::TSDataType dataType = schemas[schemaId].second;
        if (dataType != TSDataType::OBJECT) {
            throw std::invalid_argument("The data type of schemaId " + std::to_string(schemaId) + " is not OBJECT.");
        }

        // Create a byte array of size [1 (isEOF) + 8 (offset) + content size]
        std::vector<uint8_t> val(content.size() + 9);

        // Write the isEOF flag (1 byte)
        val[0] = isEOF ? 1 : 0;

        // Write the 8-byte offset in big-endian order
        for (int i = 0; i < 8; ++i) {
            val[1 + i] = static_cast<uint8_t>((offset >> (56 - i * 8)) & 0xFF);
        }

        // Append the content bytes
        std::copy(content.begin(), content.end(), val.begin() + 9);

        // Cast the value array and assign the Binary data (stored as string)
        std::string valEncoded = std::string(reinterpret_cast<char*>(val.data()), val.size());
        safe_cast<string, string>(valEncoded, ((string*)values[schemaId])[rowIndex]);
    }

    void addValue(const string& schemaName, size_t rowIndex, bool isEOF, int64_t offset,
                  const std::vector<uint8_t>& content) {
        if (schemaNameIndex.find(schemaName) == schemaNameIndex.end()) {
            throw SchemaNotFoundException(string("Schema ") + schemaName + " not found.");
        }
        size_t schemaId = schemaNameIndex[schemaName];
        addValue(schemaId, rowIndex, isEOF, offset, content);
    }

    template <typename T>
    void addValue(const string& schemaName, size_t rowIndex, const T& value) {
        if (schemaNameIndex.find(schemaName) == schemaNameIndex.end()) {
            throw SchemaNotFoundException(string("Schema ") + schemaName + " not found.");
        }
        size_t schemaId = schemaNameIndex[schemaName];
        addValue(schemaId, rowIndex, value);
    }

    void* getValue(size_t schemaId, size_t rowIndex, TSDataType::TSDataType dataType) {
        if (schemaId >= schemas.size()) {
            throw std::out_of_range("Tablet::getValue schemaId out of range: "
                + std::to_string(schemaId));
        }
        if (rowIndex >= rowSize) {
            throw std::out_of_range("Tablet::getValue rowIndex out of range: "
                + std::to_string(rowIndex));
        }

        switch (dataType) {
        case TSDataType::BOOLEAN:
            return &(reinterpret_cast<bool*>(values[schemaId])[rowIndex]);
        case TSDataType::INT32:
            return &(reinterpret_cast<int32_t*>(values[schemaId])[rowIndex]);
        case TSDataType::DATE:
            return &(reinterpret_cast<boost::gregorian::date*>(values[schemaId])[rowIndex]);
        case TSDataType::TIMESTAMP:
        case TSDataType::INT64:
            return &(reinterpret_cast<int64_t*>(values[schemaId])[rowIndex]);
        case TSDataType::FLOAT:
            return &(reinterpret_cast<float*>(values[schemaId])[rowIndex]);
        case TSDataType::DOUBLE:
            return &(reinterpret_cast<double*>(values[schemaId])[rowIndex]);
        case TSDataType::BLOB:
        case TSDataType::STRING:
        case TSDataType::OBJECT:
        case TSDataType::TEXT:
            return &(reinterpret_cast<std::string*>(values[schemaId])[rowIndex]);
        default:
            throw UnSupportedDataTypeException("Unsupported data type: "
                + std::to_string(dataType));
        }
    }

    std::shared_ptr<storage::IDeviceID> getDeviceID(int i);

    std::vector<std::pair<std::string, TSDataType::TSDataType>> getSchemas() const {
        return schemas;
    }

    void reset(); // Reset Tablet to the default state - set the rowSize to 0

    size_t getTimeBytesSize();

    size_t getValueByteSize(); // total byte size that values occupies

    void setAligned(bool isAligned);
};

class SessionUtils {
public:
    static std::string getTime(const Tablet& tablet);

    static std::string getValue(const Tablet& tablet);

    static bool isTabletContainsSingleDevice(Tablet tablet);
};

class TemplateNode {
public:
    explicit TemplateNode(const std::string& name) : name_(name) {
    }

    const std::string& getName() const {
        return name_;
    }

    virtual const std::unordered_map<std::string, std::shared_ptr<TemplateNode>>& getChildren() const {
        throw BatchExecutionException("Should call exact sub class!");
    }

    virtual bool isMeasurement() = 0;

    virtual bool isAligned() {
        throw BatchExecutionException("Should call exact sub class!");
    }

    virtual std::string serialize() const {
        throw BatchExecutionException("Should call exact sub class!");
    }

private:
    std::string name_;
};

class MeasurementNode : public TemplateNode {
public:
    MeasurementNode(const std::string& name_, TSDataType::TSDataType data_type_, TSEncoding::TSEncoding encoding_,
                    CompressionType::CompressionType compression_type_) : TemplateNode(name_) {
        this->data_type_ = data_type_;
        this->encoding_ = encoding_;
        this->compression_type_ = compression_type_;
    }

    TSDataType::TSDataType getDataType() const {
        return data_type_;
    }

    TSEncoding::TSEncoding getEncoding() const {
        return encoding_;
    }

    CompressionType::CompressionType getCompressionType() const {
        return compression_type_;
    }

    bool isMeasurement() override {
        return true;
    }

    std::string serialize() const override;

private:
    TSDataType::TSDataType data_type_;
    TSEncoding::TSEncoding encoding_;
    CompressionType::CompressionType compression_type_;
};

class InternalNode : public TemplateNode {
public:
    InternalNode(const std::string& name, bool is_aligned) : TemplateNode(name), is_aligned_(is_aligned) {
    }

    void addChild(const InternalNode& node) {
        if (this->children_.count(node.getName())) {
            throw BatchExecutionException("Duplicated child of node in template.");
        }
        this->children_[node.getName()] = std::make_shared<InternalNode>(node);
    }

    void addChild(const MeasurementNode& node) {
        if (this->children_.count(node.getName())) {
            throw BatchExecutionException("Duplicated child of node in template.");
        }
        this->children_[node.getName()] = std::make_shared<MeasurementNode>(node);
    }

    void deleteChild(const TemplateNode& node) {
        this->children_.erase(node.getName());
    }

    const std::unordered_map<std::string, std::shared_ptr<TemplateNode>>& getChildren() const override {
        return children_;
    }

    bool isMeasurement() override {
        return false;
    }

    bool isAligned() override {
        return is_aligned_;
    }

private:
    std::unordered_map<std::string, std::shared_ptr<TemplateNode>> children_;
    bool is_aligned_;
};

namespace TemplateQueryType {
enum TemplateQueryType {
    COUNT_MEASUREMENTS, IS_MEASUREMENT, PATH_EXIST, SHOW_MEASUREMENTS
};
}

class Template {
public:
    Template(const std::string& name, bool is_aligned) : name_(name), is_aligned_(is_aligned) {
    }

    const std::string& getName() const {
        return name_;
    }

    bool isAligned() const {
        return is_aligned_;
    }

    void addToTemplate(const InternalNode& child) {
        if (this->children_.count(child.getName())) {
            throw BatchExecutionException("Duplicated child of node in template.");
        }
        this->children_[child.getName()] = std::make_shared<InternalNode>(child);
    }

    void addToTemplate(const MeasurementNode& child) {
        if (this->children_.count(child.getName())) {
            throw BatchExecutionException("Duplicated child of node in template.");
        }
        this->children_[child.getName()] = std::make_shared<MeasurementNode>(child);
    }

    std::string serialize() const;

private:
    std::string name_;
    std::unordered_map<std::string, std::shared_ptr<TemplateNode>> children_;
    bool is_aligned_;
};

class Session {
private:
    std::string host_;
    int rpcPort_;
    std::vector<string> nodeUrls_;
    std::string username_;
    std::string password_;
    const TSProtocolVersion::type protocolVersion_ = TSProtocolVersion::IOTDB_SERVICE_PROTOCOL_V3;
    bool isClosed_ = true;
    std::string zoneId_;
    int fetchSize_;
    const static int DEFAULT_FETCH_SIZE = 10000;
    const static int DEFAULT_TIMEOUT_MS = 0;
    int connectTimeoutMs_;
    Version::Version version;
    std::string sqlDialect_ = "tree"; // default sql dialect
    std::string database_;
    bool enableAutoFetch_ = true;
    bool enableRedirection_ = true;
    std::shared_ptr<INodesSupplier> nodesSupplier_;
    friend class SessionConnection;
    friend class TableSession;
    std::shared_ptr<SessionConnection> defaultSessionConnection_;

    TEndPoint defaultEndPoint_;

    struct TEndPointHash {
        size_t operator()(const TEndPoint& endpoint) const {
            return std::hash<std::string>()(endpoint.ip) ^ std::hash<int>()(endpoint.port);
        }
    };

    struct TEndPointEqual {
        bool operator()(const TEndPoint& lhs, const TEndPoint& rhs) const {
            return lhs.ip == rhs.ip && lhs.port == rhs.port;
        }
    };

    using EndPointSessionMap = std::unordered_map<
        TEndPoint, shared_ptr<SessionConnection>, TEndPointHash, TEndPointEqual>;
    EndPointSessionMap endPointToSessionConnection;
    std::unordered_map<std::string, TEndPoint> deviceIdToEndpoint;
    std::unordered_map<std::shared_ptr<storage::IDeviceID>, TEndPoint> tableModelDeviceIdToEndpoint;

private:
    void removeBrokenSessionConnection(shared_ptr<SessionConnection> sessionConnection);

    static bool checkSorted(const Tablet& tablet);

    static bool checkSorted(const std::vector<int64_t>& times);

    static void sortTablet(Tablet& tablet);

    static void sortIndexByTimestamp(int* index, std::vector<int64_t>& timestamps, int length);

    void appendValues(std::string& buffer, const char* value, int size);

    void
    putValuesIntoBuffer(const std::vector<TSDataType::TSDataType>& types, const std::vector<char*>& values,
                        std::string& buf);

    int8_t getDataTypeNumber(TSDataType::TSDataType type);

    struct TsCompare {
        std::vector<int64_t>& timestamps;

        explicit TsCompare(std::vector<int64_t>& inTimestamps) : timestamps(inTimestamps) {
        };

        bool operator()(int i, int j) { return (timestamps[i] < timestamps[j]); };
    };

    std::string getVersionString(Version::Version version);

    void initZoneId();

    void initNodesSupplier(const std::vector<std::string>& nodeUrls = std::vector<std::string>());

    void initDefaultSessionConnection();

    template <typename T, typename InsertConsumer>
    void insertByGroup(std::unordered_map<std::shared_ptr<SessionConnection>, T>& insertGroup,
                       InsertConsumer insertConsumer);

    template <typename T, typename InsertConsumer>
    void insertOnce(std::unordered_map<std::shared_ptr<SessionConnection>, T>& insertGroup,
                    InsertConsumer insertConsumer);

    void insertStringRecordsWithLeaderCache(vector<string> deviceIds, vector<int64_t> times,
                                            vector<vector<string>> measurementsList, vector<vector<string>> valuesList,
                                            bool isAligned);

    void insertRecordsWithLeaderCache(vector<string> deviceIds, vector<int64_t> times,
                                      vector<vector<string>> measurementsList,
                                      const vector<vector<TSDataType::TSDataType>>& typesList,
                                      vector<vector<char*>> valuesList, bool isAligned);

    void insertTabletsWithLeaderCache(unordered_map<string, Tablet*> tablets, bool sorted, bool isAligned);

    shared_ptr<SessionConnection> getQuerySessionConnection();

    shared_ptr<SessionConnection> getSessionConnection(std::string deviceId);

    shared_ptr<SessionConnection> getSessionConnection(std::shared_ptr<storage::IDeviceID> deviceId);

    void handleQueryRedirection(TEndPoint endPoint);

    void handleRedirection(const std::string& deviceId, TEndPoint endPoint);

    void handleRedirection(const std::shared_ptr<storage::IDeviceID>& deviceId, TEndPoint endPoint);

    void setSqlDialect(const std::string& dialect) {
        this->sqlDialect_ = dialect;
    }

    void setDatabase(const std::string& database) {
        this->database_ = database;
    }

    string getDatabase() {
        return database_;
    }

    void changeDatabase(string database) {
        this->database_ = database;
    }

public:
    Session(const std::string& host, int rpcPort) : username_("root"), password_("root"), version(Version::V_1_0) {
        this->host_ = host;
        this->rpcPort_ = rpcPort;
        initZoneId();
        initNodesSupplier();
    }

    Session(const std::vector<string>& nodeUrls, const std::string& username, const std::string& password)
            : nodeUrls_(nodeUrls), username_(username), password_(password), version(Version::V_1_0) {
        initZoneId();
        initNodesSupplier(this->nodeUrls_);
    }

    Session(const std::string& host, int rpcPort, const std::string& username, const std::string& password)
        : fetchSize_(DEFAULT_FETCH_SIZE) {
        this->host_ = host;
        this->rpcPort_ = rpcPort;
        this->username_ = username;
        this->password_ = password;
        this->version = Version::V_1_0;
        initZoneId();
        initNodesSupplier();
    }

    Session(const std::string& host, int rpcPort, const std::string& username, const std::string& password,
            const std::string& zoneId, int fetchSize = DEFAULT_FETCH_SIZE) {
        this->host_ = host;
        this->rpcPort_ = rpcPort;
        this->username_ = username;
        this->password_ = password;
        this->zoneId_ = zoneId;
        this->fetchSize_ = fetchSize;
        this->version = Version::V_1_0;
        initZoneId();
        initNodesSupplier();
    }

    Session(const std::string& host, const std::string& rpcPort, const std::string& username = "user",
            const std::string& password = "password", const std::string& zoneId = "",
            int fetchSize = DEFAULT_FETCH_SIZE) {
        this->host_ = host;
        this->rpcPort_ = stoi(rpcPort);
        this->username_ = username;
        this->password_ = password;
        this->zoneId_ = zoneId;
        this->fetchSize_ = fetchSize;
        this->version = Version::V_1_0;
        initZoneId();
        initNodesSupplier();
    }

    Session(AbstractSessionBuilder* builder) {
        this->host_ = builder->host;
        this->rpcPort_ = builder->rpcPort;
        this->username_ = builder->username;
        this->password_ = builder->password;
        this->zoneId_ = builder->zoneId;
        this->fetchSize_ = builder->fetchSize;
        this->version = Version::V_1_0;
        this->sqlDialect_ = builder->sqlDialect;
        this->database_ = builder->database;
        this->enableAutoFetch_ = builder->enableAutoFetch;
        this->enableRedirection_ = builder->enableRedirections;
        this->connectTimeoutMs_ = builder->connectTimeoutMs;
        this->nodeUrls_ = builder->nodeUrls;
        initZoneId();
        initNodesSupplier(this->nodeUrls_);
    }

    ~Session();

    void open();

    void open(bool enableRPCCompression);

    void open(bool enableRPCCompression, int connectionTimeoutInMs);

    void close();

    void setTimeZone(const std::string& zoneId);

    std::string getTimeZone();

    void insertRecord(const std::string& deviceId, int64_t time, const std::vector<std::string>& measurements,
                      const std::vector<std::string>& values);

    void insertRecord(const std::string& deviceId, int64_t time, const std::vector<std::string>& measurements,
                      const std::vector<TSDataType::TSDataType>& types, const std::vector<char*>& values);

    void insertAlignedRecord(const std::string& deviceId, int64_t time, const std::vector<std::string>& measurements,
                             const std::vector<std::string>& values);

    void insertAlignedRecord(const std::string& deviceId, int64_t time, const std::vector<std::string>& measurements,
                             const std::vector<TSDataType::TSDataType>& types, const std::vector<char*>& values);

    void insertRecords(const std::vector<std::string>& deviceIds,
                       const std::vector<int64_t>& times,
                       const std::vector<std::vector<std::string>>& measurementsList,
                       const std::vector<std::vector<std::string>>& valuesList);

    void insertRecords(const std::vector<std::string>& deviceIds,
                       const std::vector<int64_t>& times,
                       const std::vector<std::vector<std::string>>& measurementsList,
                       const std::vector<std::vector<TSDataType::TSDataType>>& typesList,
                       const std::vector<std::vector<char*>>& valuesList);

    void insertAlignedRecords(const std::vector<std::string>& deviceIds,
                              const std::vector<int64_t>& times,
                              const std::vector<std::vector<std::string>>& measurementsList,
                              const std::vector<std::vector<std::string>>& valuesList);

    void insertAlignedRecords(const std::vector<std::string>& deviceIds,
                              const std::vector<int64_t>& times,
                              const std::vector<std::vector<std::string>>& measurementsList,
                              const std::vector<std::vector<TSDataType::TSDataType>>& typesList,
                              const std::vector<std::vector<char*>>& valuesList);

    void insertRecordsOfOneDevice(const std::string& deviceId,
                                  std::vector<int64_t>& times,
                                  std::vector<std::vector<std::string>>& measurementsList,
                                  std::vector<std::vector<TSDataType::TSDataType>>& typesList,
                                  std::vector<std::vector<char*>>& valuesList);

    void insertRecordsOfOneDevice(const std::string& deviceId,
                                  std::vector<int64_t>& times,
                                  std::vector<std::vector<std::string>>& measurementsList,
                                  std::vector<std::vector<TSDataType::TSDataType>>& typesList,
                                  std::vector<std::vector<char*>>& valuesList,
                                  bool sorted);

    void insertAlignedRecordsOfOneDevice(const std::string& deviceId,
                                         std::vector<int64_t>& times,
                                         std::vector<std::vector<std::string>>& measurementsList,
                                         std::vector<std::vector<TSDataType::TSDataType>>& typesList,
                                         std::vector<std::vector<char*>>& valuesList);

    void insertAlignedRecordsOfOneDevice(const std::string& deviceId,
                                         std::vector<int64_t>& times,
                                         std::vector<std::vector<std::string>>& measurementsList,
                                         std::vector<std::vector<TSDataType::TSDataType>>& typesList,
                                         std::vector<std::vector<char*>>& valuesList,
                                         bool sorted);

    void insertTablet(Tablet& tablet);

    void insertTablet(Tablet& tablet, bool sorted);

    void insertRelationalTablet(Tablet& tablet);

    void insertRelationalTabletOnce(
        const std::unordered_map<std::shared_ptr<SessionConnection>, Tablet>& relationalTabletGroup,
        bool sorted);

    void insertRelationalTabletByGroup(
        const std::unordered_map<std::shared_ptr<SessionConnection>, Tablet>& relationalTabletGroup,
        bool sorted);

    void insertRelationalTablet(Tablet& tablet, bool sorted);

    static void buildInsertTabletReq(TSInsertTabletReq& request, Tablet& tablet, bool sorted);

    void insertTablet(TSInsertTabletReq request);

    void insertAlignedTablet(Tablet& tablet);

    void insertAlignedTablet(Tablet& tablet, bool sorted);

    void insertTablets(std::unordered_map<std::string, Tablet*>& tablets);

    void insertTablets(std::unordered_map<std::string, Tablet*>& tablets, bool sorted);

    void insertAlignedTablets(std::unordered_map<std::string, Tablet*>& tablets, bool sorted = false);

    void testInsertRecord(const std::string& deviceId, int64_t time,
                          const std::vector<std::string>& measurements,
                          const std::vector<std::string>& values);

    void testInsertTablet(const Tablet& tablet);

    void testInsertRecords(const std::vector<std::string>& deviceIds,
                           const std::vector<int64_t>& times,
                           const std::vector<std::vector<std::string>>& measurementsList,
                           const std::vector<std::vector<std::string>>& valuesList);

    void deleteTimeseries(const std::string& path);

    void deleteTimeseries(const std::vector<std::string>& paths);

    void deleteData(const std::string& path, int64_t endTime);

    void deleteData(const std::vector<std::string>& paths, int64_t endTime);

    void deleteData(const std::vector<std::string>& paths, int64_t startTime, int64_t endTime);

    void setStorageGroup(const std::string& storageGroupId);

    void deleteStorageGroup(const std::string& storageGroup);

    void deleteStorageGroups(const std::vector<std::string>& storageGroups);

    void createDatabase(const std::string& database);

    void deleteDatabase(const std::string& database);

    void deleteDatabases(const std::vector<std::string>& databases);

    void createTimeseries(const std::string& path, TSDataType::TSDataType dataType, TSEncoding::TSEncoding encoding,
                          CompressionType::CompressionType compressor);

    void createTimeseries(const std::string& path, TSDataType::TSDataType dataType, TSEncoding::TSEncoding encoding,
                          CompressionType::CompressionType compressor,
                          std::map<std::string, std::string>* props, std::map<std::string, std::string>* tags,
                          std::map<std::string, std::string>* attributes,
                          const std::string& measurementAlias);

    void createMultiTimeseries(const std::vector<std::string>& paths,
                               const std::vector<TSDataType::TSDataType>& dataTypes,
                               const std::vector<TSEncoding::TSEncoding>& encodings,
                               const std::vector<CompressionType::CompressionType>& compressors,
                               std::vector<std::map<std::string, std::string>>* propsList,
                               std::vector<std::map<std::string, std::string>>* tagsList,
                               std::vector<std::map<std::string, std::string>>* attributesList,
                               std::vector<std::string>* measurementAliasList);

    void createAlignedTimeseries(const std::string& deviceId,
                                 const std::vector<std::string>& measurements,
                                 const std::vector<TSDataType::TSDataType>& dataTypes,
                                 const std::vector<TSEncoding::TSEncoding>& encodings,
                                 const std::vector<CompressionType::CompressionType>& compressors);

    bool checkTimeseriesExists(const std::string& path);

    std::unique_ptr<SessionDataSet> executeQueryStatement(const std::string& sql);

    std::unique_ptr<SessionDataSet> executeQueryStatement(const std::string& sql, int64_t timeoutInMs);

    std::unique_ptr<SessionDataSet> executeQueryStatementMayRedirect(const std::string& sql, int64_t timeoutInMs);

    void executeNonQueryStatement(const std::string& sql);

    std::unique_ptr<SessionDataSet> executeRawDataQuery(const std::vector<std::string>& paths, int64_t startTime,
                                                        int64_t endTime);

    std::unique_ptr<SessionDataSet> executeLastDataQuery(const std::vector<std::string>& paths);

    std::unique_ptr<SessionDataSet> executeLastDataQuery(const std::vector<std::string>& paths, int64_t lastTime);

    void createSchemaTemplate(const Template& templ);

    void setSchemaTemplate(const std::string& template_name, const std::string& prefix_path);

    void unsetSchemaTemplate(const std::string& prefix_path, const std::string& template_name);

    void addAlignedMeasurementsInTemplate(const std::string& template_name,
                                          const std::vector<std::string>& measurements,
                                          const std::vector<TSDataType::TSDataType>& dataTypes,
                                          const std::vector<TSEncoding::TSEncoding>& encodings,
                                          const std::vector<CompressionType::CompressionType>& compressors);

    void addAlignedMeasurementsInTemplate(const std::string& template_name,
                                          const std::string& measurement,
                                          TSDataType::TSDataType dataType,
                                          TSEncoding::TSEncoding encoding,
                                          CompressionType::CompressionType compressor);

    void addUnalignedMeasurementsInTemplate(const std::string& template_name,
                                            const std::vector<std::string>& measurements,
                                            const std::vector<TSDataType::TSDataType>& dataTypes,
                                            const std::vector<TSEncoding::TSEncoding>& encodings,
                                            const std::vector<CompressionType::CompressionType>& compressors);

    void addUnalignedMeasurementsInTemplate(const std::string& template_name,
                                            const std::string& measurement,
                                            TSDataType::TSDataType dataType,
                                            TSEncoding::TSEncoding encoding,
                                            CompressionType::CompressionType compressor);

    void deleteNodeInTemplate(const std::string& template_name, const std::string& path);

    int countMeasurementsInTemplate(const std::string& template_name);

    bool isMeasurementInTemplate(const std::string& template_name, const std::string& path);

    bool isPathExistInTemplate(const std::string& template_name, const std::string& path);

    std::vector<std::string> showMeasurementsInTemplate(const std::string& template_name);

    std::vector<std::string> showMeasurementsInTemplate(const std::string& template_name, const std::string& pattern);

    bool checkTemplateExists(const std::string& template_name);
};

template <typename T, typename InsertConsumer>
void Session::insertByGroup(std::unordered_map<std::shared_ptr<SessionConnection>, T>& insertGroup,
                            InsertConsumer insertConsumer) {
    std::vector<std::future<void>> futures;

    for (auto& entry : insertGroup) {
        auto connection = entry.first;
        auto& req = entry.second;
        futures.emplace_back(std::async(std::launch::async, [=, &req]() mutable {
            try {
                insertConsumer(connection, req);
            }
            catch (const RedirectException& e) {
                for (const auto& deviceEndPoint : e.deviceEndPointMap) {
                    handleRedirection(deviceEndPoint.first, deviceEndPoint.second);
                }
            } catch (const IoTDBConnectionException& e) {
                if (endPointToSessionConnection.size() > 1) {
                    removeBrokenSessionConnection(connection);
                    try {
                        insertConsumer(defaultSessionConnection_, req);
                    }
                    catch (const RedirectException&) {
                    }
                }
                else {
                    throw;
                }
            } catch (const std::exception& e) {
                log_debug(e.what());
                throw IoTDBException(e.what());
            }
        }));
    }

    std::string errorMessages;
    for (auto& f : futures) {
        try {
            f.get();
        }
        catch (const IoTDBConnectionException& e) {
            throw;
        } catch (const std::exception& e) {
            if (!errorMessages.empty()) {
                errorMessages += ";";
            }
            errorMessages += e.what();
        }
    }

    if (!errorMessages.empty()) {
        throw StatementExecutionException(errorMessages);
    }
}

template <typename T, typename InsertConsumer>
void Session::insertOnce(std::unordered_map<std::shared_ptr<SessionConnection>, T>& insertGroup,
                         InsertConsumer insertConsumer) {
    auto connection = insertGroup.begin()->first;
    auto req = insertGroup.begin()->second;
    try {
        insertConsumer(connection, req);
    }
    catch (RedirectException e) {
        for (const auto& deviceEndPoint : e.deviceEndPointMap) {
            handleRedirection(deviceEndPoint.first, deviceEndPoint.second);
        }
    } catch (IoTDBConnectionException e) {
        if (endPointToSessionConnection.size() > 1) {
            removeBrokenSessionConnection(connection);
            try {
                insertConsumer(defaultSessionConnection_, req);
            }
            catch (RedirectException e) {
            }
        }
        else {
            throw e;
        }
    }
}

#endif // IOTDB_SESSION_H
