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

package org.apache.iotdb.db.queryengine.plan.analyze.load;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadAnalyzeException;
import org.apache.iotdb.db.exception.load.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryBlock;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryManager;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.commons.schema.MemUsageUtil.computeStringMemUsage;
import static org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor.validateDatabaseName;

public class LoadTsFileTableSchemaCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileTableSchemaCache.class);

  private static final int BATCH_FLUSH_TABLE_DEVICE_NUMBER;
  private static final long ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES;

  static {
    final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
    BATCH_FLUSH_TABLE_DEVICE_NUMBER =
        CONFIG.getLoadTsFileAnalyzeSchemaBatchFlushTableDeviceNumber();
    ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES =
        CONFIG.getLoadTsFileAnalyzeSchemaMemorySizeInBytes() <= 0
            ? ((long) BATCH_FLUSH_TABLE_DEVICE_NUMBER) << 10
            : CONFIG.getLoadTsFileAnalyzeSchemaMemorySizeInBytes();
  }

  private final LoadTsFileMemoryBlock block;

  private String database;
  private boolean needToCreateDatabase;
  private Map<String, org.apache.tsfile.file.metadata.TableSchema> tableSchemaMap;
  private final Metadata metadata;
  private final MPPQueryContext context;

  private Map<String, Set<IDeviceID>> currentBatchTable2Devices;

  // tableName -> Pair<device column count, device column mapping>
  private Map<String, Pair<Integer, Map<Integer, Integer>>> tableTagColumnMapper = new HashMap<>();

  private PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> currentModifications;
  private ITimeIndex currentTimeIndex;

  private long batchTable2DevicesMemoryUsageSizeInBytes = 0;
  private long tableTagColumnMapperMemoryUsageSizeInBytes = 0;
  private long currentModificationsMemoryUsageSizeInBytes = 0;
  private long currentTimeIndexMemoryUsageSizeInBytes = 0;

  private int currentBatchDevicesCount = 0;

  public LoadTsFileTableSchemaCache(
      final Metadata metadata, final MPPQueryContext context, final boolean needToCreateDatabase)
      throws LoadRuntimeOutOfMemoryException {
    this.block =
        LoadTsFileMemoryManager.getInstance()
            .allocateMemoryBlock(ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES);
    this.metadata = metadata;
    this.context = context;
    this.currentBatchTable2Devices = new HashMap<>();
    this.currentModifications = PatternTreeMapFactory.getModsPatternTreeMap();
    this.needToCreateDatabase = needToCreateDatabase;
  }

  public void setDatabase(final String database) {
    this.database = database;
  }

  public void setTableSchemaMap(
      final Map<String, org.apache.tsfile.file.metadata.TableSchema> tableSchemaMap) {
    this.tableSchemaMap = tableSchemaMap;
  }

  public void autoCreateAndVerify(final IDeviceID device) throws LoadAnalyzeException {
    try {
      if (ModificationUtils.isDeviceDeletedByMods(currentModifications, currentTimeIndex, device)) {
        return;
      }
    } catch (final IllegalPathException e) {
      LOGGER.warn(
          "Failed to check if device {} is deleted by mods. Will see it as not deleted.",
          device,
          e);
    }

    try {
      createTableAndDatabaseIfNecessary(device.getTableName());
    } catch (final Exception e) {
      if (IoTDBDescriptor.getInstance().getConfig().isSkipFailedTableSchemaCheck()) {
        LOGGER.info(
            "Failed to check table schema, will skip because skipFailedTableSchemaCheck is set to true, message: {}",
            e.getMessage());
      } else {
        throw e;
      }
    }

    // TODO: add permission check and record auth cost
    addDevice(device);
    if (shouldFlushDevices()) {
      flush();
    }
  }

  private void addDevice(final IDeviceID device) {
    final String tableName = device.getTableName();
    long memoryUsageSizeInBytes = 0;
    if (!currentBatchTable2Devices.containsKey(tableName)) {
      memoryUsageSizeInBytes += computeStringMemUsage(tableName);
    }
    if (currentBatchTable2Devices.computeIfAbsent(tableName, k -> new HashSet<>()).add(device)) {
      memoryUsageSizeInBytes += device.ramBytesUsed();
      currentBatchDevicesCount++;
    }

    if (memoryUsageSizeInBytes > 0) {
      batchTable2DevicesMemoryUsageSizeInBytes += memoryUsageSizeInBytes;
      block.addMemoryUsage(memoryUsageSizeInBytes);
    }
  }

  private boolean shouldFlushDevices() {
    return !block.hasEnoughMemory() || currentBatchDevicesCount >= BATCH_FLUSH_TABLE_DEVICE_NUMBER;
  }

  public void flush() {
    doAutoCreateAndVerify();
    clearDevices();
  }

  private void doAutoCreateAndVerify() throws SemanticException {
    if (currentBatchTable2Devices.isEmpty()) {
      return;
    }

    try {
      getTableSchemaValidationIterator()
          .forEachRemaining(o -> metadata.validateDeviceSchema(o, context));
    } catch (Exception e) {
      LOGGER.warn("Auto create or verify schema error.", e);
      throw new SemanticException(
          String.format("Auto create or verify schema error.  Detail: %s.", e.getMessage()));
    }
  }

  private Iterator<ITableDeviceSchemaValidation> getTableSchemaValidationIterator() {
    return currentBatchTable2Devices.keySet().stream()
        .map(this::createTableSchemaValidation)
        .iterator();
  }

  private ITableDeviceSchemaValidation createTableSchemaValidation(String tableName) {
    return new ITableDeviceSchemaValidation() {

      @Override
      public String getDatabase() {
        return database;
      }

      @Override
      public String getTableName() {
        return tableName;
      }

      @Override
      public List<Object[]> getDeviceIdList() {
        final List<Object[]> devices = new ArrayList<>();
        final Pair<Integer, Map<Integer, Integer>> tagColumnCountAndMapper =
            tableTagColumnMapper.get(tableName);
        if (Objects.isNull(tagColumnCountAndMapper)) {
          // This should not happen
          LOGGER.warn("Failed to find tag column mapping for table {}", tableName);
        }

        for (final IDeviceID device : currentBatchTable2Devices.get(tableName)) {
          if (Objects.isNull(tagColumnCountAndMapper)) {
            devices.add(Arrays.copyOfRange(device.getSegments(), 1, device.getSegments().length));
            continue;
          }

          final Object[] deviceIdArray = new String[tagColumnCountAndMapper.getLeft()];
          for (final Map.Entry<Integer, Integer> fileColumn2RealColumn :
              tagColumnCountAndMapper.getRight().entrySet()) {
            final int fileColumnIndex = fileColumn2RealColumn.getKey();
            final int realColumnIndex = fileColumn2RealColumn.getValue();
            deviceIdArray[realColumnIndex] =
                fileColumnIndex + 1 < device.getSegments().length
                    ? device.getSegments()[fileColumnIndex + 1]
                    : null;
          }
          devices.add(truncateNullSuffixesOfDeviceIdSegments(deviceIdArray));
        }
        return devices;
      }

      @Override
      public List<String> getAttributeColumnNameList() {
        return Collections.emptyList();
      }

      @Override
      public List<Object[]> getAttributeValueList() {
        return Collections.nCopies(currentBatchTable2Devices.get(tableName).size(), new Object[0]);
      }
    };
  }

  private static Object[] truncateNullSuffixesOfDeviceIdSegments(Object[] segments) {
    int lastNonNullIndex = segments.length - 1;
    while (lastNonNullIndex >= 1 && segments[lastNonNullIndex] == null) {
      lastNonNullIndex--;
    }
    return Arrays.copyOf(segments, lastNonNullIndex + 1);
  }

  public void createTableAndDatabaseIfNecessary(final String tableName)
      throws LoadAnalyzeException {
    final org.apache.tsfile.file.metadata.TableSchema schema = tableSchemaMap.remove(tableName);
    if (Objects.isNull(schema)) {
      return;
    }

    // Check on creation, do not auto-create tables or database that cannot be inserted
    AuthorityChecker.getAccessControl()
        .checkCanInsertIntoTable(
            context.getSession().getUserName(),
            new QualifiedObjectName(database, tableName),
            context);

    if (needToCreateDatabase) {
      autoCreateTableDatabaseIfAbsent(database);
      needToCreateDatabase = false;
    }
    final org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema fileSchema =
        org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema.fromTsFileTableSchema(
            tableName, schema);
    final TableSchema realSchema =
        metadata.validateTableHeaderSchema(database, fileSchema, context, true, true).orElse(null);
    if (Objects.isNull(realSchema)) {
      throw new LoadAnalyzeException(
          String.format(
              "Failed to validate schema for table {%s, %s}",
              fileSchema.getTableName(), fileSchema));
    }
    verifyTableDataTypeAndGenerateTagColumnMapper(fileSchema, realSchema);
  }

  private void autoCreateTableDatabaseIfAbsent(final String database) throws LoadAnalyzeException {
    validateDatabaseName(database);
    if (DataNodeTableCache.getInstance().isDatabaseExist(database)) {
      return;
    }

    if (!IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled()) {
      throw new LoadAnalyzeException(
          "The database "
              + database
              + " does not exist, please enable 'enable_auto_create_schema' to enable auto creation.");
    }

    AuthorityChecker.getAccessControl()
        .checkCanCreateDatabase(context.getSession().getUserName(), database, context);
    final CreateDBTask task =
        new CreateDBTask(new TDatabaseSchema(database).setIsTableModel(true), true);
    try {
      final ListenableFuture<ConfigTaskResult> future =
          task.execute(ClusterConfigTaskExecutor.getInstance());
      final ConfigTaskResult result = future.get();
      if (result.getStatusCode().getStatusCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new LoadAnalyzeException(
            String.format(
                "Auto create database failed: %s, status code: %s",
                database, result.getStatusCode()));
      }
    } catch (final Exception e) {
      throw new LoadAnalyzeException("Auto create database failed because: " + e.getMessage());
    }
  }

  private void verifyTableDataTypeAndGenerateTagColumnMapper(
      TableSchema fileSchema, TableSchema realSchema) throws LoadAnalyzeException {
    final int realTagColumnCount = realSchema.getTagColumns().size();
    final Map<Integer, Integer> tagColumnMapping =
        tableTagColumnMapper
            .computeIfAbsent(
                realSchema.getTableName(), k -> new Pair<>(realTagColumnCount, new HashMap<>()))
            .getRight();

    Map<String, Integer> tagColumnNameToIndex = new HashMap<>();
    for (int i = 0; i < realSchema.getTagColumns().size(); i++) {
      tagColumnNameToIndex.put(realSchema.getTagColumns().get(i).getName(), i);
    }
    Map<String, ColumnSchema> fieldColumnNameToSchema = new HashMap<>();
    for (ColumnSchema column : realSchema.getColumns()) {
      if (column.getColumnCategory() == TsTableColumnCategory.FIELD) {
        fieldColumnNameToSchema.put(column.getName(), column);
      }
    }

    int tagColumnIndex = 0;
    for (ColumnSchema fileColumn : fileSchema.getColumns()) {
      if (fileColumn.getColumnCategory() == TsTableColumnCategory.TAG) {
        Integer realIndex = tagColumnNameToIndex.get(fileColumn.getName());
        if (realIndex != null) {
          tagColumnMapping.put(tagColumnIndex++, realIndex);
        } else {
          throw new LoadAnalyzeException(
              String.format(
                  "Tag column %s in TsFile is not found in IoTDB table %s",
                  fileColumn.getName(), realSchema.getTableName()));
        }
      } else if (fileColumn.getColumnCategory() == TsTableColumnCategory.FIELD) {
        ColumnSchema realColumn = fieldColumnNameToSchema.get(fileColumn.getName());
        if (LOGGER.isDebugEnabled()
            && (realColumn == null || !fileColumn.getType().equals(realColumn.getType()))) {
          LOGGER.debug(
              "Data type mismatch for column {} in table {}, type in TsFile: {}, type in IoTDB: {}",
              realColumn.getName(),
              realSchema.getTableName(),
              fileColumn.getType(),
              realColumn.getType());
        }
      }
    }
    updateTableTagColumnMapperMemoryUsageSizeInBytes();
  }

  private void updateTableTagColumnMapperMemoryUsageSizeInBytes() {
    block.reduceMemoryUsage(tableTagColumnMapperMemoryUsageSizeInBytes);
    tableTagColumnMapperMemoryUsageSizeInBytes = 0;
    for (final Map.Entry<String, Pair<Integer, Map<Integer, Integer>>> entry :
        tableTagColumnMapper.entrySet()) {
      tableTagColumnMapperMemoryUsageSizeInBytes += computeStringMemUsage(entry.getKey());
      tableTagColumnMapperMemoryUsageSizeInBytes +=
          (4L + 4L * 2 * entry.getValue().getRight().size());
    }
    block.addMemoryUsage(tableTagColumnMapperMemoryUsageSizeInBytes);
  }

  public void setCurrentModificationsAndTimeIndex(
      TsFileResource resource, TsFileSequenceReader reader) throws IOException {
    clearModificationsAndTimeIndex();

    ModificationFile.readAllModifications(resource.getTsFile(), false)
        .forEach(
            modification ->
                currentModifications.append(modification.keyOfPatternTree(), modification));

    currentModificationsMemoryUsageSizeInBytes = currentModifications.ramBytesUsed();

    // If there are too many modifications, a larger memory block is needed to avoid frequent
    // flush.
    long newMemorySize =
        currentModificationsMemoryUsageSizeInBytes > ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES / 2
            ? currentModificationsMemoryUsageSizeInBytes + ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES
            : ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES;
    block.forceResize(newMemorySize);
    block.addMemoryUsage(currentModificationsMemoryUsageSizeInBytes);

    // No need to build device time index if there are no modifications
    if (!currentModifications.isEmpty() && resource.resourceFileExists()) {
      final AtomicInteger deviceCount = new AtomicInteger();
      reader
          .getAllDevicesIteratorWithIsAligned()
          .forEachRemaining(o -> deviceCount.getAndIncrement());

      currentTimeIndex = resource.getTimeIndex();
      if (currentTimeIndex instanceof FileTimeIndex) {
        currentTimeIndex = resource.buildDeviceTimeIndex();
      }
      currentTimeIndexMemoryUsageSizeInBytes = currentTimeIndex.calculateRamSize();
      block.addMemoryUsage(currentTimeIndexMemoryUsageSizeInBytes);
    }
  }

  public void setCurrentTimeIndex(final ITimeIndex timeIndex) {
    this.currentTimeIndex = timeIndex;
  }

  public void close() {
    clearDevices();
    clearTagColumnMapper();
    clearModificationsAndTimeIndex();

    block.close();

    currentBatchTable2Devices = null;
    tableTagColumnMapper = null;
  }

  private void clearDevices() {
    currentBatchTable2Devices.clear();
    block.reduceMemoryUsage(batchTable2DevicesMemoryUsageSizeInBytes);
    batchTable2DevicesMemoryUsageSizeInBytes = 0;
    currentBatchDevicesCount = 0;
  }

  private void clearModificationsAndTimeIndex() {
    currentModifications = PatternTreeMapFactory.getModsPatternTreeMap();
    currentTimeIndex = null;
    block.reduceMemoryUsage(currentModificationsMemoryUsageSizeInBytes);
    block.reduceMemoryUsage(currentTimeIndexMemoryUsageSizeInBytes);
    currentModificationsMemoryUsageSizeInBytes = 0;
    currentTimeIndexMemoryUsageSizeInBytes = 0;
  }

  public void clearTagColumnMapper() {
    tableTagColumnMapper.clear();
    block.reduceMemoryUsage(tableTagColumnMapperMemoryUsageSizeInBytes);
    tableTagColumnMapperMemoryUsageSizeInBytes = 0;
  }
}
