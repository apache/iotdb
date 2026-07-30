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
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadAnalyzeException;
import org.apache.iotdb.db.exception.load.LoadAnalyzeTypeMismatchException;
import org.apache.iotdb.db.exception.load.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
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
import java.util.concurrent.atomic.AtomicBoolean;
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
  private final boolean shouldVerifyDataType;

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
  private final AtomicBoolean needDecode4DifferentTimeColumn = new AtomicBoolean(false);

  public LoadTsFileTableSchemaCache(
      final Metadata metadata,
      final MPPQueryContext context,
      final boolean needToCreateDatabase,
      final boolean shouldVerifyDataType)
      throws LoadRuntimeOutOfMemoryException {
    this.block =
        LoadTsFileMemoryManager.getInstance()
            .allocateMemoryBlock(ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES);
    this.metadata = metadata;
    this.context = context;
    this.shouldVerifyDataType = shouldVerifyDataType;
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
    if (isDeviceDeletedByMods(device)) {
      return;
    }

    try {
      createTableAndDatabaseIfNecessary(device.getTableName());
    } catch (final Exception e) {
      if (IoTDBDescriptor.getInstance().getConfig().isSkipFailedTableSchemaCheck()) {
        LOGGER.info(
            DataNodeQueryMessages
                .FAILED_TO_CHECK_TABLE_SCHEMA_WILL_SKIP_BECAUSE_SKIPFAILEDTABLESCHEMACHECK_IS_SET_TO_TRUE,
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

  public boolean isDeviceDeletedByMods(final IDeviceID device) {
    try {
      return ModificationUtils.isDeviceDeletedByMods(
          currentModifications, currentTimeIndex, device);
    } catch (final IllegalPathException e) {
      LOGGER.warn(
          DataNodeQueryMessages
              .FAILED_TO_CHECK_IF_DEVICE_ARG_IS_DELETED_BY_MODS_WILL_SEE_IT_AS_NOT_DELETED,
          device,
          e);
      return false;
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
      LOGGER.warn(DataNodeQueryMessages.AUTO_CREATE_OR_VERIFY_SCHEMA_ERROR, e);
      throw new SemanticException(
          String.format(
              DataNodeQueryMessages.AUTO_CREATE_OR_VERIFY_SCHEMA_ERROR_DETAIL_S, e.getMessage()));
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
          LOGGER.warn(DataNodeQueryMessages.FAILED_TO_FIND_TAG_COLUMN_MAPPING_FOR_TABLE, tableName);
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
    final TableSchema fileSchema = TableSchema.fromTsFileTableSchema(tableName, schema);
    final TableSchema realSchema =
        metadata
            .validateTableHeaderSchema4TsFile(
                database, fileSchema, context, true, true, needDecode4DifferentTimeColumn)
            .orElse(null);
    if (Objects.isNull(realSchema)) {
      throw new LoadAnalyzeException(
          String.format(
              DataNodeQueryMessages
                  .QUERY_EXCEPTION_FAILED_TO_VALIDATE_SCHEMA_FOR_TABLE_S_S_D7031B7B,
              fileSchema.getTableName(),
              fileSchema));
    }
    verifyTableDataTypeAndGenerateTagColumnMapper(fileSchema, realSchema);
  }

  public boolean isNeedDecode4DifferentTimeColumn() {
    return needDecode4DifferentTimeColumn.get();
  }

  private void autoCreateTableDatabaseIfAbsent(final String database) throws LoadAnalyzeException {
    validateDatabaseName(database);
    if (DataNodeTableCache.getInstance().isDatabaseExist(database)) {
      return;
    }

    if (!IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled()) {
      throw new LoadAnalyzeException(
          String.format(
              DataNodeQueryMessages
                  .QUERY_EXCEPTION_THE_DATABASE_S_DOES_NOT_EXIST_PLEASE_ENABLE_ENABLE_AUTO_B6683D0E,
              database));
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
                DataNodeQueryMessages
                    .QUERY_EXCEPTION_AUTO_CREATE_DATABASE_FAILED_S_STATUS_CODE_S_D8EB60FA,
                database,
                result.getStatusCode()));
      }
    } catch (final Exception e) {
      throw new LoadAnalyzeException(
          DataNodeQueryMessages.AUTO_CREATE_DATABASE_FAILED_BECAUSE + e.getMessage());
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
                  DataNodeQueryMessages
                      .QUERY_EXCEPTION_TAG_COLUMN_S_IN_TSFILE_IS_NOT_FOUND_IN_IOTDB_TABLE_S_12E8C1EF,
                  fileColumn.getName(),
                  realSchema.getTableName()));
        }
      } else if (fileColumn.getColumnCategory() == TsTableColumnCategory.FIELD) {
        ColumnSchema realColumn = fieldColumnNameToSchema.get(fileColumn.getName());
        if (realColumn != null && !fileColumn.getType().equals(realColumn.getType())) {
          final String message =
              String.format(
                  "Data type mismatch for column %s in table %s, type in TsFile: %s, type in IoTDB: %s",
                  fileColumn.getName(),
                  realSchema.getTableName(),
                  fileColumn.getType(),
                  realColumn.getType());
          if (shouldVerifyDataType) {
            throw new LoadAnalyzeTypeMismatchException(message);
          }
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(message);
          }
        } else if (LOGGER.isDebugEnabled() && realColumn == null) {
          LOGGER.debug(
              DataNodeQueryMessages
                  .COLUMN_ARG_IN_TABLE_ARG_IS_NOT_FOUND_IN_IOTDB_WHILE_LOADING_TSFILE,
              fileColumn.getName(),
              realSchema.getTableName());
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
    needDecode4DifferentTimeColumn.set(false);
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
