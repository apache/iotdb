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
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadAnalyzeException;
import org.apache.iotdb.db.exception.load.LoadAnalyzeWritableViewException;
import org.apache.iotdb.db.exception.load.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.WritableViewSchema;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryBlock;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryManager;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.timecho.iotdb.db.queryengine.plan.relational.metadata.WritableViewUtils;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
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
import java.util.Optional;
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

  private Map<String, Set<IDeviceID>> currentBatchTable2Devices;

  // tableName -> Pair<device column count, device column mapping>
  private Map<String, Pair<Integer, Map<Integer, Integer>>> tableTagColumnMapper = new HashMap<>();
  private final Map<String, String> writableViewTableNameRewriteMap = new HashMap<>();
  private final Map<String, Map<String, String>> writableViewColumnNameRewriteMap = new HashMap<>();

  private PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> currentModifications;
  private List<ModEntry> currentModificationList = Collections.emptyList();
  private ITimeIndex currentTimeIndex;
  private boolean currentFileContainsObjectColumn = false;

  private long batchTable2DevicesMemoryUsageSizeInBytes = 0;
  private long tableTagColumnMapperMemoryUsageSizeInBytes = 0;
  private long currentModificationsMemoryUsageSizeInBytes = 0;
  private long currentTimeIndexMemoryUsageSizeInBytes = 0;

  private int currentBatchDevicesCount = 0;
  private final AtomicBoolean needDecode4DifferentTimeColumn = new AtomicBoolean(false);

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

  public void setCurrentFileContainsObjectColumn(final boolean currentFileContainsObjectColumn) {
    this.currentFileContainsObjectColumn = currentFileContainsObjectColumn;
  }

  public Map<String, String> getWritableViewTableNameRewriteMap() {
    return writableViewTableNameRewriteMap;
  }

  public Map<String, Map<String, String>> getWritableViewColumnNameRewriteMap() {
    return writableViewColumnNameRewriteMap;
  }

  public void autoCreateAndVerify(final IDeviceID device) throws LoadAnalyzeException {
    try {
      prepareWritableViewNativeLoadFastPathIfNecessary(device.getTableName());
      if (isDeviceDeletedByMods(device)) {
        return;
      }
      createTableAndDatabaseIfNecessary(device.getTableName());
    } catch (final LoadAnalyzeWritableViewException e) {
      throw e;
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
    addDevice(rewriteWritableViewDeviceIfNecessary(device));
    if (shouldFlushDevices()) {
      flush();
    }
  }

  private void prepareWritableViewNativeLoadFastPathIfNecessary(final String tableName)
      throws LoadAnalyzeException {
    final org.apache.tsfile.file.metadata.TableSchema schema = tableSchemaMap.get(tableName);
    if (Objects.isNull(schema)) {
      return;
    }

    final TsTable existingTable =
        DataNodeTableCache.getInstance().getTableInWrite(database, tableName);
    if (!(existingTable instanceof WritableView)) {
      return;
    }

    final WritableView writableView = (WritableView) existingTable;
    if (tryUseWritableViewNativeLoadFastPath(tableName, schema)) {
      tableSchemaMap.remove(tableName);
      return;
    }
    throwWritableViewRequiresTabletConversion(tableName, writableView);
  }

  public boolean isDeviceDeletedByMods(final IDeviceID device) {
    try {
      return ModificationUtils.isDeviceDeletedByMods(
          currentModifications, currentTimeIndex, device);
    } catch (final IllegalPathException e) {
      LOGGER.warn(
          "Failed to check if device {} is deleted by mods. Will see it as not deleted.",
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

    final TsTable existingTable =
        DataNodeTableCache.getInstance().getTableInWrite(database, tableName);
    if (existingTable instanceof WritableView) {
      final WritableView writableView = (WritableView) existingTable;
      if (tryUseWritableViewNativeLoadFastPath(tableName, schema)) {
        return;
      }
      throwWritableViewRequiresTabletConversion(tableName, writableView);
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
              "Failed to validate schema for table {%s, %s}",
              fileSchema.getTableName(), fileSchema));
    }
    verifyTableDataTypeAndGenerateTagColumnMapper(fileSchema, realSchema);
  }

  private void throwWritableViewRequiresTabletConversion(
      final String tableName, final WritableView writableView)
      throws LoadAnalyzeWritableViewException {
    throw new LoadAnalyzeWritableViewException(
        String.format(
            DataNodeQueryMessages.LOAD_TSFILE_TARGET_IS_WRITABLE_VIEW_REQUIRES_TABLET_CONVERSION,
            database,
            tableName,
            writableView.getSourceTableDatabase(),
            writableView.getSourceTableName()));
  }

  private boolean tryUseWritableViewNativeLoadFastPath(
      final String tableName, final org.apache.tsfile.file.metadata.TableSchema schema)
      throws LoadAnalyzeException {
    final Metadata viewMetadata = Objects.nonNull(metadata) ? metadata : new TableMetadataImpl();
    final Optional<TableSchema> tableSchema =
        viewMetadata.getTableSchema(
            context.getSession(), new QualifiedObjectName(database, tableName));
    if (!tableSchema.isPresent() || !(tableSchema.get() instanceof WritableViewSchema)) {
      return false;
    }

    final WritableViewSchema writableViewSchema = (WritableViewSchema) tableSchema.get();
    final Optional<NativeWritableViewLoadPlan> nativeLoadPlan =
        buildNativeWritableViewLoadPlan(tableName, schema, writableViewSchema, viewMetadata);
    if (!nativeLoadPlan.isPresent()) {
      return false;
    }

    if (!canRewriteCurrentModificationsForWritableView(
        tableName,
        nativeLoadPlan.get().getColumnNameRewriteMap(),
        nativeLoadPlan.get().getSourceColumnNames())) {
      return false;
    }

    AuthorityChecker.getAccessControl()
        .checkCanInsertIntoTable(
            context.getSession().getUserName(),
            new QualifiedObjectName(database, tableName),
            context);
    writableViewTableNameRewriteMap.put(
        tableName, writableViewSchema.getSourceTableName().getObjectName());
    if (!nativeLoadPlan.get().getColumnNameRewriteMap().isEmpty()) {
      writableViewColumnNameRewriteMap.put(
          tableName, nativeLoadPlan.get().getColumnNameRewriteMap());
    }

    final TableSchema realSchema =
        viewMetadata
            .validateTableHeaderSchema4TsFile(
                database,
                nativeLoadPlan.get().getSourceFileSchema(),
                context,
                true,
                true,
                needDecode4DifferentTimeColumn)
            .orElse(null);
    if (Objects.isNull(realSchema)) {
      throw new LoadAnalyzeException(
          String.format(
              "Failed to validate schema for writable view source table {%s, %s}",
              nativeLoadPlan.get().getSourceFileSchema().getTableName(),
              nativeLoadPlan.get().getSourceFileSchema()));
    }

    verifyTableDataTypeAndGenerateTagColumnMapper(
        nativeLoadPlan.get().getSourceFileSchema(), realSchema);
    return true;
  }

  private Optional<NativeWritableViewLoadPlan> buildNativeWritableViewLoadPlan(
      final String tableName,
      final org.apache.tsfile.file.metadata.TableSchema schema,
      final WritableViewSchema writableViewSchema,
      final Metadata viewMetadata) {
    if (!database.equals(writableViewSchema.getSourceTableName().getDatabaseName())) {
      return Optional.empty();
    }

    final Optional<TableSchema> sourceTableSchema =
        resolveSourceTableSchema(writableViewSchema, viewMetadata);
    if (!sourceTableSchema.isPresent()) {
      return Optional.empty();
    }

    final TableSchema viewFileSchema = TableSchema.fromTsFileTableSchema(tableName, schema);
    final Map<String, ColumnSchema> viewColumnSchemaMap = writableViewSchema.getColumnSchemaMap();
    final Map<String, ColumnSchema> sourceColumnSchemaMap =
        sourceTableSchema.get().getColumnSchemaMap();
    final Set<String> sourceColumnNames = new HashSet<>(sourceColumnSchemaMap.keySet());
    final Set<String> rewrittenFileColumnNames = new HashSet<>();
    final List<ColumnSchema> sourceFileColumns = new ArrayList<>();
    final Map<String, String> columnNameRewriteMap =
        buildCompatibleColumnNameRewriteMap(writableViewSchema, sourceColumnSchemaMap);

    int fileTagIndex = 0;
    for (final ColumnSchema fileColumn : viewFileSchema.getColumns()) {
      final ColumnSchema viewColumn = viewColumnSchemaMap.get(fileColumn.getName());
      if (Objects.isNull(viewColumn) || !hasSameTypeAndCategory(fileColumn, viewColumn)) {
        return Optional.empty();
      }

      final String sourceColumnName =
          Optional.ofNullable(
                  WritableViewUtils.getSourceColumnName(
                      fileColumn.getName(), writableViewSchema.getViewColumnToSourceColumnMap()))
              .orElse(fileColumn.getName());
      final ColumnSchema sourceColumn = sourceColumnSchemaMap.get(sourceColumnName);
      if (Objects.isNull(sourceColumn) || !hasSameTypeAndCategory(fileColumn, sourceColumn)) {
        return Optional.empty();
      }
      if (!rewrittenFileColumnNames.add(sourceColumnName)) {
        return Optional.empty();
      }

      if (fileColumn.getColumnCategory() == TsTableColumnCategory.TAG) {
        final Integer sourceTagIndex =
            getSourceTagColumnIndex(sourceColumnName, writableViewSchema, sourceTableSchema.get());
        if (Objects.isNull(sourceTagIndex) || sourceTagIndex != fileTagIndex) {
          return Optional.empty();
        }
        fileTagIndex++;
      }

      sourceFileColumns.add(
          new ColumnSchema(
              sourceColumnName,
              fileColumn.getType(),
              fileColumn.isHidden(),
              fileColumn.getColumnCategory()));
      if (!sourceColumnName.equals(fileColumn.getName())) {
        columnNameRewriteMap.put(fileColumn.getName(), sourceColumnName);
      }
    }

    return Optional.of(
        new NativeWritableViewLoadPlan(
            new TableSchema(
                writableViewSchema.getSourceTableName().getObjectName(), sourceFileColumns),
            columnNameRewriteMap,
            sourceColumnNames));
  }

  private static Map<String, String> buildCompatibleColumnNameRewriteMap(
      final WritableViewSchema writableViewSchema,
      final Map<String, ColumnSchema> sourceColumnSchemaMap) {
    final Map<String, String> columnNameRewriteMap = new HashMap<>();
    for (final ColumnSchema viewColumn : writableViewSchema.getColumns()) {
      final String sourceColumnName =
          WritableViewUtils.getSourceColumnName(
              viewColumn.getName(), writableViewSchema.getViewColumnToSourceColumnMap());
      if (Objects.isNull(sourceColumnName) || sourceColumnName.equals(viewColumn.getName())) {
        continue;
      }
      final ColumnSchema sourceColumn = sourceColumnSchemaMap.get(sourceColumnName);
      if (Objects.nonNull(sourceColumn) && hasSameTypeAndCategory(viewColumn, sourceColumn)) {
        columnNameRewriteMap.put(viewColumn.getName(), sourceColumnName);
      }
    }
    return columnNameRewriteMap;
  }

  private Optional<TableSchema> resolveSourceTableSchema(
      final WritableViewSchema writableViewSchema, final Metadata viewMetadata) {
    if (writableViewSchema.getSourceTableSchema().isPresent()) {
      return writableViewSchema.getSourceTableSchema();
    }
    return viewMetadata.getTableSchema(
        context.getSession(), writableViewSchema.getSourceTableName());
  }

  private static boolean hasSameTypeAndCategory(
      final ColumnSchema fileColumn, final ColumnSchema tableColumn) {
    return fileColumn.getColumnCategory() == tableColumn.getColumnCategory()
        && fileColumn.getType().equals(tableColumn.getType());
  }

  private static Integer getSourceTagColumnIndex(
      final String sourceColumnName,
      final WritableViewSchema writableViewSchema,
      final TableSchema sourceTableSchema) {
    final Integer sourceTagIndex =
        writableViewSchema.getSourceTagColumnIndexMap().get(sourceColumnName);
    return Objects.nonNull(sourceTagIndex)
        ? sourceTagIndex
        : sourceTableSchema.getIndexAmongTagColumns(sourceColumnName);
  }

  private boolean canRewriteCurrentModificationsForWritableView(
      final String tableName,
      final Map<String, String> columnNameRewriteMap,
      final Set<String> sourceColumnNames) {
    if (currentModifications.isEmpty()) {
      return true;
    }

    for (final ModEntry modification : currentModificationList) {
      if (!(modification instanceof TableDeletionEntry)) {
        return false;
      }
      final TableDeletionEntry tableDeletionEntry = (TableDeletionEntry) modification;
      if (tableName.equals(tableDeletionEntry.getTableName())
          && !canRewriteMeasurementNamesForWritableView(
              tableDeletionEntry, columnNameRewriteMap, sourceColumnNames)) {
        return false;
      }
    }
    return true;
  }

  private static boolean canRewriteMeasurementNamesForWritableView(
      final TableDeletionEntry deletion,
      final Map<String, String> columnNameRewriteMap,
      final Set<String> sourceColumnNames) {
    if (columnNameRewriteMap.isEmpty()) {
      return true;
    }
    for (final String measurementName : deletion.getPredicate().getMeasurementNames()) {
      if (!sourceColumnNames.contains(measurementName)
          && !columnNameRewriteMap.containsKey(measurementName)) {
        return false;
      }
    }
    return true;
  }

  private IDeviceID rewriteWritableViewDeviceIfNecessary(final IDeviceID device) {
    final String rewrittenTableName = writableViewTableNameRewriteMap.get(device.getTableName());
    if (Objects.isNull(rewrittenTableName)) {
      return device;
    }

    final Object[] segments = device.getSegments();
    final String[] rewrittenSegments = new String[segments.length];
    rewrittenSegments[0] = rewrittenTableName;
    for (int i = 1; i < segments.length; i++) {
      rewrittenSegments[i] = Objects.toString(segments[i], null);
    }
    return new StringArrayDeviceID(rewrittenSegments);
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
                  "Tag column %s in TsFile is not found in IoTDB table %s",
                  fileColumn.getName(), realSchema.getTableName()));
        }
      } else if (fileColumn.getColumnCategory() == TsTableColumnCategory.FIELD) {
        ColumnSchema realColumn = fieldColumnNameToSchema.get(fileColumn.getName());
        if (LOGGER.isDebugEnabled()
            && (realColumn == null || !fileColumn.getType().equals(realColumn.getType()))) {
          LOGGER.debug(
              "Data type mismatch for column {} in table {}, type in TsFile: {}, type in IoTDB: {}",
              fileColumn.getName(),
              realSchema.getTableName(),
              fileColumn.getType(),
              Objects.nonNull(realColumn) ? realColumn.getType() : null);
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

    currentModificationList = ModificationFile.readAllModifications(resource.getTsFile(), false);
    currentModificationList.forEach(
        modification -> currentModifications.append(modification.keyOfPatternTree(), modification));

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
    writableViewTableNameRewriteMap.clear();
    writableViewColumnNameRewriteMap.clear();
  }

  private void clearDevices() {
    currentBatchTable2Devices.clear();
    block.reduceMemoryUsage(batchTable2DevicesMemoryUsageSizeInBytes);
    batchTable2DevicesMemoryUsageSizeInBytes = 0;
    currentBatchDevicesCount = 0;
  }

  private void clearModificationsAndTimeIndex() {
    currentModifications = PatternTreeMapFactory.getModsPatternTreeMap();
    currentModificationList = Collections.emptyList();
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

  private static final class NativeWritableViewLoadPlan {

    private final TableSchema sourceFileSchema;
    private final Map<String, String> columnNameRewriteMap;
    private final Set<String> sourceColumnNames;

    private NativeWritableViewLoadPlan(
        final TableSchema sourceFileSchema,
        final Map<String, String> columnNameRewriteMap,
        final Set<String> sourceColumnNames) {
      this.sourceFileSchema = sourceFileSchema;
      this.columnNameRewriteMap = columnNameRewriteMap;
      this.sourceColumnNames = sourceColumnNames;
    }

    private TableSchema getSourceFileSchema() {
      return sourceFileSchema;
    }

    private Map<String, String> getColumnNameRewriteMap() {
      return columnNameRewriteMap;
    }

    private Set<String> getSourceColumnNames() {
      return sourceColumnNames;
    }
  }
}
