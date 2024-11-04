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
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.exception.VerifyMetadataException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileAnalyzeSchemaMemoryBlock;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryManager;
import org.apache.iotdb.db.utils.ModificationUtils;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

  private final LoadTsFileAnalyzeSchemaMemoryBlock block;

  private String database;
  private final Metadata metadata;
  private final MPPQueryContext context;

  private Map<String, Set<IDeviceID>> currentBatchTable2Devices;

  // tableName -> Pair<device column count, device column mapping>
  private Map<String, Pair<Integer, Map<Integer, Integer>>> tableIdColumnMapper = new HashMap<>();

  private Collection<ModEntry> currentModifications;
  private ITimeIndex currentTimeIndex;

  private long batchTable2DevicesMemoryUsageSizeInBytes = 0;
  private long tableIdColumnMapperMemoryUsageSizeInBytes = 0;
  private long currentModificationsMemoryUsageSizeInBytes = 0;
  private long currentTimeIndexMemoryUsageSizeInBytes = 0;

  private int currentBatchDevicesCount = 0;

  public LoadTsFileTableSchemaCache(Metadata metadata, MPPQueryContext context)
      throws LoadRuntimeOutOfMemoryException {
    this.block =
        LoadTsFileMemoryManager.getInstance()
            .allocateAnalyzeSchemaMemoryBlock(ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES);
    this.metadata = metadata;
    this.context = context;
    this.currentBatchTable2Devices = new HashMap<>();
    this.currentModifications = new ArrayList<>();
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public void autoCreateAndVerify(IDeviceID device) {
    try {
      if (isDeviceDeletedByMods(device)) {
        return;
      }
    } catch (IllegalPathException e) {
      LOGGER.warn(
          "Failed to check if device {} is deleted by mods. Will see it as not deleted.",
          device,
          e);
    }

    // TODO: add permission check and record auth cost
    addDevice(device);
    if (shouldFlushDevices()) {
      flush();
    }
  }

  private boolean isDeviceDeletedByMods(IDeviceID device) throws IllegalPathException {
    return currentTimeIndex != null
        && ModificationUtils.isAllDeletedByMods(
            currentModifications,
            device,
            currentTimeIndex.getStartTime(device),
            currentTimeIndex.getEndTime(device));
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
        final Pair<Integer, Map<Integer, Integer>> idColumnCountAndMapper =
            tableIdColumnMapper.get(tableName);
        if (Objects.isNull(idColumnCountAndMapper)) {
          // This should not happen
          LOGGER.warn("Failed to find id column mapping for table {}", tableName);
        }

        for (final IDeviceID device : currentBatchTable2Devices.get(tableName)) {
          if (Objects.isNull(idColumnCountAndMapper)) {
            devices.add(Arrays.copyOfRange(device.getSegments(), 1, device.getSegments().length));
            continue;
          }

          final Object[] deviceIdArray = new String[idColumnCountAndMapper.getLeft()];
          for (final Map.Entry<Integer, Integer> fileColumn2RealColumn :
              idColumnCountAndMapper.getRight().entrySet()) {
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

  public void createTable(TableSchema fileSchema, MPPQueryContext context, Metadata metadata)
      throws VerifyMetadataException {
    final TableSchema realSchema =
        metadata.validateTableHeaderSchema(database, fileSchema, context, true).orElse(null);
    if (Objects.isNull(realSchema)) {
      throw new VerifyMetadataException(
          String.format(
              "Failed to validate schema for table {%s, %s}",
              fileSchema.getTableName(), fileSchema));
    }
    verifyTableDataTypeAndGenerateIdColumnMapper(fileSchema, realSchema);
  }

  private void verifyTableDataTypeAndGenerateIdColumnMapper(
      TableSchema fileSchema, TableSchema realSchema) throws VerifyMetadataException {
    final int realIdColumnCount = realSchema.getIdColumns().size();
    final Map<Integer, Integer> idColumnMapping =
        tableIdColumnMapper
            .computeIfAbsent(
                realSchema.getTableName(), k -> new Pair<>(realIdColumnCount, new HashMap<>()))
            .getRight();
    int idColumnIndex = 0;
    for (int i = 0; i < fileSchema.getColumns().size(); i++) {
      final ColumnSchema fileColumn = fileSchema.getColumns().get(i);
      if (fileColumn.getColumnCategory() == TsTableColumnCategory.ID) {
        final int realIndex = realSchema.getIndexAmongIdColumns(fileColumn.getName());
        if (realIndex != -1) {
          idColumnMapping.put(idColumnIndex++, realIndex);
        } else {
          throw new VerifyMetadataException(
              String.format(
                  "Id column %s in TsFile is not found in IoTDB table %s",
                  fileColumn.getName(), realSchema.getTableName()));
        }
      } else if (fileColumn.getColumnCategory() == TsTableColumnCategory.MEASUREMENT) {
        final ColumnSchema realColumn =
            realSchema.getColumn(fileColumn.getName(), fileColumn.getColumnCategory());
        if (!fileColumn.getType().equals(realColumn.getType())) {
          throw new VerifyMetadataException(
              String.format(
                  "Data type mismatch for column %s in table %s, type in TsFile: %s, type in IoTDB: %s",
                  realColumn.getName(),
                  realSchema.getTableName(),
                  fileColumn.getType(),
                  realColumn.getType()));
        }
      }
    }
    updateTableIdColumnMapperMemoryUsageSizeInBytes();
  }

  private void updateTableIdColumnMapperMemoryUsageSizeInBytes() {
    block.reduceMemoryUsage(tableIdColumnMapperMemoryUsageSizeInBytes);
    tableIdColumnMapperMemoryUsageSizeInBytes = 0;
    for (final Map.Entry<String, Pair<Integer, Map<Integer, Integer>>> entry :
        tableIdColumnMapper.entrySet()) {
      tableIdColumnMapperMemoryUsageSizeInBytes += computeStringMemUsage(entry.getKey());
      tableIdColumnMapperMemoryUsageSizeInBytes +=
          (4L + 4L * 2 * entry.getValue().getRight().size());
    }
    block.addMemoryUsage(tableIdColumnMapperMemoryUsageSizeInBytes);
  }

  public void setCurrentModificationsAndTimeIndex(
      TsFileResource resource, TsFileSequenceReader reader) throws IOException {
    clearModificationsAndTimeIndex();

    currentModifications = resource.getAllModEntries();
    for (final ModEntry modification : currentModifications) {
      currentModificationsMemoryUsageSizeInBytes += modification.serializedSize();
    }

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

  public void close() {
    clearDevices();
    clearIdColumnMapper();
    clearModificationsAndTimeIndex();

    block.close();

    currentBatchTable2Devices = null;
    tableIdColumnMapper = null;
  }

  private void clearDevices() {
    currentBatchTable2Devices.clear();
    block.reduceMemoryUsage(batchTable2DevicesMemoryUsageSizeInBytes);
    batchTable2DevicesMemoryUsageSizeInBytes = 0;
    currentBatchDevicesCount = 0;
  }

  private void clearModificationsAndTimeIndex() {
    currentModifications.clear();
    currentTimeIndex = null;
    block.reduceMemoryUsage(currentModificationsMemoryUsageSizeInBytes);
    block.reduceMemoryUsage(currentTimeIndexMemoryUsageSizeInBytes);
    currentModificationsMemoryUsageSizeInBytes = 0;
    currentTimeIndexMemoryUsageSizeInBytes = 0;
  }

  public void clearIdColumnMapper() {
    tableIdColumnMapper.clear();
    block.reduceMemoryUsage(tableIdColumnMapperMemoryUsageSizeInBytes);
    tableIdColumnMapperMemoryUsageSizeInBytes = 0;
  }
}
