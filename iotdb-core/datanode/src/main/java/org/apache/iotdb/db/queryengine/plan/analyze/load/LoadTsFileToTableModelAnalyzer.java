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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.exception.LoadEmptyFileException;
import org.apache.iotdb.db.exception.LoadReadOnlyException;
import org.apache.iotdb.db.exception.VerifyMetadataException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor.validateDatabaseName;
import static org.apache.iotdb.db.utils.constant.SqlConstant.ROOT;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR_CHAR;

public class LoadTsFileToTableModelAnalyzer extends LoadTsFileAnalyzer {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(LoadTsFileToTableModelAnalyzer.class);

  private final Metadata metadata;

  // tableName -> Pair<device column count, device column mapping>
  private final Map<String, Pair<Integer, List<Integer>>> tableIdColumnMapper = new HashMap<>();

  public LoadTsFileToTableModelAnalyzer(
      LoadTsFileStatement loadTsFileStatement, Metadata metadata, MPPQueryContext context) {
    super(loadTsFileStatement, context);
    this.metadata = metadata;
  }

  public LoadTsFileToTableModelAnalyzer(
      LoadTsFile loadTsFileTableStatement, Metadata metadata, MPPQueryContext context) {
    super(loadTsFileTableStatement, context);
    this.metadata = metadata;
  }

  @Override
  public IAnalysis analyzeFileByFile(IAnalysis analysis) {
    // check if the system is read only
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(
          RpcUtils.getStatus(TSStatusCode.SYSTEM_READ_ONLY, LoadReadOnlyException.MESSAGE));
      return analysis;
    }

    try {
      autoCreateDatabase(database);
    } catch (VerifyMetadataException e) {
      LOGGER.warn("Auto create database failed: {}", database, e);
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(RpcUtils.getStatus(TSStatusCode.LOAD_FILE_ERROR, e.getMessage()));
      return analysis;
    }

    if (!doAnalyzeFileByFile(analysis)) {
      // return false means the analysis is failed because of exception
      return analysis;
    }

    LOGGER.info("Load - Analysis Stage: all tsfiles have been analyzed.");

    // data partition will be queried in the scheduler
    setRealStatement(analysis);
    return analysis;
  }

  @Override
  protected void analyzeSingleTsFile(final File tsFile)
      throws IOException, VerifyMetadataException {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      // can be reused when constructing tsfile resource
      final TsFileSequenceReaderTimeseriesMetadataIterator timeseriesMetadataIterator =
          new TsFileSequenceReaderTimeseriesMetadataIterator(reader, true, 1);

      // check if the tsfile is empty
      if (!timeseriesMetadataIterator.hasNext()) {
        throw new LoadEmptyFileException(tsFile.getAbsolutePath());
      }

      // check whether the tsfile is table-model or not
      // TODO: currently, loading a file with both tree-model and table-model data is not supported.
      //  May need to support this and remove this check in the future.
      if (Objects.isNull(reader.readFileMetadata().getTableSchemaMap())
          || reader.readFileMetadata().getTableSchemaMap().isEmpty()) {
        throw new SemanticException("Attempted to load a tree-model TsFile into table-model.");
      }

      // construct tsfile resource
      final TsFileResource tsFileResource = constructTsFileResource(reader, tsFile);

      for (Map.Entry<String, org.apache.tsfile.file.metadata.TableSchema> name2Schema :
          reader.readFileMetadata().getTableSchemaMap().entrySet()) {
        final TableSchema fileSchema =
            TableSchema.fromTsFileTableSchema(name2Schema.getKey(), name2Schema.getValue());
        final TableSchema realSchema;
        // TODO: remove this synchronized block after the metadata is thread-safe
        synchronized (metadata) {
          realSchema =
              metadata.validateTableHeaderSchema(database, fileSchema, context, true).orElse(null);
          if (Objects.isNull(realSchema)) {
            throw new VerifyMetadataException(
                String.format(
                    "Failed to validate schema for table {%s, %s}",
                    name2Schema.getKey(), name2Schema.getValue()));
          }
        }
        tableIdColumnMapper.clear();
        verifyTableDataTypeAndGenerateIdColumnMapper(fileSchema, realSchema);
      }

      long writePointCount = 0;

      while (timeseriesMetadataIterator.hasNext()) {
        final Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadata =
            timeseriesMetadataIterator.next();

        for (IDeviceID deviceId : device2TimeseriesMetadata.keySet()) {
          final ITableDeviceSchemaValidation tableSchemaValidation =
              createTableSchemaValidation(deviceId, this);
          // TODO: currently, we only validate one device at a time for table model,
          //  may need to validate in batched manner in the future.
          metadata.validateDeviceSchema(tableSchemaValidation, context);
        }

        if (!tsFileResource.resourceFileExists()) {
          TsFileResourceUtils.updateTsFileResource(device2TimeseriesMetadata, tsFileResource);
        }

        writePointCount += getWritePointCount(device2TimeseriesMetadata);
      }

      TimestampPrecisionUtils.checkTimestampPrecision(tsFileResource.getFileEndTime());
      tsFileResource.setStatus(TsFileResourceStatus.NORMAL);

      addTsFileResource(tsFileResource);
      addWritePointCount(writePointCount);
    } catch (final LoadEmptyFileException loadEmptyFileException) {
      LOGGER.warn("Failed to load empty file: {}", tsFile.getAbsolutePath());
      if (isDeleteAfterLoad) {
        FileUtils.deleteQuietly(tsFile);
      }
    }
  }

  private void verifyTableDataTypeAndGenerateIdColumnMapper(
      TableSchema fileSchema, TableSchema realSchema) throws VerifyMetadataException {
    final int realIdColumnCount = realSchema.getIdColumns().size();
    final List<Integer> idColumnMapping =
        tableIdColumnMapper
            .computeIfAbsent(
                realSchema.getTableName(), k -> new Pair<>(realIdColumnCount, new ArrayList<>()))
            .getRight();
    for (int i = 0; i < fileSchema.getColumns().size(); i++) {
      final ColumnSchema fileColumn = fileSchema.getColumns().get(i);
      if (fileColumn.getColumnCategory() == TsTableColumnCategory.ID) {
        final int realIndex = realSchema.getIndexAmongIdColumns(fileColumn.getName());
        if (realIndex != -1) {
          idColumnMapping.add(realIndex);
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
  }

  private void autoCreateDatabase(final String database) throws VerifyMetadataException {
    validateDatabaseName(database);
    final CreateDBTask task =
        new CreateDBTask(new TDatabaseSchema(ROOT + PATH_SEPARATOR_CHAR + database), true);
    try {
      final ListenableFuture<ConfigTaskResult> future =
          task.execute(ClusterConfigTaskExecutor.getInstance());
      final ConfigTaskResult result = future.get();
      if (result.getStatusCode().getStatusCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new VerifyMetadataException(
            String.format(
                "Auto create database failed: %s, status code: %s",
                database, result.getStatusCode()));
      }
    } catch (final ExecutionException | InterruptedException e) {
      throw new VerifyMetadataException("Auto create database failed because: " + e.getMessage());
    }
  }

  private ITableDeviceSchemaValidation createTableSchemaValidation(
      IDeviceID deviceId, LoadTsFileToTableModelAnalyzer analyzer) {
    return new ITableDeviceSchemaValidation() {

      @Override
      public String getDatabase() {
        return analyzer.database;
      }

      @Override
      public String getTableName() {
        return deviceId.getTableName();
      }

      @Override
      public List<Object[]> getDeviceIdList() {
        final Pair<Integer, List<Integer>> idColumnCountAndMapper =
            analyzer.tableIdColumnMapper.get(deviceId.getTableName());
        if (Objects.isNull(idColumnCountAndMapper)) {
          // This should not happen
          LOGGER.warn(
              "Failed to find id column mapping for table {}, deviceId: {}",
              deviceId.getTableName(),
              deviceId);
          return Collections.singletonList(
              Arrays.copyOfRange(deviceId.getSegments(), 1, deviceId.getSegments().length));
        }

        final Object[] deviceIdArray = new String[idColumnCountAndMapper.getLeft()];
        for (int i = 0; i < idColumnCountAndMapper.getRight().size(); i++) {
          final int j = idColumnCountAndMapper.getRight().get(i);
          deviceIdArray[j] = deviceId.getSegments()[i + 1];
        }
        return Collections.singletonList(deviceIdArray);
      }

      @Override
      public List<String> getAttributeColumnNameList() {
        return Collections.emptyList();
      }

      @Override
      public List<Object[]> getAttributeValueList() {
        return Collections.singletonList(new Object[0]);
      }
    };
  }

  @Override
  public void close() throws Exception {}
}
