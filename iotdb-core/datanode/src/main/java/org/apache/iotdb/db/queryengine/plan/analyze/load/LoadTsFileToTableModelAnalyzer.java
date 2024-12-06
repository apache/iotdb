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

import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.exception.LoadEmptyFileException;
import org.apache.iotdb.db.exception.VerifyMetadataException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.io.FileUtils;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
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

  private final LoadTsFileTableSchemaCache schemaCache;

  public LoadTsFileToTableModelAnalyzer(
      LoadTsFileStatement loadTsFileStatement, Metadata metadata, MPPQueryContext context) {
    super(loadTsFileStatement, context);
    this.metadata = metadata;
    this.schemaCache = new LoadTsFileTableSchemaCache(metadata, context);
  }

  public LoadTsFileToTableModelAnalyzer(
      LoadTsFile loadTsFileTableStatement, Metadata metadata, MPPQueryContext context) {
    super(loadTsFileTableStatement, context);
    this.metadata = metadata;
    this.schemaCache = new LoadTsFileTableSchemaCache(metadata, context);
  }

  @Override
  public IAnalysis analyzeFileByFile(IAnalysis analysis) {
    checkBeforeAnalyzeFileByFile(analysis);
    if (analysis.isFinishQueryAfterAnalyze()) {
      return analysis;
    }

    try {
      autoCreateDatabaseIfAbsent(database);
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
      if (Objects.isNull(reader.readFileMetadata().getTableSchemaMap())
          || reader.readFileMetadata().getTableSchemaMap().isEmpty()) {
        throw new SemanticException("Attempted to load a tree-model TsFile into table-model.");
      }

      // check whether the encrypt type of the tsfile is supported
      EncryptParameter param = reader.getEncryptParam();
      if (!Objects.equals(param.getType(), EncryptUtils.encryptParam.getType())
          || !Arrays.equals(param.getKey(), EncryptUtils.encryptParam.getKey())) {
        throw new SemanticException("The encryption way of the TsFile is not supported.");
      }

      // construct tsfile resource
      final TsFileResource tsFileResource = constructTsFileResource(reader, tsFile);

      schemaCache.setDatabase(database);
      schemaCache.setCurrentModificationsAndTimeIndex(tsFileResource, reader);

      for (Map.Entry<String, org.apache.tsfile.file.metadata.TableSchema> name2Schema :
          reader.readFileMetadata().getTableSchemaMap().entrySet()) {
        final TableSchema fileSchema =
            TableSchema.fromTsFileTableSchema(name2Schema.getKey(), name2Schema.getValue());
        schemaCache.createTable(fileSchema, context, metadata);
      }

      long writePointCount = 0;

      while (timeseriesMetadataIterator.hasNext()) {
        final Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadata =
            timeseriesMetadataIterator.next();

        for (IDeviceID deviceId : device2TimeseriesMetadata.keySet()) {
          schemaCache.autoCreateAndVerify(deviceId);
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

      schemaCache.flush();
      schemaCache.clearIdColumnMapper();
    } catch (final LoadEmptyFileException loadEmptyFileException) {
      LOGGER.warn("Failed to load empty file: {}", tsFile.getAbsolutePath());
      if (isDeleteAfterLoad) {
        FileUtils.deleteQuietly(tsFile);
      }
    }
  }

  private void autoCreateDatabaseIfAbsent(final String database) throws VerifyMetadataException {
    validateDatabaseName(database);
    if (DataNodeTableCache.getInstance().isDatabaseExist(database)) {
      return;
    }

    final CreateDBTask task =
        new CreateDBTask(
            new TDatabaseSchema(ROOT + PATH_SEPARATOR_CHAR + database).setIsTableModel(true), true);
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

  @Override
  public void close() throws Exception {
    schemaCache.close();
  }
}
