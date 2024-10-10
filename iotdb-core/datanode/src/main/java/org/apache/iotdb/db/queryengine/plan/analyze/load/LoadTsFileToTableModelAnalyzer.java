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

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.LoadEmptyFileException;
import org.apache.iotdb.db.exception.LoadReadOnlyException;
import org.apache.iotdb.db.exception.VerifyMetadataException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateDBTask;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
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
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.Arrays;
import java.util.Collections;
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

    // analyze tsfile metadata file by file
    for (int i = 0, tsfileNum = tsFiles.size(); i < tsfileNum; i++) {
      final File tsFile = tsFiles.get(i);

      if (tsFile.length() == 0) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("TsFile {} is empty.", tsFile.getPath());
        }
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Load - Analysis Stage: {}/{} tsfiles have been analyzed, progress: {}%",
              i + 1, tsfileNum, String.format("%.3f", (i + 1) * 100.00 / tsfileNum));
        }
        continue;
      }

      try {
        analyzeSingleTsFile(tsFile);
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Load - Analysis Stage: {}/{} tsfiles have been analyzed, progress: {}%",
              i + 1, tsfileNum, String.format("%.3f", (i + 1) * 100.00 / tsfileNum));
        }
      } catch (AuthException e) {
        setFailAnalysisForAuthException(analysis, e);
        return analysis;
      } catch (BufferUnderflowException e) {
        LOGGER.warn(
            "The file {} is not a valid tsfile. Please check the input file.", tsFile.getPath(), e);
        throw new SemanticException(
            String.format(
                "The file %s is not a valid tsfile. Please check the input file.",
                tsFile.getPath()));
      } catch (Exception e) {
        final String exceptionMessage =
            String.format(
                "The file %s is not a valid tsfile. Please check the input file. Detail: %s",
                tsFile.getPath(), e.getMessage() == null ? e.getClass().getName() : e.getMessage());
        LOGGER.warn(exceptionMessage, e);
        analysis.setFinishQueryAfterAnalyze(true);
        analysis.setFailStatus(RpcUtils.getStatus(TSStatusCode.LOAD_FILE_ERROR, exceptionMessage));
        return analysis;
      }
    }
    return analysis;
  }

  @Override
  protected void analyzeSingleTsFile(final File tsFile) throws IOException, AuthException {
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
          || reader.readFileMetadata().getTableSchemaMap().size() == 0) {
        throw new SemanticException("Attempted to load a tree-model TsFile into table-model.");
      }

      // construct tsfile resource
      final TsFileResource tsFileResource = new TsFileResource(tsFile);
      if (!tsFileResource.resourceFileExists()) {
        // it will be serialized in LoadSingleTsFileNode
        tsFileResource.updatePlanIndexes(reader.getMinPlanIndex());
        tsFileResource.updatePlanIndexes(reader.getMaxPlanIndex());
      } else {
        tsFileResource.deserialize();
      }

      for (Map.Entry<String, TableSchema> name2Schema :
          reader.readFileMetadata().getTableSchemaMap().entrySet()) {
        org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema realSchema =
            metadata
                .validateTableHeaderSchema(
                    database,
                    org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema
                        .fromTsFileTableSchema(name2Schema.getKey(), name2Schema.getValue()),
                    context,
                    true)
                .orElse(null);
        if (Objects.isNull(realSchema)) {
          LOGGER.warn("Failed to validata schema for table {}", name2Schema.getValue());
        }
      }

      long writePointCount = 0;

      final boolean isAutoCreateSchemaOrVerifySchemaEnabled =
          IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled() || isVerifySchema();
      while (timeseriesMetadataIterator.hasNext()) {
        final Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadata =
            timeseriesMetadataIterator.next();

        if (isAutoCreateSchemaOrVerifySchemaEnabled) {
          for (IDeviceID deviceId : device2TimeseriesMetadata.keySet()) {
            final ITableDeviceSchemaValidation tableSchemaValidation =
                createTableSchemaValidation(deviceId, this);
            metadata.validateDeviceSchema(tableSchemaValidation, context);
          }
        }

        if (!tsFileResource.resourceFileExists()) {
          TsFileResourceUtils.updateTsFileResource(device2TimeseriesMetadata, tsFileResource);
        }

        // TODO: how to get the correct write point count when
        //  !isAutoCreateSchemaOrVerifySchemaEnabled
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
        return Collections.singletonList(
            Arrays.copyOfRange(deviceId.getSegments(), 1, deviceId.getSegments().length));
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
