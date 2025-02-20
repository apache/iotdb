/*
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

package org.apache.iotdb.db.queryengine.plan.analyze.load;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.exception.load.LoadAnalyzeException;
import org.apache.iotdb.db.exception.load.LoadAnalyzeTypeMismatchException;
import org.apache.iotdb.db.exception.load.LoadReadOnlyException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.converter.LoadTsFileDataTypeConverter;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.List;
import java.util.Map;

public abstract class LoadTsFileAnalyzer implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileAnalyzer.class);

  // These are only used when constructed from tree model SQL
  private final LoadTsFileStatement loadTsFileTreeStatement;
  // These are only used when constructed from table model SQL
  private final LoadTsFile loadTsFileTableStatement;

  private final boolean isTableModelStatement;

  private final boolean isGeneratedByPipe;

  protected final List<File> tsFiles;
  private final List<File> tabletConvertionList = new java.util.ArrayList<>();
  protected final String statementString;
  protected final boolean isVerifySchema;

  protected final boolean isDeleteAfterLoad;

  protected final boolean isConvertOnTypeMismatch;

  protected final int tabletConversionThreshold;

  protected final boolean isAutoCreateDatabase;

  protected final int databaseLevel;

  protected final String database;

  final MPPQueryContext context;

  final IPartitionFetcher partitionFetcher = ClusterPartitionFetcher.getInstance();
  final ISchemaFetcher schemaFetcher = ClusterSchemaFetcher.getInstance();

  LoadTsFileAnalyzer(
      LoadTsFileStatement loadTsFileStatement, boolean isGeneratedByPipe, MPPQueryContext context) {
    this.loadTsFileTreeStatement = loadTsFileStatement;
    this.tsFiles = loadTsFileStatement.getTsFiles();
    this.statementString = loadTsFileStatement.toString();
    this.isVerifySchema = loadTsFileStatement.isVerifySchema();
    this.isDeleteAfterLoad = loadTsFileStatement.isDeleteAfterLoad();
    this.isConvertOnTypeMismatch = loadTsFileStatement.isConvertOnTypeMismatch();
    this.tabletConversionThreshold = loadTsFileStatement.getTabletConversionThreshold();
    this.isAutoCreateDatabase = loadTsFileStatement.isAutoCreateDatabase();
    this.databaseLevel = loadTsFileStatement.getDatabaseLevel();
    this.database = loadTsFileStatement.getDatabase();

    this.loadTsFileTableStatement = null;
    this.isTableModelStatement = false;
    this.isGeneratedByPipe = isGeneratedByPipe;
    this.context = context;
  }

  LoadTsFileAnalyzer(
      LoadTsFile loadTsFileTableStatement, boolean isGeneratedByPipe, MPPQueryContext context) {
    this.loadTsFileTableStatement = loadTsFileTableStatement;
    this.tsFiles = loadTsFileTableStatement.getTsFiles();
    this.statementString = loadTsFileTableStatement.toString();
    this.isVerifySchema = loadTsFileTableStatement.isVerifySchema();
    this.isDeleteAfterLoad = loadTsFileTableStatement.isDeleteAfterLoad();
    this.isConvertOnTypeMismatch = loadTsFileTableStatement.isConvertOnTypeMismatch();
    this.tabletConversionThreshold = loadTsFileTableStatement.getTabletConversionThreshold();
    this.isAutoCreateDatabase = loadTsFileTableStatement.isAutoCreateDatabase();
    this.databaseLevel = loadTsFileTableStatement.getDatabaseLevel();
    this.database = loadTsFileTableStatement.getDatabase();

    this.loadTsFileTreeStatement = null;
    this.isTableModelStatement = true;
    this.isGeneratedByPipe = isGeneratedByPipe;
    this.context = context;
  }

  public abstract IAnalysis analyzeFileByFile(IAnalysis analysis);

  protected boolean doAnalyzeFileByFile(IAnalysis analysis) {
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

      if (tsFile.length() < tabletConversionThreshold) {
        tabletConvertionList.add(tsFile);
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Load - Analysis Stage: {}/{} tsfiles have been analyzed, {} tsfiles have been added to the conversion list, progress: {}%",
              i + 1,
              tsfileNum,
              tabletConvertionList.size(),
              String.format("%.3f", (i + 1) * 100.00 / tsfileNum));
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
        return false;
      } catch (LoadAnalyzeException e) {
        executeTabletConversion(analysis, e);
        // just return false to STOP the analysis process,
        // the real result on the conversion will be set in the analysis.
        return false;
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
                "Loading file %s failed. Detail: %s",
                tsFile.getPath(), e.getMessage() == null ? e.getClass().getName() : e.getMessage());
        LOGGER.warn(exceptionMessage, e);
        analysis.setFinishQueryAfterAnalyze(true);
        analysis.setFailStatus(RpcUtils.getStatus(TSStatusCode.LOAD_FILE_ERROR, exceptionMessage));
        return false;
      }
    }

    return handleTabletConversionList(analysis);
  }

  protected abstract void analyzeSingleTsFile(final File tsFile)
      throws IOException, AuthException, LoadAnalyzeException;

  private boolean handleTabletConversionList(IAnalysis analysis) {
    if (tabletConvertionList.isEmpty()) {
      return true;
    }

    // 1. all mini files are converted to tablets
    setStatementTsFiles(tabletConvertionList);
    executeTabletConversion(
        analysis, new LoadAnalyzeException("Failed to convert mini file to tablet"));
    if (analysis.isFailed()) {
      return false;
    }

    // 2. remove the converted mini files from the tsFiles and load the rest
    tsFiles.removeAll(tabletConvertionList);
    setStatementTsFiles(tsFiles);
    analysis.setFinishQueryAfterAnalyze(false);
    return true;
  }

  protected void executeTabletConversion(final IAnalysis analysis, final LoadAnalyzeException e) {
    if (shouldSkipConversion(e)) {
      analysis.setFailStatus(
          new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()).setMessage(e.getMessage()));
      analysis.setFinishQueryAfterAnalyze(true);
      setRealStatement(analysis);
      return;
    }

    LoadTsFileDataTypeConverter loadTsFileDataTypeConverter =
        new LoadTsFileDataTypeConverter(isGeneratedByPipe);
    TSStatus status = doConversion(loadTsFileDataTypeConverter);

    if (status == null) {
      LOGGER.warn(
          "Load: Failed to convert to tablets from statement {}. Status is null.",
          isTableModelStatement ? loadTsFileTableStatement : loadTsFileTreeStatement);
      analysis.setFailStatus(
          new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()).setMessage(e.getMessage()));
    } else if (!loadTsFileDataTypeConverter.isSuccessful(status)) {
      LOGGER.warn(
          "Load: Failed to convert to tablets from statement {}. Status: {}",
          isTableModelStatement ? loadTsFileTableStatement : loadTsFileTreeStatement,
          status);
      analysis.setFailStatus(status);
    }

    analysis.setFinishQueryAfterAnalyze(true);
    setRealStatement(analysis);
  }

  private TSStatus doConversion(LoadTsFileDataTypeConverter converter) {
    return isTableModelStatement
        ? converter.convertForTableModel(loadTsFileTableStatement).orElse(null)
        : converter.convertForTreeModel(loadTsFileTreeStatement).orElse(null);
  }

  private boolean shouldSkipConversion(LoadAnalyzeException e) {
    return (e instanceof LoadAnalyzeTypeMismatchException) && !isConvertOnTypeMismatch;
  }

  private void setStatementTsFiles(List<File> files) {
    if (isTableModelStatement) {
      loadTsFileTableStatement.setTsFiles(files);
    } else {
      loadTsFileTreeStatement.setTsFiles(files);
    }
  }

  protected void setRealStatement(IAnalysis analysis) {
    if (isTableModelStatement) {
      // Do nothing by now.
    } else {
      analysis.setRealStatement(loadTsFileTreeStatement);
    }
  }

  protected String getStatementString() {
    return statementString;
  }

  protected TsFileResource constructTsFileResource(
      final TsFileSequenceReader reader, final File tsFile) throws IOException {
    final TsFileResource tsFileResource = new TsFileResource(tsFile);
    if (!tsFileResource.resourceFileExists()) {
      // it will be serialized in LoadSingleTsFileNode
      tsFileResource.updatePlanIndexes(reader.getMinPlanIndex());
      tsFileResource.updatePlanIndexes(reader.getMaxPlanIndex());
    } else {
      tsFileResource.deserialize();
    }
    return tsFileResource;
  }

  protected void addTsFileResource(TsFileResource tsFileResource) {
    if (isTableModelStatement) {
      loadTsFileTableStatement.addTsFileResource(tsFileResource);
    } else {
      loadTsFileTreeStatement.addTsFileResource(tsFileResource);
    }
  }

  protected void addWritePointCount(long writePointCount) {
    if (isTableModelStatement) {
      loadTsFileTableStatement.addWritePointCount(writePointCount);
    } else {
      loadTsFileTreeStatement.addWritePointCount(writePointCount);
    }
  }

  protected boolean isVerifySchema() {
    return isVerifySchema;
  }

  protected boolean isConvertOnTypeMismatch() {
    return isConvertOnTypeMismatch;
  }

  protected boolean isAutoCreateDatabase() {
    return isAutoCreateDatabase;
  }

  protected int getDatabaseLevel() {
    return databaseLevel;
  }

  protected long getWritePointCount(
      Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadata) {
    return device2TimeseriesMetadata.values().stream()
        .flatMap(List::stream)
        .mapToLong(t -> t.getStatistics().getCount())
        .sum();
  }

  protected void setFailAnalysisForAuthException(IAnalysis analysis, AuthException e) {
    analysis.setFinishQueryAfterAnalyze(true);
    analysis.setFailStatus(RpcUtils.getStatus(e.getCode(), e.getMessage()));
  }

  protected void checkBeforeAnalyzeFileByFile(IAnalysis analysis) {
    if (TSFileDescriptor.getInstance().getConfig().getEncryptFlag()) {
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(
          RpcUtils.getStatus(
              TSStatusCode.LOAD_FILE_ERROR,
              "TSFile encryption is enabled, and the Load TSFile function is disabled"));
      return;
    }

    // check if the system is read only
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(
          RpcUtils.getStatus(TSStatusCode.SYSTEM_READ_ONLY, LoadReadOnlyException.MESSAGE));
      return;
    }
  }
}
