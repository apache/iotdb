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
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadAnalyzeException;
import org.apache.iotdb.db.exception.load.LoadAnalyzeTypeMismatchException;
import org.apache.iotdb.db.exception.load.LoadEmptyFileException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;
import org.apache.iotdb.db.storageengine.load.converter.LoadTsFileDataTypeConverter;
import org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.iotdb.commons.utils.FileUtils.copyFileWithMD5Check;
import static org.apache.iotdb.commons.utils.FileUtils.moveFileWithMD5Check;
import static org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor.DATABASE_NOT_SPECIFIED;
import static org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet.ANALYSIS;
import static org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet.ANALYSIS_ASYNC_MOVE;

public class LoadTsFileAnalyzer implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileAnalyzer.class);

  private static final LoadTsFileCostMetricsSet LOAD_TSFILE_COST_METRICS_SET =
      LoadTsFileCostMetricsSet.getInstance();

  final IPartitionFetcher partitionFetcher = ClusterPartitionFetcher.getInstance();
  final ISchemaFetcher schemaFetcher = ClusterSchemaFetcher.getInstance();
  private final Metadata metadata = LocalExecutionPlanner.getInstance().metadata;
  private final AccessControl accessControl = Coordinator.getInstance().getAccessControl();

  final MPPQueryContext context;

  // Statement related
  private final LoadTsFileStatement
      loadTsFileTreeStatement; // only used when constructed from tree model SQL
  private final LoadTsFile
      loadTsFileTableStatement; // only used when constructed from table model SQL
  private final boolean
      isTableModelStatement; // Whether the statement itself is table model or not (not the TsFiles)
  private final String statementString;
  private final boolean isGeneratedByPipe;

  private final List<File> tsFiles;
  private final List<Boolean> isMiniTsFile;
  private boolean isMiniTsFileConverted = false;
  private final List<Boolean> isTableModelTsFile;
  private int isTableModelTsFileReliableIndex = -1;

  // User specified configs
  private final int databaseLevel;
  private String databaseForTableData;
  private final boolean isAsyncLoad;
  private final boolean isVerifySchema;
  private final boolean isAutoCreateDatabase;
  private final boolean isDeleteAfterLoad;
  private final boolean isConvertOnTypeMismatch;
  private final long tabletConversionThresholdBytes;

  // Schema creators for tree and table
  private TreeSchemaAutoCreatorAndVerifier treeSchemaAutoCreatorAndVerifier;
  private LoadTsFileTableSchemaCache tableSchemaCache;

  public LoadTsFileAnalyzer(
      LoadTsFileStatement loadTsFileStatement, boolean isGeneratedByPipe, MPPQueryContext context) {
    this.context = context;

    this.loadTsFileTreeStatement = loadTsFileStatement;
    this.loadTsFileTableStatement = null;
    this.isTableModelStatement = false;
    this.statementString = loadTsFileStatement.toString();
    this.isGeneratedByPipe = isGeneratedByPipe;

    this.tsFiles = loadTsFileStatement.getTsFiles();
    this.isMiniTsFile = new ArrayList<>(Collections.nCopies(this.tsFiles.size(), false));
    this.isTableModelTsFile = new ArrayList<>(Collections.nCopies(this.tsFiles.size(), false));

    this.databaseLevel = loadTsFileStatement.getDatabaseLevel();
    this.databaseForTableData = loadTsFileStatement.getDatabase();
    this.isAsyncLoad = loadTsFileStatement.isAsyncLoad();
    this.isVerifySchema = loadTsFileStatement.isVerifySchema();
    this.isAutoCreateDatabase = loadTsFileStatement.isAutoCreateDatabase();
    this.isDeleteAfterLoad = loadTsFileStatement.isDeleteAfterLoad();
    this.isConvertOnTypeMismatch = loadTsFileStatement.isConvertOnTypeMismatch();
    this.tabletConversionThresholdBytes = loadTsFileStatement.getTabletConversionThresholdBytes();
  }

  public LoadTsFileAnalyzer(
      LoadTsFile loadTsFileTableStatement, boolean isGeneratedByPipe, MPPQueryContext context) {
    this.context = context;

    this.loadTsFileTreeStatement = null;
    this.loadTsFileTableStatement = loadTsFileTableStatement;
    this.isTableModelStatement = true;
    this.statementString = loadTsFileTableStatement.toString();
    this.isGeneratedByPipe = isGeneratedByPipe;

    this.tsFiles = loadTsFileTableStatement.getTsFiles();
    this.isMiniTsFile = new ArrayList<>(Collections.nCopies(this.tsFiles.size(), false));
    this.isTableModelTsFile = new ArrayList<>(Collections.nCopies(this.tsFiles.size(), false));

    this.databaseLevel = loadTsFileTableStatement.getDatabaseLevel();
    this.databaseForTableData = loadTsFileTableStatement.getDatabase();
    this.isAsyncLoad = loadTsFileTableStatement.isAsyncLoad();
    this.isVerifySchema = loadTsFileTableStatement.isVerifySchema();
    this.isAutoCreateDatabase = loadTsFileTableStatement.isAutoCreateDatabase();
    this.isDeleteAfterLoad = loadTsFileTableStatement.isDeleteAfterLoad();
    this.isConvertOnTypeMismatch = loadTsFileTableStatement.isConvertOnTypeMismatch();
    this.tabletConversionThresholdBytes =
        loadTsFileTableStatement.getTabletConversionThresholdBytes();
  }

  protected String getStatementString() {
    return statementString;
  }

  protected int getDatabaseLevel() {
    return databaseLevel;
  }

  protected boolean isVerifySchema() {
    return isVerifySchema;
  }

  protected boolean isAutoCreateDatabase() {
    return isAutoCreateDatabase;
  }

  protected boolean isConvertOnTypeMismatch() {
    return isConvertOnTypeMismatch;
  }

  public IAnalysis analyzeFileByFile(IAnalysis analysis) {
    if (!checkBeforeAnalyzeFileByFile(analysis)) {
      return analysis;
    }

    if (isAsyncLoad && doAsyncLoad(analysis)) {
      return analysis;
    }

    try {
      if (!doAnalyzeFileByFile(analysis)) {
        return analysis;
      }

      final long startTime = System.nanoTime();
      try {
        // flush remaining metadata of tree-model, currently no need for table-model
        if (treeSchemaAutoCreatorAndVerifier != null) {
          treeSchemaAutoCreatorAndVerifier.flush();
        }
      } finally {
        LoadTsFileCostMetricsSet.getInstance()
            .recordPhaseTimeCost(ANALYSIS, System.nanoTime() - startTime);
      }
    } catch (AuthException e) {
      setFailAnalysisForAuthException(analysis, e);
      return analysis;
    } catch (LoadAnalyzeException e) {
      executeTabletConversionOnException(analysis, e);
      return analysis;
    } catch (Exception e) {
      final String exceptionMessage =
          String.format(
              "Auto create or verify schema error when executing statement %s. Detail: %s.",
              getStatementString(),
              e.getMessage() == null ? e.getClass().getName() : e.getMessage());
      LOGGER.warn(exceptionMessage, e);
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(RpcUtils.getStatus(TSStatusCode.LOAD_FILE_ERROR, exceptionMessage));
      return analysis;
    }

    LOGGER.info("Load - Analysis Stage: all tsfiles have been analyzed.");

    setTsFileModelInfoToStatement();
    if (reconstructStatementIfMiniFileConverted()) {
      // All mini tsfiles are converted to tablets, so the analysis is finished.
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
      return analysis;
    }

    // Still some tsfiles are not converted to tablets, so the load process should continue.
    setRealStatement(analysis);
    return analysis;
  }

  private boolean checkBeforeAnalyzeFileByFile(IAnalysis analysis) {
    if (!Objects.equals(TSFileDescriptor.getInstance().getConfig().getEncryptType(), "UNENCRYPTED")
        && !Objects.equals(
            TSFileDescriptor.getInstance().getConfig().getEncryptType(),
            "org.apache.tsfile.encrypt.UNENCRYPTED")) {
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(
          RpcUtils.getStatus(
              TSStatusCode.LOAD_FILE_ERROR,
              "TsFile encryption is enabled, and the Load TsFile function is disabled"));
      return false;
    }

    // check if the system is read only
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      LOGGER.info(
          "LoadTsFileAnalyzer: Current datanode is read only, will try to convert to tablets and insert later.");
    }

    return true;
  }

  private boolean doAsyncLoad(final IAnalysis analysis) {
    final long startTime = System.nanoTime();
    try {
      final String[] loadActiveListeningDirs =
          IoTDBDescriptor.getInstance().getConfig().getLoadActiveListeningDirs();
      String targetFilePath = null;
      for (int i = 0, size = loadActiveListeningDirs == null ? 0 : loadActiveListeningDirs.length;
          i < size;
          i++) {
        if (loadActiveListeningDirs[i] != null) {
          targetFilePath = loadActiveListeningDirs[i];
          break;
        }
      }
      if (targetFilePath == null) {
        LOGGER.warn("Load active listening dir is not set. Will try sync load instead.");
        return false;
      }

      try {
        if (Objects.nonNull(databaseForTableData)
            || (Objects.nonNull(context) && context.getDatabaseName().isPresent())) {
          loadTsFilesAsyncToTargetDir(
              new File(
                  targetFilePath,
                  databaseForTableData =
                      Objects.nonNull(databaseForTableData)
                          ? databaseForTableData
                          : context.getDatabaseName().get()),
              tsFiles);
        } else {
          loadTsFilesAsyncToTargetDir(new File(targetFilePath), tsFiles);
        }
      } catch (Exception e) {
        LOGGER.warn(
            "Failed to async load tsfiles {} to target dir {}. Will try sync load instead.",
            tsFiles,
            targetFilePath,
            e);
        return false;
      }

      analysis.setFinishQueryAfterAnalyze(true);
      setRealStatement(analysis);
      return true;
    } finally {
      LoadTsFileCostMetricsSet.getInstance()
          .recordPhaseTimeCost(ANALYSIS_ASYNC_MOVE, System.nanoTime() - startTime);
    }
  }

  private void loadTsFilesAsyncToTargetDir(final File targetDir, final List<File> files)
      throws IOException {
    for (final File file : files) {
      if (file == null) {
        continue;
      }

      loadTsFileAsyncToTargetDir(targetDir, file);
      loadTsFileAsyncToTargetDir(targetDir, new File(file.getAbsolutePath() + ".resource"));
      loadTsFileAsyncToTargetDir(targetDir, new File(file.getAbsolutePath() + ".mods"));
    }
  }

  private void loadTsFileAsyncToTargetDir(final File targetDir, final File file)
      throws IOException {
    if (!file.exists()) {
      return;
    }
    RetryUtils.retryOnException(
        () -> {
          if (isDeleteAfterLoad) {
            moveFileWithMD5Check(file, targetDir);
          } else {
            copyFileWithMD5Check(file, targetDir);
          }
          return null;
        });
  }

  private boolean doAnalyzeFileByFile(IAnalysis analysis) {
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

      final long startTime = System.nanoTime();
      try {
        analyzeSingleTsFile(tsFile, i);
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Load - Analysis Stage: {}/{} tsfiles have been analyzed, progress: {}%",
              i + 1, tsfileNum, String.format("%.3f", (i + 1) * 100.00 / tsfileNum));
        }
      } catch (AuthException e) {
        setFailAnalysisForAuthException(analysis, e);
        return false;
      } catch (LoadAnalyzeException e) {
        executeTabletConversionOnException(analysis, e);
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
      } finally {
        LoadTsFileCostMetricsSet.getInstance()
            .recordPhaseTimeCost(ANALYSIS, System.nanoTime() - startTime);
      }
    }

    return true;
  }

  private void analyzeSingleTsFile(final File tsFile, int i) throws Exception {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      // check whether the tsfile is tree-model or not
      final Map<String, TableSchema> tableSchemaMap = reader.getTableSchemaMap();
      final boolean isTableModelFile = Objects.nonNull(tableSchemaMap) && !tableSchemaMap.isEmpty();
      LOGGER.info(
          "TsFile {} is a {}-model file.", tsFile.getPath(), isTableModelFile ? "table" : "tree");

      // can be reused when constructing tsfile resource
      final TsFileSequenceReaderTimeseriesMetadataIterator timeseriesMetadataIterator =
          new TsFileSequenceReaderTimeseriesMetadataIterator(
              reader,
              !isTableModelFile, // currently we only need chunk metadata for tree model files
              IoTDBDescriptor.getInstance()
                  .getConfig()
                  .getLoadTsFileAnalyzeSchemaBatchReadTimeSeriesMetadataCount());

      // check if the tsfile is empty
      if (!timeseriesMetadataIterator.hasNext()) {
        throw new LoadEmptyFileException(tsFile.getAbsolutePath());
      }

      // check whether the encrypt type of the tsfile is supported
      final EncryptParameter param = reader.getEncryptParam();
      if (!Objects.equals(param.getType(), EncryptUtils.getEncryptParameter().getType())
          || !Arrays.equals(param.getKey(), EncryptUtils.getEncryptParameter().getKey())) {
        throw new SemanticException("The encryption way of the TsFile is not supported.");
      }

      this.isTableModelTsFile.set(i, isTableModelFile);
      this.isTableModelTsFileReliableIndex = i;

      if (0 <= tabletConversionThresholdBytes
          && tsFile.length() <= tabletConversionThresholdBytes
          && handleSingleMiniFile(i)) {
        return;
      }

      if (isTableModelFile) {
        doAnalyzeSingleTableFile(tsFile, reader, timeseriesMetadataIterator, tableSchemaMap);
      } else {
        doAnalyzeSingleTreeFile(tsFile, reader, timeseriesMetadataIterator);
      }
    } catch (final LoadEmptyFileException loadEmptyFileException) {
      LOGGER.warn("Empty file detected, will skip loading this file: {}", tsFile.getAbsolutePath());
      if (isDeleteAfterLoad) {
        FileUtils.deleteQuietly(tsFile);
      }
    }
  }

  private boolean handleSingleMiniFile(final int i) throws FileNotFoundException {
    final long startTime = System.nanoTime();
    try {
      final LoadTsFileDataTypeConverter loadTsFileDataTypeConverter =
          new LoadTsFileDataTypeConverter(isGeneratedByPipe);

      final TSStatus status =
          isTableModelTsFile.get(i)
              ? loadTsFileDataTypeConverter
                  .convertForTableModel(
                      new LoadTsFile(null, tsFiles.get(i).getPath(), Collections.emptyMap())
                          .setDatabase(databaseForTableData)
                          .setDeleteAfterLoad(isDeleteAfterLoad)
                          .setConvertOnTypeMismatch(isConvertOnTypeMismatch))
                  .orElse(null)
              : loadTsFileDataTypeConverter
                  .convertForTreeModel(
                      new LoadTsFileStatement(tsFiles.get(i).getPath())
                          .setDeleteAfterLoad(isDeleteAfterLoad)
                          .setConvertOnTypeMismatch(isConvertOnTypeMismatch))
                  .orElse(null);

      if (status == null || !loadTsFileDataTypeConverter.isSuccessful(status)) {
        LOGGER.warn(
            "Load: Failed to convert mini tsfile {} to tablets from statement {}. Status: {}.",
            tsFiles.get(i).getPath(),
            isTableModelStatement ? loadTsFileTableStatement : loadTsFileTreeStatement,
            status);
        return false;
      }

      // A mark of successful conversion
      isMiniTsFile.set(i, Boolean.TRUE);
      isMiniTsFileConverted = true;

      addTsFileResource(null);
      addWritePointCount(0);
      return true;
    } finally {
      LOAD_TSFILE_COST_METRICS_SET.recordPhaseTimeCost(
          LoadTsFileCostMetricsSet.ANALYSIS_CAST_TABLETS, System.nanoTime() - startTime);
    }
  }

  private void doAnalyzeSingleTreeFile(
      final File tsFile,
      final TsFileSequenceReader reader,
      final TsFileSequenceReaderTimeseriesMetadataIterator timeseriesMetadataIterator)
      throws IOException, LoadAnalyzeException, AuthException {
    // construct tsfile resource
    final TsFileResource tsFileResource = constructTsFileResource(reader, tsFile);

    long writePointCount = 0;

    getOrCreateTreeSchemaVerifier().setCurrentModificationsAndTimeIndex(tsFileResource, reader);

    final boolean isAutoCreateSchemaOrVerifySchemaEnabled =
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled() || isVerifySchema();
    while (timeseriesMetadataIterator.hasNext()) {
      final Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadata =
          timeseriesMetadataIterator.next();

      // Update time index no matter if resource file exists or not, because resource file may be
      // untrusted
      TsFileResourceUtils.updateTsFileResource(
          device2TimeseriesMetadata,
          tsFileResource,
          IoTDBDescriptor.getInstance().getConfig().isCacheLastValuesForLoad());
      getOrCreateTreeSchemaVerifier().setCurrentTimeIndex(tsFileResource.getTimeIndex());

      if (isAutoCreateSchemaOrVerifySchemaEnabled) {
        getOrCreateTreeSchemaVerifier().autoCreateAndVerify(reader, device2TimeseriesMetadata);
      }

      // TODO: how to get the correct write point count when
      //  !isAutoCreateSchemaOrVerifySchemaEnabled
      writePointCount += getWritePointCount(device2TimeseriesMetadata);
    }
    if (isAutoCreateSchemaOrVerifySchemaEnabled) {
      getOrCreateTreeSchemaVerifier().flushAndClearDeviceIsAlignedCacheIfNecessary();
    }

    TimestampPrecisionUtils.checkTimestampPrecision(tsFileResource.getFileEndTime());
    tsFileResource.setStatus(TsFileResourceStatus.NORMAL);

    addTsFileResource(tsFileResource);
    addWritePointCount(writePointCount);
  }

  private void doAnalyzeSingleTableFile(
      final File tsFile,
      final TsFileSequenceReader reader,
      final TsFileSequenceReaderTimeseriesMetadataIterator timeseriesMetadataIterator,
      final Map<String, TableSchema> tableSchemaMap)
      throws IOException, LoadAnalyzeException {
    // construct tsfile resource
    final TsFileResource tsFileResource = constructTsFileResource(reader, tsFile);

    long writePointCount = 0;

    if (Objects.isNull(databaseForTableData)) {
      // If database is not specified, use the database from current session.
      // If still not specified, throw an exception.
      final Optional<String> dbName = context.getDatabaseName();
      if (dbName.isPresent()) {
        databaseForTableData = dbName.get();
        if (isTableModelStatement) {
          loadTsFileTableStatement.setDatabase(dbName.get());
        } else {
          loadTsFileTreeStatement.setDatabase(dbName.get());
        }
      } else {
        throw new SemanticException(DATABASE_NOT_SPECIFIED);
      }
    }

    getOrCreateTableSchemaCache().setDatabase(databaseForTableData);
    getOrCreateTableSchemaCache().setTableSchemaMap(tableSchemaMap);
    getOrCreateTableSchemaCache().setCurrentModificationsAndTimeIndex(tsFileResource, reader);

    while (timeseriesMetadataIterator.hasNext()) {
      final Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadata =
          timeseriesMetadataIterator.next();

      // Update time index no matter if resource file exists or not, because resource file may be
      // untrusted
      TsFileResourceUtils.updateTsFileResource(
          device2TimeseriesMetadata,
          tsFileResource,
          IoTDBDescriptor.getInstance().getConfig().isCacheLastValuesForLoad());
      getOrCreateTableSchemaCache().setCurrentTimeIndex(tsFileResource.getTimeIndex());

      for (IDeviceID deviceId : device2TimeseriesMetadata.keySet()) {
        getOrCreateTableSchemaCache().autoCreateAndVerify(deviceId);
      }

      writePointCount += getWritePointCount(device2TimeseriesMetadata);
    }

    getOrCreateTableSchemaCache().flush();
    getOrCreateTableSchemaCache().clearIdColumnMapper();

    TimestampPrecisionUtils.checkTimestampPrecision(tsFileResource.getFileEndTime());
    tsFileResource.setStatus(TsFileResourceStatus.NORMAL);

    addTsFileResource(tsFileResource);
    addWritePointCount(writePointCount);
  }

  private TsFileResource constructTsFileResource(
      final TsFileSequenceReader reader, final File tsFile) throws IOException {
    final TsFileResource tsFileResource = new TsFileResource(tsFile);
    if (!tsFileResource.resourceFileExists()) {
      // it will be serialized in LoadSingleTsFileNode
      tsFileResource.updatePlanIndexes(reader.getMinPlanIndex());
      tsFileResource.updatePlanIndexes(reader.getMaxPlanIndex());
    } else {
      tsFileResource.deserialize();
      // Reset tsfileResource's isGeneratedByPipe mark to prevent deserializing the wrong mark.
      // If this tsfile is loaded by a pipe receiver, the correct mark will be added in
      // `listenToTsFile`
      tsFileResource.setGeneratedByPipe(isGeneratedByPipe);
    }
    return tsFileResource;
  }

  private TreeSchemaAutoCreatorAndVerifier getOrCreateTreeSchemaVerifier() {
    if (treeSchemaAutoCreatorAndVerifier == null) {
      treeSchemaAutoCreatorAndVerifier = new TreeSchemaAutoCreatorAndVerifier(this);
    }
    return treeSchemaAutoCreatorAndVerifier;
  }

  private LoadTsFileTableSchemaCache getOrCreateTableSchemaCache() {
    if (tableSchemaCache == null) {
      tableSchemaCache = new LoadTsFileTableSchemaCache(metadata, context, isAutoCreateDatabase);
    }
    return tableSchemaCache;
  }

  private void addTsFileResource(TsFileResource tsFileResource) {
    if (isTableModelStatement) {
      loadTsFileTableStatement.addTsFileResource(tsFileResource);
    } else {
      loadTsFileTreeStatement.addTsFileResource(tsFileResource);
    }
  }

  private static long getWritePointCount(
      Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadata) {
    return device2TimeseriesMetadata.values().stream()
        .flatMap(List::stream)
        .mapToLong(t -> t.getStatistics().getCount())
        .sum();
  }

  private void addWritePointCount(long writePointCount) {
    if (isTableModelStatement) {
      loadTsFileTableStatement.addWritePointCount(writePointCount);
    } else {
      loadTsFileTreeStatement.addWritePointCount(writePointCount);
    }
  }

  private void setTsFileModelInfoToStatement() {
    if (isTableModelStatement) {
      this.loadTsFileTableStatement.setIsTableModel(this.isTableModelTsFile);
    } else {
      this.loadTsFileTreeStatement.setIsTableModel(this.isTableModelTsFile);
    }
  }

  private boolean reconstructStatementIfMiniFileConverted() {
    if (!isMiniTsFileConverted) {
      return false;
    }

    return isTableModelStatement
        ? loadTsFileTableStatement.reconstructStatementIfMiniFileConverted(isMiniTsFile)
        : loadTsFileTreeStatement.reconstructStatementIfMiniFileConverted(isMiniTsFile);
  }

  private void setRealStatement(IAnalysis analysis) {
    if (isTableModelStatement) {
      // Do nothing by now.
    } else {
      analysis.setRealStatement(loadTsFileTreeStatement);
    }
  }

  private void setFailAnalysisForAuthException(IAnalysis analysis, AuthException e) {
    analysis.setFinishQueryAfterAnalyze(true);
    analysis.setFailStatus(RpcUtils.getStatus(e.getCode(), e.getMessage()));
  }

  private void executeTabletConversionOnException(
      final IAnalysis analysis, final LoadAnalyzeException e) {
    if (shouldSkipConversion(e)) {
      analysis.setFailStatus(
          new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()).setMessage(e.getMessage()));
      analysis.setFinishQueryAfterAnalyze(true);
      setRealStatement(analysis);
      return;
    }

    if (isTableModelTsFileReliableIndex < tsFiles.size() - 1) {
      try {
        getFileModelInfoBeforeTabletConversion();
      } catch (Exception e1) {
        LOGGER.warn(
            "Load: Failed to convert to tablets from statement {} because failed to read model info from file, message: {}.",
            isTableModelStatement ? loadTsFileTableStatement : loadTsFileTreeStatement,
            e1.getMessage());
        analysis.setFailStatus(
            new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()).setMessage(e.getMessage()));
        analysis.setFinishQueryAfterAnalyze(true);
        setRealStatement(analysis);
        return;
      }
    }

    final LoadTsFileDataTypeConverter loadTsFileDataTypeConverter =
        new LoadTsFileDataTypeConverter(isGeneratedByPipe);

    for (int i = 0; i < tsFiles.size(); i++) {
      final long startTime = System.nanoTime();
      try {
        final TSStatus status =
            isTableModelTsFile.get(i)
                ? loadTsFileDataTypeConverter
                    .convertForTableModel(
                        new LoadTsFile(null, tsFiles.get(i).getPath(), Collections.emptyMap())
                            .setDatabase(databaseForTableData)
                            .setDeleteAfterLoad(isDeleteAfterLoad)
                            .setConvertOnTypeMismatch(isConvertOnTypeMismatch))
                    .orElse(null)
                : loadTsFileDataTypeConverter
                    .convertForTreeModel(
                        new LoadTsFileStatement(tsFiles.get(i).getPath())
                            .setDeleteAfterLoad(isDeleteAfterLoad)
                            .setConvertOnTypeMismatch(isConvertOnTypeMismatch))
                    .orElse(null);

        if (status == null) {
          LOGGER.warn(
              "Load: Failed to convert to tablets from statement {}. Status is null.",
              isTableModelStatement ? loadTsFileTableStatement : loadTsFileTreeStatement);
          analysis.setFailStatus(
              new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode())
                  .setMessage(e.getMessage()));
          break;
        } else if (!loadTsFileDataTypeConverter.isSuccessful(status)) {
          LOGGER.warn(
              "Load: Failed to convert to tablets from statement {}. Status: {}",
              isTableModelStatement ? loadTsFileTableStatement : loadTsFileTreeStatement,
              status);
          analysis.setFailStatus(status);
          break;
        }
      } catch (final Exception e2) {
        LOGGER.warn(
            "Load: Failed to convert to tablets from statement {} because exception: {}",
            isTableModelStatement ? loadTsFileTableStatement : loadTsFileTreeStatement,
            e2.getMessage());
        analysis.setFailStatus(
            new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()).setMessage(e.getMessage()));
        break;
      } finally {
        LOAD_TSFILE_COST_METRICS_SET.recordPhaseTimeCost(
            LoadTsFileCostMetricsSet.ANALYSIS_CAST_TABLETS, System.nanoTime() - startTime);
      }
    }

    analysis.setFinishQueryAfterAnalyze(true);
    setRealStatement(analysis);
  }

  private boolean shouldSkipConversion(LoadAnalyzeException e) {
    return (e instanceof LoadAnalyzeTypeMismatchException) && !isConvertOnTypeMismatch;
  }

  private void getFileModelInfoBeforeTabletConversion() throws IOException {
    for (int i = isTableModelTsFileReliableIndex + 1; i < tsFiles.size(); i++) {
      try (final TsFileSequenceReader reader =
          new TsFileSequenceReader(tsFiles.get(i).getAbsolutePath(), true)) {
        final Map<String, TableSchema> tableSchemaMap = reader.getTableSchemaMap();
        isTableModelTsFile.set(i, Objects.nonNull(tableSchemaMap) && !tableSchemaMap.isEmpty());
        isTableModelTsFileReliableIndex = i;
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (treeSchemaAutoCreatorAndVerifier != null) {
      treeSchemaAutoCreatorAndVerifier.close();
    }
    if (tableSchemaCache != null) {
      tableSchemaCache.close();
    }
  }
}
