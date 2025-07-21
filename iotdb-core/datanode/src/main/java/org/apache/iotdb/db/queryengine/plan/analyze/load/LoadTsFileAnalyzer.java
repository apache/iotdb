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
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.LoadAnalyzeException;
import org.apache.iotdb.db.exception.LoadAnalyzeTypeMismatchException;
import org.apache.iotdb.db.exception.load.LoadEmptyFileException;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.exception.load.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.SchemaValidator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDatabaseStatement;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;
import org.apache.iotdb.db.storageengine.load.converter.LoadTsFileDataTypeConverter;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryBlock;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryManager;
import org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.db.utils.constant.SqlConstant;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
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
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.utils.FileUtils.copyFileWithMD5Check;
import static org.apache.iotdb.commons.utils.FileUtils.moveFileWithMD5Check;
import static org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet.ANALYSIS;
import static org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet.ANALYSIS_ASYNC_MOVE;

public class LoadTsFileAnalyzer implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileAnalyzer.class);

  private static final LoadTsFileCostMetricsSet LOAD_TSFILE_COST_METRICS_SET =
      LoadTsFileCostMetricsSet.getInstance();

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();
  private static final int BATCH_FLUSH_TIME_SERIES_NUMBER;
  private static final long ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES;
  private static final long FLUSH_ALIGNED_CACHE_MEMORY_SIZE_IN_BYTES;

  static {
    final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
    BATCH_FLUSH_TIME_SERIES_NUMBER = CONFIG.getLoadTsFileAnalyzeSchemaBatchFlushTimeSeriesNumber();
    ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES =
        CONFIG.getLoadTsFileAnalyzeSchemaMemorySizeInBytes() <= 0
            ? ((long) BATCH_FLUSH_TIME_SERIES_NUMBER) << 10
            : CONFIG.getLoadTsFileAnalyzeSchemaMemorySizeInBytes();
    FLUSH_ALIGNED_CACHE_MEMORY_SIZE_IN_BYTES = ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES >> 1;
  }

  private final LoadTsFileStatement loadTsFileStatement;
  private final MPPQueryContext context;

  private final IPartitionFetcher partitionFetcher;
  private final ISchemaFetcher schemaFetcher;

  private final SchemaAutoCreatorAndVerifier schemaAutoCreatorAndVerifier;

  private final boolean isGeneratedByPipe;

  private final List<File> tsFiles;
  private final List<Boolean> isMiniTsFile;
  private boolean isMiniTsFileConverted = false;

  // User specified configs
  private final int databaseLevel;
  private final boolean isAsyncLoad;
  private final boolean isVerifySchema;
  private final boolean isAutoCreateDatabase;
  private final boolean isDeleteAfterLoad;
  private final boolean isConvertOnTypeMismatch;
  private final long tabletConversionThresholdBytes;

  public LoadTsFileAnalyzer(
      LoadTsFileStatement loadTsFileStatement,
      MPPQueryContext context,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    this.loadTsFileStatement = loadTsFileStatement;
    this.context = context;

    this.partitionFetcher = partitionFetcher;
    this.schemaFetcher = schemaFetcher;

    this.schemaAutoCreatorAndVerifier = new SchemaAutoCreatorAndVerifier();

    this.isGeneratedByPipe = loadTsFileStatement.isGeneratedByPipe();

    this.tsFiles = loadTsFileStatement.getTsFiles();
    this.isMiniTsFile = new ArrayList<>(Collections.nCopies(this.tsFiles.size(), false));

    this.databaseLevel = loadTsFileStatement.getDatabaseLevel();
    this.isAsyncLoad = loadTsFileStatement.isAsyncLoad();
    this.isVerifySchema = loadTsFileStatement.isVerifySchema();
    this.isAutoCreateDatabase = loadTsFileStatement.isAutoCreateDatabase();
    this.isDeleteAfterLoad = loadTsFileStatement.isDeleteAfterLoad();
    this.isConvertOnTypeMismatch = loadTsFileStatement.isConvertOnTypeMismatch();
    this.tabletConversionThresholdBytes = loadTsFileStatement.getTabletConversionThresholdBytes();
  }

  public Analysis analyzeFileByFile(Analysis analysis) {
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
        schemaAutoCreatorAndVerifier.flush();
      } finally {
        LOAD_TSFILE_COST_METRICS_SET.recordPhaseTimeCost(
            LoadTsFileCostMetricsSet.ANALYSIS, System.nanoTime() - startTime);
      }
    } catch (AuthException e) {
      return setFailAnalysisForAuthException(analysis, e);
    } catch (LoadAnalyzeTypeMismatchException e) {
      executeTabletConversionOnException(analysis, e);
      // just return false to STOP the analysis process,
      // the real result on the conversion will be set in the analysis.
      return analysis;
    } catch (Exception e) {
      final String exceptionMessage =
          String.format(
              "Auto create or verify schema error when executing statement %s. Detail: %s.",
              loadTsFileStatement,
              e.getMessage() == null ? e.getClass().getName() : e.getMessage());
      LOGGER.warn(exceptionMessage, e);
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(RpcUtils.getStatus(TSStatusCode.LOAD_FILE_ERROR, exceptionMessage));
      return analysis;
    }

    LOGGER.info("Load - Analysis Stage: all tsfiles have been analyzed.");

    if (reconstructStatementIfMiniFileConverted()) {
      // All mini tsfiles are converted to tablets, so the analysis is finished.
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
      return analysis;
    }

    // data partition will be queried in the scheduler
    analysis.setStatement(loadTsFileStatement);
    return analysis;
  }

  private boolean doAsyncLoad(final Analysis analysis) {
    long startTime = System.nanoTime();
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
        loadTsFilesAsyncToTargetDir(new File(targetFilePath), tsFiles);
      } catch (Exception e) {
        LOGGER.warn(
            "Failed to async load tsfiles {} to target dir {}. Will try sync load instead.",
            tsFiles,
            targetFilePath,
            e);
        return false;
      }

      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
      analysis.setStatement(loadTsFileStatement);
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

  private boolean checkBeforeAnalyzeFileByFile(Analysis analysis) {
    // check if the system is read only
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      LOGGER.info(
          "LoadTsFileAnalyzer: Current datanode is read only, will try to convert to tablets and insert later.");
    }

    return true;
  }

  private boolean doAnalyzeFileByFile(Analysis analysis) {
    // analyze tsfile metadata file by file
    for (int i = 0, tsfileNum = loadTsFileStatement.getTsFiles().size(); i < tsfileNum; i++) {
      final File tsFile = loadTsFileStatement.getTsFiles().get(i);

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
      } catch (LoadAnalyzeTypeMismatchException e) {
        executeTabletConversionOnException(analysis, e);
        // just return false to STOP the analysis process,
        // the real result on the conversion will be set in the analysis.
        return false;
      } catch (Exception e) {
        final String exceptionMessage =
            String.format(
                "The file %s is not a valid tsfile. Please check the input file. Detail: %s",
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

  private void analyzeSingleTsFile(final File tsFile, int index) throws Exception {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      // can be reused when constructing tsfile resource
      final TsFileSequenceReaderTimeseriesMetadataIterator timeseriesMetadataIterator =
          new TsFileSequenceReaderTimeseriesMetadataIterator(
              reader,
              true,
              IoTDBDescriptor.getInstance()
                  .getConfig()
                  .getLoadTsFileAnalyzeSchemaBatchReadTimeSeriesMetadataCount());

      // check if the tsfile is empty
      if (!timeseriesMetadataIterator.hasNext()) {
        throw new LoadEmptyFileException(tsFile.getAbsolutePath());
      }

      if (0 <= tabletConversionThresholdBytes
          && tsFile.length() <= tabletConversionThresholdBytes
          && handleSingleMiniFile(index)) {
        return;
      }

      doAnalyzeSingleFile(tsFile, reader, timeseriesMetadataIterator);
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
          new LoadTsFileDataTypeConverter(context, isGeneratedByPipe);

      final TSStatus status =
          loadTsFileDataTypeConverter
              .convertForTreeModel(
                  new LoadTsFileStatement(tsFiles.get(i).getPath())
                      .setDeleteAfterLoad(isDeleteAfterLoad)
                      .setConvertOnTypeMismatch(isConvertOnTypeMismatch))
              .orElse(null);

      if (status == null || !loadTsFileDataTypeConverter.isSuccessful(status)) {
        LOGGER.warn(
            "Load: Failed to convert mini tsfile {} to tablets from statement {}. Status: {}.",
            tsFiles.get(i).getPath(),
            loadTsFileStatement,
            status);
        return false;
      }

      // A mark of successful conversion
      isMiniTsFile.set(i, Boolean.TRUE);
      isMiniTsFileConverted = true;

      loadTsFileStatement.addTsFileResource(null);
      loadTsFileStatement.addWritePointCount(0);
      return true;
    } finally {
      LOAD_TSFILE_COST_METRICS_SET.recordPhaseTimeCost(
          LoadTsFileCostMetricsSet.ANALYSIS_CAST_TABLETS, System.nanoTime() - startTime);
    }
  }

  private void doAnalyzeSingleFile(
      final File tsFile,
      final TsFileSequenceReader reader,
      final TsFileSequenceReaderTimeseriesMetadataIterator timeseriesMetadataIterator)
      throws IOException, LoadAnalyzeException, AuthException {
    // construct tsfile resource
    final TsFileResource tsFileResource = constructTsFileResource(reader, tsFile);

    long writePointCount = 0;

    schemaAutoCreatorAndVerifier.setCurrentModificationsAndTimeIndex(tsFileResource);

    final boolean isAutoCreateSchemaOrVerifySchemaEnabled =
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled() || isVerifySchema;

    while (timeseriesMetadataIterator.hasNext()) {
      final Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadata =
          timeseriesMetadataIterator.next();
      // Update time index no matter if resource file exists or not, because resource file may be
      // untrusted
      TsFileResourceUtils.updateTsFileResource(
          device2TimeseriesMetadata,
          tsFileResource,
          IoTDBDescriptor.getInstance().getConfig().isCacheLastValuesForLoad());
      schemaAutoCreatorAndVerifier.setCurrentTimeIndex(tsFileResource.getTimeIndex());

      if (isAutoCreateSchemaOrVerifySchemaEnabled) {
        schemaAutoCreatorAndVerifier.autoCreateAndVerify(reader, device2TimeseriesMetadata);
      }
      // TODO: how to get the correct write point count when
      //  !isAutoCreateSchemaOrVerifySchemaEnabled
      writePointCount += getWritePointCount(device2TimeseriesMetadata);
    }
    if (isAutoCreateSchemaOrVerifySchemaEnabled) {
      schemaAutoCreatorAndVerifier.flushAndClearDeviceIsAlignedCacheIfNecessary();
    }

    TimestampPrecisionUtils.checkTimestampPrecision(tsFileResource.getFileEndTime());
    tsFileResource.setStatus(TsFileResourceStatus.NORMAL);

    loadTsFileStatement.addTsFileResource(tsFileResource);
    loadTsFileStatement.addWritePointCount(writePointCount);
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

  private long getWritePointCount(
      Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadata) {
    return device2TimeseriesMetadata.values().stream()
        .flatMap(List::stream)
        .mapToLong(t -> t.getStatistics().getCount())
        .sum();
  }

  private boolean reconstructStatementIfMiniFileConverted() {
    if (!isMiniTsFileConverted) {
      return false;
    }

    return loadTsFileStatement.reconstructStatementIfMiniFileConverted(isMiniTsFile);
  }

  private Analysis setFailAnalysisForAuthException(Analysis analysis, AuthException e) {
    analysis.setFinishQueryAfterAnalyze(true);
    analysis.setFailStatus(RpcUtils.getStatus(e.getCode(), e.getMessage()));
    return analysis;
  }

  private Analysis executeTabletConversionOnException(
      final Analysis analysis, final LoadAnalyzeException e) {
    if (shouldSkipConversion(e)) {
      analysis.setFailStatus(
          new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()).setMessage(e.getMessage()));
      analysis.setFinishQueryAfterAnalyze(true);
      return analysis;
    }

    final LoadTsFileDataTypeConverter loadTsFileDataTypeConverter =
        new LoadTsFileDataTypeConverter(context, isGeneratedByPipe);
    final TSStatus status =
        loadTsFileStatement.isConvertOnTypeMismatch()
            ? loadTsFileDataTypeConverter.convertForTreeModel(loadTsFileStatement).orElse(null)
            : null;

    if (status == null) {
      LOGGER.warn(
          "Load: Failed to convert to tablets from statement {}. Status is null.",
          loadTsFileStatement);
      analysis.setFailStatus(
          new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()).setMessage(e.getMessage()));
    } else if (!loadTsFileDataTypeConverter.isSuccessful(status)) {
      LOGGER.warn(
          "Load: Failed to convert to tablets from statement {}. Status: {}",
          loadTsFileStatement,
          status);
      analysis.setFailStatus(status);
    }

    analysis.setFinishQueryAfterAnalyze(true);
    analysis.setStatement(loadTsFileStatement);
    return analysis;
  }

  private boolean shouldSkipConversion(LoadAnalyzeException e) {
    return (e instanceof LoadAnalyzeTypeMismatchException)
        && !loadTsFileStatement.isConvertOnTypeMismatch();
  }

  @Override
  public void close() {
    schemaAutoCreatorAndVerifier.close();
  }

  private final class SchemaAutoCreatorAndVerifier {
    private final LoadTsFileAnalyzeSchemaCache schemaCache;

    private SchemaAutoCreatorAndVerifier() throws LoadRuntimeOutOfMemoryException {
      this.schemaCache = new LoadTsFileAnalyzeSchemaCache();
    }

    public void setCurrentModificationsAndTimeIndex(TsFileResource resource) throws IOException {
      schemaCache.setCurrentModificationsAndTimeIndex(resource);
    }

    public void setCurrentTimeIndex(final ITimeIndex timeIndex) {
      schemaCache.setCurrentTimeIndex(timeIndex);
    }

    public void autoCreateAndVerify(
        TsFileSequenceReader reader,
        Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadataList)
        throws IOException, AuthException, LoadAnalyzeTypeMismatchException {
      for (final Map.Entry<IDeviceID, List<TimeseriesMetadata>> entry :
          device2TimeseriesMetadataList.entrySet()) {
        final IDeviceID device = entry.getKey();

        try {
          if (schemaCache.isDeviceDeletedByMods(device)) {
            continue;
          }
        } catch (IllegalPathException e) {
          LOGGER.warn(
              "Failed to check if device {} is deleted by mods. Will see it as not deleted.",
              device,
              e);
        }

        for (final TimeseriesMetadata timeseriesMetadata : entry.getValue()) {
          try {
            if (schemaCache.isTimeseriesDeletedByMods(device, timeseriesMetadata)) {
              continue;
            }
          } catch (IllegalPathException e) {
            // In aligned devices, there may be empty measurements which will cause
            // IllegalPathException.
            if (!timeseriesMetadata.getMeasurementId().isEmpty()) {
              LOGGER.warn(
                  "Failed to check if device {}, timeseries {} is deleted by mods. Will see it as not deleted.",
                  device,
                  timeseriesMetadata.getMeasurementId(),
                  e);
            }
          }

          final TSDataType dataType = timeseriesMetadata.getTsDataType();
          if (TSDataType.VECTOR.equals(dataType)) {
            schemaCache
                .clearDeviceIsAlignedCacheIfNecessary(); // must execute before add aligned cache
            schemaCache.addIsAlignedCache(device, true, false);

            // not a timeseries, skip
          } else {
            // check WRITE_DATA permission of timeseries
            long startTime = System.nanoTime();
            try {
              String userName = context.getSession().getUserName();
              if (!AuthorityChecker.SUPER_USER.equals(userName)) {
                TSStatus status;
                try {
                  List<PartialPath> paths =
                      Collections.singletonList(
                          new PartialPath(device, timeseriesMetadata.getMeasurementId()));
                  status =
                      AuthorityChecker.getTSStatus(
                          AuthorityChecker.checkFullPathListPermission(
                              userName, paths, PrivilegeType.WRITE_DATA.ordinal()),
                          paths,
                          PrivilegeType.WRITE_DATA);
                } catch (IllegalPathException e) {
                  throw new RuntimeException(e);
                }
                if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                  throw new AuthException(
                      TSStatusCode.representOf(status.getCode()), status.getMessage());
                }
              }
            } finally {
              PerformanceOverviewMetrics.getInstance()
                  .recordAuthCost(System.nanoTime() - startTime);
            }
            final Pair<CompressionType, TSEncoding> compressionEncodingPair =
                reader.readTimeseriesCompressionTypeAndEncoding(timeseriesMetadata);
            schemaCache.addTimeSeries(
                device,
                new MeasurementSchema(
                    timeseriesMetadata.getMeasurementId(),
                    dataType,
                    compressionEncodingPair.getRight(),
                    compressionEncodingPair.getLeft()));

            schemaCache.addIsAlignedCache(device, false, true);
            if (!schemaCache.getDeviceIsAligned(device)) {
              schemaCache.clearDeviceIsAlignedCacheIfNecessary();
            }
          }

          if (schemaCache.shouldFlushTimeSeries()) {
            flush();
          }
        }
      }
    }

    /**
     * This can only be invoked after all timeseries in the current tsfile have been processed.
     * Otherwise, the isAligned status may be wrong.
     */
    public void flushAndClearDeviceIsAlignedCacheIfNecessary() throws SemanticException {
      // avoid OOM when loading a tsfile with too many timeseries
      // or loading too many tsfiles at the same time
      schemaCache.clearDeviceIsAlignedCacheIfNecessary();
    }

    public void flush() throws AuthException, LoadAnalyzeTypeMismatchException {
      doAutoCreateAndVerify();

      schemaCache.clearTimeSeries();
    }

    private void doAutoCreateAndVerify()
        throws SemanticException, AuthException, LoadAnalyzeTypeMismatchException {
      if (schemaCache.getDevice2TimeSeries().isEmpty()) {
        return;
      }

      try {
        if (isVerifySchema) {
          makeSureNoDuplicatedMeasurementsInDevices();
        }

        if (isAutoCreateDatabase) {
          autoCreateDatabase();
        }

        // schema fetcher will not auto create if config set
        // isAutoCreateSchemaEnabled is false.
        final ISchemaTree schemaTree = autoCreateSchema();

        if (isVerifySchema) {
          verifySchema(schemaTree);
        }
      } catch (AuthException | LoadAnalyzeTypeMismatchException e) {
        throw e;
      } catch (Exception e) {
        LOGGER.warn("Auto create or verify schema error.", e);
        throw new SemanticException(
            String.format(
                "Auto create or verify schema error when executing statement %s.  Detail: %s.",
                loadTsFileStatement, e.getMessage()));
      }
    }

    private void makeSureNoDuplicatedMeasurementsInDevices() throws LoadAnalyzeException {
      for (final Map.Entry<IDeviceID, Set<MeasurementSchema>> entry :
          schemaCache.getDevice2TimeSeries().entrySet()) {
        final IDeviceID device = entry.getKey();
        final Map<String, MeasurementSchema> measurement2Schema = new HashMap<>();
        for (final MeasurementSchema timeseriesSchema : entry.getValue()) {
          final String measurement = timeseriesSchema.getMeasurementId();
          if (measurement2Schema.containsKey(measurement)) {
            throw new LoadAnalyzeException(
                String.format("Duplicated measurements %s in device %s.", measurement, device));
          }
          measurement2Schema.put(measurement, timeseriesSchema);
        }
      }
    }

    private void autoCreateDatabase()
        throws LoadAnalyzeException, LoadFileException, IllegalPathException, AuthException {
      final int databasePrefixNodesLength = databaseLevel + 1;
      final Set<PartialPath> databasesNeededToBeSet = new HashSet<>();

      for (final IDeviceID device : schemaCache.getDevice2TimeSeries().keySet()) {
        final PartialPath devicePath = new PartialPath(device);

        final String[] devicePrefixNodes = devicePath.getNodes();
        if (devicePrefixNodes.length < databasePrefixNodesLength) {
          throw new LoadAnalyzeException(
              String.format(
                  "Database level %d is longer than device %s.",
                  databasePrefixNodesLength, device));
        }

        final String[] databasePrefixNodes = new String[databasePrefixNodesLength];
        System.arraycopy(devicePrefixNodes, 0, databasePrefixNodes, 0, databasePrefixNodesLength);

        databasesNeededToBeSet.add(new PartialPath(databasePrefixNodes));
      }

      // 1. filter out the databases that already exist
      if (schemaCache.getAlreadySetDatabases().isEmpty()) {
        try (final ConfigNodeClient configNodeClient =
            CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
          final TGetDatabaseReq req =
              new TGetDatabaseReq(
                  Arrays.asList(
                      new ShowDatabaseStatement(new PartialPath(SqlConstant.getSingleRootArray()))
                          .getPathPattern()
                          .getNodes()),
                  SchemaConstant.ALL_MATCH_SCOPE.serialize());
          final TShowDatabaseResp resp = configNodeClient.showDatabase(req);

          for (final String databaseName : resp.getDatabaseInfoMap().keySet()) {
            schemaCache.addAlreadySetDatabase(new PartialPath(databaseName));
          }
        } catch (IOException | TException | ClientManagerException e) {
          throw new LoadFileException(e);
        }
      }
      databasesNeededToBeSet.removeAll(schemaCache.getAlreadySetDatabases());

      // 2. create the databases that do not exist
      for (final PartialPath databasePath : databasesNeededToBeSet) {
        final DatabaseSchemaStatement statement =
            new DatabaseSchemaStatement(DatabaseSchemaStatement.DatabaseSchemaStatementType.CREATE);
        statement.setDatabasePath(databasePath);
        // do not print exception log because it is not an error
        statement.setEnablePrintExceptionLog(false);
        executeSetDatabaseStatement(statement);

        schemaCache.addAlreadySetDatabase(databasePath);
      }
    }

    private void executeSetDatabaseStatement(Statement statement)
        throws LoadFileException, AuthException {
      // 1.check Authority
      TSStatus status =
          AuthorityChecker.checkAuthority(statement, context.getSession().getUserName());
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new AuthException(TSStatusCode.representOf(status.getCode()), status.getMessage());
      }

      // 2.execute setDatabase statement
      final long queryId = SessionManager.getInstance().requestQueryId();
      final ExecutionResult result =
          Coordinator.getInstance()
              .executeForTreeModel(
                  statement,
                  queryId,
                  null,
                  "",
                  partitionFetcher,
                  schemaFetcher,
                  IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold(),
                  false);
      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && result.status.code
              != TSStatusCode.DATABASE_ALREADY_EXISTS
                  .getStatusCode() // In tree model, if the user creates a conflict database
          // concurrently, for instance, the
          // database created by user is root.db.ss.a, the auto-creation failed database is root.db,
          // we wait till "getOrCreatePartition" to judge if the time series (like root.db.ss.a.e /
          // root.db.ss.a) conflicts with the created database. just do not throw exception here.
          && result.status.code != TSStatusCode.DATABASE_CONFLICT.getStatusCode()) {
        LOGGER.warn(
            "Create database error, statement: {}, result status is: {}", statement, result.status);
        throw new LoadFileException(
            String.format(
                "Create database error, statement: %s, result status is: %s",
                statement, result.status));
      }
    }

    private ISchemaTree autoCreateSchema() throws IllegalPathException {
      final List<PartialPath> deviceList = new ArrayList<>();
      final List<String[]> measurementList = new ArrayList<>();
      final List<TSDataType[]> dataTypeList = new ArrayList<>();
      final List<TSEncoding[]> encodingsList = new ArrayList<>();
      final List<CompressionType[]> compressionTypesList = new ArrayList<>();
      final List<Boolean> isAlignedList = new ArrayList<>();

      for (final Map.Entry<IDeviceID, Set<MeasurementSchema>> entry :
          schemaCache.getDevice2TimeSeries().entrySet()) {
        final int measurementSize = entry.getValue().size();
        final String[] measurements = new String[measurementSize];
        final TSDataType[] tsDataTypes = new TSDataType[measurementSize];
        final TSEncoding[] encodings = new TSEncoding[measurementSize];
        final CompressionType[] compressionTypes = new CompressionType[measurementSize];

        int index = 0;
        for (final MeasurementSchema measurementSchema : entry.getValue()) {
          measurements[index] = measurementSchema.getMeasurementId();
          tsDataTypes[index] = measurementSchema.getType();
          encodings[index] = measurementSchema.getEncodingType();
          compressionTypes[index++] = measurementSchema.getCompressor();
        }

        deviceList.add(new PartialPath(entry.getKey()));
        measurementList.add(measurements);
        dataTypeList.add(tsDataTypes);
        encodingsList.add(encodings);
        compressionTypesList.add(compressionTypes);
        isAlignedList.add(schemaCache.getDeviceIsAligned(entry.getKey()));
      }

      return SchemaValidator.validate(
          schemaFetcher,
          deviceList,
          measurementList,
          dataTypeList,
          encodingsList,
          compressionTypesList,
          isAlignedList,
          context);
    }

    private void verifySchema(ISchemaTree schemaTree)
        throws LoadAnalyzeException, IllegalPathException, LoadAnalyzeTypeMismatchException {
      for (final Map.Entry<IDeviceID, Set<MeasurementSchema>> entry :
          schemaCache.getDevice2TimeSeries().entrySet()) {
        final IDeviceID device = entry.getKey();
        final List<MeasurementSchema> tsfileTimeseriesSchemas = new ArrayList<>(entry.getValue());
        final DeviceSchemaInfo iotdbDeviceSchemaInfo =
            schemaTree.searchDeviceSchemaInfo(
                new PartialPath(device),
                tsfileTimeseriesSchemas.stream()
                    .map(MeasurementSchema::getMeasurementId)
                    .collect(Collectors.toList()));

        if (iotdbDeviceSchemaInfo == null) {
          throw new LoadAnalyzeException(
              String.format(
                  "Device %s does not exist in IoTDB and can not be created. "
                      + "Please check weather auto-create-schema is enabled.",
                  device));
        }

        // check device schema: is aligned or not
        final boolean isAlignedInTsFile = schemaCache.getDeviceIsAligned(device);
        final boolean isAlignedInIoTDB = iotdbDeviceSchemaInfo.isAligned();
        if (LOGGER.isInfoEnabled() && isAlignedInTsFile != isAlignedInIoTDB) {
          LOGGER.info(
              "Device {} in TsFile is {}, but in IoTDB is {}.",
              device,
              isAlignedInTsFile ? "aligned" : "not aligned",
              isAlignedInIoTDB ? "aligned" : "not aligned");
        }

        // check timeseries schema
        final List<MeasurementSchema> iotdbTimeseriesSchemas =
            iotdbDeviceSchemaInfo.getMeasurementSchemaList();
        for (int i = 0, n = iotdbTimeseriesSchemas.size(); i < n; i++) {
          final MeasurementSchema tsFileSchema = tsfileTimeseriesSchemas.get(i);
          final MeasurementSchema iotdbSchema = iotdbTimeseriesSchemas.get(i);
          if (iotdbSchema == null) {
            throw new LoadAnalyzeException(
                String.format(
                    "Measurement %s does not exist in IoTDB and can not be created. "
                        + "Please check weather auto-create-schema is enabled.",
                    device + TsFileConstant.PATH_SEPARATOR + tsfileTimeseriesSchemas.get(i)));
          }

          // check datatype
          if (LOGGER.isInfoEnabled() && !tsFileSchema.getType().equals(iotdbSchema.getType())) {
            LOGGER.info(
                "Measurement {}{}{} datatype not match, TsFile: {}, IoTDB: {}",
                device,
                TsFileConstant.PATH_SEPARATOR,
                iotdbSchema.getMeasurementId(),
                tsFileSchema.getType(),
                iotdbSchema.getType());
          }

          // check encoding
          if (LOGGER.isDebugEnabled()
              && !tsFileSchema.getEncodingType().equals(iotdbSchema.getEncodingType())) {
            // we allow a measurement to have different encodings in different chunks
            LOGGER.debug(
                "Encoding type not match, measurement: {}{}{}, "
                    + "TsFile encoding: {}, IoTDB encoding: {}",
                device,
                TsFileConstant.PATH_SEPARATOR,
                iotdbSchema.getMeasurementId(),
                tsFileSchema.getEncodingType().name(),
                iotdbSchema.getEncodingType().name());
          }

          // check compressor
          if (LOGGER.isDebugEnabled()
              && !tsFileSchema.getCompressor().equals(iotdbSchema.getCompressor())) {
            // we allow a measurement to have different compressors in different chunks
            LOGGER.debug(
                "Compressor not match, measurement: {}{}{}, "
                    + "TsFile compressor: {}, IoTDB compressor: {}",
                device,
                TsFileConstant.PATH_SEPARATOR,
                iotdbSchema.getMeasurementId(),
                tsFileSchema.getCompressor().name(),
                iotdbSchema.getCompressor().name());
          }
        }
      }
    }

    public void close() {
      schemaCache.close();
    }
  }

  private static class LoadTsFileAnalyzeSchemaCache {

    private final LoadTsFileMemoryBlock block;

    private Map<IDeviceID, Set<MeasurementSchema>> currentBatchDevice2TimeSeriesSchemas;
    private Map<IDeviceID, Boolean> tsFileDevice2IsAligned;
    private Set<PartialPath> alreadySetDatabases;

    private Collection<Modification> currentModifications;
    private ITimeIndex currentTimeIndex;

    private long batchDevice2TimeSeriesSchemasMemoryUsageSizeInBytes = 0;
    private long tsFileDevice2IsAlignedMemoryUsageSizeInBytes = 0;
    private long alreadySetDatabasesMemoryUsageSizeInBytes = 0;
    private long currentModificationsMemoryUsageSizeInBytes = 0;
    private long currentTimeIndexMemoryUsageSizeInBytes = 0;

    private int currentBatchTimeSeriesCount = 0;

    public LoadTsFileAnalyzeSchemaCache() throws LoadRuntimeOutOfMemoryException {
      this.block =
          LoadTsFileMemoryManager.getInstance()
              .allocateMemoryBlock(ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES);
      this.currentBatchDevice2TimeSeriesSchemas = new HashMap<>();
      this.tsFileDevice2IsAligned = new HashMap<>();
      this.alreadySetDatabases = new HashSet<>();
      this.currentModifications = new ArrayList<>();
    }

    public Map<IDeviceID, Set<MeasurementSchema>> getDevice2TimeSeries() {
      return currentBatchDevice2TimeSeriesSchemas;
    }

    public boolean getDeviceIsAligned(IDeviceID device) {
      if (!tsFileDevice2IsAligned.containsKey(device)) {
        LOGGER.warn(
            "Device {} is not in the tsFileDevice2IsAligned cache {}.",
            device,
            tsFileDevice2IsAligned);
      }
      return tsFileDevice2IsAligned.get(device);
    }

    public Set<PartialPath> getAlreadySetDatabases() {
      return alreadySetDatabases;
    }

    public void addTimeSeries(IDeviceID device, MeasurementSchema measurementSchema) {
      long memoryUsageSizeInBytes = 0;
      if (!currentBatchDevice2TimeSeriesSchemas.containsKey(device)) {
        memoryUsageSizeInBytes += estimateStringSize(((PlainDeviceID) device).toStringID());
      }
      if (currentBatchDevice2TimeSeriesSchemas
          .computeIfAbsent(device, k -> new HashSet<>())
          .add(measurementSchema)) {
        memoryUsageSizeInBytes += measurementSchema.serializedSize();
        currentBatchTimeSeriesCount++;
      }

      if (memoryUsageSizeInBytes > 0) {
        batchDevice2TimeSeriesSchemasMemoryUsageSizeInBytes += memoryUsageSizeInBytes;
        block.addMemoryUsage(memoryUsageSizeInBytes);
      }
    }

    public void addIsAlignedCache(IDeviceID device, boolean isAligned, boolean addIfAbsent) {
      long memoryUsageSizeInBytes = 0;
      if (!tsFileDevice2IsAligned.containsKey(device)) {
        memoryUsageSizeInBytes += estimateStringSize(((PlainDeviceID) device).toStringID());
      }
      if (addIfAbsent
          ? (tsFileDevice2IsAligned.putIfAbsent(device, isAligned) == null)
          : (tsFileDevice2IsAligned.put(device, isAligned) == null)) {
        memoryUsageSizeInBytes += Byte.BYTES;
      }

      if (memoryUsageSizeInBytes > 0) {
        tsFileDevice2IsAlignedMemoryUsageSizeInBytes += memoryUsageSizeInBytes;
        block.addMemoryUsage(memoryUsageSizeInBytes);
      }
    }

    public void setCurrentModificationsAndTimeIndex(TsFileResource resource) throws IOException {
      clearModificationsAndTimeIndex();

      currentModifications = resource.getModFile().getModifications();
      for (final Modification modification : currentModifications) {
        currentModificationsMemoryUsageSizeInBytes += ((Deletion) modification).getSerializedSize();
      }
      block.addMemoryUsage(currentModificationsMemoryUsageSizeInBytes);

      if (resource.resourceFileExists()) {
        currentTimeIndex = resource.getTimeIndex();
        if (currentTimeIndex instanceof FileTimeIndex) {
          currentTimeIndex = resource.buildDeviceTimeIndex();
        }
        currentTimeIndexMemoryUsageSizeInBytes = currentTimeIndex.calculateRamSize();
        block.addMemoryUsage(currentTimeIndexMemoryUsageSizeInBytes);
      }
    }

    public void setCurrentTimeIndex(final ITimeIndex timeIndex) {
      currentTimeIndex = timeIndex;
    }

    public boolean isDeviceDeletedByMods(IDeviceID device) throws IllegalPathException {
      return ModificationUtils.isDeviceDeletedByMods(
          currentModifications, currentTimeIndex, device);
    }

    public boolean isTimeseriesDeletedByMods(
        IDeviceID device, TimeseriesMetadata timeseriesMetadata) throws IllegalPathException {
      return ModificationUtils.isTimeseriesDeletedByMods(
          currentModifications,
          device,
          timeseriesMetadata.getMeasurementId(),
          timeseriesMetadata.getStatistics().getStartTime(),
          timeseriesMetadata.getStatistics().getEndTime());
    }

    public void addAlreadySetDatabase(PartialPath database) {
      long memoryUsageSizeInBytes = 0;
      if (alreadySetDatabases.add(database)) {
        memoryUsageSizeInBytes += PartialPath.estimateSize(database);
      }

      if (memoryUsageSizeInBytes > 0) {
        alreadySetDatabasesMemoryUsageSizeInBytes += memoryUsageSizeInBytes;
        block.addMemoryUsage(memoryUsageSizeInBytes);
      }
    }

    public boolean shouldFlushTimeSeries() {
      return !block.hasEnoughMemory()
          || currentBatchTimeSeriesCount >= BATCH_FLUSH_TIME_SERIES_NUMBER;
    }

    public boolean shouldFlushAlignedCache() {
      return tsFileDevice2IsAlignedMemoryUsageSizeInBytes
          >= FLUSH_ALIGNED_CACHE_MEMORY_SIZE_IN_BYTES;
    }

    public void clearTimeSeries() {
      currentBatchDevice2TimeSeriesSchemas.clear();
      block.reduceMemoryUsage(batchDevice2TimeSeriesSchemasMemoryUsageSizeInBytes);
      batchDevice2TimeSeriesSchemasMemoryUsageSizeInBytes = 0;
      currentBatchTimeSeriesCount = 0;
    }

    public void clearModificationsAndTimeIndex() {
      currentModifications.clear();
      currentTimeIndex = null;
      block.reduceMemoryUsage(currentModificationsMemoryUsageSizeInBytes);
      block.reduceMemoryUsage(currentTimeIndexMemoryUsageSizeInBytes);
      currentModificationsMemoryUsageSizeInBytes = 0;
      currentTimeIndexMemoryUsageSizeInBytes = 0;
    }

    public void clearAlignedCache() {
      tsFileDevice2IsAligned.clear();
      block.reduceMemoryUsage(tsFileDevice2IsAlignedMemoryUsageSizeInBytes);
      tsFileDevice2IsAlignedMemoryUsageSizeInBytes = 0;
    }

    public void clearDeviceIsAlignedCacheIfNecessary() {
      if (!shouldFlushAlignedCache()) {
        return;
      }

      long releaseMemoryInBytes = 0;
      final Set<IDeviceID> timeSeriesCacheKeySet =
          new HashSet<>(currentBatchDevice2TimeSeriesSchemas.keySet());
      Iterator<Map.Entry<IDeviceID, Boolean>> iterator =
          tsFileDevice2IsAligned.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<IDeviceID, Boolean> entry = iterator.next();
        if (!timeSeriesCacheKeySet.contains(entry.getKey())) {
          releaseMemoryInBytes +=
              estimateStringSize(((PlainDeviceID) entry.getKey()).toStringID()) + Byte.BYTES;
          iterator.remove();
        }
      }
      if (releaseMemoryInBytes > 0) {
        tsFileDevice2IsAlignedMemoryUsageSizeInBytes -= releaseMemoryInBytes;
        block.reduceMemoryUsage(releaseMemoryInBytes);
      }
    }

    private void clearDatabasesCache() {
      alreadySetDatabases.clear();
      block.reduceMemoryUsage(alreadySetDatabasesMemoryUsageSizeInBytes);
      alreadySetDatabasesMemoryUsageSizeInBytes = 0;
    }

    public void close() {
      clearTimeSeries();
      clearModificationsAndTimeIndex();
      clearAlignedCache();
      clearDatabasesCache();

      block.close();

      currentBatchDevice2TimeSeriesSchemas = null;
      tsFileDevice2IsAligned = null;
      alreadySetDatabases = null;
    }

    /**
     * String basic total, 32B
     *
     * <ul>
     *   <li>Object header, 8B
     *   <li>char[] reference + header + length, 8 + 4 + 8= 20B
     *   <li>hash code, 4B
     * </ul>
     */
    private static int estimateStringSize(String string) {
      // each char takes 2B in Java
      return string == null ? 0 : 32 + 2 * string.length();
    }
  }
}
