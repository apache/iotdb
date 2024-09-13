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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.LoadEmptyFileException;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.LoadReadOnlyException;
import org.apache.iotdb.db.exception.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.exception.VerifyMetadataException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
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
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileAnalyzeSchemaMemoryBlock;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryManager;
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
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class LoadTsFileAnalyzer implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileAnalyzer.class);

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();
  private static final int BATCH_FLUSH_TIME_SERIES_NUMBER;
  private static final int MAX_DEVICE_COUNT_TO_USE_DEVICE_TIME_INDEX;
  private static final long ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES;
  private static final long FLUSH_ALIGNED_CACHE_MEMORY_SIZE_IN_BYTES;

  static {
    final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
    BATCH_FLUSH_TIME_SERIES_NUMBER = CONFIG.getLoadTsFileAnalyzeSchemaBatchFlushTimeSeriesNumber();
    MAX_DEVICE_COUNT_TO_USE_DEVICE_TIME_INDEX =
        CONFIG.getLoadTsFileMaxDeviceCountToUseDeviceTimeIndex();
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

  LoadTsFileAnalyzer(
      LoadTsFileStatement loadTsFileStatement,
      MPPQueryContext context,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    this.loadTsFileStatement = loadTsFileStatement;
    this.context = context;

    this.partitionFetcher = partitionFetcher;
    this.schemaFetcher = schemaFetcher;

    this.schemaAutoCreatorAndVerifier = new SchemaAutoCreatorAndVerifier();
  }

  public Analysis analyzeFileByFile(final boolean isDeleteAfterLoad) {
    final Analysis analysis = new Analysis();

    // check if the system is read only
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(
          RpcUtils.getStatus(TSStatusCode.SYSTEM_READ_ONLY, LoadReadOnlyException.MESSAGE));
      return analysis;
    }

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

      try {
        analyzeSingleTsFile(tsFile, isDeleteAfterLoad);
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Load - Analysis Stage: {}/{} tsfiles have been analyzed, progress: {}%",
              i + 1, tsfileNum, String.format("%.3f", (i + 1) * 100.00 / tsfileNum));
        }
      } catch (AuthException e) {
        return createFailAnalysisForAuthException(e);
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

    try {
      schemaAutoCreatorAndVerifier.flush();
    } catch (AuthException e) {
      return createFailAnalysisForAuthException(e);
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

    // data partition will be queried in the scheduler
    analysis.setRealStatement(loadTsFileStatement);
    return analysis;
  }

  @Override
  public void close() {
    schemaAutoCreatorAndVerifier.close();
  }

  private void analyzeSingleTsFile(final File tsFile, final boolean isDeleteAfterLoad)
      throws IOException, AuthException {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      // can be reused when constructing tsfile resource
      final TsFileSequenceReaderTimeseriesMetadataIterator timeseriesMetadataIterator =
          new TsFileSequenceReaderTimeseriesMetadataIterator(reader, true, 1);

      // construct tsfile resource
      final TsFileResource tsFileResource = new TsFileResource(tsFile);
      if (!tsFileResource.resourceFileExists()) {
        // it will be serialized in LoadSingleTsFileNode
        tsFileResource.updatePlanIndexes(reader.getMinPlanIndex());
        tsFileResource.updatePlanIndexes(reader.getMaxPlanIndex());
      } else {
        tsFileResource.deserialize();
      }

      schemaAutoCreatorAndVerifier.setCurrentModificationsAndTimeIndex(tsFileResource, reader);

      // check if the tsfile is empty
      if (!timeseriesMetadataIterator.hasNext()) {
        throw new LoadEmptyFileException(tsFile.getAbsolutePath());
      }

      long writePointCount = 0;

      final boolean isAutoCreateSchemaOrVerifySchemaEnabled =
          IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled()
              || loadTsFileStatement.isVerifySchema();
      while (timeseriesMetadataIterator.hasNext()) {
        final Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadata =
            timeseriesMetadataIterator.next();

        if (isAutoCreateSchemaOrVerifySchemaEnabled) {
          schemaAutoCreatorAndVerifier.autoCreateAndVerify(reader, device2TimeseriesMetadata);
        }

        if (!tsFileResource.resourceFileExists()) {
          TsFileResourceUtils.updateTsFileResource(device2TimeseriesMetadata, tsFileResource);
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
    } catch (final LoadEmptyFileException loadEmptyFileException) {
      LOGGER.warn("Failed to load empty file: {}", tsFile.getAbsolutePath());
      if (isDeleteAfterLoad) {
        FileUtils.deleteQuietly(tsFile);
      }
    }
  }

  private long getWritePointCount(
      Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadata) {
    return device2TimeseriesMetadata.values().stream()
        .flatMap(List::stream)
        .mapToLong(t -> t.getStatistics().getCount())
        .sum();
  }

  private Analysis createFailAnalysisForAuthException(AuthException e) {
    Analysis analysis = new Analysis();
    analysis.setFinishQueryAfterAnalyze(true);
    analysis.setFailStatus(RpcUtils.getStatus(e.getCode(), e.getMessage()));
    return analysis;
  }

  private final class SchemaAutoCreatorAndVerifier {
    private final LoadTsFileAnalyzeSchemaCache schemaCache;

    private SchemaAutoCreatorAndVerifier() throws LoadRuntimeOutOfMemoryException {
      this.schemaCache = new LoadTsFileAnalyzeSchemaCache();
    }

    public void setCurrentModificationsAndTimeIndex(
        TsFileResource resource, TsFileSequenceReader reader) throws IOException {
      schemaCache.setCurrentModificationsAndTimeIndex(resource, reader);
    }

    public void autoCreateAndVerify(
        TsFileSequenceReader reader,
        Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadataList)
        throws IOException, AuthException {
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
            LOGGER.warn(
                "Failed to check if device {}, timeseries {} is deleted by mods. Will see it as not deleted.",
                device,
                timeseriesMetadata.getMeasurementId(),
                e);
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
                          new MeasurementPath(device, timeseriesMetadata.getMeasurementId()));
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

    public void flush() throws AuthException {
      doAutoCreateAndVerify();

      schemaCache.clearTimeSeries();
    }

    private void doAutoCreateAndVerify() throws SemanticException, AuthException {
      if (schemaCache.getDevice2TimeSeries().isEmpty()) {
        return;
      }

      try {
        if (loadTsFileStatement.isVerifySchema()) {
          makeSureNoDuplicatedMeasurementsInDevices();
        }

        if (loadTsFileStatement.isAutoCreateDatabase()) {
          autoCreateDatabase();
        }

        // schema fetcher will not auto create if config set
        // isAutoCreateSchemaEnabled is false.
        final ISchemaTree schemaTree = autoCreateSchema();

        if (loadTsFileStatement.isVerifySchema()) {
          verifySchema(schemaTree);
        }
      } catch (AuthException e) {
        throw e;
      } catch (Exception e) {
        LOGGER.warn("Auto create or verify schema error.", e);
        throw new SemanticException(
            String.format(
                "Auto create or verify schema error when executing statement %s.  Detail: %s.",
                loadTsFileStatement, e.getMessage()));
      }
    }

    private void makeSureNoDuplicatedMeasurementsInDevices() throws VerifyMetadataException {
      for (final Map.Entry<IDeviceID, Set<MeasurementSchema>> entry :
          schemaCache.getDevice2TimeSeries().entrySet()) {
        final IDeviceID device = entry.getKey();
        final Map<String, MeasurementSchema> measurement2Schema = new HashMap<>();
        for (final MeasurementSchema timeseriesSchema : entry.getValue()) {
          final String measurement = timeseriesSchema.getMeasurementId();
          if (measurement2Schema.containsKey(measurement)) {
            throw new VerifyMetadataException(
                String.format("Duplicated measurements %s in device %s.", measurement, device));
          }
          measurement2Schema.put(measurement, timeseriesSchema);
        }
      }
    }

    private void autoCreateDatabase()
        throws VerifyMetadataException, LoadFileException, IllegalPathException, AuthException {
      final int databasePrefixNodesLength = loadTsFileStatement.getDatabaseLevel() + 1;
      final Set<PartialPath> databasesNeededToBeSet = new HashSet<>();

      for (final IDeviceID device : schemaCache.getDevice2TimeSeries().keySet()) {
        final PartialPath devicePath = new PartialPath(device);

        final String[] devicePrefixNodes = devicePath.getNodes();
        if (devicePrefixNodes.length < databasePrefixNodesLength) {
          throw new VerifyMetadataException(
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
                  IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold());
      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && result.status.code != TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()
          // In tree model, if the user creates a conflict database concurrently, for instance, the
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
        throws VerifyMetadataException, IllegalPathException {
      for (final Map.Entry<IDeviceID, Set<MeasurementSchema>> entry :
          schemaCache.getDevice2TimeSeries().entrySet()) {
        final IDeviceID device = entry.getKey();
        final List<IMeasurementSchema> tsfileTimeseriesSchemas = new ArrayList<>(entry.getValue());
        final DeviceSchemaInfo iotdbDeviceSchemaInfo =
            schemaTree.searchDeviceSchemaInfo(
                new PartialPath(device),
                tsfileTimeseriesSchemas.stream()
                    .map(IMeasurementSchema::getMeasurementId)
                    .collect(Collectors.toList()));

        if (iotdbDeviceSchemaInfo == null) {
          throw new VerifyMetadataException(
              String.format(
                  "Device %s does not exist in IoTDB and can not be created. "
                      + "Please check weather auto-create-schema is enabled.",
                  device));
        }

        // check device schema: is aligned or not
        final boolean isAlignedInTsFile = schemaCache.getDeviceIsAligned(device);
        final boolean isAlignedInIoTDB = iotdbDeviceSchemaInfo.isAligned();
        if (isAlignedInTsFile != isAlignedInIoTDB) {
          throw new VerifyMetadataException(
              String.format(
                  "Device %s in TsFile is %s, but in IoTDB is %s.",
                  device,
                  isAlignedInTsFile ? "aligned" : "not aligned",
                  isAlignedInIoTDB ? "aligned" : "not aligned"));
        }

        // check timeseries schema
        final List<IMeasurementSchema> iotdbTimeseriesSchemas =
            iotdbDeviceSchemaInfo.getMeasurementSchemaList();
        for (int i = 0, n = iotdbTimeseriesSchemas.size(); i < n; i++) {
          final IMeasurementSchema tsFileSchema = tsfileTimeseriesSchemas.get(i);
          final IMeasurementSchema iotdbSchema = iotdbTimeseriesSchemas.get(i);
          if (iotdbSchema == null) {
            throw new VerifyMetadataException(
                String.format(
                    "Measurement %s does not exist in IoTDB and can not be created. "
                        + "Please check weather auto-create-schema is enabled.",
                    device + TsFileConstant.PATH_SEPARATOR + tsfileTimeseriesSchemas.get(i)));
          }

          // check datatype
          if (!tsFileSchema.getType().equals(iotdbSchema.getType())) {
            throw new VerifyMetadataException(
                String.format(
                    "Measurement %s%s%s datatype not match, TsFile: %s, IoTDB: %s",
                    device,
                    TsFileConstant.PATH_SEPARATOR,
                    iotdbSchema.getMeasurementId(),
                    tsFileSchema.getType(),
                    iotdbSchema.getType()));
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

    private final LoadTsFileAnalyzeSchemaMemoryBlock block;

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
              .allocateAnalyzeSchemaMemoryBlock(ANALYZE_SCHEMA_MEMORY_SIZE_IN_BYTES);
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
        memoryUsageSizeInBytes += device.ramBytesUsed();
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
        memoryUsageSizeInBytes += device.ramBytesUsed();
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

    public void setCurrentModificationsAndTimeIndex(
        TsFileResource resource, TsFileSequenceReader reader) throws IOException {
      clearModificationsAndTimeIndex();

      currentModifications = resource.getModFile().getModifications();
      for (final Modification modification : currentModifications) {
        currentModificationsMemoryUsageSizeInBytes += ((Deletion) modification).getSerializedSize();
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
      if (currentModifications.size() > 0 && resource.resourceFileExists()) {
        final AtomicInteger deviceCount = new AtomicInteger();
        reader
            .getAllDevicesIteratorWithIsAligned()
            .forEachRemaining(o -> deviceCount.getAndIncrement());

        // Use device time index only if the device count is less than the threshold, avoiding too
        // much memory usage
        if (deviceCount.get() < MAX_DEVICE_COUNT_TO_USE_DEVICE_TIME_INDEX) {
          currentTimeIndex = resource.getTimeIndex();
          if (currentTimeIndex instanceof FileTimeIndex) {
            currentTimeIndex = resource.buildDeviceTimeIndex();
          }
          currentTimeIndexMemoryUsageSizeInBytes = currentTimeIndex.calculateRamSize();
          block.addMemoryUsage(currentTimeIndexMemoryUsageSizeInBytes);
        }
      }
    }

    public boolean isDeviceDeletedByMods(IDeviceID device) throws IllegalPathException {
      return currentTimeIndex != null
          && ModificationUtils.isAllDeletedByMods(
              currentModifications,
              device,
              currentTimeIndex.getStartTime(device),
              currentTimeIndex.getEndTime(device));
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
          releaseMemoryInBytes += entry.getKey().ramBytesUsed() + Byte.BYTES;
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
  }
}
