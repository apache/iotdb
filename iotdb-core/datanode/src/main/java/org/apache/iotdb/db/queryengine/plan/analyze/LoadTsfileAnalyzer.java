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
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.LoadReadOnlyException;
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
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.db.utils.constant.SqlConstant;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LoadTsfileAnalyzer {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsfileAnalyzer.class);

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  private final LoadTsFileStatement loadTsFileStatement;
  private final MPPQueryContext context;

  private final IPartitionFetcher partitionFetcher;
  private final ISchemaFetcher schemaFetcher;

  private final SchemaAutoCreatorAndVerifier schemaAutoCreatorAndVerifier =
      new SchemaAutoCreatorAndVerifier();

  LoadTsfileAnalyzer(
      LoadTsFileStatement loadTsFileStatement,
      MPPQueryContext context,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    this.loadTsFileStatement = loadTsFileStatement;
    this.context = context;

    this.partitionFetcher = partitionFetcher;
    this.schemaFetcher = schemaFetcher;
  }

  public Analysis analyzeFileByFile() {
    context.setQueryType(QueryType.WRITE);

    // check if the system is read only
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      Analysis analysis = new Analysis();
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
        analyzeSingleTsFile(tsFile);
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Load - Analysis Stage: {}/{} tsfiles have been analyzed, progress: {}%",
              i + 1, tsfileNum, String.format("%.3f", (i + 1) * 100.00 / tsfileNum));
        }
      } catch (IllegalArgumentException e) {
        schemaAutoCreatorAndVerifier.clear();
        LOGGER.warn(
            "Parse file {} to resource error, this TsFile maybe empty.", tsFile.getPath(), e);
        throw new SemanticException(
            String.format("TsFile %s is empty or incomplete.", tsFile.getPath()));
      } catch (AuthException e) {
        schemaAutoCreatorAndVerifier.clear();
        Analysis analysis = new Analysis();
        analysis.setFinishQueryAfterAnalyze(true);
        analysis.setFailStatus(RpcUtils.getStatus(e.getCode(), e.getMessage()));
        return analysis;
      } catch (Exception e) {
        schemaAutoCreatorAndVerifier.clear();
        LOGGER.warn("Parse file {} to resource error.", tsFile.getPath(), e);
        throw new SemanticException(
            String.format(
                "Parse file %s to resource error, because %s", tsFile.getPath(), e.getMessage()));
      }
    }

    schemaAutoCreatorAndVerifier.flush();
    schemaAutoCreatorAndVerifier.clear();

    LOGGER.info("Load - Analysis Stage: all tsfiles have been analyzed.");

    // data partition will be queried in the scheduler
    final Analysis analysis = new Analysis();
    analysis.setStatement(loadTsFileStatement);
    return analysis;
  }

  private void analyzeSingleTsFile(File tsFile) throws IOException, AuthException {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      // can be reused when constructing tsfile resource
      final TsFileSequenceReaderTimeseriesMetadataIterator timeseriesMetadataIterator =
          new TsFileSequenceReaderTimeseriesMetadataIterator(reader, true);

      long writePointCount = 0;

      // construct tsfile resource
      final TsFileResource tsFileResource = new TsFileResource(tsFile);
      if (!tsFileResource.resourceFileExists()) {
        // it will be serialized in LoadSingleTsFileNode
        tsFileResource.updatePlanIndexes(reader.getMinPlanIndex());
        tsFileResource.updatePlanIndexes(reader.getMaxPlanIndex());
      } else {
        tsFileResource.deserialize();
      }

      // auto create or verify schema
      if (IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled()
          || loadTsFileStatement.isVerifySchema()) {
        // check if the tsfile is empty
        if (!timeseriesMetadataIterator.hasNext()) {
          LOGGER.warn("device2TimeseriesMetadata is empty, because maybe the tsfile is empty");
          return;
        }

        while (timeseriesMetadataIterator.hasNext()) {
          Map<String, List<TimeseriesMetadata>> device2TimeseriesMetadata =
              timeseriesMetadataIterator.next();

          schemaAutoCreatorAndVerifier.autoCreateAndVerify(reader, device2TimeseriesMetadata);

          if (!tsFileResource.resourceFileExists()) {
            FileLoaderUtils.updateTsFileResource(device2TimeseriesMetadata, tsFileResource);
          }
          writePointCount += getWritePointCount(device2TimeseriesMetadata);
        }

        schemaAutoCreatorAndVerifier.flushAndClearDeviceIsAlignedCacheIfNecessary();
      }

      TimestampPrecisionUtils.checkTimestampPrecision(tsFileResource.getFileEndTime());
      tsFileResource.setStatus(TsFileResourceStatus.NORMAL);

      loadTsFileStatement.addTsFileResource(tsFileResource);
      loadTsFileStatement.addWritePointCount(writePointCount);
    }
  }

  private long getWritePointCount(Map<String, List<TimeseriesMetadata>> device2TimeseriesMetadata) {
    return device2TimeseriesMetadata.values().stream()
        .flatMap(List::stream)
        .mapToLong(t -> t.getStatistics().getCount())
        .sum();
  }

  private final class SchemaAutoCreatorAndVerifier {

    private final Map<String, Boolean> tsfileDevice2IsAligned = new HashMap<>();
    private final Map<String, Set<MeasurementSchema>> currentBatchDevice2TimeseriesSchemas =
        new HashMap<>();

    private final Set<PartialPath> alreadySetDatabases = new HashSet<>();

    private SchemaAutoCreatorAndVerifier() {}

    public void autoCreateAndVerify(
        TsFileSequenceReader reader,
        Map<String, List<TimeseriesMetadata>> device2TimeseriesMetadataList)
        throws IOException, AuthException {
      for (final Map.Entry<String, List<TimeseriesMetadata>> entry :
          device2TimeseriesMetadataList.entrySet()) {
        final String device = entry.getKey();

        for (final TimeseriesMetadata timeseriesMetadata : entry.getValue()) {
          final TSDataType dataType = timeseriesMetadata.getTsDataType();
          if (TSDataType.VECTOR.equals(dataType)) {
            tsfileDevice2IsAligned.put(device, true);

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
            currentBatchDevice2TimeseriesSchemas
                .computeIfAbsent(device, o -> new HashSet<>())
                .add(
                    new MeasurementSchema(
                        timeseriesMetadata.getMeasurementId(),
                        dataType,
                        compressionEncodingPair.getRight(),
                        compressionEncodingPair.getLeft()));

            tsfileDevice2IsAligned.putIfAbsent(device, false);
          }
        }
      }

      flush();
    }

    /**
     * This can only be invoked after all timeseries in the current tsfile have been processed.
     * Otherwise, the isAligned status may be wrong.
     */
    public void flushAndClearDeviceIsAlignedCacheIfNecessary() throws SemanticException {
      // avoid OOM when loading a tsfile with too many timeseries
      // or loading too many tsfiles at the same time
      if (tsfileDevice2IsAligned.size() > 10000) {
        flush();
        tsfileDevice2IsAligned.clear();
      }
    }

    public void flush() {
      doAutoCreateAndVerify();

      currentBatchDevice2TimeseriesSchemas.clear();
    }

    private void doAutoCreateAndVerify() throws SemanticException {
      if (currentBatchDevice2TimeseriesSchemas.isEmpty()) {
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
      } catch (Exception e) {
        LOGGER.warn("Auto create or verify schema error.", e);
        throw new SemanticException(
            String.format(
                "Auto create or verify schema error when executing statement %s.",
                loadTsFileStatement));
      }
    }

    private void makeSureNoDuplicatedMeasurementsInDevices() throws VerifyMetadataException {
      for (final Map.Entry<String, Set<MeasurementSchema>> entry :
          currentBatchDevice2TimeseriesSchemas.entrySet()) {
        final String device = entry.getKey();
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
        throws VerifyMetadataException, LoadFileException, IllegalPathException {
      final int databasePrefixNodesLength = loadTsFileStatement.getDatabaseLevel() + 1;
      final Set<PartialPath> databasesNeededToBeSet = new HashSet<>();

      for (final String device : currentBatchDevice2TimeseriesSchemas.keySet()) {
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
      if (alreadySetDatabases.isEmpty()) {
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
            alreadySetDatabases.add(new PartialPath(databaseName));
          }
        } catch (IOException | TException | ClientManagerException e) {
          throw new LoadFileException(e);
        }
      }
      databasesNeededToBeSet.removeAll(alreadySetDatabases);

      // 2. create the databases that do not exist
      for (final PartialPath databasePath : databasesNeededToBeSet) {
        final DatabaseSchemaStatement statement =
            new DatabaseSchemaStatement(DatabaseSchemaStatement.DatabaseSchemaStatementType.CREATE);
        statement.setDatabasePath(databasePath);
        // do not print exception log because it is not an error
        statement.setEnablePrintExceptionLog(false);
        executeSetDatabaseStatement(statement);

        alreadySetDatabases.add(databasePath);
      }
    }

    private void executeSetDatabaseStatement(Statement statement) throws LoadFileException {
      // 1.check Authority
      TSStatus status =
          AuthorityChecker.checkAuthority(statement, context.getSession().getUserName());
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException(new IoTDBException(status.getMessage(), status.getCode()));
      }

      // 2.execute setDatabase statement
      final long queryId = SessionManager.getInstance().requestQueryId();
      final ExecutionResult result =
          Coordinator.getInstance()
              .execute(
                  statement,
                  queryId,
                  null,
                  "",
                  partitionFetcher,
                  schemaFetcher,
                  IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold());
      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && result.status.code != TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
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

      for (final Map.Entry<String, Set<MeasurementSchema>> entry :
          currentBatchDevice2TimeseriesSchemas.entrySet()) {
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
        isAlignedList.add(tsfileDevice2IsAligned.get(entry.getKey()));
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
      for (final Map.Entry<String, Set<MeasurementSchema>> entry :
          currentBatchDevice2TimeseriesSchemas.entrySet()) {
        final String device = entry.getKey();
        final List<MeasurementSchema> tsfileTimeseriesSchemas = new ArrayList<>(entry.getValue());
        final DeviceSchemaInfo iotdbDeviceSchemaInfo =
            schemaTree.searchDeviceSchemaInfo(
                new PartialPath(device),
                tsfileTimeseriesSchemas.stream()
                    .map(MeasurementSchema::getMeasurementId)
                    .collect(Collectors.toList()));

        if (iotdbDeviceSchemaInfo == null) {
          throw new VerifyMetadataException(
              String.format(
                  "Device %s does not exist in IoTDB and can not be created. "
                      + "Please check weather auto-create-schema is enabled.",
                  device));
        }

        // check device schema: is aligned or not
        final boolean isAlignedInTsFile = tsfileDevice2IsAligned.get(device);
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
        final List<MeasurementSchema> iotdbTimeseriesSchemas =
            iotdbDeviceSchemaInfo.getMeasurementSchemaList();
        for (int i = 0, n = iotdbTimeseriesSchemas.size(); i < n; i++) {
          final MeasurementSchema tsFileSchema = tsfileTimeseriesSchemas.get(i);
          final MeasurementSchema iotdbSchema = iotdbTimeseriesSchemas.get(i);
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

    public void clear() {
      tsfileDevice2IsAligned.clear();
      currentBatchDevice2TimeseriesSchemas.clear();
      alreadySetDatabases.clear();
    }
  }
}
