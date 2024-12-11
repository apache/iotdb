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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.exception.VerifyMetadataException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.SchemaValidator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDatabaseStatement;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.constant.SqlConstant;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class TreeSchemaAutoCreatorAndVerifier {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TreeSchemaAutoCreatorAndVerifier.class);

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  private final LoadTsFileToTreeModelAnalyzer loadTsFileAnalyzer;
  private final LoadTsFileTreeSchemaCache schemaCache;

  TreeSchemaAutoCreatorAndVerifier(LoadTsFileToTreeModelAnalyzer loadTsFileAnalyzer)
      throws LoadRuntimeOutOfMemoryException {
    this.loadTsFileAnalyzer = loadTsFileAnalyzer;
    this.schemaCache = new LoadTsFileTreeSchemaCache();
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
            String userName = loadTsFileAnalyzer.context.getSession().getUserName();
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
            PerformanceOverviewMetrics.getInstance().recordAuthCost(System.nanoTime() - startTime);
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
      if (loadTsFileAnalyzer.isVerifySchema()) {
        makeSureNoDuplicatedMeasurementsInDevices();
      }

      if (loadTsFileAnalyzer.isAutoCreateDatabase()) {
        autoCreateDatabase();
      }

      // schema fetcher will not auto create if config set
      // isAutoCreateSchemaEnabled is false.
      final ISchemaTree schemaTree = autoCreateSchema();

      if (loadTsFileAnalyzer.isVerifySchema()) {
        verifySchema(schemaTree);
      }
    } catch (AuthException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.warn("Auto create or verify schema error.", e);
      throw new SemanticException(
          String.format(
              "Auto create or verify schema error when executing statement %s.  Detail: %s.",
              loadTsFileAnalyzer.getStatementString(), e.getMessage()));
    }
  }

  private void makeSureNoDuplicatedMeasurementsInDevices() throws VerifyMetadataException {
    for (final Map.Entry<IDeviceID, Set<MeasurementSchema>> entry :
        schemaCache.getDevice2TimeSeries().entrySet()) {
      final IDeviceID device = entry.getKey();
      final Map<String, MeasurementSchema> measurement2Schema = new HashMap<>();
      for (final MeasurementSchema timeseriesSchema : entry.getValue()) {
        final String measurement = timeseriesSchema.getMeasurementName();
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
    final int databasePrefixNodesLength = loadTsFileAnalyzer.getDatabaseLevel() + 1;
    final Set<PartialPath> databasesNeededToBeSet = new HashSet<>();

    for (final IDeviceID device : schemaCache.getDevice2TimeSeries().keySet()) {
      final PartialPath devicePath = new PartialPath(device);

      final String[] devicePrefixNodes = devicePath.getNodes();
      if (devicePrefixNodes.length < databasePrefixNodesLength) {
        throw new VerifyMetadataException(
            String.format(
                "Database level %d is longer than device %s.", databasePrefixNodesLength, device));
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
        AuthorityChecker.checkAuthority(
            statement, loadTsFileAnalyzer.context.getSession().getUserName());
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
                loadTsFileAnalyzer.partitionFetcher,
                loadTsFileAnalyzer.schemaFetcher,
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
        measurements[index] = measurementSchema.getMeasurementName();
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
        loadTsFileAnalyzer.schemaFetcher,
        deviceList,
        measurementList,
        dataTypeList,
        encodingsList,
        compressionTypesList,
        isAlignedList,
        loadTsFileAnalyzer.context);
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
                  .map(IMeasurementSchema::getMeasurementName)
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
                  iotdbSchema.getMeasurementName(),
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
              iotdbSchema.getMeasurementName(),
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
              iotdbSchema.getMeasurementName(),
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
