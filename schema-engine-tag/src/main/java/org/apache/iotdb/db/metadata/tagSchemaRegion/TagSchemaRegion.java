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
package org.apache.iotdb.db.metadata.tagSchemaRegion;

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AlignedTimeseriesException;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.SchemaDirCreationFailureException;
import org.apache.iotdb.db.metadata.LocalSchemaProcessor;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.metadata.idtable.IDTableManager;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceEntry;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.idtable.entry.DiskSchemaEntry;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.metadata.idtable.entry.InsertMeasurementMNode;
import org.apache.iotdb.db.metadata.idtable.entry.SHA256DeviceID;
import org.apache.iotdb.db.metadata.idtable.entry.SchemaEntry;
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaRegionUtils;
import org.apache.iotdb.db.metadata.tagSchemaRegion.deviceidlist.DeviceIDList;
import org.apache.iotdb.db.metadata.tagSchemaRegion.deviceidlist.IDeviceIDList;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.TagInvertedIndex;
import org.apache.iotdb.db.metadata.tagSchemaRegion.utils.MeasurementPathUtils;
import org.apache.iotdb.db.metadata.tagSchemaRegion.utils.PathTagConverterUtils;
import org.apache.iotdb.db.metadata.tagSchemaRegion.utils.ShowTimeSeriesResultUtils;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.MeasurementSchemaInfo;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplateInClusterPlan;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.external.api.ISeriesNumerLimiter;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

/** tag schema region */
public class TagSchemaRegion implements ISchemaRegion {
  private static final Logger logger = LoggerFactory.getLogger(TagSchemaRegion.class);

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  // when a path ends with ".**", it represents batch processing
  private final String TAIL = ".**";

  private final IStorageGroupMNode storageGroupMNode;
  private final String storageGroupFullPath;
  private final SchemaRegionId schemaRegionId;
  private final String schemaRegionDirPath;

  // tag inverted index
  private final TagInvertedIndex tagInvertedIndex;

  // manager device id -> INT32 id
  private final IDeviceIDList deviceIDList;

  // manager timeSeries
  private final IDTable idTable;

  private final ISeriesNumerLimiter seriesNumerLimiter;

  public TagSchemaRegion(
      PartialPath storageGroup,
      SchemaRegionId schemaRegionId,
      IStorageGroupMNode storageGroupMNode,
      ISeriesNumerLimiter seriesNumerLimiter)
      throws MetadataException {
    storageGroupFullPath = storageGroup.getFullPath();
    this.schemaRegionId = schemaRegionId;
    String storageGroupDirPath = config.getSchemaDir() + File.separator + storageGroupFullPath;
    schemaRegionDirPath = storageGroupDirPath + File.separator + schemaRegionId.getId();
    this.storageGroupMNode = storageGroupMNode;
    this.seriesNumerLimiter = seriesNumerLimiter;
    idTable = IDTableManager.getInstance().getIDTable(storageGroup);
    tagInvertedIndex = new TagInvertedIndex(schemaRegionDirPath);
    deviceIDList = new DeviceIDList(schemaRegionDirPath);
    init();
  }

  @Override
  public void init() throws MetadataException {
    // must enableIDTableLogFile or deviceIDTransformationMethod=="Plain"
    if (!config.isEnableIDTableLogFile()
        && config.getDeviceIDTransformationMethod().equals("SHA256")) {
      throw new MetadataException(
          "enableIDTableLogFile OR deviceIDTransformationMethod==\"Plain\"");
    }
    File schemaRegionFolder = SystemFileFactory.INSTANCE.getFile(schemaRegionDirPath);
    if (!schemaRegionFolder.exists()) {
      if (schemaRegionFolder.mkdirs()) {
        logger.info("create schema region folder {}", schemaRegionDirPath);
      } else {
        if (!schemaRegionFolder.exists()) {
          logger.error("create schema region folder {} failed.", schemaRegionDirPath);
          throw new SchemaDirCreationFailureException(schemaRegionDirPath);
        }
      }
    }
    logger.info("initialized successfully: {}", this);
  }

  @Override
  @TestOnly
  public void clear() {
    try {
      tagInvertedIndex.clear();
      deviceIDList.clear();
    } catch (IOException e) {
      logger.error("clear tag inverted index failed", e);
    }
  }

  @Override
  public void forceMlog() {
    // no need to record mlog
  }

  @Override
  public SchemaRegionId getSchemaRegionId() {
    return schemaRegionId;
  }

  @Override
  public String getStorageGroupFullPath() {
    return storageGroupFullPath;
  }

  @Override
  public void deleteSchemaRegion() throws MetadataException {
    clear();
    SchemaRegionUtils.deleteSchemaRegionFolder(schemaRegionDirPath, logger);
  }

  @Override
  public boolean createSnapshot(File snapshotDir) {
    // todo implement this
    throw new UnsupportedOperationException("Tag mode currently doesn't support snapshot feature.");
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    // todo implement this
    throw new UnsupportedOperationException("Tag mode currently doesn't support snapshot feature.");
  }

  private void createTagInvertedIndex(PartialPath devicePath) {
    IDeviceID deviceID = DeviceIDFactory.getInstance().getDeviceID(devicePath);
    Map<String, String> tagsMap =
        PathTagConverterUtils.pathToTags(storageGroupFullPath, devicePath.getFullPath());
    synchronized (deviceIDList) {
      deviceIDList.add(deviceID);
      tagInvertedIndex.addTags(tagsMap, deviceIDList.size() - 1);
    }
  }

  private List<Integer> getDeviceIDsByInvertedIndex(PartialPath path) {
    Map<String, String> tags =
        PathTagConverterUtils.pathToTags(storageGroupFullPath, path.getFullPath());
    return tagInvertedIndex.getMatchedIDs(tags);
  }

  private void createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props)
      throws MetadataException {
    createTimeseries(
        new CreateTimeSeriesPlan(path, dataType, encoding, compressor, props, null, null, null), 0);
  }

  private void createAlignedTimeSeries(
      PartialPath prefixPath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors)
      throws MetadataException {
    createAlignedTimeSeries(
        new CreateAlignedTimeSeriesPlan(
            prefixPath, measurements, dataTypes, encodings, compressors, null, null, null));
  }

  @Override
  public void createTimeseries(CreateTimeSeriesPlan plan, long offset) throws MetadataException {
    PartialPath devicePath = plan.getPath().getDevicePath();
    PartialPath path =
        new PartialPath(
            PathTagConverterUtils.pathToTagsSortPath(storageGroupFullPath, devicePath.getFullPath())
                + "."
                + plan.getPath().getMeasurement());
    plan.setPath(path);
    devicePath = plan.getPath().getDevicePath();
    DeviceEntry deviceEntry = idTable.getDeviceEntry(devicePath.getFullPath());
    if (deviceEntry != null) {
      if (deviceEntry.isAligned()) {
        throw new AlignedTimeseriesException(
            "Timeseries under this entity is not aligned, please use createTimeseries or change entity.",
            devicePath.getFullPath() + "." + plan.getPath().getMeasurement());
      } else if (deviceEntry.getMeasurementMap().containsKey(plan.getPath().getMeasurement())) {
        throw new PathAlreadyExistException(
            devicePath.getFullPath() + "." + plan.getPath().getMeasurement());
      }
    }
    idTable.createTimeseries(plan);
    // write the device path for the first time
    if (deviceEntry == null) {
      createTagInvertedIndex(devicePath);
    }
  }

  @Override
  public void createAlignedTimeSeries(CreateAlignedTimeSeriesPlan plan) throws MetadataException {
    PartialPath devicePath = plan.getPrefixPath();
    PartialPath path =
        new PartialPath(
            PathTagConverterUtils.pathToTagsSortPath(
                storageGroupFullPath, devicePath.getFullPath()));
    plan.setPrefixPath(path);
    devicePath = plan.getPrefixPath();
    DeviceEntry deviceEntry = idTable.getDeviceEntry(devicePath.getFullPath());
    if (deviceEntry != null) {
      if (!deviceEntry.isAligned()) {
        throw new AlignedTimeseriesException(
            "Timeseries under this entity is aligned, please use createAlignedTimeseries or change entity.",
            devicePath.getFullPath());
      } else {
        filterExistingMeasurements(plan, deviceEntry.getMeasurementMap().keySet());
        if (plan.getMeasurements().size() == 0)
          throw new PathAlreadyExistException(devicePath.getFullPath());
      }
    }
    idTable.createAlignedTimeseries(plan);
    // write the device path for the first time
    if (deviceEntry == null) {
      createTagInvertedIndex(devicePath);
    }
  }

  private void filterExistingMeasurements(
      CreateAlignedTimeSeriesPlan plan, Set<String> measurementSet) {
    List<String> measurements = plan.getMeasurements();
    List<TSDataType> dataTypes = plan.getDataTypes();
    List<TSEncoding> encodings = plan.getEncodings();
    List<CompressionType> compressors = plan.getCompressors();

    List<String> tmpMeasurements = new LinkedList<>();
    List<TSDataType> tmpDataTypes = new LinkedList<>();
    List<TSEncoding> tmpEncodings = new LinkedList<>();
    List<CompressionType> tmpCompressors = new LinkedList<>();
    for (int i = 0; i < measurements.size(); i++) {
      String measurement = measurements.get(i);
      if (!measurementSet.contains(measurement)) {
        tmpMeasurements.add(measurements.get(i));
        tmpDataTypes.add(dataTypes.get(i));
        tmpEncodings.add(encodings.get(i));
        tmpCompressors.add(compressors.get(i));
      }
    }
    plan.setMeasurements(tmpMeasurements);
    plan.setDataTypes(tmpDataTypes);
    plan.setEncodings(tmpEncodings);
    plan.setCompressors(tmpCompressors);
  }

  @Override
  public Pair<Integer, Set<String>> deleteTimeseries(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    throw new UnsupportedOperationException("deleteTimeseries");
  }

  @Override
  public int constructSchemaBlackList(PathPatternTree patternTree) throws MetadataException {
    throw new UnsupportedOperationException("constructSchemaBlackList");
  }

  @Override
  public void rollbackSchemaBlackList(PathPatternTree patternTree) throws MetadataException {
    throw new UnsupportedOperationException("rollbackSchemaBlackList");
  }

  @Override
  public List<PartialPath> fetchSchemaBlackList(PathPatternTree patternTree)
      throws MetadataException {
    throw new UnsupportedOperationException("fetchSchemaBlackList");
  }

  @Override
  public void deleteTimeseriesInBlackList(PathPatternTree patternTree) throws MetadataException {
    throw new UnsupportedOperationException("deleteTimeseriesInBlackList");
  }

  @Override
  public void autoCreateDeviceMNode(AutoCreateDeviceMNodePlan plan) throws MetadataException {
    throw new UnsupportedOperationException("autoCreateDeviceMNode");
  }

  @Override
  public boolean isPathExist(PartialPath path) throws MetadataException {
    throw new UnsupportedOperationException("isPathExist");
  }

  @Override
  public int getAllTimeseriesCount(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    int res = 0;
    List<IDeviceID> deviceIDs = getDeviceIdFromInvertedIndex(pathPattern);
    for (IDeviceID deviceID : deviceIDs) {
      res += idTable.getDeviceEntry(deviceID.toStringID()).getMeasurementMap().keySet().size();
    }
    return res;
  }

  @Override
  public int getAllTimeseriesCount(
      PartialPath pathPattern, Map<Integer, Template> templateMap, boolean isPrefixMatch)
      throws MetadataException {
    throw new UnsupportedOperationException("getAllTimeseriesCount");
  }

  @Override
  public int getAllTimeseriesCount(
      PartialPath pathPattern, boolean isPrefixMatch, String key, String value, boolean isContains)
      throws MetadataException {
    throw new UnsupportedOperationException("getAllTimeseriesCount");
  }

  @Override
  public Map<PartialPath, Integer> getMeasurementCountGroupByLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException {
    throw new UnsupportedOperationException("getMeasurementCountGroupByLevel");
  }

  @Override
  public Map<PartialPath, Integer> getMeasurementCountGroupByLevel(
      PartialPath pathPattern,
      int level,
      boolean isPrefixMatch,
      String key,
      String value,
      boolean isContains)
      throws MetadataException {
    throw new UnsupportedOperationException("getMeasurementCountGroupByLevel");
  }

  @Override
  public int getDevicesNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    synchronized (deviceIDList) {
      if (pathPattern.getFullPath().length() <= storageGroupFullPath.length()) {
        return deviceIDList.size();
      } else {
        return getDeviceIDsByInvertedIndex(pathPattern).size();
      }
    }
  }

  @Override
  public int getNodesCountInGivenLevel(PartialPath pathPattern, int level, boolean isPrefixMatch)
      throws MetadataException {
    throw new UnsupportedOperationException("getNodesCountInGivenLevel");
  }

  @Override
  public List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern,
      int nodeLevel,
      boolean isPrefixMatch,
      LocalSchemaProcessor.StorageGroupFilter filter)
      throws MetadataException {
    throw new UnsupportedOperationException("getNodesListInGivenLevel");
  }

  @Override
  public Set<TSchemaNode> getChildNodePathInNextLevel(PartialPath pathPattern)
      throws MetadataException {
    throw new UnsupportedOperationException("getChildNodePathInNextLevel");
  }

  @Override
  public Set<String> getChildNodeNameInNextLevel(PartialPath pathPattern) throws MetadataException {
    throw new UnsupportedOperationException("getChildNodeNameInNextLevel");
  }

  @Override
  public Set<PartialPath> getBelongedDevices(PartialPath timeseries) throws MetadataException {
    throw new UnsupportedOperationException("getBelongedDevices");
  }

  @Override
  public Set<PartialPath> getMatchedDevices(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    List<IDeviceID> deviceIDs = getDeviceIdFromInvertedIndex(pathPattern);
    Set<PartialPath> matchedDevices = new HashSet<>();
    String devicePath = pathPattern.getFullPath();
    // exact query
    if (!devicePath.endsWith(TAIL) && !devicePath.equals(storageGroupFullPath)) {
      DeviceEntry deviceEntry = idTable.getDeviceEntry(devicePath);
      if (deviceEntry != null) {
        matchedDevices.add(pathPattern);
      }
      return matchedDevices;
    }
    List<String> devicePaths = getDevicePaths(deviceIDs);
    for (String path : devicePaths) {
      matchedDevices.add(new PartialPath(path));
    }
    return matchedDevices;
  }

  private List<String> getDevicePaths(List<IDeviceID> deviceIDS) {
    List<String> devicePaths = new ArrayList<>();
    if (config.getDeviceIDTransformationMethod().equals("SHA256")) {
      List<SchemaEntry> schemaEntries = new ArrayList<>();
      for (IDeviceID deviceID : deviceIDS) {
        DeviceEntry deviceEntry = idTable.getDeviceEntry(deviceID.toStringID());
        Map<String, SchemaEntry> map = deviceEntry.getMeasurementMap();
        // For each device, only one SchemaEntry needs to be obtained
        for (Map.Entry<String, SchemaEntry> entry : map.entrySet()) {
          schemaEntries.add(entry.getValue());
          break;
        }
      }
      List<DiskSchemaEntry> diskSchemaEntries = idTable.getDiskSchemaEntries(schemaEntries);
      for (DiskSchemaEntry diskSchemaEntry : diskSchemaEntries) {
        devicePaths.add(diskSchemaEntry.getDevicePath());
      }
    } else {
      for (IDeviceID deviceID : deviceIDS) {
        devicePaths.add(deviceID.toStringID());
      }
    }
    return devicePaths;
  }

  private List<SchemaEntry> getSchemaEntries(List<IDeviceID> deviceIDS) {
    List<SchemaEntry> schemaEntries = new ArrayList<>();
    for (IDeviceID deviceID : deviceIDS) {
      DeviceEntry deviceEntry = idTable.getDeviceEntry(deviceID.toStringID());
      Map<String, SchemaEntry> schemaMap = deviceEntry.getMeasurementMap();
      for (Map.Entry<String, SchemaEntry> entry : schemaMap.entrySet()) {
        schemaEntries.add(entry.getValue());
      }
    }
    return schemaEntries;
  }

  private List<MeasurementPath> getMeasurementPaths(List<IDeviceID> deviceIDS)
      throws IllegalPathException {
    List<MeasurementPath> measurementPaths = new ArrayList<>();
    if (config.getDeviceIDTransformationMethod().equals("SHA256")) {
      List<SchemaEntry> schemaEntries = getSchemaEntries(deviceIDS);
      List<DiskSchemaEntry> diskSchemaEntries = idTable.getDiskSchemaEntries(schemaEntries);
      for (DiskSchemaEntry diskSchemaEntry : diskSchemaEntries) {
        MeasurementPath measurementPath =
            MeasurementPathUtils.generateMeasurementPath(diskSchemaEntry);
        measurementPaths.add(measurementPath);
      }
    } else {
      for (IDeviceID deviceID : deviceIDS) {
        DeviceEntry deviceEntry = idTable.getDeviceEntry(deviceID.toStringID());
        Map<String, SchemaEntry> schemaMap = deviceEntry.getMeasurementMap();
        for (Map.Entry<String, SchemaEntry> entry : schemaMap.entrySet()) {
          MeasurementPath measurementPath =
              MeasurementPathUtils.generateMeasurementPath(
                  deviceID.toStringID(), entry.getKey(), entry.getValue(), deviceEntry.isAligned());
          measurementPaths.add(measurementPath);
        }
      }
    }
    return measurementPaths;
  }

  @Override
  public Pair<List<ShowDevicesResult>, Integer> getMatchedDevices(ShowDevicesPlan plan)
      throws MetadataException {
    throw new UnsupportedOperationException("getMatchedDevices");
  }

  @Override
  public List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    PartialPath devicePath = pathPattern.getDevicePath();
    if (devicePath.getFullPath().endsWith(TAIL)) {
      return getMeasurementPathsWithBatchQuery(devicePath, isPrefixMatch);
    } else {
      return getMeasurementPathsWithPointQuery(devicePath, isPrefixMatch);
    }
  }

  private List<MeasurementPath> getMeasurementPathsWithPointQuery(
      PartialPath devicePath, boolean isPrefixMatch) throws MetadataException {
    List<MeasurementPath> measurementPaths = new LinkedList<>();
    String path =
        PathTagConverterUtils.pathToTagsSortPath(storageGroupFullPath, devicePath.getFullPath());
    DeviceEntry deviceEntry = idTable.getDeviceEntry(path);
    if (deviceEntry == null) return measurementPaths;
    Map<String, SchemaEntry> schemaMap = deviceEntry.getMeasurementMap();
    for (Map.Entry<String, SchemaEntry> entry : schemaMap.entrySet()) {
      MeasurementPath measurementPath =
          MeasurementPathUtils.generateMeasurementPath(
              path, entry.getKey(), entry.getValue(), deviceEntry.isAligned());
      measurementPaths.add(measurementPath);
    }
    return measurementPaths;
  }

  private List<MeasurementPath> getMeasurementPathsWithBatchQuery(
      PartialPath devicePath, boolean isPrefixMatch) throws MetadataException {
    List<IDeviceID> deviceIDs = getDeviceIdFromInvertedIndex(devicePath);
    return getMeasurementPaths(deviceIDs);
  }

  @Override
  public Pair<List<MeasurementPath>, Integer> getMeasurementPathsWithAlias(
      PartialPath pathPattern, int limit, int offset, boolean isPrefixMatch)
      throws MetadataException {
    List<MeasurementPath> res = getMeasurementPaths(pathPattern, isPrefixMatch);
    Pair<List<MeasurementPath>, Integer> result = new Pair<>(res, 0);
    return result;
  }

  @Override
  public List<MeasurementPath> fetchSchema(
      PartialPath pathPattern, Map<Integer, Template> templateMap) throws MetadataException {
    throw new UnsupportedOperationException("fetchSchema");
  }

  @Override
  public Pair<List<ShowTimeSeriesResult>, Integer> showTimeseries(
      ShowTimeSeriesPlan plan, QueryContext context) throws MetadataException {
    List<ShowTimeSeriesResult> ShowTimeSeriesResults = new ArrayList<>();
    Pair<List<ShowTimeSeriesResult>, Integer> result = new Pair<>(ShowTimeSeriesResults, 0);
    String path = plan.getPath().getFullPath();
    // point query
    if (!path.endsWith(TAIL)) {
      path = PathTagConverterUtils.pathToTagsSortPath(storageGroupFullPath, path);
      DeviceEntry deviceEntry = idTable.getDeviceEntry(path);
      if (deviceEntry != null) {
        Map<String, SchemaEntry> measurementMap = deviceEntry.getMeasurementMap();
        for (String m : measurementMap.keySet()) {
          SchemaEntry schemaEntry = measurementMap.get(m);
          ShowTimeSeriesResults.add(
              ShowTimeSeriesResultUtils.generateShowTimeSeriesResult(
                  storageGroupFullPath, path, m, schemaEntry));
        }
      }
      return result;
    }
    // batch query
    List<IDeviceID> deviceIDs = getDeviceIdFromInvertedIndex(plan.getPath());
    for (IDeviceID deviceID : deviceIDs) {
      getTimeSeriesResultOfDeviceFromIDTable(ShowTimeSeriesResults, deviceID);
    }
    return result;
  }

  private void getTimeSeriesResultOfDeviceFromIDTable(
      List<ShowTimeSeriesResult> ShowTimeSeriesResults, IDeviceID deviceID) {
    Map<String, SchemaEntry> measurementMap =
        idTable.getDeviceEntry(deviceID.toStringID()).getMeasurementMap();
    if (deviceID instanceof SHA256DeviceID) {
      for (String m : measurementMap.keySet()) {
        SchemaEntry schemaEntry = measurementMap.get(m);
        List<SchemaEntry> schemaEntries = new ArrayList<>();
        schemaEntries.add(schemaEntry);
        List<DiskSchemaEntry> diskSchemaEntries = idTable.getDiskSchemaEntries(schemaEntries);
        DiskSchemaEntry diskSchemaEntry = diskSchemaEntries.get(0);
        ShowTimeSeriesResults.add(
            ShowTimeSeriesResultUtils.generateShowTimeSeriesResult(
                storageGroupFullPath, diskSchemaEntry.seriesKey, schemaEntry));
      }
    } else {
      for (String m : measurementMap.keySet()) {
        SchemaEntry schemaEntry = measurementMap.get(m);
        ShowTimeSeriesResults.add(
            ShowTimeSeriesResultUtils.generateShowTimeSeriesResult(
                storageGroupFullPath, deviceID.toStringID(), m, schemaEntry));
      }
    }
  }

  private List<IDeviceID> getDeviceIdFromInvertedIndex(PartialPath devicePath)
      throws MetadataException {
    String path = devicePath.getFullPath();
    if (path.endsWith(TAIL)) {
      path = path.substring(0, path.length() - TAIL.length());
      devicePath = new PartialPath(path);
    }
    synchronized (deviceIDList) {
      if (devicePath.getFullPath().length() <= storageGroupFullPath.length()) {
        return deviceIDList.getAllDeviceIDS();
      } else {
        List<IDeviceID> IDS = new LinkedList<>();
        List<Integer> ids = getDeviceIDsByInvertedIndex(devicePath);
        if (ids.size() > 0) {
          for (int id : ids) {
            IDS.add(deviceIDList.get(id));
          }
        }
        return IDS;
      }
    }
  }

  @Override
  public List<MeasurementPath> getAllMeasurementByDevicePath(PartialPath devicePath)
      throws PathNotExistException {
    throw new UnsupportedOperationException("getAllMeasurementByDevicePath");
  }

  @Override
  public IMNode getDeviceNode(PartialPath path) throws MetadataException {
    DeviceEntry deviceEntry = idTable.getDeviceEntry(path.getFullPath());
    if (deviceEntry == null) throw new PathNotExistException(path.getFullPath());
    return new EntityMNode(storageGroupMNode, path.getFullPath());
  }

  @Override
  public IMeasurementMNode getMeasurementMNode(PartialPath fullPath) throws MetadataException {
    throw new UnsupportedOperationException("getMeasurementMNode");
  }

  @Override
  public void changeAlias(PartialPath path, String alias) throws MetadataException, IOException {
    throw new UnsupportedOperationException("changeAlias");
  }

  @Override
  public void upsertTagsAndAttributes(
      String alias,
      Map<String, String> tagsMap,
      Map<String, String> attributesMap,
      PartialPath fullPath)
      throws MetadataException, IOException {
    throw new UnsupportedOperationException("upsertTagsAndAttributes");
  }

  @Override
  public void addAttributes(Map<String, String> attributesMap, PartialPath fullPath)
      throws MetadataException, IOException {
    throw new UnsupportedOperationException("addAttributes");
  }

  @Override
  public void addTags(Map<String, String> tagsMap, PartialPath fullPath)
      throws MetadataException, IOException {
    throw new UnsupportedOperationException("addTags");
  }

  @Override
  public void dropTagsOrAttributes(Set<String> keySet, PartialPath fullPath)
      throws MetadataException, IOException {
    throw new UnsupportedOperationException("dropTagsOrAttributes");
  }

  @Override
  public void setTagsOrAttributesValue(Map<String, String> alterMap, PartialPath fullPath)
      throws MetadataException, IOException {
    throw new UnsupportedOperationException("setTagsOrAttributesValue");
  }

  @Override
  public void renameTagOrAttributeKey(String oldKey, String newKey, PartialPath fullPath)
      throws MetadataException, IOException {
    throw new UnsupportedOperationException("renameTagOrAttributeKey");
  }

  @Override
  public IMNode getSeriesSchemasAndReadLockDevice(InsertPlan plan)
      throws MetadataException, IOException {
    PartialPath devicePath = plan.getDevicePath();
    devicePath =
        new PartialPath(
            PathTagConverterUtils.pathToTagsSortPath(
                storageGroupFullPath, devicePath.getFullPath()));
    plan.setDevicePath(devicePath);
    String[] measurementList = plan.getMeasurements();
    IMeasurementMNode[] measurementMNodes = plan.getMeasurementMNodes();
    checkAlignedAndAutoCreateSeries(plan);
    IMNode deviceMNode = getDeviceNode(devicePath);
    IMeasurementMNode measurementMNode;
    DeviceEntry deviceEntry = idTable.getDeviceEntry(devicePath.getFullPath());
    Map<String, SchemaEntry> schemaMap = deviceEntry.getMeasurementMap();
    for (int i = 0; i < measurementList.length; i++) {
      SchemaEntry schemaEntry = schemaMap.get(measurementList[i]);
      measurementMNode = new InsertMeasurementMNode(measurementList[i], schemaEntry, null);
      // check type is match
      try {
        SchemaRegionUtils.checkDataTypeMatch(plan, i, schemaEntry.getTSDataType());
      } catch (DataTypeMismatchException mismatchException) {
        if (!config.isEnablePartialInsert()) {
          throw mismatchException;
        } else {
          // mark failed measurement
          plan.markFailedMeasurementInsertion(i, mismatchException);
          continue;
        }
      }
      measurementMNodes[i] = measurementMNode;
    }
    plan.setDeviceID(deviceEntry.getDeviceID());
    plan.setDevicePath(new PartialPath(deviceEntry.getDeviceID().toStringID(), false));
    return deviceMNode;
  }

  @Override
  public DeviceSchemaInfo getDeviceSchemaInfoWithAutoCreate(
      PartialPath devicePath,
      String[] measurements,
      Function<Integer, TSDataType> getDataType,
      TSEncoding[] encodings,
      CompressionType[] compressionTypes,
      boolean aligned)
      throws MetadataException {
    List<MeasurementSchemaInfo> measurementSchemaInfoList = new ArrayList<>(measurements.length);
    for (int i = 0; i < measurements.length; i++) {
      SchemaEntry schemaEntry = getSchemaEntry(devicePath.getFullPath(), measurements[i]);
      if (schemaEntry == null) {
        if (config.isAutoCreateSchemaEnabled()) {
          if (aligned) {
            TSDataType dataType = getDataType.apply(i);
            internalAlignedCreateTimeseries(
                devicePath,
                Collections.singletonList(measurements[i]),
                Collections.singletonList(dataType),
                Collections.singletonList(
                    encodings[i] == null ? getDefaultEncoding(dataType) : encodings[i]),
                Collections.singletonList(
                    compressionTypes[i] == null
                        ? TSFileDescriptor.getInstance().getConfig().getCompressor()
                        : compressionTypes[i]));

          } else {
            internalCreateTimeseries(
                devicePath.concatNode(measurements[i]),
                getDataType.apply(i),
                encodings[i],
                compressionTypes[i]);
          }
        }
        schemaEntry = getSchemaEntry(devicePath.getFullPath(), measurements[i]);
      }
      measurementSchemaInfoList.add(
          new MeasurementSchemaInfo(
              measurements[i],
              new MeasurementSchema(
                  measurements[i],
                  schemaEntry.getTSDataType(),
                  schemaEntry.getTSEncoding(),
                  schemaEntry.getCompressionType()),
              null));
    }
    return new DeviceSchemaInfo(devicePath, aligned, measurementSchemaInfoList);
  }

  private SchemaEntry getSchemaEntry(String devicePath, String measurementName) {
    DeviceEntry deviceEntry = idTable.getDeviceEntry(devicePath);
    if (deviceEntry == null) return null;
    return deviceEntry.getSchemaEntry(measurementName);
  }

  private void checkAlignedAndAutoCreateSeries(InsertPlan plan) throws MetadataException {
    String[] measurementList = plan.getMeasurements();
    try {
      if (plan.isAligned()) {
        internalAlignedCreateTimeseries(
            plan.getDevicePath(),
            Arrays.asList(measurementList),
            Arrays.asList(plan.getDataTypes()));
      } else {
        internalCreateTimeseries(
            plan.getDevicePath().concatNode(measurementList[0]), plan.getDataTypes()[0]);
      }
    } catch (MetadataException e) {
      if (!(e instanceof PathAlreadyExistException)) {
        throw e;
      }
    }
  }

  /** create timeseries ignoring PathAlreadyExistException */
  private void internalCreateTimeseries(PartialPath path, TSDataType dataType)
      throws MetadataException {
    createTimeseries(
        path,
        dataType,
        getDefaultEncoding(dataType),
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
  }

  /** create timeseries ignoring PathAlreadyExistException */
  private void internalCreateTimeseries(
      PartialPath path, TSDataType dataType, TSEncoding encoding, CompressionType compressor)
      throws MetadataException {
    if (encoding == null) {
      encoding = getDefaultEncoding(dataType);
    }
    if (compressor == null) {
      compressor = TSFileDescriptor.getInstance().getConfig().getCompressor();
    }
    createTimeseries(path, dataType, encoding, compressor, Collections.emptyMap());
  }

  /** create aligned timeseries ignoring PathAlreadyExistException */
  private void internalAlignedCreateTimeseries(
      PartialPath prefixPath, List<String> measurements, List<TSDataType> dataTypes)
      throws MetadataException {
    List<TSEncoding> encodings = new ArrayList<>();
    List<CompressionType> compressors = new ArrayList<>();
    for (TSDataType dataType : dataTypes) {
      encodings.add(getDefaultEncoding(dataType));
      compressors.add(TSFileDescriptor.getInstance().getConfig().getCompressor());
    }
    createAlignedTimeSeries(prefixPath, measurements, dataTypes, encodings, compressors);
  }

  /** create aligned timeseries ignoring PathAlreadyExistException */
  private void internalAlignedCreateTimeseries(
      PartialPath prefixPath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors)
      throws MetadataException {
    createAlignedTimeSeries(prefixPath, measurements, dataTypes, encodings, compressors);
  }

  @Override
  public Set<String> getPathsSetTemplate(String templateName) throws MetadataException {
    throw new UnsupportedOperationException("getPathsSetTemplate");
  }

  @Override
  public Set<String> getPathsUsingTemplate(String templateName) throws MetadataException {
    throw new UnsupportedOperationException("getPathsUsingTemplate");
  }

  @Override
  public boolean isTemplateAppendable(Template template, List<String> measurements)
      throws MetadataException {
    throw new UnsupportedOperationException("isTemplateAppendable");
  }

  @Override
  public void setSchemaTemplate(SetTemplatePlan plan) throws MetadataException {
    throw new UnsupportedOperationException("setSchemaTemplate");
  }

  @Override
  public void unsetSchemaTemplate(UnsetTemplatePlan plan) throws MetadataException {
    throw new UnsupportedOperationException("unsetSchemaTemplate");
  }

  @Override
  public void setUsingSchemaTemplate(ActivateTemplatePlan plan) throws MetadataException {
    throw new UnsupportedOperationException("setUsingSchemaTemplate");
  }

  @Override
  public void activateSchemaTemplate(ActivateTemplateInClusterPlan plan, Template template)
      throws MetadataException {
    throw new UnsupportedOperationException("activateSchemaTemplate");
  }

  @Override
  public List<String> getPathsUsingTemplate(int templateId) throws MetadataException {
    throw new UnsupportedOperationException("getPathsUsingTemplate");
  }

  @Override
  public IMNode getMNodeForTrigger(PartialPath fullPath) throws MetadataException {
    throw new UnsupportedOperationException("getMNodeForTrigger");
  }

  @Override
  public void releaseMNodeAfterDropTrigger(IMNode node) throws MetadataException {
    throw new UnsupportedOperationException("releaseMNodeAfterDropTrigger");
  }

  @Override
  public String toString() {
    return "TagSchemaRegion{"
        + "storageGroupFullPath='"
        + storageGroupFullPath
        + '\''
        + ", schemaRegionId="
        + schemaRegionId
        + ", schemaRegionDirPath='"
        + schemaRegionDirPath
        + '\''
        + '}';
  }
}
