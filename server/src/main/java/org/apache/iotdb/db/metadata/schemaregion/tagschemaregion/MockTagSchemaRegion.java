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

package org.apache.iotdb.db.metadata.schemaregion.tagschemaregion;

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AlignedTimeseriesException;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
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

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

public class MockTagSchemaRegion implements ISchemaRegion {

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final String TAIL = ".**";
  private final IStorageGroupMNode storageGroupMNode;
  private String storageGroupFullPath;
  private SchemaRegionId schemaRegionId;

  private Map<String, Map<String, List<Integer>>> tagInvertedIndex;

  private List<IDeviceID> deviceIDS;

  private IDTable idTable;

  private final ISeriesNumerLimiter seriesNumerLimiter;

  public MockTagSchemaRegion(
      PartialPath storageGroup,
      SchemaRegionId schemaRegionId,
      IStorageGroupMNode storageGroupMNode,
      ISeriesNumerLimiter seriesNumerLimiter)
      throws MetadataException {

    storageGroupFullPath = storageGroup.getFullPath();
    this.schemaRegionId = schemaRegionId;
    this.storageGroupMNode = storageGroupMNode;
    this.deviceIDS = new ArrayList<>();
    this.seriesNumerLimiter = seriesNumerLimiter;
    tagInvertedIndex = new ConcurrentHashMap<>();
    idTable = IDTableManager.getInstance().getIDTable(storageGroup);
    init();
  }

  @NotNull
  private Map<String, String> pathToTags(String path) {
    if (path.length() <= storageGroupFullPath.length()) return new TreeMap<>();
    String devicePath = path.substring(storageGroupFullPath.length() + 1);
    String[] tags = devicePath.split("\\.");
    Map<String, String> tagsMap = new TreeMap<>();
    for (int i = 0; i < tags.length; i += 2) {
      tagsMap.put(tags[i], tags[i + 1]);
    }
    return tagsMap;
  }

  public String tagsToPath(Map<String, String> tags) {
    StringBuilder stringBuilder = new StringBuilder(storageGroupFullPath);
    for (String tagKey : tags.keySet()) {
      stringBuilder.append(".").append(tagKey).append(".").append(tags.get(tagKey));
    }
    return stringBuilder.toString();
  }

  @Override
  public void init() throws MetadataException {
    if (!config.isEnableIDTableLogFile()
        && config.getDeviceIDTransformationMethod().equals("SHA256")) {
      throw new MetadataException(
          "enableIDTableLogFile OR deviceIDTransformationMethod==\"Plain\"");
    }
  }

  @Override
  public void clear() {
    return;
  }

  @Override
  public void forceMlog() {
    return;
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
    return;
  }

  @Override
  public boolean createSnapshot(File snapshotDir) {
    return false;
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    return;
  }

  private void createTagInvertedIndex(PartialPath devicePath) {
    IDeviceID deviceID = DeviceIDFactory.getInstance().getDeviceID(devicePath);
    Map<String, String> tagsMap = pathToTags(devicePath.getFullPath());

    deviceIDS.add(deviceID);

    for (String tagkey : tagsMap.keySet()) {
      String tagValue = tagsMap.get(tagkey);
      Map<String, List<Integer>> tagkeyMap =
          tagInvertedIndex.computeIfAbsent(tagkey, key -> new HashMap<>());
      List<Integer> ids = tagkeyMap.computeIfAbsent(tagValue, key -> new ArrayList<>());
      ids.add(deviceIDS.size() - 1);
    }
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

  @Override // [iotdb|newIotdb/创建非对齐时间序列] [newIotdb/insert 2自动创建时间序列]
  public void createTimeseries(CreateTimeSeriesPlan plan, long offset) throws MetadataException {
    PartialPath devicePath = plan.getPath().getDevicePath();
    Map<String, String> tags = pathToTags(devicePath.getFullPath());
    PartialPath path = new PartialPath(tagsToPath(tags) + "." + plan.getPath().getMeasurement());
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
    if (deviceEntry == null) {
      createTagInvertedIndex(devicePath);
    }
  }

  @Override // [iotdb|newIotdb/对齐时间序列] [newIotdb/insert 2自动创建时间序列]
  public void createAlignedTimeSeries(CreateAlignedTimeSeriesPlan plan) throws MetadataException {
    PartialPath devicePath = plan.getPrefixPath();
    Map<String, String> tags = pathToTags(devicePath.getFullPath());
    PartialPath path = new PartialPath(tagsToPath(tags));
    plan.setPrefixPath(path);
    devicePath = plan.getPrefixPath();
    DeviceEntry deviceEntry = idTable.getDeviceEntry(devicePath.getFullPath());
    if (deviceEntry != null) {
      if (!deviceEntry.isAligned()) {
        throw new AlignedTimeseriesException(
            "Timeseries under this entity is aligned, please use createAlignedTimeseries or change entity.",
            devicePath.getFullPath());
      } else {
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
          if (!deviceEntry.getMeasurementMap().containsKey(measurement)) {
            tmpMeasurements.add(measurements.get(i));
            tmpDataTypes.add(dataTypes.get(i));
            tmpEncodings.add(encodings.get(i));
            tmpCompressors.add(compressors.get(i));
          }
        }
        if (tmpMeasurements.size() == 0)
          throw new PathAlreadyExistException(devicePath.getFullPath());
        plan.setMeasurements(tmpMeasurements);
        plan.setDataTypes(tmpDataTypes);
        plan.setEncodings(tmpEncodings);
        plan.setCompressors(tmpCompressors);
      }
    }
    idTable.createAlignedTimeseries(plan);
    if (deviceEntry == null) {
      createTagInvertedIndex(devicePath);
    }
  }

  @Override
  public Pair<Integer, Set<String>> deleteTimeseries(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public int constructSchemaBlackList(PathPatternTree patternTree) throws MetadataException {
    return 0;
  }

  @Override
  public void rollbackSchemaBlackList(PathPatternTree patternTree) throws MetadataException {}

  @Override
  public List<PartialPath> fetchSchemaBlackList(PathPatternTree patternTree)
      throws MetadataException {
    return null;
  }

  @Override
  public void deleteTimeseriesInBlackList(PathPatternTree patternTree) throws MetadataException {}

  @Override
  public void autoCreateDeviceMNode(AutoCreateDeviceMNodePlan plan) throws MetadataException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public boolean isPathExist(PartialPath path) throws MetadataException {
    throw new UnsupportedOperationException("");
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
    return 0;
  }

  @Override
  public int getAllTimeseriesCount(
      PartialPath pathPattern, boolean isPrefixMatch, String key, String value, boolean isContains)
      throws MetadataException {
    return 0;
  }

  @Override
  public Map<PartialPath, Integer> getMeasurementCountGroupByLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException {
    throw new UnsupportedOperationException("");
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
    return null;
  }

  @Override
  public int getDevicesNum(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    if (pathPattern.getFullPath().length() <= storageGroupFullPath.length()) {
      return deviceIDS.size();
    } else {
      return getDeviceIDsByInvertedIndex(pathPattern).size();
    }
  }

  @Override
  public int getNodesCountInGivenLevel(PartialPath pathPattern, int level, boolean isPrefixMatch)
      throws MetadataException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern,
      int nodeLevel,
      boolean isPrefixMatch,
      LocalSchemaProcessor.StorageGroupFilter filter)
      throws MetadataException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public Set<TSchemaNode> getChildNodePathInNextLevel(PartialPath pathPattern)
      throws MetadataException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public Set<String> getChildNodeNameInNextLevel(PartialPath pathPattern) throws MetadataException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public Set<PartialPath> getBelongedDevices(PartialPath timeseries) throws MetadataException {
    throw new UnsupportedOperationException("");
  }

  @Override // [newIotdb/show timeseries] [newIotdb/count device] [newIotdb/count timeseries]
  public Set<PartialPath> getMatchedDevices(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    List<IDeviceID> deviceIDs = getDeviceIdFromInvertedIndex(pathPattern);
    Set<PartialPath> res = new HashSet<>();
    String devicePath = pathPattern.getFullPath();
    if (!devicePath.endsWith(TAIL) && !devicePath.equals(storageGroupFullPath)) {
      DeviceEntry deviceEntry = idTable.getDeviceEntry(devicePath);
      if (deviceEntry != null) {
        res.add(pathPattern);
      }
      return res;
    }
    for (IDeviceID deviceID : deviceIDs) {
      if (deviceID instanceof SHA256DeviceID) {
        DeviceEntry deviceEntry = idTable.getDeviceEntry(deviceID.toStringID());
        Map<String, SchemaEntry> map = deviceEntry.getMeasurementMap();
        for (String m : map.keySet()) {
          SchemaEntry schemaEntry = map.get(m);
          List<SchemaEntry> schemaEntries = new ArrayList<>();
          schemaEntries.add(schemaEntry);
          List<DiskSchemaEntry> diskSchemaEntries = idTable.getDiskSchemaEntries(schemaEntries);
          DiskSchemaEntry diskSchemaEntry = diskSchemaEntries.get(0);
          res.add(
              new PartialPath(
                  diskSchemaEntry.seriesKey.substring(
                      0,
                      diskSchemaEntry.seriesKey.length()
                          - diskSchemaEntry.measurementName.length()
                          - 1)));
          break;
        }
      } else {
        res.add(new PartialPath(deviceID.toStringID()));
      }
    }
    return res;
  }

  @Override
  public Pair<List<ShowDevicesResult>, Integer> getMatchedDevices(ShowDevicesPlan plan)
      throws MetadataException {
    throw new UnsupportedOperationException("");
  }

  @Override // [newIotDB / insert1,3] [newIotDB/select] [newIotdb/select count()] [newIotdb/select
  // .. groupby level]
  public List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException {
    PartialPath devicePath = pathPattern.getDevicePath();
    // 批量查询.路径以".**"结尾,如：
    // root.sg.tag1.a.**
    // root.sg.tagx.c.tag2.v.**
    // 点查询.路径不以".**",直接走IDTable,精确查询
    if (devicePath.getFullPath().endsWith(TAIL)) {
      return getMeasurementPathsWithBatchQuery(devicePath, isPrefixMatch);
    } else {
      return getMeasurementPathsWithPointQuery(devicePath, isPrefixMatch);
    }
  }

  private List<MeasurementPath> getMeasurementPathsWithPointQuery(
      PartialPath devicePath, boolean isPrefixMatch) throws MetadataException {
    List<MeasurementPath> res = new LinkedList<>();
    String path = devicePath.getFullPath();
    Map<String, String> tags = pathToTags(path);
    path = tagsToPath(tags);
    DeviceEntry deviceEntry = idTable.getDeviceEntry(path);
    if (deviceEntry == null) return res;
    Map<String, SchemaEntry> schemaMap = deviceEntry.getMeasurementMap();
    for (String measurement : schemaMap.keySet()) {
      SchemaEntry schemaEntry = schemaMap.get(measurement);
      MeasurementPath measurementPath =
          new MeasurementPath(
              path,
              measurement,
              new MeasurementSchema(
                  measurement,
                  schemaEntry.getTSDataType(),
                  schemaEntry.getTSEncoding(),
                  schemaEntry.getCompressionType()));
      measurementPath.setUnderAlignedEntity(deviceEntry.isAligned());
      res.add(measurementPath);
    }

    return res;
  }

  private List<MeasurementPath> getMeasurementPathsWithBatchQuery(
      PartialPath devicePath, boolean isPrefixMatch) throws MetadataException {
    List<MeasurementPath> res = new LinkedList<>();
    List<IDeviceID> deviceIDs = getDeviceIdFromInvertedIndex(devicePath);
    for (IDeviceID deviceID : deviceIDs) {
      DeviceEntry deviceEntry = idTable.getDeviceEntry(deviceID.toStringID());
      Map<String, SchemaEntry> schemaMap = deviceEntry.getMeasurementMap();
      if (deviceID instanceof SHA256DeviceID) {
        for (String measurement : schemaMap.keySet()) {
          SchemaEntry schemaEntry = schemaMap.get(measurement);
          List<SchemaEntry> schemaEntries = new ArrayList<>();
          schemaEntries.add(schemaEntry);
          List<DiskSchemaEntry> diskSchemaEntries = idTable.getDiskSchemaEntries(schemaEntries);
          DiskSchemaEntry diskSchemaEntry = diskSchemaEntries.get(0);
          MeasurementPath measurementPath =
              new MeasurementPath(
                  new PartialPath(diskSchemaEntry.seriesKey),
                  new MeasurementSchema(
                      measurement,
                      schemaEntry.getTSDataType(),
                      schemaEntry.getTSEncoding(),
                      schemaEntry.getCompressionType()));
          measurementPath.setUnderAlignedEntity(deviceEntry.isAligned());
          res.add(measurementPath);
        }
      } else {
        for (String measurement : schemaMap.keySet()) {
          SchemaEntry schemaEntry = schemaMap.get(measurement);
          MeasurementPath measurementPath =
              new MeasurementPath(
                  deviceID.toStringID(),
                  measurement,
                  new MeasurementSchema(
                      measurement,
                      schemaEntry.getTSDataType(),
                      schemaEntry.getTSEncoding(),
                      schemaEntry.getCompressionType()));
          measurementPath.setUnderAlignedEntity(deviceEntry.isAligned());
          res.add(measurementPath);
        }
      }
    }
    return res;
  }

  // [iotdb/select] [iotdb/select last] [iotdb/select count()] [iotdb/select ...groupby level]
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
    return null;
  }

  // show 时间序列
  @Override // [iotdb/show timeseries]
  public Pair<List<ShowTimeSeriesResult>, Integer> showTimeseries(
      ShowTimeSeriesPlan plan, QueryContext context) throws MetadataException {
    List<ShowTimeSeriesResult> res = new ArrayList<>();
    Pair<List<ShowTimeSeriesResult>, Integer> result = new Pair<>(res, 0);
    String path = plan.getPath().getFullPath();
    if (!path.endsWith(TAIL)) {
      Map<String, String> tags = pathToTags(path);
      path = tagsToPath(tags);
      DeviceEntry deviceEntry = idTable.getDeviceEntry(path);
      if (deviceEntry != null) {
        Map<String, SchemaEntry> measurementMap = deviceEntry.getMeasurementMap();
        for (String m : measurementMap.keySet()) {
          SchemaEntry schemaEntry = measurementMap.get(m);
          res.add(
              new ShowTimeSeriesResult(
                  path + "." + m,
                  "null",
                  storageGroupFullPath,
                  schemaEntry.getTSDataType(),
                  schemaEntry.getTSEncoding(),
                  schemaEntry.getCompressionType(),
                  schemaEntry.getLastTime(),
                  new HashMap<>(),
                  new HashMap<>()));
        }
      }
      return result;
    }
    List<IDeviceID> deviceIDs = getDeviceIdFromInvertedIndex(plan.getPath());
    for (IDeviceID deviceID : deviceIDs) {
      getTimeSeriesResultOfDeviceFromIDTable(res, deviceID);
    }
    return result;
  }

  private List<IDeviceID> getDeviceIdFromInvertedIndex(PartialPath devicePath)
      throws MetadataException {
    String path = devicePath.getFullPath();
    if (path.endsWith(TAIL)) {
      path = path.substring(0, path.length() - TAIL.length());
      devicePath = new PartialPath(path);
    }
    if (devicePath.getFullPath().length() <= storageGroupFullPath.length()) {
      return deviceIDS;
    } else {
      List<IDeviceID> res = new LinkedList<>();
      List<Integer> ids = getDeviceIDsByInvertedIndex(devicePath);
      if (ids.size() > 0) {
        for (int id : ids) {
          res.add(deviceIDS.get(id));
        }
      }
      return res;
    }
  }

  private void getTimeSeriesResultOfDeviceFromIDTable(
      List<ShowTimeSeriesResult> res, IDeviceID deviceID) {
    Map<String, SchemaEntry> measurementMap =
        idTable.getDeviceEntry(deviceID.toStringID()).getMeasurementMap();
    if (deviceID instanceof SHA256DeviceID) {
      for (String m : measurementMap.keySet()) {
        SchemaEntry schemaEntry = measurementMap.get(m);
        List<SchemaEntry> schemaEntries = new ArrayList<>();
        schemaEntries.add(schemaEntry);
        List<DiskSchemaEntry> diskSchemaEntries = idTable.getDiskSchemaEntries(schemaEntries);
        DiskSchemaEntry diskSchemaEntry = diskSchemaEntries.get(0);
        res.add(
            new ShowTimeSeriesResult(
                diskSchemaEntry.seriesKey,
                "null",
                storageGroupFullPath,
                schemaEntry.getTSDataType(),
                schemaEntry.getTSEncoding(),
                schemaEntry.getCompressionType(),
                schemaEntry.getLastTime(),
                new HashMap<>(),
                new HashMap<>()));
      }
    } else {
      for (String m : measurementMap.keySet()) {
        SchemaEntry schemaEntry = measurementMap.get(m);
        res.add(
            new ShowTimeSeriesResult(
                deviceID.toStringID() + "." + m,
                "null",
                storageGroupFullPath,
                schemaEntry.getTSDataType(),
                schemaEntry.getTSEncoding(),
                schemaEntry.getCompressionType(),
                schemaEntry.getLastTime(),
                new HashMap<>(),
                new HashMap<>()));
      }
    }
  }

  private List<Integer> getDeviceIDsByInvertedIndex(PartialPath path) {
    Map<String, String> tags = pathToTags(path.getFullPath());
    List<List> idsCollection = new ArrayList<>(tags.keySet().size());
    for (String tagKey : tags.keySet()) {
      if (!tagInvertedIndex.containsKey(tagKey)
          || !tagInvertedIndex.get(tagKey).containsKey(tags.get(tagKey))) {
        return new ArrayList<>();
      }
      List<Integer> ids = tagInvertedIndex.get(tagKey).get(tags.get(tagKey));
      idsCollection.add(new ArrayList(ids));
    }
    if (idsCollection.size() == 0) return new ArrayList<>();
    List<Integer> ids = idsCollection.get(0);
    for (int i = 1; i < idsCollection.size(); i++) {
      List<Integer> list = idsCollection.get(i);
      ids.retainAll(list);
    }
    return ids;
  }

  @Override
  public List<MeasurementPath> getAllMeasurementByDevicePath(PartialPath devicePath)
      throws PathNotExistException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public IMNode getDeviceNode(PartialPath path) throws MetadataException {
    DeviceEntry deviceEntry = idTable.getDeviceEntry(path.getFullPath());
    if (deviceEntry == null) throw new PathNotExistException(path.getFullPath());
    return new EntityMNode(storageGroupMNode, path.getFullPath());
  }

  @Override
  public IMeasurementMNode getMeasurementMNode(PartialPath fullPath) throws MetadataException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public void changeAlias(PartialPath path, String alias) throws MetadataException, IOException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public void upsertTagsAndAttributes(
      String alias,
      Map<String, String> tagsMap,
      Map<String, String> attributesMap,
      PartialPath fullPath)
      throws MetadataException, IOException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public void addAttributes(Map<String, String> attributesMap, PartialPath fullPath)
      throws MetadataException, IOException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public void addTags(Map<String, String> tagsMap, PartialPath fullPath)
      throws MetadataException, IOException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public void dropTagsOrAttributes(Set<String> keySet, PartialPath fullPath)
      throws MetadataException, IOException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public void setTagsOrAttributesValue(Map<String, String> alterMap, PartialPath fullPath)
      throws MetadataException, IOException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public void renameTagOrAttributeKey(String oldKey, String newKey, PartialPath fullPath)
      throws MetadataException, IOException {
    throw new UnsupportedOperationException("");
  }

  // insert data
  @Override // [iotdb/insert ]
  public IMNode getSeriesSchemasAndReadLockDevice(InsertPlan plan)
      throws MetadataException, IOException {
    PartialPath devicePath = plan.getDevicePath();
    Map<String, String> tags = pathToTags(devicePath.getFullPath());
    devicePath = new PartialPath(tagsToPath(tags));
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
      //      measurementMNode =
      //          new MeasurementMNode(
      //              deviceMNode,
      //              measurementList[i],
      //              new MeasurementSchema(
      //                  measurementList[i],
      //                  schemaEntry.getTSDataType(),
      //                  schemaEntry.getTSEncoding(),
      //                  schemaEntry.getCompressionType()),
      //              null);
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

  private SchemaEntry getSchemaEntry(String devicePath, String measurementName) {
    DeviceEntry deviceEntry = idTable.getDeviceEntry(devicePath);
    if (deviceEntry == null) return null;
    return deviceEntry.getSchemaEntry(measurementName);
  }

  @Override
  public DeviceSchemaInfo getDeviceSchemaInfoWithAutoCreate(
      PartialPath devicePath,
      String[] measurements,
      Function<Integer, TSDataType> getDataType,
      boolean aligned)
      throws MetadataException {
    List<MeasurementSchemaInfo> measurementSchemaInfoList = new ArrayList<>(measurements.length);
    for (int i = 0; i < measurements.length; i++) {
      SchemaEntry schemaEntry = getSchemaEntry(devicePath.getFullPath(), measurements[i]);
      if (schemaEntry == null) {
        if (config.isAutoCreateSchemaEnabled()) {
          if (aligned) {
            internalAlignedCreateTimeseries(
                devicePath,
                Collections.singletonList(measurements[i]),
                Collections.singletonList(getDataType.apply(i)));

          } else {
            internalCreateTimeseries(devicePath.concatNode(measurements[i]), getDataType.apply(i));
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

  private void internalCreateTimeseries(PartialPath path, TSDataType dataType)
      throws MetadataException {
    createTimeseries(
        path,
        dataType,
        getDefaultEncoding(dataType),
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
  }

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

  @Override
  public Set<String> getPathsSetTemplate(String templateName) throws MetadataException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public Set<String> getPathsUsingTemplate(String templateName) throws MetadataException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public boolean isTemplateAppendable(Template template, List<String> measurements)
      throws MetadataException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public void setSchemaTemplate(SetTemplatePlan plan) throws MetadataException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public void unsetSchemaTemplate(UnsetTemplatePlan plan) throws MetadataException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public void setUsingSchemaTemplate(ActivateTemplatePlan plan) throws MetadataException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public void activateSchemaTemplate(ActivateTemplateInClusterPlan plan, Template template)
      throws MetadataException {}

  @Override
  public List<String> getPathsUsingTemplate(int templateId) throws MetadataException {
    return null;
  }

  @Override
  public IMNode getMNodeForTrigger(PartialPath fullPath) throws MetadataException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public void releaseMNodeAfterDropTrigger(IMNode node) throws MetadataException {
    throw new UnsupportedOperationException("");
  }
}
