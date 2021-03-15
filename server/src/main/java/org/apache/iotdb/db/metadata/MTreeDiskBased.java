package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.cache.MNodeCache;
import org.apache.iotdb.db.metadata.metafile.MetaFileAccess;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MTreeDiskBased implements MTreeInterface {

  private MNodeCache cache;

  private MetaFileAccess metaFile;

  private ReadWriteLock lock = new ReentrantReadWriteLock();
  private Lock readLock = lock.readLock();
  private Lock writeLock = lock.writeLock();

  @Override
  public MeasurementMNode createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException {
    return null;
  }

  @Override
  public MNode getDeviceNodeWithAutoCreating(PartialPath deviceId, int sgLevel)
      throws MetadataException {
    return null;
  }

  @Override
  public boolean isPathExist(PartialPath path) {
    return false;
  }

  @Override
  public void setStorageGroup(PartialPath path) throws MetadataException {}

  @Override
  public List<MeasurementMNode> deleteStorageGroup(PartialPath path) throws MetadataException {
    return null;
  }

  @Override
  public boolean isStorageGroup(PartialPath path) {
    return false;
  }

  @Override
  public Pair<PartialPath, MeasurementMNode> deleteTimeseriesAndReturnEmptyStorageGroup(
      PartialPath path) throws MetadataException {
    return null;
  }

  @Override
  public MeasurementSchema getSchema(PartialPath path) throws MetadataException {
    return null;
  }

  @Override
  public MNode getNodeByPathWithStorageGroupCheck(PartialPath path) throws MetadataException {
    return null;
  }

  @Override
  public StorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException {
    return null;
  }

  @Override
  public StorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException {
    return null;
  }

  @Override
  public MNode getNodeByPath(PartialPath path) throws MetadataException {
    return null;
  }

  @Override
  public List<String> getStorageGroupByPath(PartialPath path) throws MetadataException {
    return null;
  }

  @Override
  public List<PartialPath> getAllStorageGroupPaths() {
    return null;
  }

  @Override
  public List<PartialPath> searchAllRelatedStorageGroups(PartialPath path)
      throws MetadataException {
    return null;
  }

  @Override
  public List<PartialPath> getStorageGroupPaths(PartialPath prefixPath) throws MetadataException {
    return null;
  }

  @Override
  public List<StorageGroupMNode> getAllStorageGroupNodes() {
    return null;
  }

  @Override
  public PartialPath getStorageGroupPath(PartialPath path) throws StorageGroupNotSetException {
    return null;
  }

  @Override
  public boolean checkStorageGroupByPath(PartialPath path) {
    return false;
  }

  @Override
  public List<PartialPath> getAllTimeseriesPath(PartialPath prefixPath) throws MetadataException {
    return null;
  }

  @Override
  public Pair<List<PartialPath>, Integer> getAllTimeseriesPathWithAlias(
      PartialPath prefixPath, int limit, int offset) throws MetadataException {
    return null;
  }

  @Override
  public int getAllTimeseriesCount(PartialPath prefixPath) throws MetadataException {
    return 0;
  }

  @Override
  public int getDevicesNum(PartialPath prefixPath) throws MetadataException {
    return 0;
  }

  @Override
  public int getStorageGroupNum(PartialPath prefixPath) throws MetadataException {
    return 0;
  }

  @Override
  public int getNodesCountInGivenLevel(PartialPath prefixPath, int level) throws MetadataException {
    return 0;
  }

  @Override
  public List<Pair<PartialPath, String[]>> getAllMeasurementSchemaByHeatOrder(
      ShowTimeSeriesPlan plan, QueryContext queryContext) throws MetadataException {
    return null;
  }

  @Override
  public List<Pair<PartialPath, String[]>> getAllMeasurementSchema(ShowTimeSeriesPlan plan)
      throws MetadataException {
    return null;
  }

  @Override
  public List<Pair<PartialPath, String[]>> getAllMeasurementSchema(
      ShowTimeSeriesPlan plan, boolean removeCurrentOffset) throws MetadataException {
    return null;
  }

  @Override
  public Set<String> getChildNodePathInNextLevel(PartialPath path) throws MetadataException {
    return null;
  }

  @Override
  public Set<String> getChildNodeInNextLevel(PartialPath path) throws MetadataException {
    return null;
  }

  @Override
  public Set<PartialPath> getDevices(PartialPath prefixPath) throws MetadataException {
    return null;
  }

  @Override
  public List<ShowDevicesResult> getDevices(ShowDevicesPlan plan) throws MetadataException {
    return null;
  }

  @Override
  public List<PartialPath> getNodesList(PartialPath path, int nodeLevel) throws MetadataException {
    return null;
  }

  @Override
  public List<PartialPath> getNodesList(
      PartialPath path, int nodeLevel, MManager.StorageGroupFilter filter)
      throws MetadataException {
    return null;
  }

  @Override
  public Map<String, String> determineStorageGroup(PartialPath path) throws IllegalPathException {
    return null;
  }

  @Override
  public void serializeTo(String snapshotPath) throws IOException {}
}
