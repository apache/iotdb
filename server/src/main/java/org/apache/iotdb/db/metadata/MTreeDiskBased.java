package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.exception.metadata.*;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.db.conf.IoTDBConstant.*;
import static org.apache.iotdb.db.conf.IoTDBConstant.SDT_COMP_MAX_TIME;

public class MTreeDiskBased implements MTreeInterface {

  private static final Logger logger = LoggerFactory.getLogger(MTreeDiskBased.class);

  private MNodeCache cache;

  private MetaFileAccess metaFile;

  private MNode root;

  private ReadWriteLock lock = new ReentrantReadWriteLock();
  private Lock readLock = lock.readLock();
  private Lock writeLock = lock.writeLock();

  public MNode getMNode(PartialPath path) throws Exception{
    MNode result=cache.get(path);
    if(result!=null){
      return result;
    }
    result=metaFile.read(path);
    return result;
  }

  @Override
  public MeasurementMNode createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException {
    String[] nodeNames = path.getNodes();
    if (nodeNames.length <= 2 || !nodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }
    checkTimeseries(path.getFullPath());
    MNode cur = root;
    boolean hasSetStorageGroup = false;
    // e.g, path = root.sg.d1.s1,  create internal nodes and set cur to d1 node
    for (int i = 1; i < nodeNames.length - 1; i++) {
      String nodeName = nodeNames[i];
      if (cur instanceof StorageGroupMNode) {
        hasSetStorageGroup = true;
      }
      if (!cur.hasChild(nodeName)) {
        if (!hasSetStorageGroup) {
          throw new StorageGroupNotSetException("Storage group should be created first");
        }
        cur.addChild(nodeName, new MNode(cur, nodeName));
      }
      cur = cur.getChild(nodeName);
    }

    if (props != null && props.containsKey(LOSS) && props.get(LOSS).equals(SDT)) {
      checkSDTFormat(path.getFullPath(), props);
    }

    String leafName = nodeNames[nodeNames.length - 1];

    // synchronize check and add, we need addChild and add Alias become atomic operation
    // only write on mtree will be synchronized
    synchronized (this) {
      MNode child = cur.getChild(leafName);
      if (child instanceof MeasurementMNode || child instanceof StorageGroupMNode) {
        throw new PathAlreadyExistException(path.getFullPath());
      }

      if (alias != null) {
        MNode childByAlias = cur.getChild(alias);
        if (childByAlias instanceof MeasurementMNode) {
          throw new AliasAlreadyExistException(path.getFullPath(), alias);
        }
      }

      // this measurementMNode could be a leaf or not.
      MeasurementMNode measurementMNode =
          new MeasurementMNode(cur, leafName, alias, dataType, encoding, compressor, props);
      if (child != null) {
        cur.replaceChild(measurementMNode.getName(), measurementMNode);
      } else {
        cur.addChild(leafName, measurementMNode);
      }

      // link alias to LeafMNode
      if (alias != null) {
        cur.addAlias(alias, measurementMNode);
      }

      return measurementMNode;
    }
  }

  private void checkTimeseries(String timeseries) throws IllegalPathException {
    if (!IoTDBConfig.NODE_PATTERN.matcher(timeseries).matches()) {
      throw new IllegalPathException(
          String.format("The timeseries name contains unsupported character. %s", timeseries));
    }
  }

  // check if sdt parameters are valid
  private void checkSDTFormat(String path, Map<String, String> props)
      throws IllegalParameterOfPathException {
    if (!props.containsKey(SDT_COMP_DEV)) {
      throw new IllegalParameterOfPathException("SDT compression deviation is required", path);
    }

    try {
      double d = Double.parseDouble(props.get(SDT_COMP_DEV));
      if (d < 0) {
        throw new IllegalParameterOfPathException(
            "SDT compression deviation cannot be negative", path);
      }
    } catch (NumberFormatException e) {
      throw new IllegalParameterOfPathException("SDT compression deviation formatting error", path);
    }

    long compMinTime = sdtCompressionTimeFormat(SDT_COMP_MIN_TIME, props, path);
    long compMaxTime = sdtCompressionTimeFormat(SDT_COMP_MAX_TIME, props, path);

    if (compMaxTime <= compMinTime) {
      throw new IllegalParameterOfPathException(
          "SDT compression maximum time needs to be greater than compression minimum time", path);
    }
  }

  private long sdtCompressionTimeFormat(String compTime, Map<String, String> props, String path)
      throws IllegalParameterOfPathException {
    boolean isCompMaxTime = compTime.equals(SDT_COMP_MAX_TIME);
    long time = isCompMaxTime ? Long.MAX_VALUE : 0;
    String s = isCompMaxTime ? "maximum" : "minimum";
    if (props.containsKey(compTime)) {
      try {
        time = Long.parseLong(props.get(compTime));
        if (time < 0) {
          throw new IllegalParameterOfPathException(
              String.format("SDT compression %s time cannot be negative", s), path);
        }
      } catch (IllegalParameterOfPathException e) {
        throw new IllegalParameterOfPathException(
            String.format("SDT compression %s time formatting error", s), path);
      }
    } else {
      logger.info("{} enabled SDT but did not set compression {} time", path, s);
    }
    return time;
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

  private MNode getNode(PartialPath path) throws IOException {
    MNode node = cache.get(path);
    readLock.lock();
    if (node == null) {
      node = metaFile.read(path);
    } else if (!node.isLoaded()) {
      node = metaFile.read(node.getPosition());
    }
    readLock.unlock();
    return node;
  }
}
