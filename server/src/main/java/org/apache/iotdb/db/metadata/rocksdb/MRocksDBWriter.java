package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.primitives.Bytes;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.*;

public class MRocksDBWriter {
  private static final Logger logger = LoggerFactory.getLogger(MRocksDBManager.class);

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private RocksDBReadWriteHandler readWriteHandler;

  static {
    RocksDB.loadLibrary();
  }

  public MRocksDBWriter() throws MetadataException {
    try {
      readWriteHandler = new RocksDBReadWriteHandler();
    } catch (RocksDBException e) {
      logger.error("create RocksDBReadWriteHandler fail", e);
      throw new MetadataException(e);
    }
  }

  public void init() throws MetadataException {
    // TODO: scan to init tag manager
    // TODO: warn up cache if needed
  }

  /**
   * Set storage group of the given path to MTree.
   *
   * @param storageGroup root.node.(node)*
   */
  public void setStorageGroup(PartialPath storageGroup) throws MetadataException {
    String[] nodes = storageGroup.getNodes();
    try {
      int len = nodes.length;
      for (int i = 0; i < nodes.length; i++) {
        String levelKey = RocksDBUtils.toLevelKey(nodes, i);
        CheckKeyResult keyCheckResult =
            readWriteHandler.keyExist(
                levelKey, RocksDBMNodeType.INTERNAL, RocksDBMNodeType.STORAGE_GROUP);
        if (!keyCheckResult.existAnyKey()) {
          if (i < len - 1) {
            readWriteHandler.createNode(
                levelKey, RocksDBMNodeType.INTERNAL, DEFAULT_INTERNAL_NODE_VALUE);
          } else {
            readWriteHandler.createNode(
                levelKey, RocksDBMNodeType.STORAGE_GROUP, DEFAULT_SG_NODE_VALUE);
          }
        } else if (keyCheckResult.getResult(RocksDBMNodeType.STORAGE_GROUP)) {
          throw new StorageGroupAlreadySetException(storageGroup.toString());
        }
      }
    } catch (RocksDBException | InterruptedException e) {
      throw new MetadataException(e);
    }
  }

  /**
   * Add one timeseries to metadata, if the timeseries already exists, throw exception
   *
   * @param path the timeseries path
   * @param dataType the dateType {@code DataType} of the timeseries
   * @param encoding the encoding function {@code Encoding} of the timeseries
   * @param compressor the compressor function {@code Compressor} of the time series
   */
  public void createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException {}

  private void createTimeSeries(
      PartialPath path,
      MeasurementSchema schema,
      String alias,
      Map<String, String> tags,
      Map<String, String> attributes)
      throws MetadataException {
    String[] nodes = path.getNodes();
    try {
      int sgIndex = indexSgNode(nodes);
      if (sgIndex < 0) {
        if (!config.isAutoCreateSchemaEnabled()) {
          throw new StorageGroupNotSetException(path.getFullPath());
        }
        PartialPath storageGroupPath =
            MetaUtils.getStorageGroupPathByLevel(path, config.getDefaultStorageGroupLevel());
        if (storageGroupPath.getNodeLength() > path.getNodeLength() - 2) {
          throw new MetadataException("Storage Group Node and Entity Node could not be same");
        }
        setStorageGroup(storageGroupPath);
        sgIndex = storageGroupPath.getNodeLength() - 1;
      }

      // make sure sg node and entity node are different
      if (sgIndex > nodes.length - 3) {
        throw new MetadataException("Storage Group Node and Entity Node could not be same");
      }

      // LOCK
      createTimeSeriesRecursively(
          nodes, nodes.length, sgIndex + 1, schema, alias, tags, attributes);
      // UNLOCK
    } catch (RocksDBException | InterruptedException | IOException e) {
      throw new MetadataException(e);
    }
  }

  public void createTimeseries(CreateTimeSeriesPlan plan) throws MetadataException {
    createTimeseries(
        plan.getPath(),
        plan.getDataType(),
        plan.getEncoding(),
        plan.getCompressor(),
        plan.getProps(),
        plan.getAlias());

    // TODO: persist tags and update tag index
  }

  private void createTimeSeriesRecursively(
      String nodes[],
      int start,
      int end,
      MeasurementSchema schema,
      String alias,
      Map<String, String> tags,
      Map<String, String> attributes)
      throws InterruptedException, MetadataException, RocksDBException, IOException {
    if (start <= end) {
      // nodes before "end" must exist
      return;
    }
    String levelPath = RocksDBUtils.constructKey(nodes, start - 1);
    CheckKeyResult checkResult =
        readWriteHandler.keyExist(
            levelPath,
            RocksDBMNodeType.INTERNAL,
            RocksDBMNodeType.ENTITY,
            RocksDBMNodeType.MEASUREMENT);
    if (!checkResult.existAnyKey()) {
      createTimeSeriesRecursively(nodes, start - 1, end, schema, alias, tags, attributes);
      if (start == nodes.length) {
        // TODO: create timeseries Node
      } else if (start == nodes.length - 1) {
        // create entity node
        readWriteHandler.createNode(levelPath, RocksDBMNodeType.ENTITY, DEFAULT_ENTITY_NODE_VALUE);
      } else {
        // create internal node
        readWriteHandler.createNode(
            levelPath, RocksDBMNodeType.ENTITY, DEFAULT_INTERNAL_NODE_VALUE);
      }
    } else if (start == nodes.length) {
      throw new PathAlreadyExistException("");
    } else if (checkResult.getResult(RocksDBMNodeType.MEASUREMENT)
        || checkResult.getResult(RocksDBMNodeType.ALISA)) {
      throw new PathAlreadyExistException("Measurement node exists in the path");
    } else if (start == nodes.length - 1 && !checkResult.getResult(RocksDBMNodeType.ENTITY)) {
      // TODO: convert the parent node to entity if it exist and is internal node
      WriteBatch writeBatch = new WriteBatch();
      //      writeBatch.delete();
      //      writeBatch.put();
    }
  }

  private void createTimeSeriesInRockDB(
      String[] nodes,
      String key,
      MeasurementSchema schema,
      String alias,
      Map<String, String> tags,
      Map<String, String> attributes)
      throws IOException, RocksDBException, MetadataException, InterruptedException {
    // create timeseries node
    WriteBatch batch = new WriteBatch();
    byte[] value = readWriteHandler.buildMeasurementNodeValue(schema, alias, tags, attributes);
    byte[] pathKey = key.getBytes();
    batch.put(pathKey, value);
    batch.put(readWriteHandler.getCFHByName(TABLE_NAME_MEASUREMENT), pathKey, EMPTY_NODE_VALUE);

    if (StringUtils.isNotEmpty(alias)) {
      String[] aliasNodes = Arrays.copyOf(nodes, nodes.length);
      aliasNodes[nodes.length - 1] = alias;
      String aliasKey = RocksDBUtils.constructKey(aliasNodes, aliasNodes.length - 1);
      if (!readWriteHandler.keyExist(aliasKey)) {
        batch.put(
            aliasKey.getBytes(),
            Bytes.concat(new byte[] {DATA_VERSION, NODE_TYPE_ALIAS}, key.getBytes()));
        readWriteHandler.batchCreateNode(key, aliasKey, batch);
      } else {
        throw new AliasAlreadyExistException(key, alias);
      }
    } else {
      readWriteHandler.batchCreateNode(key, batch);
    }
  }

  public IMeasurementMNode getMeasurementMNode(PartialPath fullPath) throws MetadataException {
    String[] nodes = fullPath.getNodes();
    String key = RocksDBUtils.constructKey(nodes, nodes.length - 1);
    try {
      byte[] value = rocksDB.get(key.getBytes());
      if (value == null) {
        logger.warn("path not exist: {}", key);
        throw new MetadataException("key not exist");
      }
      IMeasurementMNode node = new MeasurementMNode(null, fullPath.getFullPath(), null, null);
      return node;
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }

  /** Check whether the given path contains a storage group */
  public boolean checkStorageGroupByPath(PartialPath path) throws RocksDBException {
    String[] nodes = path.getNodes();
    // ignore the first element: "root"
    for (int i = 1; i < nodes.length; i++) {
      String levelPath = RocksDBUtils.constructKey(nodes, i);
      if (readWriteHandler.typeKyeExist(levelPath, RocksDBMNodeType.STORAGE_GROUP)) {
        return true;
      }
    }
    return false;
  }

  private int indexSgNode(String[] nodes) throws RocksDBException {
    int result = -1;
    // ignore the first element: "root"
    for (int i = 1; i < nodes.length; i++) {
      String levelPath = RocksDBUtils.constructKey(nodes, i);
      if (readWriteHandler.typeKyeExist(levelPath, RocksDBMNodeType.STORAGE_GROUP)) {
        result = i;
        break;
      }
    }
    return result;
  }
  /**
   * Get child node path in the next level of the given path pattern.
   *
   * <p>give pathPattern and the child nodes is those matching pathPattern.*
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1,
   * return [root.sg1.d1, root.sg1.d2]
   *
   * @param pathPattern The given path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  public Set<String> getChildNodePathInNextLevel(PartialPath pathPattern) {
    String[] nodes = pathPattern.getNodes();
    String startKey =
        RocksDBUtils.constructKey(nodes, nodes.length - 1, nodes.length)
            + PATH_SEPARATOR
            + (char) (ZERO + nodes.length);
    return readWriteHandler.getAllByPrefix(startKey);
  }
  /**
   * Get all nodes matching the given path pattern in the given level. The level of the path should
   * match the nodeLevel. 1. The given level equals the path level with out **, e.g. give path
   * root.*.d.* and the level should be 4. 2. The given level is greater than path level with **,
   * e.g. give path root.** and the level could be 2 or 3.
   *
   * @param pathPattern can be a pattern of a full path.
   * @param nodeLevel the level should match the level of the path
   * @return A List instance which stores all node at given level
   */
  public List<PartialPath> getNodesListInGivenLevel(PartialPath pathPattern, int nodeLevel)
      throws MetadataException {
    // TODO: ignore pathPattern with *, all nodeLevel are start from "root.*"
    List<PartialPath> results = new ArrayList<>();
    if (nodeLevel == 0) {
      results.add(new PartialPath(PATH_ROOT));
      return results;
    }
    // TODO: level one usually only contains small numbers, query in serialize
    Set<String> paths;
    StringBuilder builder = new StringBuilder();
    if (nodeLevel <= 5) {
      char level = (char) (ZERO + nodeLevel);
      String prefix = builder.append(ROOT).append(PATH_SEPARATOR).append(level).toString();
      paths = readWriteHandler.getAllByPrefix(prefix);
    } else {
      paths = ConcurrentHashMap.newKeySet();
      char upperLevel = (char) (ZERO + nodeLevel - 1);
      String prefix = builder.append(ROOT).append(PATH_SEPARATOR).append(upperLevel).toString();
      Set<String> parentPaths = readWriteHandler.getAllByPrefix(prefix);
      parentPaths
          .parallelStream()
          .forEach(
              x -> {
                String targetPrefix = RocksDBUtils.getNextLevelOfPath(x, upperLevel);
                paths.addAll(readWriteHandler.getAllByPrefix(targetPrefix));
              });
    }
    return RocksDBUtils.convertToPartialPath(paths, nodeLevel);
  }

  public static void main(String[] args) throws RocksDBException, MetadataException, IOException {}
}
