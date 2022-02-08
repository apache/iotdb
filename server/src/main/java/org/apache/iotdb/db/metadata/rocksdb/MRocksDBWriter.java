package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.AlignedTimeseriesException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.utils.MetaFormatUtils;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.primitives.Bytes;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.Holder;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
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
    MetaFormatUtils.checkStorageGroup(storageGroup.getFullPath());
    String[] nodes = storageGroup.getNodes();
    try {
      int len = nodes.length;
      for (int i = 0; i < nodes.length; i++) {
        String levelKey = RocksDBUtils.getLevelPath(nodes, i);
        CheckKeyResult keyCheckResult = readWriteHandler.keyExistByAllTypes(levelKey);
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
      throws MetadataException {
    createTimeSeries(
        path,
        new MeasurementSchema(path.getMeasurement(), dataType, encoding, compressor, props),
        alias,
        null,
        null);
  }

  public void createTimeseries(CreateTimeSeriesPlan plan) throws MetadataException {
    createTimeSeries(
        plan.getPath(),
        new MeasurementSchema(
            plan.getPath().getMeasurement(),
            plan.getDataType(),
            plan.getEncoding(),
            plan.getCompressor(),
            plan.getProps()),
        plan.getAlias(),
        plan.getTags(),
        plan.getAttributes());
  }

  private void createTimeSeries(
      PartialPath path,
      MeasurementSchema schema,
      String alias,
      Map<String, String> tags,
      Map<String, String> attributes)
      throws MetadataException {
    // regular check
    MetaFormatUtils.checkTimeseries(path);
    MetaFormatUtils.checkTimeseriesProps(path.getFullPath(), schema.getProps());

    // sg check and create
    String[] nodes = path.getNodes();
    SchemaUtils.checkDataTypeWithEncoding(schema.getType(), schema.getEncodingType());
    int sgIndex = ensureStorageGroup(path, path.getNodeLength() - 2);

    try {
      createTimeSeriesRecursively(nodes, nodes.length, schema, alias, tags, attributes);
      // TODO: insert node to tag table
      // TODO: load tags to memory
    } catch (RocksDBException | InterruptedException | IOException e) {
      throw new MetadataException(e);
    }
  }

  private void createTimeSeriesRecursively(
      String nodes[],
      int start,
      MeasurementSchema schema,
      String alias,
      Map<String, String> tags,
      Map<String, String> attributes)
      throws InterruptedException, MetadataException, RocksDBException, IOException {
    if (start <= 1) {
      // nodes "root" must exist and don't need to check
      return;
    }
    String levelPath = RocksDBUtils.getLevelPath(nodes, start - 1);
    Holder<byte[]> holder = new Holder<>();
    CheckKeyResult checkResult = readWriteHandler.keyExistByAllTypes(levelPath, holder);
    if (!checkResult.existAnyKey()) {
      createTimeSeriesRecursively(nodes, start - 1, schema, alias, tags, attributes);
      if (start == nodes.length) {
        createTimeSeriesNode(nodes, levelPath, schema, alias, tags, attributes);
      } else if (start == nodes.length - 1) {
        // create entity node
        try {
          readWriteHandler.createNode(
              levelPath, RocksDBMNodeType.ENTITY, DEFAULT_ENTITY_NODE_VALUE);
        } catch (PathAlreadyExistException e) {
          Holder<byte[]> tempHolder = new Holder<>();
          if (readWriteHandler.keyExistByType(levelPath, RocksDBMNodeType.ENTITY, tempHolder)) {
            logger.info("Entity node created by another thread: {}", levelPath);
          } else {
            throw new PathAlreadyExistException("Entity type node is expected for " + levelPath);
          }
        }
      } else {
        // create internal node
        try {
          readWriteHandler.createNode(
              levelPath, RocksDBMNodeType.ENTITY, DEFAULT_INTERNAL_NODE_VALUE);
        } catch (PathAlreadyExistException e) {
          Holder<byte[]> tempHolder = new Holder<>();
          if (readWriteHandler.keyExistByType(levelPath, RocksDBMNodeType.INTERNAL, tempHolder)) {
            logger.info("Internal node created by another thread: {}", levelPath);
          } else {
            throw new PathAlreadyExistException("Internal type node is expected for " + levelPath);
          }
        }
      }
    } else {
      if (start == nodes.length) {
        throw new PathAlreadyExistException("Measurement node already exists");
      }

      if (checkResult.getResult(RocksDBMNodeType.MEASUREMENT)
          || checkResult.getResult(RocksDBMNodeType.ALISA)) {
        throw new PathAlreadyExistException("Path contains measurement node");
      }

      if (start == nodes.length - 1) {
        if (checkResult.getResult(RocksDBMNodeType.INTERNAL)) {
          // convert the parent node to entity if it is internal node
          WriteBatch batch = new WriteBatch();
          byte[] internalKey = RocksDBUtils.toInternalNodeKey(levelPath);
          byte[] entityKey = RocksDBUtils.toEntityNodeKey(levelPath);
          batch.delete(internalKey);
          batch.put(entityKey, DEFAULT_ENTITY_NODE_VALUE);
          readWriteHandler.convertToEntityNode(levelPath, entityKey, batch);
        } else if (checkResult.getResult(RocksDBMNodeType.ENTITY)) {
          if ((holder.getValue()[1] & FLAG_IS_ALIGNED) != 0) {
            throw new AlignedTimeseriesException(
                "Timeseries under this entity is aligned, please use createAlignedTimeseries or change entity.",
                levelPath);
          }
        } else {
          throw new MetadataException(
              "parent of measurement could only be entity or internal node");
        }
      }
    }
  }

  private void createTimeSeriesNode(
      String[] nodes,
      String levelPath,
      MeasurementSchema schema,
      String alias,
      Map<String, String> tags,
      Map<String, String> attributes)
      throws IOException, RocksDBException, MetadataException, InterruptedException {
    // create time-series node
    WriteBatch batch = new WriteBatch();
    byte[] value = RocksDBUtils.buildMeasurementNodeValue(schema, alias, tags, attributes);
    byte[] measurementKey = RocksDBUtils.toMeasurementNodeKey(levelPath);
    batch.put(measurementKey, value);

    // measurement with tags will save in a separate table at the same time
    if (tags != null && !tags.isEmpty()) {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      ReadWriteIOUtils.write(tags, outputStream);
      batch.put(
          readWriteHandler.getCFHByName(TABLE_NAME_TAGS),
          measurementKey,
          outputStream.toByteArray());
    }

    if (StringUtils.isNotEmpty(alias)) {
      String[] aliasNodes = Arrays.copyOf(nodes, nodes.length);
      aliasNodes[nodes.length - 1] = alias;
      String aliasLevelPath = RocksDBUtils.getLevelPath(aliasNodes, aliasNodes.length - 1);
      byte[] aliasNodeKey = RocksDBUtils.toAliasNodeKey(aliasLevelPath);
      if (!readWriteHandler.keyExist(aliasNodeKey)) {
        batch.put(
            aliasLevelPath.getBytes(),
            Bytes.concat(new byte[] {DATA_VERSION, NODE_TYPE_ALIAS}, levelPath.getBytes()));
        readWriteHandler.batchCreateTwoKeys(levelPath, aliasLevelPath, batch);
      } else {
        throw new AliasAlreadyExistException(levelPath, alias);
      }
    } else {
      readWriteHandler.batchCreateOneKey(levelPath, measurementKey, batch);
    }
  }

  private int ensureStorageGroup(PartialPath path, int entityIndex) throws MetadataException {
    int sgIndex = -1;
    String[] nodes = path.getNodes();
    try {
      sgIndex = indexOfSgNode(nodes);
      if (sgIndex < 0) {
        if (!config.isAutoCreateSchemaEnabled()) {
          throw new StorageGroupNotSetException(path.getFullPath());
        }
        PartialPath sgPath =
            MetaUtils.getStorageGroupPathByLevel(path, config.getDefaultStorageGroupLevel());
        if ((entityIndex - sgPath.getNodeLength()) < 1) {
          throw new MetadataException("Storage Group Node and Entity Node could not be same!");
        }
        setStorageGroup(sgPath);
        sgIndex = sgPath.getNodeLength() - 1;
      }
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }

    // make sure sg node and entity node are different
    if ((entityIndex - sgIndex) < 1) {
      throw new MetadataException("Storage Group Node and Entity Node could not be same!");
    }

    return sgIndex;
  }

  public void createAlignedTimeSeries(
      PartialPath prefixPath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors)
      throws MetadataException {
    createAlignedTimeSeries(
        new CreateAlignedTimeSeriesPlan(
            prefixPath, measurements, dataTypes, encodings, compressors, null));
  }

  /**
   * create aligned timeseries
   *
   * @param plan CreateAlignedTimeSeriesPlan
   */
  public void createAlignedTimeSeries(CreateAlignedTimeSeriesPlan plan) throws MetadataException {
    PartialPath prefixPath = plan.getPrefixPath();
    List<String> measurements = plan.getMeasurements();
    List<TSDataType> dataTypes = plan.getDataTypes();
    List<TSEncoding> encodings = plan.getEncodings();

    for (int i = 0; i < measurements.size(); i++) {
      SchemaUtils.checkDataTypeWithEncoding(dataTypes.get(i), encodings.get(i));
      MetaFormatUtils.checkNodeName(measurements.get(i));
    }

    int sgIndex = ensureStorageGroup(prefixPath, prefixPath.getNodeLength() - 1);

    try {
      createEntityRecursively(prefixPath.getNodes(), prefixPath.getNodeLength(), sgIndex + 1, true);
      WriteBatch batch = new WriteBatch();
      String[] locks = new String[measurements.size()];
      for (int i = 0; i < measurements.size(); i++) {
        String measurement = measurements.get(i);
        String levelPath = RocksDBUtils.getMeasurementLevelPath(prefixPath.getNodes(), measurement);
        locks[i] = levelPath;
        MeasurementSchema schema =
            new MeasurementSchema(measurement, dataTypes.get(i), encodings.get(i));
        byte[] key = RocksDBUtils.toMeasurementNodeKey(levelPath);
        byte[] value = RocksDBUtils.buildMeasurementNodeValue(schema, null, null, null);
        batch.put(key, value);
      }
      readWriteHandler.batchCreateWithLocks(locks, batch);
      // TODO: update cache if necessary
    } catch (InterruptedException | RocksDBException | IOException e) {
      throw new MetadataException(e);
    }
  }

  /**
   * The method assume Storage Group Node has been created
   *
   * @param nodes
   * @param start
   * @param end
   * @param aligned
   */
  private void createEntityRecursively(String[] nodes, int start, int end, boolean aligned)
      throws RocksDBException, MetadataException, InterruptedException {
    if (start <= end) {
      // nodes before "end" must exist
      return;
    }
    String levelPath = RocksDBUtils.getLevelPath(nodes, start - 1);
    Holder<byte[]> holder = new Holder<>();
    CheckKeyResult checkResult = readWriteHandler.keyExistByAllTypes(levelPath, holder);
    if (!checkResult.existAnyKey()) {
      createEntityRecursively(nodes, start - 1, end, aligned);
      if (start == nodes.length) {
        byte[] nodeKey = RocksDBUtils.toEntityNodeKey(levelPath);
        byte[] value = aligned ? new byte[] {FLAG_IS_ALIGNED} : new byte[] {DEFAULT_FLAG};
        readWriteHandler.createNode(levelPath, nodeKey, value);
      } else {
        readWriteHandler.createNode(
            levelPath, RocksDBMNodeType.INTERNAL, DEFAULT_ENTITY_NODE_VALUE);
      }
    } else {
      if (start == nodes.length) {
        if (!checkResult.getResult(RocksDBMNodeType.ENTITY)) {
          throw new PathAlreadyExistException("Node already exists but not entity");
        }

        if ((holder.getValue()[1] & FLAG_IS_ALIGNED) != 0) {
          throw new PathAlreadyExistException("Entity node exists but not aligned");
        }
      } else if (checkResult.getResult(RocksDBMNodeType.MEASUREMENT)
          || checkResult.getResult(RocksDBMNodeType.ALISA)) {
        throw new PathAlreadyExistException("Path contains measurement node");
      }
    }
  }

  //  public IMeasurementMNode getMeasurementMNode(PartialPath fullPath) throws MetadataException {
  //    String[] nodes = fullPath.getNodes();
  //    String key = RocksDBUtils.constructKey(nodes, nodes.length - 1);
  //    try {
  //      byte[] value = rocksDB.get(key.getBytes());
  //      if (value == null) {
  //        logger.warn("path not exist: {}", key);
  //        throw new MetadataException("key not exist");
  //      }
  //      IMeasurementMNode node = new MeasurementMNode(null, fullPath.getFullPath(), null, null);
  //      return node;
  //    } catch (RocksDBException e) {
  //      throw new MetadataException(e);
  //    }
  //  }

  /** Check whether the given path contains a storage group */
  public boolean checkStorageGroupByPath(PartialPath path) throws RocksDBException {
    String[] nodes = path.getNodes();
    // ignore the first element: "root"
    for (int i = 1; i < nodes.length; i++) {
      String levelPath = RocksDBUtils.getLevelPath(nodes, i);
      if (readWriteHandler.keyExistByType(levelPath, RocksDBMNodeType.STORAGE_GROUP)) {
        return true;
      }
    }
    return false;
  }

  private int indexOfSgNode(String[] nodes) throws RocksDBException {
    int result = -1;
    // ignore the first element: "root"
    for (int i = 1; i < nodes.length; i++) {
      String levelPath = RocksDBUtils.getLevelPath(nodes, i);
      if (readWriteHandler.keyExistByType(levelPath, RocksDBMNodeType.STORAGE_GROUP)) {
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
        RocksDBUtils.getLevelPath(nodes, nodes.length - 1, nodes.length)
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
