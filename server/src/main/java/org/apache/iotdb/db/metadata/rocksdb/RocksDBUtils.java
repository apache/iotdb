package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.primitives.Bytes;
import org.apache.commons.lang3.ArrayUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.*;

public class RocksDBUtils {

  protected static RocksDBMNodeType[] NODE_TYPE_ARRAY = new RocksDBMNodeType[NODE_TYPE_ALIAS + 1];

  static {
    NODE_TYPE_ARRAY[NODE_TYPE_INTERNAL] = RocksDBMNodeType.INTERNAL;
    NODE_TYPE_ARRAY[NODE_TYPE_SG] = RocksDBMNodeType.STORAGE_GROUP;
    NODE_TYPE_ARRAY[NODE_TYPE_ENTITY] = RocksDBMNodeType.ENTITY;
    NODE_TYPE_ARRAY[NODE_TYPE_MEASUREMENT] = RocksDBMNodeType.MEASUREMENT;
    NODE_TYPE_ARRAY[NODE_TYPE_ALIAS] = RocksDBMNodeType.ALISA;
  }

  protected static byte[] constructDataBlock(byte type, String data) {
    byte[] dataInBytes = data.getBytes();
    int size = dataInBytes.length;
    return Bytes.concat(new byte[] {type}, BytesUtils.intToBytes(size), dataInBytes);
  }

  protected static byte[] toInternalNodeKey(String levelPath) {
    return toRocksDBKey(levelPath, NODE_TYPE_INTERNAL);
  }

  protected static byte[] toStorageNodeKey(String levelPath) {
    return toRocksDBKey(levelPath, NODE_TYPE_SG);
  }

  protected static byte[] toEntityNodeKey(String levelPath) {
    return toRocksDBKey(levelPath, NODE_TYPE_ENTITY);
  }

  protected static byte[] toMeasurementNodeKey(String levelPath) {
    return toRocksDBKey(levelPath, NODE_TYPE_MEASUREMENT);
  }

  protected static byte[] toAliasNodeKey(String levelPath) {
    return toRocksDBKey(levelPath, NODE_TYPE_ALIAS);
  }

  protected static byte[] toRocksDBKey(String levelPath, byte type) {
    return Bytes.concat(new byte[] {type}, levelPath.getBytes());
  }

  public static String getLevelPath(String[] nodes, int end) {
    return getLevelPath(nodes, end, end);
  }

  public static String getLevelPath(String[] nodes, int end, int level) {
    StringBuilder builder = new StringBuilder();
    builder.append(ROOT);
    char depth = (char) (ZERO + level);
    for (int i = 1; i <= end; i++) {
      builder.append(PATH_SEPARATOR).append(depth).append(nodes[i]);
    }
    return builder.toString();
  }

  public static String getMeasurementLevelPath(String[] prefixPath, String measurement) {
    String[] nodes = ArrayUtils.add(prefixPath, measurement);
    return getLevelPath(nodes, nodes.length - 1);
  }

  public static List<PartialPath> convertToPartialPath(Collection<String> paths, int level) {
    return paths
        .parallelStream()
        .map(x -> getPartialPathFromInnerPath(x, level))
        .collect(Collectors.toList());
  }

  public static String getNextLevelOfPath(String innerPath, int currentLevel) {
    char levelChar = (char) (ZERO + currentLevel);
    String old = PATH_SEPARATOR + levelChar;
    String target = PATH_SEPARATOR + (char) (levelChar + 1);
    return innerPath.replace(old, target);
  }

  public static String getNextLevelOfPath(String innerPath, char currentLevel) {
    String old = PATH_SEPARATOR + currentLevel;
    String target = PATH_SEPARATOR + (char) (currentLevel + 1);
    return innerPath.replace(old, target);
  }

  public static PartialPath getPartialPathFromInnerPath(String path, int level) {
    char charLevel = (char) (ZERO + level);
    return getPartialPathFromInnerPath(path, charLevel);
  }

  public static PartialPath getPartialPathFromInnerPath(String path, char level) {
    String pathWithoutLevel = path.replace(PATH_SEPARATOR + level, PATH_SEPARATOR);
    String[] nodes = pathWithoutLevel.split(ESCAPE_PATH_SEPARATOR);
    nodes[0] = PATH_ROOT;
    return new PartialPath(nodes);
  }

  public static byte[] buildMeasurementNodeValue(
      MeasurementSchema schema,
      String alias,
      Map<String, String> tags,
      Map<String, String> attributes)
      throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ReadWriteIOUtils.write(DATA_VERSION, outputStream);

    byte flag = DEFAULT_FLAG;
    if (alias != null) {
      flag = (byte) (flag | FLAG_HAS_ALIAS);
    }

    if (tags != null && tags.size() > 0) {
      flag = (byte) (flag | FLAG_HAS_TAGS);
    }

    if (attributes != null && attributes.size() > 0) {
      flag = (byte) (flag | FLAG_HAS_ATTRIBUTES);
    }

    ReadWriteIOUtils.write(flag, outputStream);
    schema.serializeTo(outputStream);

    if (alias != null) {
      ReadWriteIOUtils.write(DATA_BLOCK_TYPE_ALIAS, outputStream);
      ReadWriteIOUtils.write(alias, outputStream);
    }

    if (tags != null && tags.size() > 0) {
      ReadWriteIOUtils.write(DATA_BLOCK_TYPE_TAGS, outputStream);
      ReadWriteIOUtils.write(tags, outputStream);
    }

    if (attributes != null && attributes.size() > 0) {
      ReadWriteIOUtils.write(DATA_BLOCK_TYPE_TAGS, outputStream);
      ReadWriteIOUtils.write(tags, outputStream);
    }
    return outputStream.toByteArray();
  }
}
