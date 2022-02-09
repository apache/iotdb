package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.google.common.primitives.Bytes;
import org.apache.commons.lang3.ArrayUtils;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Holder;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.db.metadata.rocksdb.RockDBConstants.*;

public class RocksDBUtils {

  private static final Logger logger = LoggerFactory.getLogger(RocksDBUtils.class);

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

    if (schema != null) {
      ReadWriteIOUtils.write(DATA_BLOCK_TYPE_SCHEMA, outputStream);
      schema.serializeTo(outputStream);
    }

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

  public static byte[] buildAliasNodeValue(byte[] originKey) {
    byte[] prefix = new byte[] {DATA_VERSION, DEFAULT_FLAG, DATA_BLOCK_TYPE_ORIGIN_KEY};
    byte[] len = BytesUtils.intToBytes(originKey.length);
    return BytesUtils.concatByteArray(BytesUtils.concatByteArray(prefix, len), originKey);
  }

  public static byte[] readOriginKey(ByteBuffer buffer) {
    int len = ReadWriteIOUtils.readInt(buffer);
    return ReadWriteIOUtils.readBytes(buffer, len);
  }

  public static int indexOfDataBlockType(byte[] data, byte type) {
    if ((data[1] & FLAG_SET_TTL) == 0) {
      return -1;
    }

    int index = -1;
    ByteBuffer byteBuffer = ByteBuffer.wrap(data);
    // skip the version flag and node type flag
    ReadWriteIOUtils.readBytes(byteBuffer, 2);
    while (byteBuffer.hasRemaining()) {
      byte blockType = ReadWriteIOUtils.readByte(byteBuffer);
      switch (blockType) {
        case DATA_BLOCK_TYPE_TTL:
          ReadWriteIOUtils.readLong(byteBuffer);
          break;
        case DATA_BLOCK_TYPE_ALIAS:
          ReadWriteIOUtils.readString(byteBuffer);
          break;
        case DATA_BLOCK_TYPE_ORIGIN_KEY:
          readOriginKey(byteBuffer);
          break;
        case DATA_BLOCK_TYPE_SCHEMA:
          MeasurementSchema.deserializeFrom(byteBuffer);
          break;
        case DATA_BLOCK_TYPE_TAGS:
        case DATA_BLOCK_TYPE_ATTRIBUTES:
          ReadWriteIOUtils.readMap(byteBuffer);
          break;
        default:
          break;
      }
      // got the data we need,don't need to read any more
      if (type == blockType) {
        index = byteBuffer.arrayOffset();
        break;
      }
    }
    return index;
  }

  public static byte[] updateTTL(byte[] origin, long ttl) {
    int index = indexOfDataBlockType(origin, DATA_BLOCK_TYPE_TTL);
    if (index == -1) {
      byte[] ttlBlock = new byte[Long.BYTES + 1];
      ttlBlock[0] = DATA_BLOCK_TYPE_TTL;
      BytesUtils.longToBytes(ttl, ttlBlock, 1);
      origin[1] = (byte) (origin[1] | FLAG_SET_TTL);
      return BytesUtils.concatByteArray(origin, ttlBlock);
    } else {
      BytesUtils.longToBytes(ttl, origin, index + 1);
      return origin;
    }
  }

  private static final char START_FLAG = '\u0019';
  private static final char SPLIT_FLAG = '.';

  /**
   * parse value and return a specified type. if no data is required, null is returned.
   *
   * @param value value written in default table
   * @param type the type of value to obtain
   */
  public static Object parseNodeValue(byte[] value, byte type) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(value);
    // skip the version flag and node type flag
    ReadWriteIOUtils.readByte(byteBuffer);
    // get block type
    byte flag = ReadWriteIOUtils.readByte(byteBuffer);

    Object obj = null;
    // this means that the following data contains the information we need
    if ((flag & type) > 0) {
      while (byteBuffer.hasRemaining()) {
        byte blockType = ReadWriteIOUtils.readByte(byteBuffer);
        switch (blockType) {
          case DATA_BLOCK_TYPE_TTL:
            obj = ReadWriteIOUtils.readLong(byteBuffer);
            break;
          case DATA_BLOCK_TYPE_ALIAS:
            obj = ReadWriteIOUtils.readString(byteBuffer);
            break;
          case DATA_BLOCK_TYPE_ORIGIN_KEY:
            obj = readOriginKey(byteBuffer);
            break;
          case DATA_BLOCK_TYPE_SCHEMA:
            obj = MeasurementSchema.deserializeFrom(byteBuffer);
            break;
          case DATA_BLOCK_TYPE_TAGS:
          case DATA_BLOCK_TYPE_ATTRIBUTES:
            obj = ReadWriteIOUtils.readMap(byteBuffer);
            break;
          default:
            break;
        }
        // got the data we need,don't need to read any more
        if (type == blockType) {
          break;
        }
      }
    }
    return obj;
  }

  /**
   * get inner name by converting partial path.
   *
   * @param partialPath the path needed to be converted.
   * @param level the level needed to be added.
   * @param nodeType specified type
   * @return inner name
   */
  public static String convertPartialPathToInner(String partialPath, int level, byte nodeType) {
    char lastChar = START_FLAG;
    StringBuilder stringBuilder = new StringBuilder();
    for (char c : partialPath.toCharArray()) {
      if (START_FLAG == lastChar) {
        stringBuilder.append(nodeType);
      }
      if (SPLIT_FLAG == lastChar) {
        stringBuilder.append(level);
      }
      stringBuilder.append(c);
      lastChar = c;
    }
    return stringBuilder.toString();
  }

  public static String convertPartialPathToInnerByNodes(String[] nodes, int level, byte nodeType) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(nodeType).append(TsFileConstant.PATH_ROOT);
    for (String str : nodes) {
      stringBuilder.append(SPLIT_FLAG).append(level).append(str);
    }
    return stringBuilder.toString();
  }

  public static Set<String> getKeyByPrefix(RocksDB rocksDB, String innerName) {
    RocksIterator iterator = rocksDB.newIterator();
    Set<String> result = new HashSet<>();
    for (iterator.seek(innerName.getBytes()); iterator.isValid(); iterator.next()) {
      String keyStr = new String(iterator.key());
      if (!keyStr.startsWith(innerName)) {
        break;
      }
      result.add(keyStr);
    }
    return result;
  }

  public static int getLevelByPartialPath(String partialPath) {
    int levelCount = 0;
    for (char c : partialPath.toCharArray()) {
      if (SPLIT_FLAG == c) {
        levelCount++;
      }
    }
    return levelCount;
  }

  public static String findBelongToSpecifiedNodeType(
      String[] nodes, RocksDB rocksDB, byte nodeType) {
    String innerPathName;
    for (int level = nodes.length; level > 0; level--) {
      String[] copy = Arrays.copyOf(nodes, level);
      innerPathName = convertPartialPathToInnerByNodes(copy, level, nodeType);
      boolean isBelongToType = rocksDB.keyMayExist(innerPathName.getBytes(), new Holder<>());
      if (isBelongToType) {
        return innerPathName;
      }
    }
    return null;
  }

  /**
   * Statistics the number of all data entries for a specified column family
   *
   * @param rocksDB rocksdb
   * @param columnFamilyHandle specified column family handle
   * @return total number in this column family
   */
  public static long countNodesNum(RocksDB rocksDB, ColumnFamilyHandle columnFamilyHandle) {
    RocksIterator iter;
    if (columnFamilyHandle == null) {
      iter = rocksDB.newIterator();
    } else {
      iter = rocksDB.newIterator(columnFamilyHandle);
    }
    long count = 0;
    for (iter.seekToFirst(); iter.isValid(); iter.next()) {
      count++;
    }
    return count;
  }

  /**
   * Count the number of keys with the specified prefix in the specified column family
   *
   * @param rocksDB rocksdb
   * @param columnFamilyHandle specified column family handle
   * @param nodeType specified prefix
   * @return total number in this column family
   */
  public static long countNodesNumByType(
      RocksDB rocksDB, ColumnFamilyHandle columnFamilyHandle, byte nodeType) {
    RocksIterator iter;
    if (columnFamilyHandle == null) {
      iter = rocksDB.newIterator();
    } else {
      iter = rocksDB.newIterator(columnFamilyHandle);
    }
    long count = 0;
    for (iter.seek(new byte[] {nodeType}); iter.isValid(); iter.next()) {
      if (iter.key()[0] == nodeType) {
        count++;
      }
    }
    return count;
  }

  public static String getPathByInnerName(String innerName) {
    char[] keyConvertToCharArray = innerName.toCharArray();
    StringBuilder stringBuilder = new StringBuilder();
    char lastChar = START_FLAG;
    for (char c : keyConvertToCharArray) {
      if (SPLIT_FLAG == lastChar || START_FLAG == lastChar) {
        lastChar = c;
        continue;
      }
      stringBuilder.append(c);
      lastChar = c;
    }
    return stringBuilder.toString();
  }
}
