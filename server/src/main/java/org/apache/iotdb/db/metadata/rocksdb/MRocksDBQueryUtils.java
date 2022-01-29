package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.rocksdb.Holder;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_ROOT;

public class MRocksDBQueryUtils {

  private static final char START_FLAG = '\u0019';
  private static final char SPLIT_FLAG = '.';
  private static final byte[] ALL_NODE_TYPE =
      new byte[] {
        RockDBConstants.NODE_TYPE_INTERNAL,
        RockDBConstants.NODE_TYPE_ENTITY,
        RockDBConstants.NODE_TYPE_SG,
        RockDBConstants.NODE_TYPE_MEASUREMENT,
        RockDBConstants.NODE_TYPE_ALIAS
      };

  private static final Logger logger = LoggerFactory.getLogger(MRocksDBQueryUtils.class);

  /**
   * parse value and return a specified type. if no data is required, null is returned.
   *
   * @param value value written in default table
   * @param type the type of value to obtain
   */
  public static Object getValueByParse(byte[] value, byte type) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(value);
    // skip the version flag and node type flag
    ReadWriteIOUtils.readByte(byteBuffer);
    // get block type
    byte flag = ReadWriteIOUtils.readByte(byteBuffer);
    // this means that the following data contains the information we need
    if ((flag & type) > 0) {
      while (byteBuffer.hasRemaining()) {
        byte blockType = ReadWriteIOUtils.readByte(byteBuffer);
        Object obj = new Object();
        switch (blockType) {
          case RockDBConstants.FLAG_SET_TTL:
            obj = ReadWriteIOUtils.readLong(byteBuffer);
            break;
          case RockDBConstants.FLAG_HAS_ALIAS:
            obj = ReadWriteIOUtils.readString(byteBuffer);
            break;
          case RockDBConstants.FLAG_HAS_TAGS:
          case RockDBConstants.FLAG_HAS_ATTRIBUTES:
            obj = ReadWriteIOUtils.readMap(byteBuffer);
            break;
          default:
            break;
        }
        // got the data we need,don't need to read any more
        if (type == blockType) {
          return obj;
        }
      }
    }
    return null;
  }

  /**
   * get all possible inner paths within the specified level range
   *
   * @param partialPath path to be processed
   * @param startLevel min level
   * @param endLevel max level
   * @return all inner name
   */
  public static Set<String> getAllPossiblePath(String partialPath, int startLevel, int endLevel) {
    if (startLevel > endLevel || partialPath == null) {
      logger.error(
          "Can not get path by these params,partialPath: [{}],startLevel:[{}],endLevel:[{}]",
          partialPath,
          startLevel,
          endLevel);
      return Collections.emptySet();
    }
    Set<String> allPath = new HashSet<>();
    for (byte nodeType : ALL_NODE_TYPE) {
      for (int i = startLevel; i <= endLevel; i++) {
        allPath.add(convertPartialPathToInner(partialPath, i, nodeType));
      }
    }

    return allPath;
  }

  /**
   * get all possible inner paths within the specified level range. the results are output in
   * groups. e.g. return [["sroot.2x.2x","sroot.3x.3x"],["mroot.2x.2x","mroot.3x.3x"]] if input is
   * ("x.x" ,2,3)
   *
   * @param partialPath path to be processed
   * @param startLevel min level
   * @param endLevel max level
   * @return all inner name
   */
  public static List<Set<String>> getAllPossiblePathGroupByType(
      String partialPath, int startLevel, int endLevel) {
    if (startLevel > endLevel || partialPath == null) {
      logger.error(
          "Can not get path by these params,partialPath: [{}],startLevel:[{}],endLevel:[{}]",
          partialPath,
          startLevel,
          endLevel);
      return Collections.emptyList();
    }
    List<Set<String>> allPath = new ArrayList<>();
    for (byte nodeType : ALL_NODE_TYPE) {
      allPath.add(getSpecifiedPossiblePath(partialPath, startLevel, endLevel, nodeType));
    }
    return allPath;
  }

  /**
   * get specified possible inner paths within the specified level range. e.g. return
   * ["sroot.2x.2x","sroot.3x.3x"] if input is ("x.x" ,2,3,s)
   *
   * @param partialPath path to be processed
   * @param startLevel min level
   * @param endLevel max level
   * @return all inner name
   */
  public static Set<String> getSpecifiedPossiblePath(
      String partialPath, int startLevel, int endLevel, byte nodeType) {
    if (startLevel > endLevel || partialPath == null) {
      logger.error(
          "Can not get path by these params,partialPath: [{}],startLevel:[{}],endLevel:[{}]",
          partialPath,
          startLevel,
          endLevel);
      return Collections.emptySet();
    }
    Set<String> allPath = new HashSet<>();
    for (int i = startLevel; i <= endLevel; i++) {
      allPath.add(convertPartialPathToInner(partialPath, i, nodeType));
    }
    return allPath;
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
    stringBuilder.append(nodeType).append(PATH_ROOT);
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
}
