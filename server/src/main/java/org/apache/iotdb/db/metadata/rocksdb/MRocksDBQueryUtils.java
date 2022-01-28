package org.apache.iotdb.db.metadata.rocksdb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MRocksDBQueryUtils {

  private static final int MAX_LEVEL = 10;
  private static final char START_FLAG = '\u0019';
  private static final char SPLIT_FLAG = '.';
  private static final byte[] ALL_NODE_TYPE = new byte[]{RockDBConstants.NODE_TYPE_INNER,
      RockDBConstants.NODE_TYPE_ENTITY, RockDBConstants.NODE_TYPE_SG,
      RockDBConstants.NODE_TYPE_MEASUREMENT};

  private static final Logger logger = LoggerFactory.getLogger(MRocksDBQueryUtils.class);

  /**
   * parse value and return a specified type. if no data is required, null is returned.
   *
   * @param value value written in default table
   * @param type  the type of value to obtain
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
   * @param startLevel  min level
   * @param endLevel    max level
   * @return all inner name
   */
  public static Set<String> getAllPossiblePath(String partialPath, int startLevel, int endLevel) {
    if (startLevel > endLevel || partialPath == null) {
      logger
          .error("Can not get path by these params,partialPath: [{}],startLevel:[{}],endLevel:[{}]",
              partialPath, startLevel, endLevel);
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
   * @param startLevel  min level
   * @param endLevel    max level
   * @return all inner name
   */
  public static List<Set<String>> getAllPossiblePathGroupByType(String partialPath, int startLevel,
      int endLevel) {
    if (startLevel > endLevel || partialPath == null) {
      logger
          .error("Can not get path by these params,partialPath: [{}],startLevel:[{}],endLevel:[{}]",
              partialPath, startLevel, endLevel);
      return Collections.emptyList();
    }
    List<Set<String>> allPath = new ArrayList<>();
    for (byte nodeType : ALL_NODE_TYPE) {
      Set<String> allPathGroupByCurrType = new HashSet<>();
      for (int i = startLevel; i <= endLevel; i++) {
        allPathGroupByCurrType.add(convertPartialPathToInner(partialPath, i, nodeType));
      }
      allPath.add(allPathGroupByCurrType);
    }
    return allPath;
  }


  /**
   * get inner name by converting partial path.
   *
   * @param partialPath the path needed to be converted.
   * @param level       the level needed to be added.
   * @param nodeType    specified type
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
}
