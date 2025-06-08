package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.statistics.StatisticsManager;

import org.apache.tsfile.enums.TSDataType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.planner.OperatorTreeGenerator.UNKNOWN_DATATYPE;

public class OperatorGeneratorUtil {

  public static long calculateStatementSizePerLine(
      Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap) {
    long maxStatementSize = Long.BYTES;
    List<TSDataType> dataTypes =
        targetPathToDataTypeMap.values().stream()
            .flatMap(stringTSDataTypeMap -> stringTSDataTypeMap.values().stream())
            .collect(Collectors.toList());
    for (TSDataType dataType : dataTypes) {
      maxStatementSize += getValueSizePerLine(dataType);
    }
    return maxStatementSize;
  }

  private static long getValueSizePerLine(TSDataType tsDataType) {
    switch (tsDataType) {
      case INT32:
      case DATE:
        return Integer.BYTES;
      case INT64:
      case TIMESTAMP:
        return Long.BYTES;
      case FLOAT:
        return Float.BYTES;
      case DOUBLE:
        return Double.BYTES;
      case BOOLEAN:
        return Byte.BYTES;
      case TEXT:
      case BLOB:
      case STRING:
        return StatisticsManager.getInstance().getMaxBinarySizeInBytes();
      default:
        throw new UnsupportedOperationException(UNKNOWN_DATATYPE + tsDataType);
    }
  }
}
