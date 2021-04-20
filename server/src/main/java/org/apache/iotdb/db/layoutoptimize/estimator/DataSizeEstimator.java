package org.apache.iotdb.db.layoutoptimize.estimator;

import org.apache.iotdb.db.conf.adapter.CompressionRatio;
import org.apache.iotdb.db.exception.layoutoptimize.DataSizeInfoNotExistsException;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataSizeEstimator {
  private static final DataSizeEstimator INSTANCE = new DataSizeEstimator();
  // storage group -> List<Pair<dataPoint, dataSize>>
  private Map<String, List<Pair<Long, Long>>> dataPointToMemSize = new HashMap<>();

  private DataSizeEstimator() {}

  public static DataSizeEstimator getInstance() {
    return INSTANCE;
  }

  /**
   * TODO: return the chunkSize in disk according to the measurement point num
   *
   * @param storageGroup
   * @param pointNum
   * @return
   */
  public long getChunkSizeInDisk(String storageGroup, long pointNum)
      throws DataSizeInfoNotExistsException {
    long chunkSizeInMem = getChunkSizeInMemory(storageGroup, pointNum);
    double compressionRatio = CompressionRatio.getInstance().getRatio();
    return (long) (chunkSizeInMem * compressionRatio);
  }

  /**
   * TODO: return the number of data point according to the chunk size in disk
   *
   * @param storageGroup
   * @param chunkSize
   * @return
   */
  public long getPointNumInDisk(String storageGroup, long chunkSize) {
    return 0L;
  }

  /**
   * TODO: return the data size in the memory according to the number of data point
   *
   * @param storageGroup
   * @param pointNum
   * @return
   */
  public long getChunkSizeInMemory(String storageGroup, long pointNum)
      throws DataSizeInfoNotExistsException {
    List<Pair<Long, Long>> dataPointList = dataPointToMemSize.getOrDefault(storageGroup, null);
    if (dataPointList == null || dataPointList.size() == 0) {
      throw new DataSizeInfoNotExistsException(
          String.format(
              "the data info of storage group %s does not exist in DataSizeEstimator",
              storageGroup));
    }
    long chunkSize = -1L;
    for (int i = 0; i < dataPointList.size() - 1; ++i) {
      if (dataPointList.get(i).left <= pointNum && dataPointList.get(i + 1).left > pointNum) {
        double deltaX = dataPointList.get(i + 1).left - dataPointList.get(i).left;
        double deltaY = dataPointList.get(i + 1).right - dataPointList.get(i).right;
        chunkSize =
            (long)
                (((double) (pointNum - dataPointList.get(i).left)) / deltaX * deltaY
                    + dataPointList.get(i).right);
        break;
      }
    }
    if (chunkSize == -1L) {
      Pair<Long, Long> lastData = dataPointList.get(dataPointList.size() - 1);
      chunkSize = (long) (((double) (pointNum / lastData.left)) * lastData.right);
    }
    return chunkSize;
  }

  /**
   * return the number of data point according to the data size in the memory
   *
   * @param storageGroup
   * @param chunkSize
   * @return
   */
  public long getPointNumInMemory(String storageGroup, long chunkSize) {
    return 0L;
  }

  public void addDataInfo(String storageGroup, long dataPointNum, long dataSizeInMem) {
    if (!dataPointToMemSize.containsKey(storageGroup)) {
      dataPointToMemSize.put(storageGroup, new ArrayList<>());
    }
    int insertPos = 0;
    List<Pair<Long, Long>> dataList = dataPointToMemSize.get(storageGroup);
    for (int i = 0; i < dataList.size(); i++) {
      if (dataList.get(i).left > dataPointNum) {
        insertPos = i;
      }
    }
    dataList.add(insertPos, new Pair<>(dataPointNum, dataSizeInMem));
  }
}
