package org.apache.iotdb.db.layoutoptimize.estimator;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.adapter.CompressionRatio;
import org.apache.iotdb.db.exception.layoutoptimize.DataSizeInfoNotExistsException;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataSizeEstimator {
  private static final Logger logger = LoggerFactory.getLogger(DataSizeEstimator.class);
  private static final DataSizeEstimator INSTANCE = new DataSizeEstimator();
  // storage group -> List<Pair<dataPoint, dataSize>>
  private Map<String, List<Pair<Long, Long>>> dataPointToMemSize = new HashMap<>();
  private File dataSizeFile =
      new File(
          IoTDBDescriptor.getInstance().getConfig().getLayoutDir()
              + File.separator
              + "dataSizeEsitmate.info");

  private DataSizeEstimator() {
    recoverFromFile();
  }

  public static DataSizeEstimator getInstance() {
    return INSTANCE;
  }

  /**
   * get the chunk size in the disk according to the number of data point
   *
   * @param storageGroup the storage group where the measurement is located
   * @param pointNum the number of data point
   * @return the chunk size in the disk in byte
   * @throws DataSizeInfoNotExistsException
   */
  public long getChunkSizeInDisk(String storageGroup, long pointNum)
      throws DataSizeInfoNotExistsException {
    long chunkSizeInMem = getChunkSizeInMemory(storageGroup, pointNum);
    double compressionRatio = CompressionRatio.getInstance().getRatio();
    return (long) (chunkSizeInMem / compressionRatio);
  }

  /**
   * get the number of data point in the disk according to the data chunk size
   *
   * @param storageGroup the storage group where the measurement is located
   * @param chunkSize the data chunk size in the disk in byte
   * @return the number of data point
   * @throws DataSizeInfoNotExistsException
   */
  public long getPointNumInDisk(String storageGroup, long chunkSize)
      throws DataSizeInfoNotExistsException {
    List<Pair<Long, Long>> dataPointList = dataPointToMemSize.getOrDefault(storageGroup, null);
    if (dataPointList == null || dataPointList.size() == 0) {
      throw new DataSizeInfoNotExistsException(
          String.format(
              "the data info of storage group %s does not exist in DataSizeEstimator",
              storageGroup));
    }
    double compressionRatio = CompressionRatio.getInstance().getRatio();
    long pointNum = -1L;
    for (int i = 0; i < dataPointList.size() - 1; i++) {
      if (dataPointList.get(i).right <= chunkSize && dataPointList.get(i + 1).right > chunkSize) {
        double deltaX = dataPointList.get(i + 1).right - dataPointList.get(i).right;
        double deltaY = dataPointList.get(i + 1).left - dataPointList.get(i).left;
        pointNum =
            (long)
                ((chunkSize * compressionRatio - dataPointList.get(i).right) / deltaX * deltaY
                    + dataPointList.get(i).left);
      }
    }
    if (pointNum == -1L) {
      Pair<Long, Long> lastData = dataPointList.get(dataPointList.size() - 1);
      pointNum = (long) ((double) (chunkSize * compressionRatio / lastData.right) * lastData.left);
    }
    return pointNum;
  }

  /**
   * get the data chunk size in memory according to the number of data point
   *
   * @param storageGroup the storage group where the measurement is located
   * @param pointNum the number of data point
   * @return the size of data chunk in the memory in byte
   * @throws DataSizeInfoNotExistsException
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
      chunkSize = (long) ((((double) pointNum / (double) lastData.left)) * lastData.right);
    }
    return chunkSize;
  }

  /**
   * get the number of data point according to the size of data chunk in memory
   *
   * @param storageGroup the storage group where the measurement is located
   * @param chunkSize the size of the data chunk in memory
   * @return the number of data point
   * @throws DataSizeInfoNotExistsException
   */
  public long getPointNumInMemory(String storageGroup, long chunkSize)
      throws DataSizeInfoNotExistsException {
    List<Pair<Long, Long>> dataPointList = dataPointToMemSize.getOrDefault(storageGroup, null);
    if (dataPointList == null || dataPointList.size() == 0) {
      throw new DataSizeInfoNotExistsException(
          String.format(
              "the data info of storage group %s does not exist in DataSizeEstimator",
              storageGroup));
    }
    long pointNum = -1L;
    for (int i = 0; i < dataPointList.size() - 1; i++) {
      if (dataPointList.get(i).right <= chunkSize && dataPointList.get(i + 1).right > chunkSize) {
        double deltaX = dataPointList.get(i + 1).right - dataPointList.get(i).right;
        double deltaY = dataPointList.get(i + 1).left - dataPointList.get(i).left;
        pointNum =
            (long)
                ((chunkSize - dataPointList.get(i).right) / deltaX * deltaY
                    + dataPointList.get(i).left);
      }
    }
    if (pointNum == -1L) {
      Pair<Long, Long> lastData = dataPointList.get(dataPointList.size() - 1);
      pointNum = (long) ((double) (chunkSize / lastData.right) * lastData.left);
    }
    return pointNum;
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
    persistDataInfo();
  }

  public boolean persistDataInfo() {
    Gson gson = new Gson();
    String json = gson.toJson(dataPointToMemSize);
    try {
      File layoutDir = new File(IoTDBDescriptor.getInstance().getConfig().getLayoutDir());
      if (!layoutDir.exists()) {
        layoutDir.mkdirs();
      }
      if (!dataSizeFile.exists()) {
        dataSizeFile.createNewFile();
      }
      BufferedOutputStream outputStream =
          new BufferedOutputStream(new FileOutputStream(dataSizeFile));
      outputStream.write(json.getBytes(StandardCharsets.UTF_8));
      outputStream.flush();
      outputStream.close();
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  public boolean recoverFromFile() {
    logger.info("recovering from file");
    try {
      if (!dataSizeFile.exists()) {
        logger.info("cannot find {}", dataSizeFile);
        return false;
      }
      BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(dataSizeFile));
      byte[] buffer = new byte[(int) dataSizeFile.length()];
      inputStream.read(buffer);
      String json = new String(buffer);
      Map<String, List<LinkedTreeMap<String, Double>>> tmpMap =
          new Gson().fromJson(json, dataPointToMemSize.getClass());
      for (Map.Entry<String, List<LinkedTreeMap<String, Double>>> entry : tmpMap.entrySet()) {
        dataPointToMemSize.put(entry.getKey(), new ArrayList<>());
        List<Pair<Long, Long>> list = dataPointToMemSize.get(entry.getKey());
        for (LinkedTreeMap<String, Double> item : entry.getValue()) {
          list.add(new Pair<>(item.get("left").longValue(), item.get("right").longValue()));
        }
      }
      return true;
    } catch (IOException e) {
      return false;
    }
  }
}
