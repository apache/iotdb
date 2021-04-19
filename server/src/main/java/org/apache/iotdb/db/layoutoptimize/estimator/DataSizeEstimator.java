package org.apache.iotdb.db.layoutoptimize.estimator;

public class DataSizeEstimator {
  private static final DataSizeEstimator INSTANCE = new DataSizeEstimator();

  private DataSizeEstimator() {}

  public DataSizeEstimator getInstance() {
    return INSTANCE;
  }

  /**
   * TODO: return the chunkSize in disk according to the measurement point num
   *
   * @param deviceId
   * @param pointNum
   * @return
   */
  public long getChunkSizeInDisk(String deviceId, long pointNum) {
    return 0L;
  }

  /**
   * TODO: return the number of data point according to the chunk size in disk
   *
   * @param deviceId
   * @param chunkSize
   * @return
   */
  public long getPointNumInDisk(String deviceId, long chunkSize) {
    return 0L;
  }

  /**
   * TODO: return the data size in the memory according to the number of data point
   *
   * @param deviceId
   * @param pointNum
   * @return
   */
  public long getChunkSizeInMemory(String deviceId, long pointNum) {
    return 0L;
  }

  /**
   * return the number of data point according to the data size in the memory
   *
   * @param deviceId
   * @param chunkSize
   * @return
   */
  public long getPointNumInMemory(String deviceId, long chunkSize) {
    return 0L;
  }
}
