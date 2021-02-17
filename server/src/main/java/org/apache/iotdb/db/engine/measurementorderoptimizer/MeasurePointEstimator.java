package org.apache.iotdb.db.engine.measurementorderoptimizer;

import com.csvreader.CsvReader;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class MeasurePointEstimator {
  /**
   * This class estimates the number of measure point according to empirical value
   */
  private final File EMPIRICAL_FILE;
  private static final Logger LOGGER = LoggerFactory.getLogger(MeasurePointEstimator.class);
  List<Pair<Long, Integer>> empiricalData = new ArrayList<>();

  public static class MeasurePointEstimatorHolder {
    private static final MeasurePointEstimator estimator = new MeasurePointEstimator();
  }

  public static MeasurePointEstimator getInstance() {
    return MeasurePointEstimatorHolder.estimator;
  }

  private MeasurePointEstimator() {
    String systemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    String empiricalFilePath = systemDir + File.separator + "experiment" + File.separator + "measure_point.csv";
    EMPIRICAL_FILE = new File(empiricalFilePath);
    if (!EMPIRICAL_FILE.exists()) {
      LOGGER.error("File " + EMPIRICAL_FILE.getAbsolutePath() + " does not exist");
      return;
    } else {
      LOGGER.info("Reading from " + EMPIRICAL_FILE.getAbsolutePath());
    }
    readEmpiricalData();
  }

  /**
   * Read the empirical data from the csv file
   */
  private void readEmpiricalData() {
    try {
      CsvReader reader = new CsvReader(EMPIRICAL_FILE.getAbsolutePath());
      reader.readHeaders();
      while(reader.readRecord()) {
        String chunkSize = reader.get("ChunkSize");
        String pointNum = reader.get("PointNum");
        empiricalData.add(new Pair<>(stringDataToBytes(chunkSize), Integer.valueOf(pointNum)));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Convert the data size in string to long
   * @param data: Data size in string, such as "10KB", "20MB"
   * @return The number of byte in Long
   */
  private long stringDataToBytes(String data) {
    String[] suffixes = {"B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};
    long result = 0;
    for(int i = suffixes.length - 1; i >= 0; --i) {
      if (data.endsWith(suffixes[i])) {
        int pos = data.indexOf(suffixes[i]);
        float base = Float.valueOf(data.substring(0, pos));
        for(int j = 0; j < i; ++j) {
          base *= 1024.0;
        }
        result = (long)base;
        break;
      }
    }
    return result;
  }

  /**
   * Get the number of measure point according to the chunk size by linear interpolation.
   * @param chunkSize: The number of byte of the chunk size
   * @return The number of the measure point.
   */
  public int getMeasurePointNum(long chunkSize) {
    if (empiricalData.size() == 0) {
      readEmpiricalData();
    }
    for(int i = 0; i < empiricalData.size() - 1; ++i) {
      if (chunkSize >= empiricalData.get(i).left &&
      chunkSize < empiricalData.get(i + 1).left) {
        double deltaY = empiricalData.get(i+1).right - empiricalData.get(i).right;
        double deltaX = empiricalData.get(i+1).left - empiricalData.get(i).left;
        double dx = chunkSize - empiricalData.get(i).left;
        double result = empiricalData.get(i).right;
        result += (dx / deltaX) * deltaY;
        return (int)result;
      }
    }
    if (chunkSize == empiricalData.get(empiricalData.size() - 1).left) {
      return empiricalData.get(empiricalData.size() - 1).right;
    }
    if (chunkSize > empiricalData.get(empiricalData.size() - 1).left) {
      LOGGER.error("ChunkSize " + chunkSize + " is too large");
    } else if (chunkSize < 0) {
      LOGGER.error("ChunkSize " + chunkSize + " is invalid");
    }
    return -1;
  }

}
