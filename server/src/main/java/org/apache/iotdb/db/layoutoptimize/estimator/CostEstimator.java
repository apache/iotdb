package org.apache.iotdb.db.layoutoptimize.estimator;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.queryrecord.QueryRecord;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CostEstimator {
  private DiskInfo diskInfo = new DiskInfo();
  private static final CostEstimator INSTANCE = new CostEstimator();

  private CostEstimator() {}

  public CostEstimator getInstance() {
    return INSTANCE;
  }

  /**
   * Estimate the cost of a query according to the modeling of query process in IoTDB
   *
   * @param query the query to be estimated
   * @param physicalOrder the physical order of chunk in tsfile
   * @param chunkSize the average chunk size
   * @return the cost in milliseconds
   */
  public double estimate(QueryRecord query, List<String> physicalOrder, long chunkSize) {
    if (!diskInfo.hasInit) {
      File diskInfoFile =
          new File(
              IoTDBDescriptor.getInstance().getConfig().getSystemDir()
                  + File.separator
                  + "disk.info");
      try {
        diskInfo.readDiskInfo(diskInfoFile);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    // TODO: get the sample rate, get the chunk group num
    int chunkGroupNum = 1;
    double readCost =
        ((double) chunkSize) * query.getMeasurements().size() / diskInfo.READ_SPEED * chunkGroupNum;
    Set<String> measurements = new HashSet<>(query.getMeasurements());
    int firstMeasurementPos = -1;
    for (int i = 0; i < physicalOrder.size(); i++) {
      if (measurements.contains(physicalOrder.get(i))) {
        firstMeasurementPos = i;
        break;
      }
    }
    double initSeekCost = getSeekCost(firstMeasurementPos * chunkSize);
    double intermediateSeekCost = 0.0d;
    int seekCount = 0;
    int lastIdx = 0;
    for (int i = firstMeasurementPos + 1; i < physicalOrder.size(); i++) {
      if (measurements.contains(physicalOrder.get(i))) {
        intermediateSeekCost += getSeekCost(chunkSize * seekCount);
        seekCount = 0;
        lastIdx = i;
      } else {
        seekCount++;
      }
    }
    double fromLastToFirstSeek =
        getSeekCost((physicalOrder.size() - lastIdx - 1 + firstMeasurementPos) * chunkSize);
    intermediateSeekCost += fromLastToFirstSeek;
    intermediateSeekCost *= chunkGroupNum;
    return intermediateSeekCost + initSeekCost + readCost;
  }

  private double getSeekCost(long seekDistance) {
    for (int i = 0; i < diskInfo.seekData.size() - 1; i++) {
      if (seekDistance >= diskInfo.seekData.get(i).left
          && seekDistance < diskInfo.seekData.get(i + 1).left) {
        double deltaX = seekDistance - diskInfo.seekData.get(i).left;
        double deltaY = diskInfo.seekData.get(i + 1).right - diskInfo.seekData.get(i).right;
        return deltaX
                / ((double) diskInfo.seekData.get(i + 1).left - diskInfo.seekData.get(i).left)
                * deltaY
            + diskInfo.seekData.get(i).right;
      }
    }
    return -1.0d;
  }

  private static class DiskInfo {
    public DiskInfoReader reader;
    public List<Pair<Long, Long>> seekData;
    public double READ_SPEED;
    boolean hasInit = false;

    /**
     * read the seek info of the data
     *
     * @param infoFile
     * @throws IOException
     */
    public void readDiskInfo(File infoFile) throws IOException {
      reader = new DiskInfoReader(infoFile);
      READ_SPEED = reader.getReadSpeed();
      seekData = new ArrayList<>();
      while (reader.hasNext()) {
        String[] nextLine = reader.getNextSeekData();
        seekData.add(new Pair<>(Long.valueOf(nextLine[0]), Long.valueOf(nextLine[1])));
      }
      hasInit = true;
    }
  }
}
