package org.apache.iotdb.db.rescon;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.timeindex.TimeIndexLevel;

import org.apache.iotdb.db.utils.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;

public class TsFileResourceManager {
  private static final Logger logger = LoggerFactory.getLogger(TsFileResourceManager.class);

  private static IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  /** threshold total memory for all TimeIndex */
  private static double TIME_INDEX_MEMORY_THRESHOLD =
      CONFIG.getAllocateMemoryForRead() * CONFIG.getTimeIndexMemoryProportion();

  /** thread number for timeIndex degradation */
  private static int TIME_INDEX_DEGRADE_THREAD = CONFIG.getTimeIndexDegradeThread();

  /** thread pool to implement the degrade task */
  private ExecutorService degradeThreadPool =
      IoTDBThreadPoolFactory.newFixedThreadPool(
          TIME_INDEX_DEGRADE_THREAD, "TimeIndex_Degrade_Pool");

  /** store the sealed TsFileResource, sorted by priority of TimeIndex */
  private PriorityQueue<TsFileResource> sealedTsFileResources =
      new PriorityQueue<>(TsFileResource::compareIndexDegradePriority);

  /** total used memory for TimeIndex */
  private long totalTimeIndexMemCost;

  @TestOnly
  public static void setTimeIndexMemoryProportion(double timeIndexMemoryProportion) {
    TIME_INDEX_MEMORY_THRESHOLD =
            CONFIG.getAllocateMemoryForRead() * timeIndexMemoryProportion;
  }

  /**
   * add the closed TsFileResource into priorityQueue and increase memory cost of timeIndex, once
   * memory cost is larger than threshold, degradation is triggered.
   */
  public synchronized void registerSealedTsFileResource(TsFileResource tsFileResource) {
    sealedTsFileResources.add(tsFileResource);
    totalTimeIndexMemCost += tsFileResource.calculateRamSize();
    System.out.println("memory used " + totalTimeIndexMemCost + " " + TIME_INDEX_MEMORY_THRESHOLD);
    chooseTsFileResourceToDegrade();
  }

  /** once degradation is triggered, the total memory for timeIndex should reduce */
  public synchronized void releaseTimeIndexMemCost(long memCost) {
    totalTimeIndexMemCost -= memCost;
  }

  /**
   * choose the top TsFileResource in priorityQueue to degrade until the memory is smaller than
   * threshold.
   */
  private void chooseTsFileResourceToDegrade() {
    while (BigDecimal.valueOf(totalTimeIndexMemCost)
            .compareTo(BigDecimal.valueOf(TIME_INDEX_MEMORY_THRESHOLD))
        > 0) {
      TsFileResource tsFileResource = sealedTsFileResources.poll();
      if (TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType())
          == TimeIndexLevel.FILE_TIME_INDEX) {
        logger.error("Can't degrade any more");
        throw new RuntimeException("Can't degrade any more");
      }
      long memoryReduce = tsFileResource.releaseMemory();
      releaseTimeIndexMemCost(memoryReduce);
    }
  }

  private TsFileResourceManager() {}

  public static TsFileResourceManager getInstance() {
    return TsFileResourceManager.InstanceHolder.instance;
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static TsFileResourceManager instance = new TsFileResourceManager();
  }
}
