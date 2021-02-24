package org.apache.iotdb.db.engine.measurementorderoptimizer;

import org.apache.iotdb.db.engine.divergentdesign.Replica;
import org.apache.iotdb.db.engine.measurementorderoptimizer.costmodel.CostModel;
import org.apache.iotdb.db.query.workloadmanager.Workload;
import org.apache.iotdb.db.query.workloadmanager.WorkloadManager;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.GroupByQueryRecord;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.QueryRecord;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MultiReplicaOrderOptimizer {
  private int replicaNum = 3;
  private int maxIter = 500000;
  private float breakPoint = 1e-2f;
  private List<QueryRecord> queryRecords;
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiReplicaOrderOptimizer.class);
  private String deviceID;
  private Replica[] replicas;
  private List<String> measurementOrder;
  private List<QueryRecord> records;
  private List<Long> chunkSize;
  private final float SA_INIT_TEMPERATURE = 100.0f;
  private final float COOLING_RATE = 0.999971f;
  private List<Double> costList = new LinkedList<>();
  private static long CHUNK_SIZE_STEP_NUM = 70000l;
  private final float CHUNK_SIZE_LOWER_BOUND = 0.8f;
  private final float CHUNK_SIZE_UPPER_BOUND = 1.3f;

  public MultiReplicaOrderOptimizer(String deviceID) {
    this.deviceID = deviceID;
    measurementOrder = new ArrayList<>(MeasurementOrderOptimizer.
            getInstance().getMeasurementsOrder(deviceID));
    replicas = new Replica[replicaNum];
    for (int i = 0; i < replicaNum; ++i) {
      replicas[i] = new Replica(deviceID, measurementOrder,
              MeasurementOrderOptimizer.getInstance().getAverageChunkSize(deviceID));
    }
    records = new ArrayList<>(WorkloadManager.getInstance().getRecord(deviceID));
    chunkSize = new ArrayList<>(MeasurementOrderOptimizer.getInstance().getChunkSize(deviceID));
  }

  public MultiReplicaOrderOptimizer(String deviceID, int replicaNum) {
    this.deviceID = deviceID;
    measurementOrder = new ArrayList<>(MeasurementOrderOptimizer.
            getInstance().getMeasurementsOrder(deviceID));
    this.replicaNum = replicaNum;
    replicas = new Replica[replicaNum];
    for (int i = 0; i < replicaNum; ++i) {
      replicas[i] = new Replica(deviceID, measurementOrder,
              MeasurementOrderOptimizer.getInstance().getAverageChunkSize(deviceID));
    }
    records = new ArrayList<>(WorkloadManager.getInstance().getRecord(deviceID));
    chunkSize = new ArrayList<>(MeasurementOrderOptimizer.getInstance().getChunkSize(deviceID));
  }

  public void setMaxIter(int iter) {
    maxIter = iter;
  }

  public Pair<Replica[], Workload[]> optimizeBySA() {
    double curCost = getCostAndWorkloadPartitionForCurReplicas(records, replicas).left;
    LOGGER.info("Ori cost: " + curCost);
    Pair<Long, Long> chunkBound = getChunkSizeBound(records);
    long chunkLowerBound = chunkBound.left;
    long chunkUpperBound = chunkBound.right;
    float temperature = SA_INIT_TEMPERATURE;
    long optimizeStartTime = System.currentTimeMillis();
    Random r = new Random();
    Workload[] workloadPartition = null;
    int k = 0;
    for (; k < maxIter && System.currentTimeMillis() - optimizeStartTime < 30l * 60l * 1000l; ++k) {
      temperature = temperature * COOLING_RATE;
      int selectedReplica = r.nextInt(replicaNum);
      int swapLeft = r.nextInt(measurementOrder.size());
      int swapRight = r.nextInt(measurementOrder.size());
      while (swapLeft == swapRight) {
        swapLeft = r.nextInt(measurementOrder.size());
        swapRight = r.nextInt(measurementOrder.size());
      }
      replicas[selectedReplica].swapMeasurementPos(swapLeft, swapRight);
      Pair<Float, Workload[]> costAndWorkloadPartition = getCostAndWorkloadPartitionForCurReplicas(records, replicas);
      double newCost = costAndWorkloadPartition.left;
      workloadPartition = costAndWorkloadPartition.right;
      float probability = r.nextFloat();
      probability = probability < 0 ? -probability : probability;
      probability %= 1.0f;
      if (newCost < curCost ||
              Math.exp((curCost - newCost) / temperature) > probability) {
        curCost = newCost;
      } else {
        replicas[selectedReplica].swapMeasurementPos(swapLeft, swapRight);
      }
      costList.add(curCost);
      if (k % 500 == 0) {
        LOGGER.info(String.format("Epoch %d: curCost %.3f", k, curCost));
      }
    }
    LOGGER.info("Final cost: " + curCost);
    LOGGER.info("Loop count: " + k);
    return new Pair<>(replicas, workloadPartition);
  }

  public Pair<Replica[], Workload[]> optimizeBySAWithChunkSizeAdjustment() {
    double curCost = getCostAndWorkloadPartitionForCurReplicas(records, replicas).left;
    LOGGER.info("Ori cost: " + curCost);
    Pair<Long, Long> chunkBound = getChunkSizeBound(records);
    long chunkLowerBound = chunkBound.left;
    long chunkUpperBound = chunkBound.right;
    float temperature = SA_INIT_TEMPERATURE;
    long optimizeStartTime = System.currentTimeMillis();
    Random r = new Random();
    Workload[] workloadPartition = null;
    int k = 0;
    for (; k < maxIter && System.currentTimeMillis() - optimizeStartTime < 30l * 60l * 1000l; ++k) {
      temperature = temperature * COOLING_RATE;
      int selectedReplica = r.nextInt(replicaNum);
      int swapLeft = r.nextInt(measurementOrder.size());
      int swapRight = r.nextInt(measurementOrder.size());
      while (swapLeft == swapRight) {
        swapLeft = r.nextInt(measurementOrder.size());
        swapRight = r.nextInt(measurementOrder.size());
      }
      replicas[selectedReplica].swapMeasurementPos(swapLeft, swapRight);
      long newChunkSize = Math.abs(r.nextLong());
      newChunkSize = newChunkSize % (chunkUpperBound - chunkLowerBound) + chunkLowerBound;
      long curChunkSize = replicas[selectedReplica].getAverageChunkSize();
      replicas[selectedReplica].setAverageChunkSize(newChunkSize);
      Pair<Float, Workload[]> costAndWorkloadPartition = getCostAndWorkloadPartitionForCurReplicas(records, replicas);
      double newCost = costAndWorkloadPartition.left;
      workloadPartition = costAndWorkloadPartition.right;
      float probability = r.nextFloat();
      probability = probability < 0 ? -probability : probability;
      probability %= 1.0f;
      if (newCost < curCost ||
              Math.exp((curCost - newCost) / temperature) > probability) {
        curCost = newCost;
      } else {
        replicas[selectedReplica].swapMeasurementPos(swapLeft, swapRight);
        replicas[selectedReplica].setAverageChunkSize(curChunkSize);
      }
      costList.add(curCost);
      if (k % 500 == 0) {
        LOGGER.info(String.format("Epoch %d: curCost %.3f", k, curCost));
      }
    }
    LOGGER.info("Final cost: " + curCost);
    LOGGER.info("Loop count: " + k);
    return new Pair<>(replicas, workloadPartition);
  }

  private Pair<Float, Workload[]> getCostAndWorkloadPartitionForCurReplicas(List<QueryRecord> records, Replica[] replicas) {
    float totalCost = 0.0f;
    Workload[] workloads = new Workload[replicaNum];
    for (int i = 0; i < replicaNum; ++i) {
      workloads[i] = new Workload();
    }
    List<Integer> indexes = new ArrayList<>();
    for (int i = 0; i < replicaNum; ++i) {
      indexes.add(i);
    }
    for (QueryRecord record : records) {
      List<QueryRecord> tmpList = new ArrayList<>();
      tmpList.add(record);
      float minCost = Float.MAX_VALUE;
      int minIdx = 0;
      Collections.shuffle(indexes);
      for (Integer i : indexes) {
        Replica replica = replicas[i];
        float curCost = CostModel.approximateAggregationQueryCostWithTimeRange(tmpList, replica.getMeasurements(), replica.getAverageChunkSize());
        if (curCost < minCost) {
          minCost = curCost;
          minIdx = i;
        }
      }
      workloads[minIdx].addRecord(record);
    }
    for (int i = 0; i < replicaNum; ++i) {
      float curCost = CostModel.approximateAggregationQueryCostWithTimeRange(workloads[i].getRecords(), replicas[i].getMeasurements(), replicas[i].getAverageChunkSize());
      if (curCost > totalCost) {
        totalCost = curCost;
      }
    }
    return new Pair<>(totalCost, workloads);
  }

  public List<Double> getCostList() {
    return costList;
  }

  /**
   * Optimize the chunk size
   */
  public static Pair<List<Long>, List<Double>> chunkSizeOptimize(GroupByQueryRecord query, List<String> measurementOrder) {
    int visitLength = (int)query.getVisitLength();
    long visitChunkSize = MeasurePointEstimator.getInstance().getChunkSize(visitLength);
    long chunkSizeUpperBound = (long) (70.0f * visitChunkSize);
    long chunkSizeLowerBound = (long) (0.1f * visitChunkSize);
    long chunkSizeStep = (chunkSizeUpperBound - chunkSizeLowerBound) / CHUNK_SIZE_STEP_NUM;
    List<Long> chunkSizeList = new LinkedList<>();
    List<Double> costList = new LinkedList<>();
    for (long i = 0; i < CHUNK_SIZE_STEP_NUM; ++i) {
      long curChunkSize = chunkSizeLowerBound + chunkSizeStep * i;
      float curCost = CostModel.approximateSingleAggregationQueryCostWithTimeRange(query, measurementOrder, curChunkSize);
      chunkSizeList.add(curChunkSize);
      costList.add((double) curCost);
    }
    return new Pair<>(chunkSizeList, costList);
  }

  private Pair<Long, Long> getChunkSizeBound(List<QueryRecord> records) {
    long lowerBound = Long.MAX_VALUE;
    long upperBound = Long.MIN_VALUE;
    for(QueryRecord record : records) {
      if (record instanceof GroupByQueryRecord) {
        long visitLength = ((GroupByQueryRecord) record).getVisitLength();
        long visitChunkSize = MeasurePointEstimator.getInstance().getChunkSize((int) visitLength);
        if (visitChunkSize * CHUNK_SIZE_LOWER_BOUND < lowerBound) {
          lowerBound = (long) (visitChunkSize * CHUNK_SIZE_LOWER_BOUND);
        }
        if (visitChunkSize * CHUNK_SIZE_UPPER_BOUND > upperBound) {
          upperBound = (long) (visitChunkSize * CHUNK_SIZE_UPPER_BOUND);
        }
      }
    }

    return new Pair<>(lowerBound, upperBound);
  }
}
