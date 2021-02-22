package org.apache.iotdb.db.engine.measurementorderoptimizer;

import org.apache.iotdb.db.engine.divergentdesign.Replica;
import org.apache.iotdb.db.engine.measurementorderoptimizer.costmodel.CostModel;
import org.apache.iotdb.db.query.workloadmanager.Workload;
import org.apache.iotdb.db.query.workloadmanager.WorkloadManager;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.QueryRecord;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
  private final float COOLING_RATE = 0.95f;

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

  public Replica[] optimizeBySA() {
    float curCost = getCostAndWorkloadPartitionForCurReplicas(records, replicas).left;
    LOGGER.info("Ori cost: " + curCost);
    float temperature = SA_INIT_TEMPERATURE;
    Random r = new Random();
    int k = 0;
    for (; k < maxIter; ++k) {
      temperature = temperature * COOLING_RATE;

      int swapReplica = r.nextInt(replicaNum);
      int swapLeft = r.nextInt(measurementOrder.size());
      int swapRight = r.nextInt(measurementOrder.size());
      while (swapLeft == swapRight) {
        swapLeft = r.nextInt(measurementOrder.size());
        swapRight = r.nextInt(measurementOrder.size());
      }
      replicas[swapReplica].swapMeasurementPos(swapLeft, swapRight);
      Pair<Float, Workload[]> costAndWorkloadPartition = getCostAndWorkloadPartitionForCurReplicas(records, replicas);
      float newCost = costAndWorkloadPartition.left;
      float probability = r.nextFloat();
      probability = probability < 0 ? -probability : probability;
      probability %= 1.0f;
      if (newCost < curCost ||
              Math.exp((curCost - newCost) / temperature) > probability) {
        curCost = newCost;
      } else {
        replicas[swapReplica].swapMeasurementPos(swapLeft, swapRight);
      }
      LOGGER.info(String.format("Epoch %d: curCost %.3f", k, curCost));
    }
    LOGGER.info("Final cost: " + curCost);
    LOGGER.info("Loop count: " + k);
    return replicas;
  }

  private Pair<Float, Workload[]> getCostAndWorkloadPartitionForCurReplicas(List<QueryRecord> records, Replica[] replicas) {
    float totalCost = 0.0f;
    Workload[] workloads = new Workload[replicaNum];
    for(int i = 0; i < replicaNum; ++i) {
      workloads[i] = new Workload();
    }
    for (QueryRecord record : records) {
      List<QueryRecord> tmpList = new ArrayList<>();
      tmpList.add(record);
      float minCost = Float.MAX_VALUE;
      int minIdx = 0;
      for (int i = 0; i < replicaNum; ++i) {
        Replica replica = replicas[i];
        float curCost = CostModel.approximateAggregationQueryCostWithTimeRange(tmpList, replica.getMeasurements(), replica.getChunkSize());
        if (curCost < minCost) {
          minCost = curCost;
          minIdx = i;
        }
      }
      workloads[minIdx].addRecord(record);
    }
    for(int i = 0; i < replicaNum; ++i) {
      float curCost = CostModel.approximateAggregationQueryCostWithTimeRange(workloads[i].getRecords(), replicas[i].getMeasurements(), replicas[i].getChunkSize());
      if (curCost > totalCost) {
        totalCost = curCost;
      }
    }
    return new Pair<>(totalCost, workloads);
  }
}
