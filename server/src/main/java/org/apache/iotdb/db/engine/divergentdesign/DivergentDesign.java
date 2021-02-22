package org.apache.iotdb.db.engine.divergentdesign;

import org.apache.iotdb.db.engine.measurementorderoptimizer.MeasurementOrderOptimizer;
import org.apache.iotdb.db.engine.measurementorderoptimizer.costmodel.CostModel;
import org.apache.iotdb.db.query.workloadmanager.Workload;
import org.apache.iotdb.db.query.workloadmanager.WorkloadManager;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.QueryRecord;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Query;
import java.util.*;

/**
 * This class implement divergent design for multiple replicas.
 */
public class DivergentDesign {
  private List<Workload> workloads;
  private Replica[] replicas;
  private int replicaNum = 3;
  private int balanceFactor = 1;
  private int maxIter = 100;
  private float breakPoint = 1e-2f;
  private List<QueryRecord> queryRecords;
  private String deviceID;
  private static final Logger LOGGER = LoggerFactory.getLogger(DivergentDesign.class);

  public DivergentDesign(String deviceID) {
    workloads = new ArrayList<>();
    replicas = new Replica[replicaNum];
    queryRecords = new ArrayList<>();
    this.deviceID = deviceID;
  }

  public DivergentDesign(int replicaNum, String deviceID) {
    this.replicaNum = replicaNum;
    workloads = new ArrayList<>();
    replicas = new Replica[replicaNum];
    queryRecords = new ArrayList<>();
    this.deviceID = deviceID;
  }

  public void addWorkload(Workload workload) {
    workloads.add(workload);
  }

  public void addQueryRecord(QueryRecord record) {
    queryRecords.add(record);
  }

  public void getQueryOrderFromManager() {
    queryRecords.addAll(WorkloadManager.getInstance().getRecord(deviceID));
  }

  public void setMaxIter(int maxIter) {
    this.maxIter = maxIter;
  }

  public Pair<Replica[], Workload[]> optimize() {
    if (queryRecords.size() == 0) {
      getQueryOrderFromManager();
    }
    if (queryRecords.size() == 0) {
      return null;
    }

    Workload[] curWorkloadPartition = null;
    Workload[] nextWorkloadPartition = getRandomWorkloadPartition();
    Replica[] curReplica = null;
    Replica[] nextReplica = new Replica[replicaNum];
    for(int k = 0; k < replicaNum; ++k) {
      nextReplica[k] = databaseAdvisor(nextWorkloadPartition[k]);
    }
    float curCost = 0.0f;
    float nextCost = totalCost(nextWorkloadPartition, nextReplica);
    int i = 0;
    do {
      curCost = nextCost;
      curWorkloadPartition = nextWorkloadPartition;
      curReplica = nextReplica;
      nextWorkloadPartition = new Workload[replicaNum];
      for (int j = 0; j < replicaNum; ++j) {
        nextWorkloadPartition[j] = new Workload();
      }
      for (QueryRecord record : queryRecords) {
        int[] indexes = getCostPermutation(record, curReplica);
        for (int j = 0; j < balanceFactor; ++j) {
          nextWorkloadPartition[indexes[j]].addRecord(record);
        }
      }
      nextReplica = new Replica[replicaNum];
      for(int j = 0; j < replicaNum; ++j) {
        nextReplica[j] = databaseAdvisor(nextWorkloadPartition[j]);
      }
      ++i;
      nextCost = totalCost(nextWorkloadPartition, nextReplica);
      LOGGER.info(String.format("Epoch%d Cur cost: %.3f, New cost: %.3f", i, curCost, nextCost));
    } while (i < maxIter && Math.abs(curCost - nextCost) > breakPoint);
    curWorkloadPartition = nextWorkloadPartition;

    return new Pair<>(nextReplica, curWorkloadPartition);
  }

  /**
   * Return a random partition of workloads
   *
   * @return The random partition of the workloads
   */
  private Workload[] getRandomWorkloadPartition() {
    Workload[] partition = new Workload[replicaNum];
    for (int i = 0; i < replicaNum; ++i) {
      partition[i] = new Workload();
    }
    Random r = new Random();
    for (QueryRecord record : queryRecords) {
      Set<Integer> indexes = new HashSet<>();
      while (indexes.size() < balanceFactor) {
        indexes.add(r.nextInt(replicaNum));
      }
      for (Integer idx : indexes) {
        partition[idx].addRecord(record);
      }
    }
    return partition;
  }

  /**
   * Return the optimal replica structure according to the workload
   *
   * @param workload: The query workload
   * @return The optimal structure of replica
   */
  private Replica databaseAdvisor(Workload workload) {
    return MeasurementOrderOptimizer.getInstance().getOptimalReplica(workload, deviceID);
  }

  /**
   * Get the permutation of the cost when the record is executed on the replicas
   *
   * @param record:   The query record
   * @param replicas: The replicas
   * @return The index of the replicas
   */
  private int[] getCostPermutation(QueryRecord record, Replica[] replicas) {
    List<Pair<Float, Integer>> costList = new ArrayList<>();
    for (int i = 0; i < replicas.length; ++i) {
      costList.add(new Pair<>(getCostForSingleQuery(record, replicas[i]), i));
    }
    Collections.sort(costList, (o1, o2) -> {
      if (o1.left > o2.left) {
        return 1;
      } else if (o1.left < o2.left) {
        return -1;
      } else {
        return 0;
      }
    });
    int[] permutationIdx = new int[replicas.length];
    for (int i = 0; i < permutationIdx.length; ++i) {
      permutationIdx[i] = costList.get(i).right;
    }
    return permutationIdx;
  }

  /**
   * Return the total cost of workloads on the replicas
   *
   * @param workloads: The query workload
   * @return The cost of executing the workload on the replicas.
   */
  private float totalCost(Workload[] workloads) {
    float cost = 0.0f;
    Replica[] replicasForCurWorkload = new Replica[replicaNum];
    for (int i = 0; i < workloads.length; ++i) {
      replicasForCurWorkload[i] = databaseAdvisor(workloads[i]);

    }
    return cost;
  }

  private float totalCost(Workload[] workloads, Replica[] replicas) {
    float cost = 0.0f;
    for (int i = 0; i < workloads.length; ++i) {
      float curCost = CostModel.approximateAggregationQueryCostWithTimeRange(
              workloads[i].getRecords(), replicas[i].getMeasurements(), replicas[i].getChunkSize());
      if (curCost > cost) {
        cost = curCost;
      }
    }
    return cost;
  }

  /**
   * Return the cost of executing a single workload
   *
   * @param workload: The workload to be executed
   * @return The cost of executing the workload
   */
  private float getCostForSingleWorkload(Workload workload) {
    Replica replica = databaseAdvisor(workload);
    float cost = 0.0f;
    List<QueryRecord> records = workload.getRecords();
    for (QueryRecord record : records) {
      cost += getCostForSingleQuery(record, replica);
    }
    return cost;
  }

  /**
   * Calculate the cost of executing a query on a replica
   *
   * @param record:  The query record
   * @param replica: The replica
   * @return The cost of executing a query on a replica
   */
  private float getCostForSingleQuery(QueryRecord record, Replica replica) {
    return replica.calCostForQuery(record);
  }

}
