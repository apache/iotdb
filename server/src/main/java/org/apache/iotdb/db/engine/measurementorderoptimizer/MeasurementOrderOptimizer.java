package org.apache.iotdb.db.engine.measurementorderoptimizer;

import org.apache.iotdb.db.engine.measurementorderoptimizer.costmodel.CostModel;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.QueryRecord;

import java.util.*;

public class MeasurementOrderOptimizer {
  private final static MeasurementOrderOptimizer orderOptimizer = new MeasurementOrderOptimizer();
  Map<String, List<String>> measurementsMap = new HashMap<>();
  Map<String, Map<String, Long>> chunkMap = new HashMap<>();
  List<QueryRecord> queryRecords = Collections.synchronizedList(new ArrayList<>());
  public static final int SA_MAX_ITERATION = 200;
  public static final float SA_INIT_TEMPERATURE = 100.0f;
  public static final float SA_COOLING_RATE = 0.02f;

  public static MeasurementOrderOptimizer getInstance() {
    return orderOptimizer;
  }

  /**
   * Get the measurement order for a specific device, remember to call optimize before
   * this function to get a optimized order, otherwise the order may not be optimized.
   */
  public List<String> getMeasurementsOrder(String deviceId) {
    return measurementsMap.getOrDefault(deviceId, null);
  }

  public void addMeasurements(String deviceId, List<String> measurements) {
    measurementsMap.put(deviceId, measurements);
  }

  public void addQueryRecord(QueryRecord record) {
    synchronized (queryRecords) {
      queryRecords.add(record);
    }
  }

  public void addQueryRecords(QueryRecord[] records) {
    synchronized (queryRecords) {
      for (int i = 0; i < records.length; ++i) {
        queryRecords.add(records[i]);
      }
    }
  }

  public void addQueryRecords(List<QueryRecord> records) {
    synchronized (queryRecords) {
      queryRecords.addAll(records);
    }
  }

  /**
   * Run the optimization algorithm to get the optimized measurements order for a specified device
   *
   * @param algorithmType: The algorithm used to optimize the order
   * @param deviceID: the ID of the device to be optimized
   */
  public void optimize(String deviceID, MeasurementOptimizationType algorithmType) {
    switch (algorithmType) {
      case SA: {
        optimizeBySA(deviceID);
        break;
      }
      case GA: {
        optimizeByGA(deviceID);
        break;
      }
    }
  }

  /**
   * Run the optimization algorithm to get the optimized measurements order for all devices
   * @param algorithmType: The algorithm used to optimized the order
   */
  public void optimize(MeasurementOptimizationType algorithmType) {
    switch (algorithmType) {
      case SA: {
        optimizeBySA();
        break;
      }
      case GA: {
        optimizeByGA();
        break;
      }
    }
  }

  private void optimizeBySA() {
    for (String deviceID: measurementsMap.keySet()) {
      optimizeBySA(deviceID);
    }
  }

  /**
   * This function implements Simulated Annealing algorithm to get an optimized measurements order
   * 1.  S := S0, e := Cost(Q, S0), t:= t0
   * 2.  for k := 1 to k_max do:
   * 3.     t := Temperature(t, cooling_rate);
   * 4.     S' := Neighbor(S);
   * 5.     e' := Cost(Q, S');
   * 6.     if (e' < e) || (exp((e-e')/t) > random(0, 1)) then
   * 7.         S := S';
   * 8.         e := e';
   * 9.     endif
   * 10. endfor
   * 11. return S;
   */
  private void optimizeBySA(String deviceID) {
    List<QueryRecord> queryRecordsForCurDevice = new ArrayList<>();
    // Collect the query for current device
    for(QueryRecord queryRecord: queryRecords) {
      if (queryRecord.getDevice().equals(deviceID)) {
        queryRecordsForCurDevice.add(queryRecord);
      }
    }
    List<String> curMeasurementOrder = measurementsMap.get(deviceID);

    // Collect the chunksize for current device
    List<Long> chunkSize = new ArrayList<>();
    Map<String, Long> chunkSizeMapForCurDevice = chunkMap.get(deviceID);
    for(String measurement: curMeasurementOrder) {
      chunkSize.add(chunkSizeMapForCurDevice.get(measurement));
    }
    float curCost = CostModel.approximateAggregationQueryCostWithoutTimeRange(queryRecordsForCurDevice,
            curMeasurementOrder, chunkSize);
    float temperature = SA_INIT_TEMPERATURE;
    Random r = new Random();

    // Run the main loop of Simulated Annealing
    for(int k = 0; k < SA_MAX_ITERATION; ++k) {
      temperature = updateTemperature(temperature);

      // Generate a neighbor state
      int swapPosFirst = r.nextInt() % curMeasurementOrder.size();
      int swapPosSecond = r.nextInt() % curMeasurementOrder.size();
      swap(curMeasurementOrder, swapPosFirst, swapPosSecond);
      swap(chunkSize, swapPosFirst, swapPosSecond);

      float newCost = CostModel.approximateAggregationQueryCostWithoutTimeRange(queryRecordsForCurDevice,
              curMeasurementOrder, chunkSize);
      if (newCost < curCost ||
              Math.exp((curCost - newCost) / temperature) > (r.nextFloat() % 1.0)) {
        // Accept the new status
        curCost = newCost;
      } else {
        // Recover the origin status
        swap(curMeasurementOrder, swapPosFirst, swapPosSecond);
        swap(chunkSize, swapPosFirst, swapPosSecond);
      }
    }

    measurementsMap.put(deviceID, curMeasurementOrder);
  }

  private void swap(List list, int posFirst, int posSecond) {
    Object temp = list.get(posFirst);
    list.set(posFirst, list.get(posSecond));
    list.set(posSecond, temp);
  }

  private float updateTemperature(float f) {
    return f * (1.0f - SA_COOLING_RATE);
  }

  private void optimizeByGA() {
    for (String deviceID: measurementsMap.keySet()) {
      optimizeByGA(deviceID);
    }
  }

  // TODO: implement the GA algorithm
  private void optimizeByGA(String deviceID) {

  }
}
