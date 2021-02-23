package org.apache.iotdb.db.engine.measurementorderoptimizer;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.divergentdesign.Replica;
import org.apache.iotdb.db.engine.measurementorderoptimizer.costmodel.CostModel;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.workloadmanager.Workload;
import org.apache.iotdb.db.query.workloadmanager.WorkloadManager;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.QueryRecord;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.*;

public class MeasurementOrderOptimizer {
  // DeviceId -> List<Measurement>
  Map<String, List<String>> measurementsMap = new HashMap<>();
  // Set<Measurement Full Path>
  Set<String> measurementSet = new HashSet<>();
  // DeviceId -> Measurement -> ChunkSize
  Map<String, Map<String, Long>> chunkMap = new HashMap<>();
  // DeviceId -> ChunkGroupCount
  Map<String, Integer> chunkGroupCountMap = new HashMap<>();
  List<QueryRecord> queryRecords = new ArrayList<>();
  public static final int SA_MAX_ITERATION = 350000;
  public static final float SA_INIT_TEMPERATURE = 2.0f;
  public static final float SA_COOLING_RATE = 0.02f;
  private static final Logger LOGGER = LoggerFactory.getLogger(MeasurementOrderOptimizer.class);

  private MeasurementOrderOptimizer() {
  }

  private static class MeasurementOrderOptimizerHolder {
    private final static MeasurementOrderOptimizer INSTANCE = new MeasurementOrderOptimizer();
  }

  public static MeasurementOrderOptimizer getInstance() {
    return MeasurementOrderOptimizerHolder.INSTANCE;
  }

  /**
   * Get the metadata from the MManager
   */
  private void updateMetadata() {
    MManager manager = MManager.getInstance();
    List<PartialPath> storagePaths = manager.getAllStorageGroupPaths();
    for (PartialPath storagePath : storagePaths) {
      try {
        List<PartialPath> measurementPaths = manager.getAllTimeseriesPath(storagePath);
        for (PartialPath measurementPath : measurementPaths) {
          if (!measurementSet.contains(measurementPath.getFullPath())) {
            // Add the measurement to optimizer
            measurementSet.add(measurementPath.getFullPath());
            if (!measurementsMap.containsKey(measurementPath.getDevice())) {
              measurementsMap.put(measurementPath.getDevice(), new ArrayList<>());
            }
            measurementsMap.get(measurementPath.getDevice()).add(measurementPath.getMeasurement());
          }
        }
      } catch (MetadataException e) {
        e.printStackTrace();
      }
    }
    sortByLexicographicOrder();
  }

  /**
   * Sort the order of each measurement by lexicographic order
   */
  private void sortByLexicographicOrder() {
    for (String device : measurementsMap.keySet()) {
      List<String> measurements = measurementsMap.get(device);
      Collections.sort(measurements, new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
          if (o1.length() < o2.length()) {
            return -1;
          } else if (o2.length() < o1.length()) {
            return 1;
          }
          for (int i = 0; i < o1.length(); ++i) {
            if (o1.charAt(i) < o2.charAt(i)) {
              return -1;
            } else if (o2.charAt(i) < o1.charAt(i)) {
              return 1;
            }
          }
          return 0;
        }
      });
      measurementsMap.put(device, measurements);
    }
  }

  /**
   * Get the measurement order for a specific device, remember to call optimize before
   * this function to get a optimized order, otherwise the order may not be optimized.
   */
  public List<String> getMeasurementsOrder(String deviceId) {
    if (measurementsMap.containsKey(deviceId)) {
      return measurementsMap.get(deviceId);
    } else {
      // Get the measurements from MManager
      updateMetadata();
      return measurementsMap.getOrDefault(deviceId, null);
    }
  }

  public synchronized void addMeasurements(String deviceId, List<String> measurements) {
    if (!measurementsMap.containsKey(deviceId)) {
      measurementsMap.put(deviceId, new ArrayList<>());
    }
    measurementsMap.get(deviceId).addAll(measurements);
    for (String measurement : measurements) {
      measurementSet.add(deviceId + "." + measurement);
    }
  }

  public synchronized void addQueryRecord(QueryRecord record) {
    synchronized (queryRecords) {
      queryRecords.add(record);
    }
  }

  public synchronized void addQueryRecords(QueryRecord[] records) {
    synchronized (queryRecords) {
      for (int i = 0; i < records.length; ++i) {
        queryRecords.add(records[i]);
      }
    }
  }

  public synchronized void addQueryRecords(List<QueryRecord> records) {
    synchronized (queryRecords) {
      queryRecords.addAll(records);
    }
  }

  public synchronized void getQueryRecordFromManager() {
    WorkloadManager manager = WorkloadManager.getInstance();
    List<QueryRecord> recordList = new ArrayList<>(manager.getRecords());
    queryRecords.addAll(recordList);
  }

  public synchronized boolean readMetadataFromFile() {
    String filepath = IoTDBDescriptor.getInstance().getConfig().getSystemDir() + File.separator
            + "experiment" + File.separator + "metadata.json";
    File recordFile = new File(filepath);
    if (!recordFile.exists()) {
      LOGGER.error("Record file " + recordFile.getAbsolutePath() + " does not exist");
      return false;
    }
    LOGGER.info("Reading from " + recordFile.getAbsolutePath());
    try {
      byte[] buffer = new byte[(int) recordFile.length()];
      InputStream inputStream = new FileInputStream(recordFile);
      inputStream.read(buffer);
      String jsonText = new String(buffer);
      JSONArray metadataArray = JSONArray.parseArray(jsonText);
      for(int i = 0; i < metadataArray.size(); ++i) {
        JSONObject metadata = (JSONObject) metadataArray.get(i);
        String deviceID = metadata.getString("device");
        JSONArray sensors = metadata.getJSONArray("sensors");
        long chunkSize = metadata.getLong("chunkSize");
        int chunkGroupNum = metadata.getInteger("chunkGroupNum");
        List<String> measurements = new ArrayList<>();
        for(int j = 0; j < sensors.size(); ++j) {
          measurements.add(sensors.getString(j));
        }
        measurementsMap.put(deviceID, measurements);
        if (!chunkMap.containsKey(deviceID)) {
          chunkMap.put(deviceID, new HashMap<>());
        }
        for(String measurement : measurements) {
          chunkMap.get(deviceID).put(measurement, chunkSize);
        }
        chunkGroupCountMap.put(deviceID, chunkGroupNum);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return true;
  }

  /**
   * Run the optimization algorithm to get the optimized measurements order for a specified device
   *
   * @param deviceID:      The ID of the device to be optimized
   * @param algorithmType: The algorithm used to optimize the order
   */
  public synchronized void optimizeOrder(String deviceID, MeasurementOptimizationType algorithmType) {
    switch (algorithmType) {
      case SA: {
        optimizeOrderBySA(deviceID);
        break;
      }
      case GA: {
        optimizeOrderByGA(deviceID);
        break;
      }
    }
  }

  /**
   * Run the optimization algorithm to get hte optimized chunk size for a specified device
   *
   * @param deviceID:      The ID of the device to be optimized
   * @param algorithmType: The algorithm used to optimize the order
   */
  public synchronized void optimizeChunkSize(String deviceID, MeasurementOptimizationType algorithmType) {
    switch (algorithmType) {
      case SA: {
        optimizeChunkSizeBySA(deviceID);
        break;
      }
      case GA: {
        optimizeChunkSizeByGA(deviceID);
        break;
      }
    }
  }

  public synchronized void setChunkSize(String deviceId, String measurementId, long chunkSize) {
    if (!chunkMap.containsKey(deviceId)) {
      chunkMap.put(deviceId, new HashMap<>());
    }
    chunkMap.get(deviceId).put(measurementId, chunkSize);
  }

  /**
   * Run the optimization algorithm to get the optimized measurements order for all devices
   *
   * @param algorithmType: The algorithm used to optimized the order
   */
  public synchronized void optimizeOrder(MeasurementOptimizationType algorithmType) {
    switch (algorithmType) {
      case SA: {
        optimizeOrderBySA();
        break;
      }
      case GA: {
        optimizeOrderByGA();
        break;
      }
    }
  }

  /**
   * Run the optimization algorithm to get the optimized chunk size for all devices
   *
   * @param algorithmType: The algorithm used to optimized the order
   */
  public synchronized void optimizeChunkSize(MeasurementOptimizationType algorithmType) {
    switch (algorithmType) {
      case SA: {
        optimizeChunkSizeBySA();
        break;
      }
      case GA: {
        optimizeChunkSizeByGA();
        break;
      }
    }
  }

  private void optimizeOrderBySA() {
    for (String deviceID : measurementsMap.keySet()) {
      optimizeOrderBySA(deviceID);
    }
  }

  private void optimizeChunkSizeBySA() {
    for (String deviceID : measurementsMap.keySet()) {
      optimizeChunkSizeBySA(deviceID);
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
  private void optimizeOrderBySA(String deviceID) {
    if (queryRecords.size() == 0) {
      getQueryRecordFromManager();
    }
    List<QueryRecord> queryRecordsForCurDevice = new ArrayList<>();
    // Collect the query for current device
    for (QueryRecord queryRecord : queryRecords) {
      if (queryRecord.getDevice().equals(deviceID)) {
        queryRecordsForCurDevice.add(queryRecord);
      }
    }
    List<String> curMeasurementOrder = measurementsMap.get(deviceID);

    // Collect the chunksize for current device
    List<Long> chunkSize = new ArrayList<>();
    Map<String, Long> chunkSizeMapForCurDevice = chunkMap.get(deviceID);
    for (String measurement : curMeasurementOrder) {
      chunkSize.add(chunkSizeMapForCurDevice.get(measurement));
    }
    float curCost = CostModel.approximateAggregationQueryCostWithoutTimeRange(queryRecordsForCurDevice,
            curMeasurementOrder, chunkSize, chunkGroupCountMap.get(deviceID));
    float temperature = SA_INIT_TEMPERATURE;
    Random r = new Random();

    // Run the main loop of Simulated Annealing
    for (int k = 0; k < SA_MAX_ITERATION; ++k) {
      temperature = updateTemperature(temperature);

      // Generate a neighbor state
      int swapPosFirst = 0;
      int swapPosSecond = 0;
      while (swapPosSecond == swapPosFirst) {
        swapPosFirst = r.nextInt();
        swapPosFirst = swapPosFirst < 0 ? -swapPosFirst : swapPosFirst;
        swapPosFirst %= curMeasurementOrder.size();
        swapPosSecond = r.nextInt();
        swapPosSecond = swapPosSecond < 0 ? -swapPosSecond : swapPosSecond;
        swapPosSecond %= curMeasurementOrder.size();
      }
      swap(curMeasurementOrder, swapPosFirst, swapPosSecond);
      swap(chunkSize, swapPosFirst, swapPosSecond);

      float newCost = CostModel.approximateAggregationQueryCostWithoutTimeRange(queryRecordsForCurDevice,
              curMeasurementOrder, chunkSize, chunkGroupCountMap.get(deviceID));
      float probability = r.nextFloat();
      probability = probability < 0 ? -probability : probability;
      probability %= 1.0;
      if (newCost < curCost ||
              Math.exp((curCost - newCost) / temperature) > probability) {
        // Accept the new status
        curCost = newCost;
      } else {
        // Recover the origin status
        swap(curMeasurementOrder, swapPosFirst, swapPosSecond);
        swap(chunkSize, swapPosFirst, swapPosSecond);
      }
//      if (k % 500 == 0) {
//        LOGGER.info(String.format("Epoch %d: Cur cost %.3f", k, curCost));
//      }
    }

    measurementsMap.put(deviceID, curMeasurementOrder);
  }

  /**
   * TODO: Get optimal chunk size by SA algorithm
   * @param deviceID: The device to be optimized
   */
  private void optimizeChunkSizeBySA(String deviceID) {
    List<String> measurementOrder = measurementsMap.get(deviceID);
    // measurementID -> <ChunkSize, MeasurePointNumber>
    Map<String, Pair<Long, Integer>> chunkConfigMap = new HashMap<>();
    for (String measurementId : measurementOrder) {
      long chunkSize = chunkMap.get(deviceID).get(measurementId);
      chunkConfigMap.put(measurementId,
              new Pair<>(chunkSize, MeasurePointEstimator.getInstance().getMeasurePointNum(chunkSize)));
    }
    float temperature = SA_INIT_TEMPERATURE;
    Random r = new Random();
  }

  private void swap(List list, int posFirst, int posSecond) {
    Object temp = list.get(posFirst);
    list.set(posFirst, list.get(posSecond));
    list.set(posSecond, temp);
  }

  private float updateTemperature(float f) {
    return f * (1.0f - SA_COOLING_RATE);
  }

  private void optimizeOrderByGA() {
    for (String deviceID : measurementsMap.keySet()) {
      optimizeOrderByGA(deviceID);
    }
  }

  // TODO: implement the GA algorithm
  private void optimizeOrderByGA(String deviceID) {

  }

  private void optimizeChunkSizeByGA() {
    for (String deviceID : measurementsMap.keySet()) {
      optimizeChunkSizeByGA(deviceID);
    }
  }

  // TODO: implement the GA algorithm
  private void optimizeChunkSizeByGA(String deviceID) {

  }

  public List<Long> getChunkSize(String deviceId) {
    List<String> measurementOrder = measurementsMap.get(deviceId);
    Map<String, Long> chunkSizeForCurDevice = chunkMap.get(deviceId);
    List<Long> chunkSize = new ArrayList<>();
    for (int i = 0; i < measurementOrder.size(); ++i) {
      chunkSize.add(chunkSizeForCurDevice.get(measurementOrder.get(i)));
    }
    return chunkSize;
  }

  public Replica getOptimalReplica(Workload workload, String deviceID) {
    List<QueryRecord> queryRecordsForCurDevice = workload.getRecords();
    List<String> curMeasurementOrder = new ArrayList<>(measurementsMap.get(deviceID));

    // Collect the chunksize for current device
    List<Long> chunkSize = new ArrayList<>();
    Map<String, Long> chunkSizeMapForCurDevice = chunkMap.get(deviceID);
    for (String measurement : curMeasurementOrder) {
      chunkSize.add(chunkSizeMapForCurDevice.get(measurement));
    }
    float curCost = CostModel.approximateAggregationQueryCostWithoutTimeRange(queryRecordsForCurDevice,
            curMeasurementOrder, chunkSize, chunkGroupCountMap.get(deviceID));
    float temperature = SA_INIT_TEMPERATURE;
    Random r = new Random();

    // Run the main loop of Simulated Annealing
    for (int k = 0; k < SA_MAX_ITERATION; ++k) {
      temperature = updateTemperature(temperature);

      // Generate a neighbor state
      int swapPosFirst = 0;
      int swapPosSecond = 0;
      while (swapPosSecond == swapPosFirst) {
        swapPosFirst = r.nextInt();
        swapPosFirst = swapPosFirst < 0 ? -swapPosFirst : swapPosFirst;
        swapPosFirst %= curMeasurementOrder.size();
        swapPosSecond = r.nextInt();
        swapPosSecond = swapPosSecond < 0 ? -swapPosSecond : swapPosSecond;
        swapPosSecond %= curMeasurementOrder.size();
      }
      swap(curMeasurementOrder, swapPosFirst, swapPosSecond);
      swap(chunkSize, swapPosFirst, swapPosSecond);

      float newCost = CostModel.approximateAggregationQueryCostWithoutTimeRange(queryRecordsForCurDevice,
              curMeasurementOrder, chunkSize, chunkGroupCountMap.get(deviceID));
      float probability = r.nextFloat();
      probability = probability < 0 ? -probability : probability;
      probability %= 1.0;
      if (newCost < curCost ||
              Math.exp((curCost - newCost) / temperature) > probability) {
        // Accept the new status
        curCost = newCost;
      } else {
        // Recover the origin status
        swap(curMeasurementOrder, swapPosFirst, swapPosSecond);
        swap(chunkSize, swapPosFirst, swapPosSecond);
      }
    }
    BigInteger totalChunkSize = new BigInteger(String.valueOf(0));
    for(int i = 0; i < chunkSize.size(); ++i) {
      totalChunkSize = totalChunkSize.add(new BigInteger(String.valueOf(chunkSize.get(i))));
    }
    long averageChunkSize = totalChunkSize.divide(new BigInteger(String.valueOf(chunkSize.size()))).longValue();
    Replica optimizedReplica = new Replica(deviceID, curMeasurementOrder, averageChunkSize);
    return optimizedReplica;
  }

  public long getAverageChunkSize(String deviceID) {
    List<Long> chunkSize = getChunkSize(deviceID);
    BigInteger totalChunkSize = new BigInteger(String.valueOf(0));
    for(int i = 0; i < chunkSize.size(); ++i) {
      totalChunkSize = totalChunkSize.add(new BigInteger(String.valueOf(chunkSize.get(i))));
    }
    long averageChunkSize = totalChunkSize.divide(new BigInteger(String.valueOf(chunkSize.size()))).longValue();
    return averageChunkSize;
  }

  public static void main(String[] args) {
    List<String> a = new ArrayList<>();
    a.add("Z");
    a.add("e");
    a.add("a");
    a.add("s");
    a.add("123");
    a.add("A");
    Collections.sort(a);
    System.out.println(a.toString());
  }
}
