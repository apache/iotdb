package org.apache.iotdb.db.nvm.recover;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.engine.memtable.AbstractMemTable;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.nvm.memtable.NVMPrimitiveMemTable;
import org.apache.iotdb.db.nvm.space.NVMDataSpace;
import org.apache.iotdb.db.nvm.space.NVMSpaceManager;
import org.apache.iotdb.db.nvm.space.NVMSpaceMetadataManager;
import org.apache.iotdb.tsfile.utils.Pair;

public class NVMMemtableRecoverPerformer {

  private final static NVMMemtableRecoverPerformer INSTANCE = new NVMMemtableRecoverPerformer();

  private Map<String, Map<String, Map<String, Pair<List<NVMDataSpace>, List<NVMDataSpace>>>>> dataMap;

  private NVMMemtableRecoverPerformer() {}

  public void init() throws StartupException {
    try {
      dataMap = recoverDataInNVM();
    } catch (IOException e) {
      throw new StartupException(e);
    }
  }

  public static NVMMemtableRecoverPerformer getInstance() {
    return INSTANCE;
  }

  private Map<String, Map<String, Map<String, Pair<List<NVMDataSpace>, List<NVMDataSpace>>>>> recoverDataInNVM()
      throws IOException {
    Map<String, Map<String, Map<String, Pair<List<NVMDataSpace>, List<NVMDataSpace>>>>> dataMap = new HashMap<>();
    Map<String, Map<String, Map<String, Pair<List<Integer>, List<Integer>>>>> indexMap = NVMSpaceMetadataManager.getInstance().getTimeseriesTVIndexMap();
    List<NVMDataSpace> dataList = NVMSpaceManager.getInstance().getAllNVMData();

    for (Entry<String, Map<String, Map<String, Pair<List<Integer>, List<Integer>>>>> sgIndexEntry : indexMap
        .entrySet()) {
      Map<String, Map<String, Pair<List<NVMDataSpace>, List<NVMDataSpace>>>> deviceDataMap = new HashMap<>(sgIndexEntry.getValue().size());
      dataMap.put(sgIndexEntry.getKey(), deviceDataMap);

      for (Entry<String, Map<String, Pair<List<Integer>, List<Integer>>>> deviceIndexEntry : sgIndexEntry
          .getValue().entrySet()) {
        Map<String, Pair<List<NVMDataSpace>, List<NVMDataSpace>>> measurementDataMap = new HashMap<>(deviceIndexEntry.getValue().size());
        deviceDataMap.put(deviceIndexEntry.getKey(), measurementDataMap);

        for (Entry<String, Pair<List<Integer>, List<Integer>>> measurementIndexEntry : deviceIndexEntry
            .getValue().entrySet()) {
          List<NVMDataSpace> timeList = convertIndexListToDataList(measurementIndexEntry.getValue().left, dataList);
          List<NVMDataSpace> valueList = convertIndexListToDataList(measurementIndexEntry.getValue().right, dataList);
          Pair<List<NVMDataSpace>, List<NVMDataSpace>> tvDataListPair = new Pair<>(timeList, valueList);
          measurementDataMap.put(measurementIndexEntry.getKey(), tvDataListPair);
        }
      }
    }
    return dataMap;
  }

  private List<NVMDataSpace> convertIndexListToDataList(List<Integer> indexList, List<NVMDataSpace> totalDataList) {
    List<NVMDataSpace> dataList = new ArrayList<>(indexList.size());
    for (Integer index : indexList) {
      dataList.add(totalDataList.get(index));
    }
    return dataList;
  }

  public void reconstructMemtable(NVMPrimitiveMemTable memTable, TsFileResource tsFileResource) {
    String sgId = memTable.getStorageGroupId();
    Map<String, Map<String, Pair<List<NVMDataSpace>, List<NVMDataSpace>>>> dataOfSG = dataMap.get(sgId);
    memTable.loadData(dataOfSG);

    Map<String, Long>[] maps = getMinMaxTimeMapFromData(dataOfSG);
    Map<String, Long> minTimeMap = maps[0];
    Map<String, Long> maxTimeMap = maps[1];
    minTimeMap.forEach((k, v) -> tsFileResource.updateStartTime(k, v));
    maxTimeMap.forEach((k, v) -> tsFileResource.updateEndTime(k, v));
  }

  private Map<String, Long>[] getMinMaxTimeMapFromData(Map<String, Map<String, Pair<List<NVMDataSpace>, List<NVMDataSpace>>>> sgDataMap) {
    Map<String, Long> minTimeMap = new HashMap<>();
    Map<String, Long> maxTimeMap = new HashMap<>();
    for (Entry<String, Map<String, Pair<List<NVMDataSpace>, List<NVMDataSpace>>>> deviceDataMapEntry : sgDataMap
        .entrySet()) {
      String deviceId = deviceDataMapEntry.getKey();
      Map<String, Pair<List<NVMDataSpace>, List<NVMDataSpace>>> measurementDataMap = deviceDataMapEntry.getValue();
      long minTime = Long.MAX_VALUE;
      long maxTime = Long.MIN_VALUE;
      for (Pair<List<NVMDataSpace>, List<NVMDataSpace>> tvListPair : measurementDataMap.values()) {
        List<NVMDataSpace> timeSpaceList = tvListPair.left;
        for (int i = 0; i < timeSpaceList.size(); i++) {
          NVMDataSpace timeSpace = timeSpaceList.get(i);
          int unitNum = timeSpace.getUnitNum();
          if (i == timeSpaceList.size() - 1) {
            unitNum = timeSpace.getValidUnitNum();
          }

          for (int j = 0; j < unitNum; j++) {
            long time = (long) timeSpace.get(j);
            minTime = Math.min(minTime, time);
            maxTime = Math.max(maxTime, time);
          }
        }
      }
      if (minTime != Long.MAX_VALUE) {
        minTimeMap.put(deviceId, minTime);
      }
      if (maxTime != Long.MIN_VALUE) {
        maxTimeMap.put(deviceId, maxTime);
      }
    }
    Map<String, Long>[] res = new Map[2];
    res[0] = minTimeMap;
    res[1] = maxTimeMap;
    return res;
  }
}
