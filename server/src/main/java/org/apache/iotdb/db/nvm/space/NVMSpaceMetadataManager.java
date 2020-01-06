package org.apache.iotdb.db.nvm.space;

import static org.apache.iotdb.db.nvm.space.NVMSpaceManager.NVMSPACE_NUM_MAX;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.iotdb.db.nvm.metadata.DataTypeMemo;
import org.apache.iotdb.db.nvm.metadata.FreeSpaceBitMap;
import org.apache.iotdb.db.nvm.metadata.SpaceCount;
import org.apache.iotdb.db.nvm.metadata.TSDataMap;
import org.apache.iotdb.db.nvm.metadata.TimeValueMapper;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;

public class NVMSpaceMetadataManager {

  private static final long SPACE_COUNT_FIELD_BYTE_SIZE = Integer.BYTES;
  private static final long BITMAP_FIELD_BYTE_SIZE = Byte.BYTES * NVMSPACE_NUM_MAX;
  private static final long DATATYPE_FIELD_BYTE_SIZE = Short.BYTES * NVMSPACE_NUM_MAX;
  private static final long TVMAP_FIELD_BYTE_SIZE = NVMSpaceManager.getPrimitiveTypeByteSize(TSDataType.INT32) * NVMSPACE_NUM_MAX;
  private static final long TIMESERIES_FIELD_BYTE_SIZE = 0;

  private final static NVMSpaceMetadataManager INSTANCE = new NVMSpaceMetadataManager();

  private SpaceCount spaceCount;
  private FreeSpaceBitMap freeSpaceBitMap;
  private DataTypeMemo dataTypeMemo;
  private TimeValueMapper timeValueMapper;
  private TSDataMap tsDataMap;

  private NVMSpaceManager spaceManager = NVMSpaceManager.getInstance();

  private NVMSpaceMetadataManager() {}

  public void init() throws IOException {
    spaceCount = new SpaceCount(spaceManager.allocateSpace(SPACE_COUNT_FIELD_BYTE_SIZE));
    freeSpaceBitMap = new FreeSpaceBitMap(spaceManager.allocateSpace(BITMAP_FIELD_BYTE_SIZE));
    dataTypeMemo = new DataTypeMemo(spaceManager.allocateSpace(DATATYPE_FIELD_BYTE_SIZE));
    timeValueMapper = new TimeValueMapper(spaceManager.allocateSpace(TVMAP_FIELD_BYTE_SIZE));
    tsDataMap = new TSDataMap(spaceManager.allocateSpace(TIMESERIES_FIELD_BYTE_SIZE));
  }

  public static NVMSpaceMetadataManager getInstance() {
    return INSTANCE;
  }

  public void updateCount(int v) {
    spaceCount.put(v);
  }

  public int getCount() {
    return spaceCount.get();
  }

  public void registerTVSpace(NVMDataSpace timeSpace, NVMDataSpace valueSpace, String sgId, String deviceId, String measurementId) {
    int timeSpaceIndex = timeSpace.getIndex();
    int valueSpaceIndex = valueSpace.getIndex();
    freeSpaceBitMap.update(timeSpaceIndex, false);
    freeSpaceBitMap.update(valueSpaceIndex, false);
    dataTypeMemo.set(timeSpaceIndex, timeSpace.getDataType());
    dataTypeMemo.set(valueSpaceIndex, valueSpace.getDataType());

    timeValueMapper.map(timeSpaceIndex, valueSpaceIndex);
    tsDataMap.addSpaceToTimeSeries(timeSpaceIndex, valueSpaceIndex, sgId, deviceId, measurementId);
  }

  public void unregisterSpace(NVMDataSpace space) {
    freeSpaceBitMap.update(space.getIndex(), true);
  }

  public Map<String, Map<String, Map<String, Pair<List<Integer>, List<Integer>>>>> getValidSpaceIndexMap() {
    Set<Integer> validSpaceIndexSet = freeSpaceBitMap.getValidSpaceIndexSet();
    Map<String, Map<String, Map<String, Pair<List<Integer>, List<Integer>>>>> tsTVMap = tsDataMap.generateTSPathTVPairListMap();
    for (Map<String, Map<String, Pair<List<Integer>, List<Integer>>>> dmTVMap : tsTVMap.values()) {
      for (Map<String, Pair<List<Integer>, List<Integer>>> mTVMap : dmTVMap.values()) {
        for (Pair<List<Integer>, List<Integer>> tvIndexListPair : mTVMap.values()) {
          List<Integer> timeIndexList = tvIndexListPair.left;
          List<Integer> valueIndexList = tvIndexListPair.right;

          Set<Integer> toBeRemovedIndexSet = new TreeSet<>((o1, o2) -> o2 - o1);
          for (int i = 0; i < timeIndexList.size(); i++) {
            if (!validSpaceIndexSet.contains(timeIndexList.get(i))) {
              toBeRemovedIndexSet.add(i);
            }
          }
          for (int i = 0; i < timeIndexList.size(); i++) {
            if (!validSpaceIndexSet.contains(timeIndexList.get(i))) {
              toBeRemovedIndexSet.add(i);
            }
          }

          for (Integer index : toBeRemovedIndexSet) {
            timeIndexList.remove(index);
            valueIndexList.remove(index);
          }
        }
      }
    }
    return tsTVMap;
  }

  public List<TSDataType> getDataTypeList(int count) {
    return dataTypeMemo.getDataTypeList(count);
  }
}
