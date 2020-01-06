package org.apache.iotdb.db.nvm.space;

import static org.apache.iotdb.db.nvm.space.NVMSpaceManager.NVMSPACE_NUM_MAX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.iotdb.db.nvm.metadata.DataTypeMemo;
import org.apache.iotdb.db.nvm.metadata.FreeSpaceBitMap;
import org.apache.iotdb.db.nvm.metadata.SpaceCount;
import org.apache.iotdb.db.nvm.metadata.TimeseriesTimeIndexMapper;
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
  private TimeseriesTimeIndexMapper timeseriesTimeIndexMapper;

  private NVMSpaceManager spaceManager = NVMSpaceManager.getInstance();

  private NVMSpaceMetadataManager() {}

  public void init() throws IOException {
    spaceCount = new SpaceCount(spaceManager.allocateSpace(SPACE_COUNT_FIELD_BYTE_SIZE));
    freeSpaceBitMap = new FreeSpaceBitMap(spaceManager.allocateSpace(BITMAP_FIELD_BYTE_SIZE));
    dataTypeMemo = new DataTypeMemo(spaceManager.allocateSpace(DATATYPE_FIELD_BYTE_SIZE));
    timeValueMapper = new TimeValueMapper(spaceManager.allocateSpace(TVMAP_FIELD_BYTE_SIZE));
    timeseriesTimeIndexMapper = new TimeseriesTimeIndexMapper(spaceManager.allocateSpace(TIMESERIES_FIELD_BYTE_SIZE));
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
    timeseriesTimeIndexMapper
        .mapTimeIndexToTimeSeries(timeSpaceIndex, sgId, deviceId, measurementId);
  }

  public void unregisterSpace(NVMDataSpace space) {
    freeSpaceBitMap.update(space.getIndex(), true);
  }

  public Map<String, Map<String, Map<String, Pair<List<Integer>, List<Integer>>>>> getTimeseriesTVIndexMap() {
    Map<String, Map<String, Map<String, Pair<List<Integer>, List<Integer>>>>> tsTVIndexMap = new HashMap<>();
    List<Integer> validSpaceIndexList = freeSpaceBitMap.getValidSpaceIndexList();
    for (Integer timeSpaceIndex : validSpaceIndexList) {
      int valueSpaceIndex = timeValueMapper.get(timeSpaceIndex);
      String[] timeseries = timeseriesTimeIndexMapper.getTimeseries(timeSpaceIndex);

      Map<String, Map<String, Pair<List<Integer>, List<Integer>>>> deviceTVMap = tsTVIndexMap.computeIfAbsent(timeseries[0], k -> new HashMap<>());
      Map<String, Pair<List<Integer>, List<Integer>>> measurementTVMap = deviceTVMap.computeIfAbsent(timeseries[1], k -> new HashMap<>());
      Pair<List<Integer>, List<Integer>> tvPairList = measurementTVMap.computeIfAbsent(timeseries[2], k -> new Pair<>(new ArrayList<>(), new ArrayList<>()));
      tvPairList.left.add(timeSpaceIndex);
      tvPairList.right.add(valueSpaceIndex);
    }
    return tsTVIndexMap;
  }

  public List<TSDataType> getDataTypeList(int count) {
    return dataTypeMemo.getDataTypeList(count);
  }
}
