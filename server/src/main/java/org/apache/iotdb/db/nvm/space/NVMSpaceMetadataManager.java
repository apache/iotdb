package org.apache.iotdb.db.nvm.space;

import static org.apache.iotdb.db.nvm.space.NVMSpaceManager.NVMSPACE_NUM_MAX;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.nvm.metadata.DataTypeMemo;
import org.apache.iotdb.db.nvm.metadata.OffsetMemo;
import org.apache.iotdb.db.nvm.metadata.SpaceCount;
import org.apache.iotdb.db.nvm.metadata.SpaceStatusBitMap;
import org.apache.iotdb.db.nvm.metadata.TimeValueMapper;
import org.apache.iotdb.db.nvm.metadata.TimeseriesTimeIndexMapper;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class NVMSpaceMetadataManager {

  private static final long SPACE_COUNT_FIELD_BYTE_SIZE = Integer.BYTES;
  private static final long BITMAP_FIELD_BYTE_SIZE = Byte.BYTES * NVMSPACE_NUM_MAX;
  private static final long OFFSET_FIELD_BYTE_SIZE = Long.BYTES * NVMSPACE_NUM_MAX;
  private static final long DATATYPE_FIELD_BYTE_SIZE = Short.BYTES * NVMSPACE_NUM_MAX;
  private static final long TVMAP_FIELD_BYTE_SIZE = NVMSpaceManager.getPrimitiveTypeByteSize(TSDataType.INT32) * NVMSPACE_NUM_MAX;
  private static final long TSTIMEMAP_FIELD_BYTE_SIZE = NVMSpaceManager.getPrimitiveTypeByteSize(TSDataType.INT32) * 3 * NVMSPACE_NUM_MAX;

  private final static NVMSpaceMetadataManager INSTANCE = new NVMSpaceMetadataManager();

  private SpaceCount spaceCount;
  private SpaceStatusBitMap spaceStatusBitMap;
  private OffsetMemo offsetMemo;
  private DataTypeMemo dataTypeMemo;
  private TimeValueMapper timeValueMapper;
  private TimeseriesTimeIndexMapper timeseriesTimeIndexMapper;

  private NVMSpaceManager spaceManager;

  private NVMSpaceMetadataManager() {}

  public void init() throws IOException {
    spaceManager = NVMSpaceManager.getInstance();

    spaceCount = new SpaceCount(spaceManager.allocateSpace(SPACE_COUNT_FIELD_BYTE_SIZE));
    spaceStatusBitMap = new SpaceStatusBitMap(spaceManager.allocateSpace(BITMAP_FIELD_BYTE_SIZE));
    offsetMemo = new OffsetMemo(spaceManager.allocateSpace(OFFSET_FIELD_BYTE_SIZE));
    dataTypeMemo = new DataTypeMemo(spaceManager.allocateSpace(DATATYPE_FIELD_BYTE_SIZE));
    timeValueMapper = new TimeValueMapper(spaceManager.allocateSpace(TVMAP_FIELD_BYTE_SIZE));
    timeseriesTimeIndexMapper = new TimeseriesTimeIndexMapper(spaceManager.allocateSpace(
        TSTIMEMAP_FIELD_BYTE_SIZE));
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
    spaceStatusBitMap.setUse(timeSpaceIndex, true);
    spaceStatusBitMap.setUse(valueSpaceIndex, false);
    offsetMemo.set(timeSpaceIndex, timeSpace.getOffset());
    offsetMemo.set(valueSpaceIndex, valueSpace.getOffset());
    dataTypeMemo.set(timeSpaceIndex, timeSpace.getDataType());
    dataTypeMemo.set(valueSpaceIndex, valueSpace.getDataType());

    timeValueMapper.map(timeSpaceIndex, valueSpaceIndex);
    timeseriesTimeIndexMapper
        .mapTimeIndexToTimeSeries(timeSpaceIndex, sgId, deviceId, measurementId);
  }

  public void unregisterSpace(NVMDataSpace space) {
    spaceStatusBitMap.setFree(space.getIndex());
  }

  public List<Integer> getValidTimeSpaceIndexList() {
    return spaceStatusBitMap.getValidTimeSpaceIndexList(getCount() / 2);
  }

  public int getValueSpaceIndexByTimeSpaceIndex(int timeSpaceIndex) {
    return timeValueMapper.get(timeSpaceIndex);
  }

  public long getOffsetBySpaceIndex(int spaceIndex) {
    return offsetMemo.get(spaceIndex);
  }

  public TSDataType getDatatypeBySpaceIndex(int spaceIndex) {
    return dataTypeMemo.get(spaceIndex);
  }

  public String[] getTimeseriesBySpaceIndex(int spaceIndex) {
    return timeseriesTimeIndexMapper.getTimeseries(spaceIndex);
  }
}
