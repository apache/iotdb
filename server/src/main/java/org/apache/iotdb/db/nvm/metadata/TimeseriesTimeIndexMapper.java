package org.apache.iotdb.db.nvm.metadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.nvm.space.NVMSpace;
import org.apache.iotdb.db.nvm.space.NVMSpaceManager;
import org.apache.iotdb.db.nvm.space.NVMStringSpace;
import org.apache.iotdb.tsfile.utils.Pair;

public class TimeseriesTimeIndexMapper extends NVMSpaceMetadata {

  // TODO
  private final long STRING_SPACE_SIZE_MAX = 1000;

  private NVMStringSpace sgSapce;
  private NVMStringSpace deviceSapce;
  private NVMStringSpace measurementSapce;

  public TimeseriesTimeIndexMapper(NVMSpace space) throws IOException {
    super(space);

    initTimeseriesSpaces();
  }

  private void initTimeseriesSpaces() throws IOException {
    NVMSpaceManager spaceManager = NVMSpaceManager.getInstance();
    sgSapce = spaceManager.allocateStringSpace(STRING_SPACE_SIZE_MAX);
    deviceSapce = spaceManager.allocateStringSpace(STRING_SPACE_SIZE_MAX);
    measurementSapce = spaceManager.allocateStringSpace(STRING_SPACE_SIZE_MAX);
  }

  public void mapTimeIndexToTimeSeries(int timeSpaceIndex, String sgId,
      String deviceId, String measurementId) {
    int sgIndex = sgSapce.put(sgId);
    int deviceIndex = deviceSapce.put(sgId);
    int measurementIndex = measurementSapce.put(sgId);

    mapTimeIndexToTimeSeries(timeSpaceIndex, sgIndex, deviceIndex, measurementIndex);
  }

  private void mapTimeIndexToTimeSeries(int timeSpaceIndex, int sgIndex, int deviceIndex, int measurementIndex) {
    int index = timeSpaceIndex * 3;
    ByteBuffer byteBuffer = space.getByteBuffer();
    byteBuffer.putInt(index, sgIndex);
    byteBuffer.putInt(index + 1, deviceIndex);
    byteBuffer.putInt(index + 2, measurementIndex);
  }

  public String[] getTimeseries(int timeSpaceIndex) {
    ByteBuffer byteBuffer = space.getByteBuffer();
    int index = timeSpaceIndex * 3;
    String[] timeseries = new String[3];
    timeseries[0] = sgSapce.get(byteBuffer.getInt(index));
    timeseries[1] = deviceSapce.get(byteBuffer.getInt(index + 1));
    timeseries[2] = measurementSapce.get(byteBuffer.getInt(index + 2));
    return timeseries;
  }
}
