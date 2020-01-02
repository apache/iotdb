package org.apache.iotdb.db.nvm.metadata;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.utils.Pair;

public class TSDataMap extends NVMSpaceMetadata {

  public TSDataMap(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  public void addSpaceToTimeSeries(String sgId, String deviceId, String measurementId,
      int timeSpaceIndex,
      int valueSpaceIndex) {

  }

  public Map<String, Map<String, Map<String, List<Pair<Integer, Integer>>>>> generateTSPathTVPairListMap() {
    return null;
  }
}
