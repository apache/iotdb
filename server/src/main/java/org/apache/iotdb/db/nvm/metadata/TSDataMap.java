package org.apache.iotdb.db.nvm.metadata;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.nvm.space.NVMSpace;
import org.apache.iotdb.tsfile.utils.Pair;

public class TSDataMap extends NVMSpaceMetadata {

  public TSDataMap(NVMSpace space) {
    super(space);
  }

  public void addSpaceToTimeSeries(int timeSpaceIndex, int valueSpaceIndex, String sgId,
      String deviceId, String measurementId) {
    // TODO
  }

  public Map<String, Map<String, Map<String, Pair<List<Integer>, List<Integer>>>>> generateTSPathTVPairListMap() {
    return null;
  }
}
