package org.apache.iotdb.db.nvm.recover;

import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.nvm.metadata.DataTypeMemo;
import org.apache.iotdb.db.nvm.metadata.FreeSpaceBitMap;
import org.apache.iotdb.db.nvm.metadata.TSDataMap;
import org.apache.iotdb.tsfile.utils.Pair;

public class NVMMemtableRecoverPerformer {

  private FileChannel nvmFileChannel;
  private final MapMode MAP_MODE = MapMode.READ_WRITE;

  /**
   * metadata fields
   */
  private FreeSpaceBitMap freeSpaceBitMap;
  private DataTypeMemo dataTypeMemo;
  private TSDataMap tsDataMap;

  public Map<String, Map<String, Map<String, List<Pair<Integer, Integer>>>>> getValidTSPathTVPairListMap() {
    Set<Integer> validSpaceIndexSet = freeSpaceBitMap.getValidSpaceIndexSet();
    Map<String, Map<String, Map<String, List<Pair<Integer, Integer>>>>> tsTVMap = tsDataMap.generateTSPathTVPairListMap();
    for (Map<String, Map<String, List<Pair<Integer, Integer>>>> dmTVMap : tsTVMap.values()) {
      for (Map<String, List<Pair<Integer, Integer>>> mTVMap : dmTVMap.values()) {
        for (List<Pair<Integer, Integer>> tvList : mTVMap.values()) {
          Iterator<Pair<Integer, Integer>> iterator = tvList.iterator();
          while (iterator.hasNext()) {
            Pair<Integer, Integer> tvPair = iterator.next();
            if (!validSpaceIndexSet.contains(tvPair.left) || !validSpaceIndexSet.contains(tvPair.right)) {
              iterator.remove();
            }
          }
        }
      }
    }
    return tsTVMap;
  }

  public void reconstructMemtable(String sgId, PrimitiveMemTable memTable) {

  }
}
