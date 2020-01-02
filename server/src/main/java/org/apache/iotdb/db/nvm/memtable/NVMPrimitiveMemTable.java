package org.apache.iotdb.db.nvm.memtable;

import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.engine.memtable.AbstractMemTable;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.engine.memtable.TimeValuePairSorter;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.utils.datastructure.NVMTVList;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class NVMPrimitiveMemTable extends AbstractMemTable {

  public NVMPrimitiveMemTable(String sgId) {
    super(sgId);
  }

  public NVMPrimitiveMemTable(Map<String, Map<String, IWritableMemChunk>> memTableMap, String sgId) {
    super(memTableMap, sgId);
  }

  @Override
  protected IWritableMemChunk genMemSeries(String deviceId, String measurementId, TSDataType dataType) {
    return new NVMWritableMemChunk(dataType,
        (NVMTVList) TVListAllocator.getInstance().allocate(storageGroupId, deviceId, measurementId, dataType, true));
  }

  @Override
  public IMemTable copy() {
    Map<String, Map<String, IWritableMemChunk>> newMap = new HashMap<>(getMemTableMap());

    return new NVMPrimitiveMemTable(newMap, storageGroupId);
  }

  @Override
  public boolean isSignalMemTable() {
    return false;
  }

  @Override
  public int hashCode() {return (int) getVersion();}

  @Override
  public boolean equals(Object obj) {
    return this == obj;
  }

  @Override
  public ReadOnlyMemChunk query(String deviceId, String measurement, TSDataType dataType,
      Map<String, String> props, long timeLowerBound) {
    TimeValuePairSorter sorter;
    if (!checkPath(deviceId, measurement)) {
      return null;
    } else {
      long undeletedTime = findUndeletedTime(deviceId, measurement, timeLowerBound);
      IWritableMemChunk memChunk = memTableMap.get(deviceId).get(measurement);
      IWritableMemChunk chunkCopy = new NVMWritableMemChunk(dataType,
          (NVMTVList) memChunk.getTVList().clone());
      chunkCopy.setTimeOffset(undeletedTime);
      sorter = chunkCopy;
    }
    return new ReadOnlyMemChunk(dataType, sorter, props);
  }
}
