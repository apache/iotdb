package org.apache.iotdb.db.nvm.memtable;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.utils.datastructure.NVMTVList;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType.TsBinary;
import org.apache.iotdb.db.utils.TsPrimitiveType.TsBoolean;
import org.apache.iotdb.db.utils.TsPrimitiveType.TsDouble;
import org.apache.iotdb.db.utils.TsPrimitiveType.TsFloat;
import org.apache.iotdb.db.utils.TsPrimitiveType.TsInt;
import org.apache.iotdb.db.utils.TsPrimitiveType.TsLong;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NVMWritableMemChunk implements IWritableMemChunk {

  private static final Logger logger = LoggerFactory.getLogger(NVMWritableMemChunk.class);

  private TSDataType dataType;
  private NVMTVList list;
  private List<TimeValuePair> sortedList;

  public NVMWritableMemChunk(TSDataType dataType, NVMTVList list) {
    this.dataType = dataType;
    this.list = list;
  }

  @Override
  public void write(long insertTime, Object objectValue) {
    switch (dataType) {
      case BOOLEAN:
        putBoolean(insertTime, (boolean) objectValue);
        break;
      case INT32:
        putInt(insertTime, (int) objectValue);
        break;
      case INT64:
        putLong(insertTime, (long) objectValue);
        break;
      case FLOAT:
        putFloat(insertTime, (float) objectValue);
        break;
      case DOUBLE:
        putDouble(insertTime, (double) objectValue);
        break;
      case TEXT:
        putBinary(insertTime, (Binary) objectValue);
        break;
      default:
        throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
    }
    sortedList = null;
  }

  @Override
  public void write(long[] times, Object valueList, TSDataType dataType, List<Integer> indexes) {
    switch (dataType) {
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) valueList;
        if (times.length == indexes.size()) {
          putBooleans(times, boolValues);
          break;
        }
        for (Integer index : indexes) {
          putBoolean(times[index], boolValues[index]);
        }
        break;
      case INT32:
        int[] intValues = (int[]) valueList;
        if (times.length == indexes.size()) {
          putInts(times, intValues);
          break;
        }
        for (Integer index : indexes) {
          putInt(times[index], intValues[index]);
        }
        break;
      case INT64:
        long[] longValues = (long[]) valueList;
        if (times.length == indexes.size()) {
          putLongs(times, longValues);
          break;
        }
        for (Integer index : indexes) {
          putLong(times[index], longValues[index]);
        }
        break;
      case FLOAT:
        float[] floatValues = (float[]) valueList;
        if (times.length == indexes.size()) {
          putFloats(times, floatValues);
          break;
        }
        for (Integer index : indexes) {
          putFloat(times[index], floatValues[index]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) valueList;
        if (times.length == indexes.size()) {
          putDoubles(times, doubleValues);
          break;
        }
        for (Integer index : indexes) {
          putDouble(times[index], doubleValues[index]);
        }
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) valueList;
        if (times.length == indexes.size()) {
          putBinaries(times, binaryValues);
          break;
        }
        for (Integer index : indexes) {
          putBinary(times[index], binaryValues[index]);
        }
        break;
      default:
        throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
    }
    sortedList = null;
  }


  @Override
  public void putLong(long t, long v) {
    list.putLong(t, v);
  }

  @Override
  public void putInt(long t, int v) {
    list.putInt(t, v);
  }

  @Override
  public void putFloat(long t, float v) {
    list.putFloat(t, v);
  }

  @Override
  public void putDouble(long t, double v) {
    list.putDouble(t, v);
  }

  @Override
  public void putBinary(long t, Binary v) {
    list.putBinary(t, v);
  }

  @Override
  public void putBoolean(long t, boolean v) {
    list.putBoolean(t, v);
  }

  @Override
  public void putLongs(long[] t, long[] v) {
    list.putLongs(t, v);
  }

  @Override
  public void putInts(long[] t, int[] v) {
    list.putInts(t, v);
  }

  @Override
  public void putFloats(long[] t, float[] v) {
    list.putFloats(t, v);
  }

  @Override
  public void putDoubles(long[] t, double[] v) {
    list.putDoubles(t, v);
  }

  @Override
  public void putBinaries(long[] t, Binary[] v) {
    list.putBinaries(t, v);
  }

  @Override
  public void putBooleans(long[] t, boolean[] v) {
    list.putBooleans(t, v);
  }

  @Override
  public synchronized NVMTVList getSortedTVList() {
    list.sort();
    return list;
  }

  @Override
  public NVMTVList getTVList() {
    return list;
  }

  @Override
  public long count() {
    return list.size();
  }

  @Override
  public TSDataType getType() {
    return dataType;
  }

  @Override
  public void setTimeOffset(long offset) {
    list.setTimeOffset(offset);
  }

  @Override
  public synchronized List<TimeValuePair> getSortedTimeValuePairList() {
    if (sortedList != null) {
      return sortedList;
    }
    sortedList = new ArrayList<>();
    list.sort();
    for (int i = 0; i < list.size(); i++) {
      long time = list.getTime(i);
      if (time < list.getTimeOffset() ||
          (i + 1 < list.size() && (time == list.getTime(i + 1)))) {
        continue;
      }
      switch (dataType) {
        case BOOLEAN:
          sortedList.add(new TimeValuePair(time, new TsBoolean(list.getBoolean(i))));
          break;
        case INT32:
          sortedList.add(new TimeValuePair(time, new TsInt(list.getInt(i))));
          break;
        case INT64:
          sortedList.add(new TimeValuePair(time, new TsLong(list.getLong(i))));
          break;
        case FLOAT:
          sortedList.add(new TimeValuePair(time, new TsFloat(list.getFloat(i))));
          break;
        case DOUBLE:
          sortedList.add(new TimeValuePair(time, new TsDouble(list.getDouble(i))));
          break;
        case TEXT:
          sortedList.add(new TimeValuePair(time, new TsBinary(list.getBinary(i))));
          break;
        default:
          logger.error("Unsupported data type: {}", dataType);
          break;
      }
    }
    return this.sortedList;
  }

  @Override
  public boolean isEmpty() {
    return list.size() == 0;
  }

  @Override
  public long getMinTime() {
    return list.getMinTime();
  }

  @Override
  public void delete(long upperBound) {
    list.delete(upperBound);
  }
}
