package org.apache.iotdb.db.nvm.rescon;

import java.util.ArrayDeque;
import java.util.EnumMap;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.nvm.exception.NVMSpaceManagerException;
import org.apache.iotdb.db.nvm.space.NVMDataSpace;
import org.apache.iotdb.db.nvm.space.NVMSpaceManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class NVMPrimitiveArrayPool {

  /**
   * data type -> Array<PrimitiveArray>
   */
  private static final EnumMap<TSDataType, ArrayDeque<NVMDataSpace>> primitiveArraysMap = new EnumMap<>(TSDataType.class);

  public static final int ARRAY_SIZE = 128;

  static {
    primitiveArraysMap.put(TSDataType.BOOLEAN, new ArrayDeque());
    primitiveArraysMap.put(TSDataType.INT32, new ArrayDeque());
    primitiveArraysMap.put(TSDataType.INT64, new ArrayDeque());
    primitiveArraysMap.put(TSDataType.FLOAT, new ArrayDeque());
    primitiveArraysMap.put(TSDataType.DOUBLE, new ArrayDeque());
    primitiveArraysMap.put(TSDataType.TEXT, new ArrayDeque());
  }

  public static NVMPrimitiveArrayPool getInstance() {
    return INSTANCE;
  }

  private static final NVMPrimitiveArrayPool INSTANCE = new NVMPrimitiveArrayPool();

  private NVMPrimitiveArrayPool() {}

  public synchronized NVMDataSpace getPrimitiveDataListByType(TSDataType dataType, boolean isTime) {
    ArrayDeque<NVMDataSpace> dataListQueue = primitiveArraysMap.computeIfAbsent(dataType, k ->new ArrayDeque<>());
    NVMDataSpace nvmSpace = dataListQueue.poll();

    if (nvmSpace == null) {
      try {
        long size = NVMSpaceManager.getPrimitiveTypeByteSize(dataType);
        nvmSpace = NVMSpaceManager.getInstance().allocateDataSpace(size * ARRAY_SIZE, dataType, isTime);
      } catch (NVMSpaceManagerException e) {
        e.printStackTrace();
        System.exit(0);
        // TODO
      }
    }
    return nvmSpace;
  }

  public synchronized void release(NVMDataSpace nvmSpace, TSDataType dataType) {
    primitiveArraysMap.get(dataType).add(nvmSpace);
  }

//  /**
//   * @param size needed capacity
//   * @return an array of primitive data arrays
//   */
//  public synchronized NVMDataSpace[] getDataListsByType(TSDataType dataType, int size) {
//    int arrayNumber = (int) Math.ceil((float) size / (float) ARRAY_SIZE);
//    NVMDataSpace[] nvmSpaces = new NVMDataSpace[arrayNumber];
//    for (int i = 0; i < arrayNumber; i++) {
//      nvmSpaces[i] = getPrimitiveDataListByType(dataType);
//    }
//    return nvmSpaces;
//  }
}
