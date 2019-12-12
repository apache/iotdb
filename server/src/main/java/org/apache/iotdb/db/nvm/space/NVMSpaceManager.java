package org.apache.iotdb.db.nvm.space;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NVMSpaceManager {

  private static final Logger logger = LoggerFactory.getLogger(NVMSpaceManager.class);

  private static String NVM_PATH;
  private static FileChannel nvmFileChannel;
  private static final MapMode MAP_MODE = MapMode.READ_WRITE;
  private static long nvmSize;
  private static AtomicLong curOffset = new AtomicLong(0L);

  public static void init(String path) {
    try {
      NVM_PATH = path;
      nvmFileChannel = new RandomAccessFile(NVM_PATH, "rw").getChannel();
      nvmSize = nvmFileChannel.size();
    } catch (IOException e) {
      logger.error("Fail to open NVM space at {}.", NVM_PATH, e);
      // TODO deal with error
    }
  }

  public static int getPrimitiveTypeByteSize(TSDataType dataType) {
    int size = 0;
    switch (dataType) {
      case BOOLEAN:
        size = 8;
        break;
      case INT32:
        size = Integer.SIZE;
        break;
      case INT64:
        size = Long.SIZE;
        break;
      case FLOAT:
        size = Float.SIZE;
        break;
      case DOUBLE:
        size = Double.SIZE;
        break;
      case TEXT:
        // TODO
        break;
      default:
        throw new UnSupportedDataTypeException("DataType: " + dataType);
    }
    return size >> 3;
  }

  public static NVMSpace allocate(long size, TSDataType dataType) {
    long offset = curOffset.getAndAdd(size);
    if (offset + size > nvmSize) {
      // TODO throw exception
    }

    try {
      return new NVMSpace(offset, size, nvmFileChannel.map(MAP_MODE, offset, size), dataType);
    } catch (IOException e) {
      // TODO deal with error
      e.printStackTrace();
      return null;
    }
  }

  public static class NVMSpace {

    private long offset;
    private long size;
    private ByteBuffer byteBuffer;
    private TSDataType dataType;

    private NVMSpace(long offset, long size, ByteBuffer byteBuffer, TSDataType dataType) {
      this.offset = offset;
      this.size = size;
      this.byteBuffer = byteBuffer;
      this.dataType = dataType;
    }

    public long getOffset() {
      return offset;
    }

    public long getSize() {
      return size;
    }

    public ByteBuffer getByteBuffer() {
      return byteBuffer;
    }

    @Override
    public NVMSpace clone() {
      NVMSpace cloneSpace = NVMSpaceManager.allocate(size, dataType);
      int position = byteBuffer.position();
      byteBuffer.rewind();
      cloneSpace.getByteBuffer().put(byteBuffer);
      byteBuffer.position(position);
      cloneSpace.getByteBuffer().flip();
      return cloneSpace;
    }

    public Object get(int index) {
      int objectSize = NVMSpaceManager.getPrimitiveTypeByteSize(dataType);
      index *= objectSize;
      Object object = null;
      switch (dataType) {
        case BOOLEAN:
          object = byteBuffer.get(index);
          break;
        case INT32:
          object = byteBuffer.getInt(index);
          break;
        case INT64:
          object = byteBuffer.getLong(index);
          break;
        case FLOAT:
          object = byteBuffer.getFloat(index);
          break;
        case DOUBLE:
          object = byteBuffer.getDouble(index);
          break;
        case TEXT:
          // TODO
          break;
      }
      return object;
    }

    public void set(int index, Object object) {
      int objectSize = NVMSpaceManager.getPrimitiveTypeByteSize(dataType);
      index *= objectSize;
      switch (dataType) {
        case BOOLEAN:
          object = byteBuffer.put(index, (byte) object);
          break;
        case INT32:
          object = byteBuffer.putInt(index, (int) object);
          break;
        case INT64:
          object = byteBuffer.putLong(index, (long) object);
          break;
        case FLOAT:
          object = byteBuffer.putFloat(index, (float) object);
          break;
        case DOUBLE:
          object = byteBuffer.putDouble(index, (double) object);
          break;
        case TEXT:
          // TODO
          break;
      }
    }
  }

  public static String getNvmPath() {
    return NVM_PATH;
  }

  public static void setNvmPath(String nvmPath) {
    NVM_PATH = nvmPath;
  }
}
