package org.apache.iotdb.db.nvm.space;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NVMSpaceManager {

  private static final Logger logger = LoggerFactory.getLogger(NVMSpaceManager.class);

  private final static NVMSpaceManager INSTANCE = new NVMSpaceManager();

  private String NVM_PATH;
  private FileChannel nvmFileChannel;
  private final MapMode MAP_MODE = MapMode.READ_WRITE;
  private long nvmSize;
  private AtomicLong curOffset = new AtomicLong(0L);

  private NVMSpaceManager() {
//    init();
  }

  public void init() throws StartupException {
    try {
      NVM_PATH = IoTDBDescriptor.getInstance().getConfig().getNvmDir() + "/nvmFile";
      FSFactoryProducer.getFSFactory().getFile(IoTDBDescriptor.getInstance().getConfig().getNvmDir()).mkdirs();
      nvmFileChannel = new RandomAccessFile(NVM_PATH, "rw").getChannel();
      nvmSize = nvmFileChannel.size();
    } catch (IOException e) {
      logger.error("Fail to open NVM space at {}.", NVM_PATH, e);
      throw new StartupException(e);
    }
  }

  public void close() throws IOException {
    nvmFileChannel.close();
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

  public NVMSpace allocate(long size, TSDataType dataType) {
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

  public class NVMSpace {

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
      NVMSpace cloneSpace = NVMSpaceManager.getInstance().allocate(size, dataType);
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

  public static NVMSpaceManager getInstance() {
    return INSTANCE;
  }
}
