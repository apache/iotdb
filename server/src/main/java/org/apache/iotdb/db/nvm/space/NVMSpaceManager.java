package org.apache.iotdb.db.nvm.space;

import static org.apache.iotdb.db.nvm.rescon.NVMPrimitiveArrayPool.ARRAY_SIZE;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NVMSpaceManager {

  private static final Logger logger = LoggerFactory.getLogger(NVMSpaceManager.class);

  private static final String NVM_FILE_NAME = "nvmFile";
  public static final int NVMSPACE_NUM_MAX = 1000000;

  private final static NVMSpaceManager INSTANCE = new NVMSpaceManager();

  private String nvmFilePath;
  private FileChannel nvmFileChannel;
  private final MapMode MAP_MODE = MapMode.READ_WRITE;
  private long nvmSize;

  private AtomicInteger curDataSpaceIndex = new AtomicInteger(0);
  private long curOffset = 0L;

  private NVMSpaceMetadataManager metadataManager = NVMSpaceMetadataManager.getInstance();

  private NVMSpaceManager() {}

  public void init() throws StartupException {
    try {
      String nvmDir = IoTDBDescriptor.getInstance().getConfig().getNvmDir();
      nvmFilePath = nvmDir + File.separatorChar + NVM_FILE_NAME;
      File nvmDirFile = FSFactoryProducer.getFSFactory().getFile(nvmDir);
      nvmDirFile.mkdirs();
      nvmSize = nvmDirFile.getUsableSpace();
      nvmFileChannel = new RandomAccessFile(nvmFilePath, "rw").getChannel();

      metadataManager.init();
    } catch (IOException e) {
      logger.error("Fail to open NVM space at {}.", nvmFilePath, e);
      throw new StartupException(e);
    }
  }

  public void close() throws IOException {
    nvmFileChannel.close();
  }

  public synchronized NVMSpace allocateSpace(long size) throws IOException {
    logger.debug("Try to allocate NVMSpace from {} to {}", curOffset, curOffset + size);
    NVMSpace nvmSpace = new NVMSpace(curOffset, size, nvmFileChannel.map(MAP_MODE, curOffset, size));
    curOffset += size;
    return nvmSpace;
  }

  public synchronized NVMDataSpace allocateDataSpace(long size, TSDataType dataType) {
    checkIsFull();

    try {
      logger.debug("Try to allocate NVMDataSpace from {} to {}", curOffset, curOffset + size);
      int index = curDataSpaceIndex.getAndIncrement();
      NVMDataSpace nvmSpace = new NVMDataSpace(
          curOffset, size, nvmFileChannel.map(MAP_MODE, curOffset, size), index, dataType);
      nvmSpace.refreshData();
      metadataManager.updateCount(curDataSpaceIndex.get());
      curOffset += size;
      return nvmSpace;
    } catch (IOException e) {
      // TODO deal with error
      logger.error("Fail to allocate {} nvm space at {}.", size, curOffset);
      e.printStackTrace();
      return null;
    }
  }

  private void checkIsFull() {
    // TODO
  }

  public NVMDataSpace getNVMDataSpaceByIndex(int spaceIndex) throws IOException {
    long offset = metadataManager.getOffsetBySpaceIndex(spaceIndex);
    TSDataType dataType = metadataManager.getDatatypeBySpaceIndex(spaceIndex);
    int size = computeDataSpaceSizeByDataType(dataType);
    return recoverData(offset, size, spaceIndex, dataType);
  }

  private int computeDataSpaceSizeByDataType(TSDataType dataType) {
    return getPrimitiveTypeByteSize(dataType) * ARRAY_SIZE;
  }

  private synchronized NVMDataSpace recoverData(long offset, long size, int index, TSDataType dataType) throws IOException {
    logger.debug("Try to recover NVMSpace from {} to {}", offset, offset + size);
    NVMDataSpace nvmSpace = new NVMDataSpace(offset, size, nvmFileChannel.map(MAP_MODE, offset, size), index, dataType);
    return nvmSpace;
  }

  public static NVMSpaceManager getInstance() {
    return INSTANCE;
  }

  public static int getPrimitiveTypeByteSize(TSDataType dataType) {
    int size = 0;
    switch (dataType) {
      case BOOLEAN:
        size = Byte.BYTES;
        break;
      case INT32:
        size = Integer.BYTES;
        break;
      case INT64:
        size = Long.BYTES;
        break;
      case FLOAT:
        size = Float.BYTES;
        break;
      case DOUBLE:
        size = Double.BYTES;
        break;
      case TEXT:
        // TODO
        break;
      default:
        throw new UnSupportedDataTypeException("DataType: " + dataType);
    }
    return size;
  }
}
