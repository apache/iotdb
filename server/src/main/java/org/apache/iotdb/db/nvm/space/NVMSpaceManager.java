package org.apache.iotdb.db.nvm.space;

import static org.apache.iotdb.db.nvm.rescon.NVMPrimitiveArrayPool.ARRAY_SIZE;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;
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
      nvmFilePath = nvmDir + File.pathSeparatorChar + NVM_FILE_NAME;
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

  public synchronized NVMSpace allocateSpace(long size) throws IOException {
    NVMSpace nvmSpace = new NVMSpace(curOffset, size, nvmFileChannel.map(MAP_MODE, curOffset, size));
    curOffset += size;
    return nvmSpace;
  }

  public synchronized NVMStringSpace allocateStringSpace(long size) throws IOException {
    NVMStringSpace nvmSpace = new NVMStringSpace(curOffset, size, nvmFileChannel.map(MAP_MODE, curOffset, size));
    curOffset += size;
    return nvmSpace;
  }

  public synchronized NVMDataSpace allocateDataSpace(long size, TSDataType dataType) {
    checkIsFull();

    try {
      logger.trace("Try to allocate {} nvm space at {}.", size, curOffset);
      int index = curDataSpaceIndex.getAndIncrement();
      NVMDataSpace nvmSpace = new NVMDataSpace(
          curOffset, size, nvmFileChannel.map(MAP_MODE, curOffset, size), index, dataType);
      nvmSpace.refreshData();
      metadataManager.updateCount(index);
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

  public List<NVMDataSpace> getAllNVMData() throws IOException {
    int spaceCount = metadataManager.getCount();
    List<NVMDataSpace> nvmDataList = new ArrayList<>(spaceCount);
    List<TSDataType> dataTypeList = metadataManager.getDataTypeList(spaceCount);
    long curOffset = 0;
    for (int i = 0; i < spaceCount; i++) {
      TSDataType dataType = dataTypeList.get(i);
      int spaceSize = NVMSpaceManager.getPrimitiveTypeByteSize(dataType) * ARRAY_SIZE;
      NVMDataSpace nvmDataSpace = recoverData(curOffset, spaceSize, i, dataType);
      nvmDataList.add(nvmDataSpace);

      curOffset += spaceSize;
    }
    return nvmDataList;
  }

  private synchronized NVMDataSpace recoverData(long offset, long size, int index, TSDataType dataType) throws IOException {
    NVMDataSpace nvmSpace = new NVMDataSpace(curOffset, size, nvmFileChannel.map(MAP_MODE, curOffset, size), index, dataType);
    curOffset += size;
    return nvmSpace;
  }

  public static NVMSpaceManager getInstance() {
    return INSTANCE;
  }
}
