package org.apache.iotdb.db.nvm.space;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.nvm.metadata.DataTypeMemo;
import org.apache.iotdb.db.nvm.metadata.FreeSpaceBitMap;
import org.apache.iotdb.db.nvm.metadata.TSDataMap;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NVMSpaceManager {

  private static final Logger logger = LoggerFactory.getLogger(NVMSpaceManager.class);

  private static final String NVM_FILE_NAME = "nvmFile";
  private static final int NVMSPACE_NUM_MAX = 1000000;

  private static final long BITMAP_FIELD_OFFSET = 0L;
  private static final long BITMAP_FIELD_BYTE_SIZE = Byte.BYTES * NVMSPACE_NUM_MAX;

  private static final long DATATYPE_FIELD_OFFSET = BITMAP_FIELD_OFFSET + BITMAP_FIELD_BYTE_SIZE;
  private static final long DATATYPE_FIELD_BYTE_SIZE = Short.BYTES * NVMSPACE_NUM_MAX;

  private static final long TSID_FIELD_OFFSET = DATATYPE_FIELD_OFFSET + DATATYPE_FIELD_BYTE_SIZE;
  private static final long TSID_FIELD_BYTE_SIZE = getPrimitiveTypeByteSize(TSDataType.INT64) * NVMSPACE_NUM_MAX;

  private static final long TVMAP_FIELD_OFFSET = TSID_FIELD_OFFSET + TSID_FIELD_BYTE_SIZE;
  private static final long TVMAP_FIELD_BYTE_SIZE = getPrimitiveTypeByteSize(TSDataType.INT32) * 2 * NVMSPACE_NUM_MAX;

  private static final long DATA_FILED_OFFSET = TVMAP_FIELD_OFFSET + TVMAP_FIELD_BYTE_SIZE;

  private final static NVMSpaceManager INSTANCE = new NVMSpaceManager();

  private String nvmFilePath;
  private FileChannel nvmFileChannel;
  private final MapMode MAP_MODE = MapMode.READ_WRITE;
  private long nvmSize;

  /**
   * metadata fields
   */
  private FreeSpaceBitMap freeSpaceBitMap;
  private DataTypeMemo dataTypeMemo;
  private TSDataMap tsDataMap;

  /**
   * data field
   */
  private AtomicInteger curDataSpaceIndex = new AtomicInteger(0);
  private long curDataOffset = DATA_FILED_OFFSET;

  private NVMSpaceManager() {}

  public void init() throws StartupException {
    try {
      String nvmDir = IoTDBDescriptor.getInstance().getConfig().getNvmDir();
      nvmFilePath = nvmDir + File.pathSeparatorChar + NVM_FILE_NAME;
      File nvmDirFile = FSFactoryProducer.getFSFactory().getFile(nvmDir);
      nvmDirFile.mkdirs();
      nvmSize = nvmDirFile.getUsableSpace();
      nvmFileChannel = new RandomAccessFile(nvmFilePath, "rw").getChannel();

      initMetadataFields();
    } catch (IOException e) {
      logger.error("Fail to open NVM space at {}.", nvmFilePath, e);
      throw new StartupException(e);
    }
  }

  private void initMetadataFields() throws IOException {
    freeSpaceBitMap = new FreeSpaceBitMap(nvmFileChannel.map(MAP_MODE, BITMAP_FIELD_OFFSET, BITMAP_FIELD_BYTE_SIZE));
    dataTypeMemo = new DataTypeMemo(nvmFileChannel.map(MAP_MODE, DATATYPE_FIELD_OFFSET, DATATYPE_FIELD_BYTE_SIZE));
    tsDataMap = new TSDataMap(nvmFileChannel.map(MAP_MODE, TSID_FIELD_OFFSET, TSID_FIELD_BYTE_SIZE));
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

  public synchronized NVMSpace allocateSpace(long offset, long size) throws IOException {
    return new NVMSpace(offset, size, nvmFileChannel.map(MAP_MODE, offset, size));
  }

  public synchronized NVMDataSpace allocateDataSpace(long size, TSDataType dataType) {
    // TODO check if full

    try {
      logger.trace("Try to allocate {} nvm space at {}.", size, curDataOffset);
      NVMDataSpace nvmSpace = new NVMDataSpace(
          curDataOffset, size, nvmFileChannel.map(MAP_MODE, curDataOffset, size), curDataSpaceIndex
          .getAndIncrement(), dataType);
      curDataOffset += size;
      return nvmSpace;
    } catch (IOException e) {
      // TODO deal with error
      logger.error("Fail to allocate {} nvm space at {}.", size, curDataOffset);
      e.printStackTrace();
      return null;
    }
  }

  public void registerNVMDataSpace(NVMDataSpace nvmDataSpace) {
    int spaceIndex = nvmDataSpace.getIndex();
    freeSpaceBitMap.update(spaceIndex, false);
    dataTypeMemo.set(spaceIndex, nvmDataSpace.getDataType());
  }

  public void unregisterNVMDataSpace(NVMDataSpace nvmDataSpace) {
    freeSpaceBitMap.update(nvmDataSpace.getIndex(), true);
  }

  public void addSpaceToTimeSeries(String sgId, String deviceId, String measurementId, int timeSpaceIndex, int valueSpaceIndex) {
    tsDataMap.addSpaceToTimeSeries(sgId, deviceId, measurementId, timeSpaceIndex, valueSpaceIndex);
  }

  public static NVMSpaceManager getInstance() {
    return INSTANCE;
  }
}
