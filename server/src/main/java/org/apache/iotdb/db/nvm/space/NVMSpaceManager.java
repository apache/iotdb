/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.nvm.space;

import static org.apache.iotdb.db.rescon.NVMPrimitiveArrayPool.ARRAY_SIZE;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.nvm.NVMSpaceManagerException;
import org.apache.iotdb.db.nvm.NVMSpaceMetadataManager;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NVMSpaceManager {

  private static final Logger logger = LoggerFactory.getLogger(NVMSpaceManager.class);

  private static final String NVM_FILE_NAME = "nvmFile";
  public static final int NVMSPACE_NUM_MAX = 1000000;
  private static final int TEXT_AVERAGE_SIZE_IN_BYTES = 100;

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
    logger.trace("Try to allocate NVMSpace from {} to {}", curOffset, curOffset + size);
    NVMSpace nvmSpace = new NVMSpace(curOffset, size, nvmFileChannel.map(MAP_MODE, curOffset, size));
    curOffset += size;
    return nvmSpace;
  }

  public synchronized NVMDataSpace allocateDataSpace(long size, TSDataType dataType, boolean isTime)
      throws NVMSpaceManagerException {
    checkIsNVMFull(size);

    try {
      logger.trace("Try to allocate NVMDataSpace from {} to {}", curOffset, curOffset + size);
      int index = curDataSpaceIndex.getAndIncrement();
      NVMDataSpace nvmSpace;
      if (dataType == TSDataType.TEXT) {
        nvmSpace = new NVMBinaryDataSpace(curOffset, size, nvmFileChannel.map(MAP_MODE, curOffset, size),
            index, false);
      } else {
        nvmSpace = new NVMDataSpace(curOffset, size, nvmFileChannel.map(MAP_MODE, curOffset, size),
            index, dataType, isTime);
      }
      curOffset += size;

      return nvmSpace;
    } catch (IOException e) {
      logger.error("Fail to allocate {} nvm space at {}.", size, curOffset);
      throw new NVMSpaceManagerException(e.getMessage());
    }
  }

  private void checkIsNVMFull(long sizeToAllocate) throws NVMSpaceManagerException {
    if (curOffset + sizeToAllocate > nvmSize) {
      throw new NVMSpaceManagerException("NVM space is used up, can't allocate more. (total: " + nvmSize + ", used: " + curOffset + ", to allocate: " + sizeToAllocate + ")");
    }
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
    logger.trace("Try to recover NVMSpace from {} to {}", offset, offset + size);
    NVMDataSpace nvmSpace;
    if (dataType == TSDataType.TEXT) {
      nvmSpace = new NVMBinaryDataSpace(offset, size, nvmFileChannel.map(MAP_MODE, offset, size),
          index, true);
    } else {
     nvmSpace = new NVMDataSpace(offset, size, nvmFileChannel.map(MAP_MODE, offset, size), index, dataType,
          false);
    }
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
        size = TEXT_AVERAGE_SIZE_IN_BYTES;
        break;
      default:
        throw new UnSupportedDataTypeException("DataType: " + dataType);
    }
    return size;
  }
}
