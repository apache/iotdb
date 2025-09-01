/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.utils.cte;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class DiskSpiller {
  private static final String FILE_SUFFIX = ".cteTemp";
  private final String folderPath;
  private final String filePrefix;

  private int fileIndex;
  private boolean folderCreated = false;
  private final TsBlockSerde serde = new TsBlockSerde();

  public DiskSpiller(String folderPath, String filePrefix) {
    this.folderPath = folderPath;
    this.filePrefix = filePrefix + "-";
    this.fileIndex = 0;
  }

  private void createFolder(String folderPath) throws IoTDBException {
    try {
      Path path = Paths.get(folderPath);
      Files.createDirectories(path);
      folderCreated = true;
    } catch (IOException e) {
      throw new IoTDBException(
          "Create folder error: " + folderPath,
          e,
          TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  public void spill(List<TsBlock> tsBlocks) throws IoTDBException {
    if (!folderCreated) {
      createFolder(folderPath);
    }
    String fileName = filePrefix + String.format("%05d", fileIndex) + FILE_SUFFIX;
    fileIndex++;

    writeData(tsBlocks, fileName);
  }

  public List<String> getFilePaths() {
    List<String> filePaths = new ArrayList<>();
    for (int i = 0; i < fileIndex; i++) {
      filePaths.add(filePrefix + String.format("%05d", i) + FILE_SUFFIX);
    }
    return filePaths;
  }

  public List<FileSpillerReader> getReaders() throws IoTDBException {
    List<String> filePaths = getFilePaths();
    List<FileSpillerReader> fileReaders = new ArrayList<>();
    try {
      for (String filePath : filePaths) {
        fileReaders.add(new FileSpillerReader(filePath, serde));
      }
    } catch (IOException e) {
      throw new IoTDBException(
          "Can't get file for FileSpillerReader, check if the file exists: " + filePaths,
          e,
          TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    return fileReaders;
  }

  private void writeData(List<TsBlock> tsBlocks, String fileName) throws IoTDBException {
    Path filePath = Paths.get(fileName);
    try (FileChannel fileChannel =
        FileChannel.open(
            filePath,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.CREATE)) {
      for (TsBlock tsBlock : tsBlocks) {
        ByteBuffer tsBlockBuffer = serde.serialize(tsBlock);
        ByteBuffer length = ByteBuffer.allocate(4);
        length.putInt(tsBlockBuffer.capacity());
        length.flip();
        fileChannel.write(length);
        fileChannel.write(tsBlockBuffer);
      }
    } catch (IOException e) {
      throw new IoTDBException(
          "Can't write CTE data to file: " + fileName,
          e,
          TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }
}
