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

package org.apache.iotdb.db.schemaengine.schemaregion.tag;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TagLogFile implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TagLogFile.class);
  private File tagFile;
  private FileChannel fileChannel;
  private static final String LENGTH_EXCEED_MSG =
      "Tag/Attribute exceeds the max length limit. "
          + "Please enlarge tag_attribute_total_size in iotdb-system.properties";

  private static final int MAX_LENGTH =
      CommonDescriptor.getInstance().getConfig().getTagAttributeTotalSize();

  private static final int RECORD_FLUSH_INTERVAL =
      IoTDBDescriptor.getInstance().getConfig().getTagAttributeFlushInterval();
  private int unFlushedRecordNum = 0;

  public TagLogFile(String schemaDir, String logFileName) throws IOException {

    File metadataDir = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!metadataDir.exists()) {
      if (metadataDir.mkdirs()) {
        logger.info("create schema folder {}.", metadataDir);
      } else {
        logger.info("create schema folder {} failed.", metadataDir);
      }
    }

    tagFile = SystemFileFactory.INSTANCE.getFile(schemaDir + File.separator + logFileName);

    this.fileChannel =
        FileChannel.open(
            tagFile.toPath(),
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE);
    // move the current position to the tail of the file
    try {
      this.fileChannel.position(fileChannel.size());
    } catch (ClosedByInterruptException e) {
      // ignore
    }
  }

  public synchronized void copyTo(File targetFile) throws IOException {
    // flush os buffer
    fileChannel.force(true);
    FileUtils.copyFile(tagFile, targetFile);
  }

  /**
   * Read tags and attributes from tag file.
   *
   * @return tags map, attributes map
   * @throws IOException error occurred when reading disk
   */
  public Pair<Map<String, String>, Map<String, String>> read(long position) throws IOException {
    if (position < 0) {
      return new Pair<>(Collections.emptyMap(), Collections.emptyMap());
    }
    ByteBuffer byteBuffer = parseByteBuffer(fileChannel, position);
    return new Pair<>(ReadWriteIOUtils.readMap(byteBuffer), ReadWriteIOUtils.readMap(byteBuffer));
  }

  public Map<String, String> readTag(long position) throws IOException {
    ByteBuffer byteBuffer = parseByteBuffer(fileChannel, position);
    return ReadWriteIOUtils.readMap(byteBuffer);
  }

  public static ByteBuffer parseByteBuffer(FileChannel fileChannel, long position)
      throws IOException {
    // Read the first block
    ByteBuffer byteBuffer = ByteBuffer.allocate(MAX_LENGTH);
    fileChannel.read(byteBuffer, position);
    byteBuffer.flip();
    if (byteBuffer.limit() > 0) { // This indicates that there is data at this position
      int firstInt = ReadWriteIOUtils.readInt(byteBuffer); // first int
      byteBuffer.position(0);
      if (firstInt < -1) { // This position is blockNum, the original data occupies multiple blocks
        int blockNum = -firstInt;
        ByteBuffer byteBuffers = ByteBuffer.allocate(blockNum * MAX_LENGTH);
        byteBuffers.put(byteBuffer);
        byteBuffers.position(4); // Skip blockNum
        for (int i = 1; i < blockNum; i++) {
          long nextPosition = ReadWriteIOUtils.readLong(byteBuffers);
          // read one offset, then use filechannel's read to read it
          byteBuffers.position(MAX_LENGTH * i);
          byteBuffers.limit(MAX_LENGTH * (i + 1));
          fileChannel.read(byteBuffers, nextPosition);
          byteBuffers.position(4 + i * Long.BYTES);
        }
        byteBuffers.limit(byteBuffers.capacity());
        return byteBuffers;
      }
    }
    return byteBuffer;
  }

  private List<Long> parseOffsetList(long position) throws IOException {
    List<Long> blockOffset = new ArrayList<>();
    blockOffset.add(position);
    // Read the first block
    ByteBuffer byteBuffer = ByteBuffer.allocate(MAX_LENGTH);
    fileChannel.read(byteBuffer, position);
    byteBuffer.flip();
    if (byteBuffer.limit() > 0) { // This indicates that there is data at this position
      int firstInt = ReadWriteIOUtils.readInt(byteBuffer); // first int
      byteBuffer.position(0);
      if (firstInt < -1) { // This position is blockNum, the original data occupies multiple blocks
        int blockNum = -firstInt;
        int blockOffsetStoreLen =
            (((blockNum - 1) * Long.BYTES + 4) / MAX_LENGTH + 1)
                * MAX_LENGTH; // blockOffset storage length
        ByteBuffer blockBuffer = ByteBuffer.allocate(blockOffsetStoreLen);
        blockBuffer.put(byteBuffer);
        blockBuffer.position(4); // Skip blockNum

        for (int i = 1; i < blockNum; i++) {
          blockOffset.add(ReadWriteIOUtils.readLong(blockBuffer));
          // Every time you read an offset, use filechannel's read to read it
          if (MAX_LENGTH * (i + 1)
              <= blockOffsetStoreLen) { // Compared with directly reading bytebuffer, some reading
            // operations are reduced, only the content of offset is
            // read
            blockBuffer.position(MAX_LENGTH * i);
            blockBuffer.limit(MAX_LENGTH * (i + 1));
            fileChannel.read(blockBuffer, blockOffset.get(i));
            blockBuffer.position(4 + i * Long.BYTES);
          }
        }
      }
    }
    return blockOffset;
  }

  public long write(Map<String, String> tagMap, Map<String, String> attributeMap)
      throws IOException, MetadataException {
    ByteBuffer byteBuffer = convertMapToByteBuffer(tagMap, attributeMap);
    return write(byteBuffer, -1);
  }

  /**
   * This method does not modify this file's current position.
   *
   * @throws IOException IOException
   * @throws MetadataException metadata exception
   */
  public void write(Map<String, String> tagMap, Map<String, String> attributeMap, long position)
      throws IOException, MetadataException {
    ByteBuffer byteBuffer = convertMapToByteBuffer(tagMap, attributeMap);
    write(byteBuffer, position);
  }

  /**
   * @param byteBuffer the data of record to be persisted
   * @param position the target position to store the record in tagFile
   * @return beginning position of the record in tagFile
   */
  private synchronized long write(ByteBuffer byteBuffer, long position) throws IOException {
    // Correct the initial offset of the write
    if (position < 0) {
      // append the record to file tail
      position = fileChannel.size();
    }
    // Read the original data to get the original space offset
    List<Long> blockOffset = parseOffsetList(position);
    // write read data
    int blockNumReal = byteBuffer.capacity() / MAX_LENGTH;
    if (blockNumReal < 1) {
      throw new RuntimeException(
          "ByteBuffer capacity is smaller than tagAttributeTotalSize, which is not allowed.");
    }
    if (blockNumReal == 1 && blockOffset.size() == 1) {
      // If the original data occupies only one block and the new data occupies only one block, the
      // original space is used
      fileChannel.write(byteBuffer, blockOffset.get(0));
    } else {
      // if the original space is larger than the new space, the original space is used
      if (blockOffset.size() >= blockNumReal) {
        ByteBuffer byteBufferFinal = ByteBuffer.allocate(blockOffset.size() * MAX_LENGTH);
        byteBufferFinal.putInt(-blockOffset.size());
        for (int i = 1; i < blockOffset.size(); i++) {
          byteBufferFinal.putLong(blockOffset.get(i));
        }
        if (blockNumReal > 1) { // real data occupies multiple blocks
          byteBuffer.position((blockNumReal - 1) * Long.BYTES + 4);
        } else { // real data occupies only one block
          byteBuffer.position(0);
        }
        byteBufferFinal.put(byteBuffer);
        for (int i = 0; i < blockOffset.size(); i++) {
          byteBufferFinal.position(i * MAX_LENGTH);
          byteBufferFinal.limit((i + 1) * MAX_LENGTH);
          fileChannel.write(byteBufferFinal, blockOffset.get(i));
        }
      } else {
        // if the original space is smaller than the new space, the new space is used
        // prepare the bytebuffer to store the offset of each block
        int blockOffsetStoreLen = (blockNumReal - 1) * Long.BYTES + 4;
        ByteBuffer byteBufferOffset = ByteBuffer.allocate(blockOffsetStoreLen);
        byteBufferOffset.putInt(-blockNumReal);

        // write the data to the file
        for (int i = 0; i < blockNumReal; i++) {
          byteBuffer.position(i * MAX_LENGTH);
          byteBuffer.limit((i + 1) * MAX_LENGTH);

          // what we want to write is the original space
          if (i < blockOffset.size()) {
            if (i > 0) { // first block has been written
              byteBufferOffset.putLong(blockOffset.get(i));
            }
            fileChannel.write(byteBuffer, blockOffset.get(i));
          } else {
            // what we want to write is the new space
            // TODO
            byteBufferOffset.putLong(fileChannel.size());
            blockOffset.add(fileChannel.size());
            fileChannel.write(byteBuffer, fileChannel.size());
          }
        }

        // write the offset of each block to the file
        byteBufferOffset.flip();
        for (int i = 0; i < blockOffset.size(); i++) {
          byteBufferOffset.position(i * MAX_LENGTH);
          if ((i + 1) * MAX_LENGTH > byteBufferOffset.capacity()) {
            byteBufferOffset.limit(byteBufferOffset.capacity());
            fileChannel.write(byteBufferOffset, blockOffset.get(i));
            break;
          } else {
            byteBufferOffset.limit((i + 1) * MAX_LENGTH);
            fileChannel.write(byteBufferOffset, blockOffset.get(i));
          }
        }
      }
    }

    unFlushedRecordNum++;
    if (unFlushedRecordNum >= RECORD_FLUSH_INTERVAL) {
      fileChannel.force(true);
      unFlushedRecordNum = 0;
    }
    return position;
  }

  private ByteBuffer convertMapToByteBuffer(
      Map<String, String> tagMap, Map<String, String> attributeMap) throws MetadataException {
    int totalMapSize = calculateMapSize(tagMap) + calculateMapSize(attributeMap);
    ByteBuffer byteBuffer;
    if (totalMapSize <= MAX_LENGTH) {
      byteBuffer = ByteBuffer.allocate(MAX_LENGTH);
    } else {
      // get the minSolution from Num*MAX_LENGTH < TotalMapSize + 4 + Long.BYTES*Num <=
      // MAX_LENGTH*(Num + 1)
      double NumMinLimit = (totalMapSize + 4 - MAX_LENGTH) / (double) (MAX_LENGTH - Long.BYTES);
      // blockNum = num + 1
      int blockNum = (int) Math.ceil(NumMinLimit) + 1;

      byteBuffer = ByteBuffer.allocate(blockNum * MAX_LENGTH);
      // 4 bytes for blockNumSize, blockNum*Long.BYTES for blockOffset
      byteBuffer.position(4 + (blockNum - 1) * Long.BYTES);
    }
    serializeMap(tagMap, byteBuffer);
    serializeMap(attributeMap, byteBuffer);
    // set position to 0 and the content in this buffer could be read
    byteBuffer.position(0);
    return byteBuffer;
  }

  public static int calculateMapSize(Map<String, String> map) {
    int length = 0;
    if (map != null) {
      length += 4; // mapSize is 4 byte
      // while mapSize is 0, this for loop will not be executed
      for (Map.Entry<String, String> entry : map.entrySet()) {
        int entryLength = 0;
        String key = entry.getKey();
        String value = entry.getValue();

        entryLength += 4; // keySize is 4 byte
        if (key != null) {
          entryLength +=
              key.getBytes()
                  .length; // only key is not null then add key length, while key is null, only
          // store the keySize marker which is -1 (4 bytes)
        }

        entryLength += 4; // valueSize is 4 byte
        if (value != null) {
          entryLength +=
              value.getBytes()
                  .length; // only value is not null then add value length, while value is null,
          // only store the valueSize marker which is -1 (4 bytes)
        }
        length += entryLength;
      }
    } else {
      length += 4; // while map is null, the mapSize is writed to -1 which is 4 byte
    }
    return length;
  }

  private void serializeMap(Map<String, String> map, ByteBuffer byteBuffer)
      throws MetadataException {
    try {
      if (map == null) {
        ReadWriteIOUtils.write(0, byteBuffer);
      } else {
        ReadWriteIOUtils.write(map, byteBuffer);
      }
    } catch (BufferOverflowException e) {
      throw new MetadataException(LENGTH_EXCEED_MSG);
    }
  }

  @Override
  public void close() throws IOException {
    if (Objects.nonNull(fileChannel) && fileChannel.isOpen()) {
      fileChannel.force(true);
      fileChannel.close();
      fileChannel = null;
    }
  }
}
