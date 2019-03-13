/**
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

package org.apache.iotdb.tsfile.write.writer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a restorable tsfile which do not depend on a restore file.
 */
public class NativeRestorableIOWriter extends TsFileIOWriter {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(NativeRestorableIOWriter.class);

  private long truncatedPosition = -1;
  private Map<String, MeasurementSchema> knownSchemas = new HashMap<>();

  long getTruncatedPosition() {
    return truncatedPosition;
  }

  public NativeRestorableIOWriter(File file) throws IOException {
    super();
    long fileSize;
    if (!file.exists()) {
      this.out = new DefaultTsFileOutput(file, true);
      startFile();
      return;
    } else {
      fileSize = file.length();
      this.out = new DefaultTsFileOutput(file, true);
    }

    //we need to read data to recover TsFileIOWriter.chunkGroupMetaDataList
    //and remove broken data if exists.

    ChunkMetaData currentChunk;
    String measurementID;
    TSDataType dataType;
    long fileOffsetOfChunk;
    long startTimeOfChunk = 0;
    long endTimeOfChunk = 0;
    long numOfPoints = 0;

    ChunkGroupMetaData currentChunkGroup;
    List<ChunkMetaData> chunks = null;
    String deviceID;
    long startOffsetOfChunkGroup = 0;
    long endOffsetOfChunkGroup;
    long versionOfChunkGroup = 0;
    boolean haveReadAnUnverifiedGroupFooter = false;
    boolean newGroup = true;

    TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath(), false);
    if (fileSize <= magicStringBytes.length) {
      LOGGER.debug("{} only has magic header, does not worth to recover.", file.getAbsolutePath());
      reader.close();
      this.out.truncate(0);
      startFile();
      truncatedPosition = magicStringBytes.length;
      return;
    }
    String magic = reader.readHeadMagic(true);
    if (!magic.equals(new String(magicStringBytes))) {
      throw new IOException(String
          .format("%s is not using TsFile format, and will be ignored...", file.getAbsolutePath()));
    }
    if (reader.readTailMagic().equals(magic)) {
      LOGGER.debug("{} is an complete TsFile.", file.getAbsolutePath());
      canWrite = false;
      reader.close();
      out.close();
      return;
    }

    // not a complete file, we will recover it...
    truncatedPosition = magicStringBytes.length;
    boolean goon = true;
    byte marker;
    try {
      while (goon && (marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
            //this is a chunk.
            if (haveReadAnUnverifiedGroupFooter) {
              //now we are sure that the last ChunkGroupFooter is complete.
              haveReadAnUnverifiedGroupFooter = false;
              truncatedPosition = reader.position() - 1;
              newGroup = true;
            }
            if (newGroup) {
              chunks = new ArrayList<>();
              startOffsetOfChunkGroup = reader.position() - 1;
              newGroup = false;
            }
            //if there is something wrong with a chunk, we will drop this part of data
            // (the whole ChunkGroup)
            ChunkHeader header = reader.readChunkHeader();
            measurementID = header.getMeasurementID();
            knownSchemas.putIfAbsent(measurementID,
                new MeasurementSchema(measurementID, header.getDataType(),
                    header.getEncodingType(), header.getCompressionType()));
            dataType = header.getDataType();
            fileOffsetOfChunk = reader.position() - 1;
            if (header.getNumOfPages() > 0) {
              PageHeader pageHeader = reader.readPageHeader(header.getDataType());
              numOfPoints += pageHeader.getNumOfValues();
              startTimeOfChunk = pageHeader.getMinTimestamp();
              endTimeOfChunk = pageHeader.getMaxTimestamp();
              reader.skipPageData(pageHeader);
            }
            for (int j = 1; j < header.getNumOfPages() - 1; j++) {
              //a new Page
              PageHeader pageHeader = reader.readPageHeader(header.getDataType());
              reader.skipPageData(pageHeader);
            }
            if (header.getNumOfPages() > 1) {
              PageHeader pageHeader = reader.readPageHeader(header.getDataType());
              endTimeOfChunk = pageHeader.getMaxTimestamp();
              reader.skipPageData(pageHeader);
            }
            currentChunk = new ChunkMetaData(measurementID, dataType, fileOffsetOfChunk,
                startTimeOfChunk, endTimeOfChunk);
            currentChunk.setNumOfPoints(numOfPoints);
            chunks.add(currentChunk);
            numOfPoints = 0;
            break;
          case MetaMarker.CHUNK_GROUP_FOOTER:
            //this is a chunk group
            //if there is something wrong with the chunkGroup Footer, we will drop this part of data
            //because we can not guarantee the correction of the deviceId.
            ChunkGroupFooter chunkGroupFooter = reader.readChunkGroupFooter();
            deviceID = chunkGroupFooter.getDeviceID();
            endOffsetOfChunkGroup = reader.position();
            currentChunkGroup = new ChunkGroupMetaData(deviceID, chunks, startOffsetOfChunkGroup);
            currentChunkGroup.setEndOffsetOfChunkGroup(endOffsetOfChunkGroup);
            currentChunkGroup.setVersion(versionOfChunkGroup++);
            chunkGroupMetaDataList.add(currentChunkGroup);
            // though we have read the current ChunkMetaData from Disk, it may be incomplete.
            // because if the file only loses one byte, the ChunkMetaData.deserialize() returns ok,
            // while the last filed of the ChunkMetaData is incorrect.
            // So, only reading the next MASK, can make sure that this ChunkMetaData is complete.
            haveReadAnUnverifiedGroupFooter = true;
            break;

          default:
            // it is impossible that we read an incorrect data.
            MetaMarker.handleUnexpectedMarker(marker);
            goon = false;
        }
      }
      //now we read the tail of the file, so we are sure that the last ChunkGroupFooter is complete.
      truncatedPosition = reader.position() - 1;
    } catch (IOException e2) {
      //if it is the end of the file, and we read an unverifiedGroupFooter, we must remove this ChunkGroup
      if (haveReadAnUnverifiedGroupFooter && !chunkGroupMetaDataList.isEmpty()) {
        chunkGroupMetaDataList.remove(chunkGroupMetaDataList.size() - 1);
      }
    } finally {
      //something wrong or all data is complete. We will discard current FileMetadata
      // so that we can continue to write data into this tsfile.
      LOGGER.info("File {} has {} bytes, and will be truncated from {}.",
          file.getAbsolutePath(), file.length(), truncatedPosition);
      out.truncate(truncatedPosition);
      reader.close();
    }
  }

  @Override
  public Map<String, MeasurementSchema> getKnownSchema() {
    return knownSchemas;
  }
}
