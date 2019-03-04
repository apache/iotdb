/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.write.writer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a restorable tsfile which do not depend on a restore file.
 */
public class NativeRestorableTsFileIOWriter extends TsFileIOWriter {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(NativeRestorableTsFileIOWriter.class);


  public NativeRestorableTsFileIOWriter(String insertFilePath) throws IOException {
    super();
    File insertFile = new File(insertFilePath);

    if (!insertFile.exists()) {
      this.out = new DefaultTsFileOutput(insertFile);
      startFile();
      return;
    }

    //we need to read data to recover TsFileIOWriter.chunkGroupMetaDataList
    //and remove broken data if exists.
    List<ChunkGroupMetaData> metadatas = this.getChunkGroupMetaDatas();

    ChunkMetaData currentChunk;
    String measurementID;
    TSDataType dataType;
    long fileOffsetOfChunk;
    long startTimeOfChunk = 0;
    long endTimeOfChunk;

    ChunkGroupMetaData currentChunkGroup;
    List<ChunkMetaData> chunks = null;
    String deviceID;
    long startOffsetOfChunkGroup = 0;
    long endOffsetOfChunkGroup;
    long versionOfChunkGroup = 0;
    boolean newGroup = true;

    TsFileSequenceReader reader = new TsFileSequenceReader(insertFilePath);
    if (reader.fileSize() <= 4) {
      LOGGER.debug("{} does not worth to recover, will create a new one.", insertFilePath);
      reader.close();
      this.out.truncate(0);
      startFile();
      return;
    }
    if (reader.readTailMagic().equals(reader.readHeadMagic())) {
      LOGGER.debug("{} is an complete TsFile.", insertFilePath);
      complete = true;
      return;
    }

    // not a complete file, we will recover it...
    long pos = magicStringBytes.length;
    boolean goon = true;
    byte marker;
    try {
      while (goon && (marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
            //this is a chunk.
            if (newGroup) {
              chunks = new ArrayList<>();
              startOffsetOfChunkGroup = reader.position() - 1;
              newGroup = false;
            }
            //if there is something wrong with a chunk, we will drop this part of data
            // (the whole ChunkGroup)
            try {
              ChunkHeader header = reader.readChunkHeader();
              measurementID = header.getMeasurementID();
              dataType = header.getDataType();
              fileOffsetOfChunk = reader.position() - 1;
              if (header.getNumOfPages() > 0) {
                PageHeader pageHeader = reader.readPageHeader(header.getDataType());
                startTimeOfChunk = pageHeader.getMinTimestamp();
                reader.skipPageData(pageHeader);
              }
              for (int j = 1; j < header.getNumOfPages() - 1; j++) {
                //a new Page
                PageHeader pageHeader = reader.readPageHeader(header.getDataType());
                reader.skipPageData(pageHeader);
              }
              if (header.getNumOfPages() > 1) {
                PageHeader pageHeader = reader.readPageHeader(header.getDataType());
                endTimeOfChunk = pageHeader.getMinTimestamp();
                reader.skipPageData(pageHeader);
              } else {
                endTimeOfChunk = startTimeOfChunk;
              }
              currentChunk = new ChunkMetaData(measurementID, dataType, fileOffsetOfChunk,
                  startTimeOfChunk, endTimeOfChunk);
              chunks.add(currentChunk);
            } catch (IOException e) {
              goon = false;
            }
            break;
          case MetaMarker.CHUNK_GROUP_FOOTER:
            //this is a chunk group
            //if there is something wrong with the chunkGroup Footer, we will drop this part of data
            //because we can not guarantee the correction of the deviceId.
            try {
              ChunkGroupFooter chunkGroupFooter = reader.readChunkGroupFooter();
              deviceID = chunkGroupFooter.getDeviceID();
              endOffsetOfChunkGroup = reader.position();
              currentChunkGroup = new ChunkGroupMetaData(deviceID, chunks, startOffsetOfChunkGroup);
              currentChunkGroup.setEndOffsetOfChunkGroup(endOffsetOfChunkGroup);
              currentChunkGroup.setVersion(versionOfChunkGroup++);//TODO is this OK?
              metadatas.add(currentChunkGroup);
              newGroup = true;
              //we get a complete chunk group now.
              pos = getPos();
            } catch (IOException e) {
              goon = false;
            }
            break;
          default:
            //no data else
            goon = false;
        }
      }
    } catch (IOException e2) {
    } finally {
      //something wrong or all data is complete. We will discard current FileMetadata
      out.truncate(pos);
    }


  }


}
