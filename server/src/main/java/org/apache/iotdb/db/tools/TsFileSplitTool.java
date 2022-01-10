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

package org.apache.iotdb.db.tools;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TsFileSplitTool extends TsFileRewriteTool {

  private static final Logger logger = LoggerFactory.getLogger(TsFileSplitTool.class);

  private String filename;
  private TsFileSequenceReader reader;

  /**
   * If the chunk point num is lower than this threshold, it will be deserialized into points,
   * default is 100
   */
  private final long chunkPointNumLowerBoundInCompaction = 100;

  public static void main(String[] args) throws IOException {
    String fileName = "C:\\Users\\v-zesongsun\\Desktop\\test0.tsfile"; // args[0];
    logger.info("Splitting TsFile {} ...", fileName);
    try {
      new TsFileSplitTool(fileName).run();
    } catch (IllegalPathException | PageException e) {
      e.printStackTrace();
    }
  }

  /**
   * construct TsFileSketchTool
   *
   * @param filename input file path
   */
  public TsFileSplitTool(String filename) {
    super();
    try {
      this.filename = filename;
      this.reader = new TsFileSequenceReader(filename);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /** entry of tool */
  public void run() throws IOException, IllegalPathException, PageException {
    Iterator<List<Path>> pathIterator = reader.getPathsIterator();
    Set<String> devices = new HashSet<>();
    String fileNamePrefix = filename.substring(0, filename.length() - 7).concat("-");
    int fileIndex = 0;
    TsFileIOWriter tsFileIOWriter = null;
    while (pathIterator.hasNext()) {
      for (Path path : pathIterator.next()) {
        String deviceId = path.getDevice();
        if (devices.add(deviceId)) {
          if (tsFileIOWriter != null) {
            // seal last TsFile
            tsFileIOWriter.endFile();
          }

          // open a new TsFile
          tsFileIOWriter =
              new TsFileIOWriter(
                  FSFactoryProducer.getFSFactory()
                      .getFile(fileNamePrefix + fileIndex + TsFileConstant.TSFILE_SUFFIX));
          fileIndex++;
        }

        List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(path);
        assert tsFileIOWriter != null;
        tsFileIOWriter.startChunkGroup(deviceId);
        boolean firstChunkInChunkGroup = true;

        for (ChunkMetadata chunkMetadata : chunkMetadataList) {
          Chunk chunk = reader.readMemChunk(chunkMetadata);

          ChunkHeader chunkHeader = chunk.getHeader();
          MeasurementSchema measurementSchema =
              new MeasurementSchema(
                  chunkHeader.getMeasurementID(),
                  chunkHeader.getDataType(),
                  chunkHeader.getEncodingType(),
                  chunkHeader.getCompressionType());
          TSDataType dataType = chunkHeader.getDataType();
          List<PageHeader> pageHeadersInChunk = new ArrayList<>();
          List<ByteBuffer> dataInChunk = new ArrayList<>();
          int dataSize = chunkHeader.getDataSize();
          while (dataSize > 0) {
            // a new Page
            PageHeader pageHeader =
                reader.readPageHeader(
                    dataType, chunkHeader.getChunkType() == MetaMarker.CHUNK_HEADER);
            boolean needToDecode = pageHeader.getStatistics() == null; // ignore modification
            ByteBuffer pageData =
                !needToDecode
                    ? reader.readCompressedPage(pageHeader)
                    : reader.readPage(pageHeader, chunkHeader.getCompressionType());
            pageHeadersInChunk.add(pageHeader);
            dataInChunk.add(pageData);
            dataSize -= pageHeader.getSerializedPageSize();
          }
          reWriteChunk(
              deviceId, firstChunkInChunkGroup, measurementSchema, pageHeadersInChunk, dataInChunk);
        }
        tsFileIOWriter.endChunkGroup();
      }
    }
    logger.info("TsFile {} is split into {} new files.", filename, devices.size());
  }

  protected void reWriteChunk(
      String deviceId,
      boolean firstChunkInChunkGroup,
      MeasurementSchema schema,
      List<PageHeader> pageHeadersInChunk,
      List<ByteBuffer> pageDataInChunk)
      throws IOException, PageException {
    valueDecoder = Decoder.getDecoderByType(schema.getEncodingType(), schema.getType());
    Map<Long, ChunkWriterImpl> partitionChunkWriterMap = new HashMap<>();
    for (int i = 0; i < pageDataInChunk.size(); i++) {
      writePage(schema, pageHeadersInChunk.get(i), pageDataInChunk.get(i), partitionChunkWriterMap);
    }
    for (Map.Entry<Long, ChunkWriterImpl> entry : partitionChunkWriterMap.entrySet()) {
      long partitionId = entry.getKey();
      TsFileIOWriter tsFileIOWriter = partitionWriterMap.get(partitionId);
      if (firstChunkInChunkGroup || !tsFileIOWriter.isWritingChunkGroup()) {
        tsFileIOWriter.startChunkGroup(deviceId);
      }
      // write chunks to their own upgraded tsFiles
      IChunkWriter chunkWriter = entry.getValue();
      chunkWriter.writeToFileWriter(tsFileIOWriter);
    }
  }
}
