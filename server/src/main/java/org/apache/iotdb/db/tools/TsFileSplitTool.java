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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class TsFileSplitTool {

  private static final Logger logger = LoggerFactory.getLogger(TsFileSplitTool.class);

  private final String filename;

  /**
   * If the chunk point num is lower than this threshold, it will be deserialized into points,
   * default is 100
   */
  private final long chunkPointNumLowerBoundInCompaction =
      IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();

  public static void main(String[] args) throws IOException {
    String fileName = args[0];
    logger.info("Splitting TsFile {} ...", fileName);
    try {
      new TsFileSplitTool(fileName).run();
    } catch (IllegalPathException | PageException e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * construct TsFileSketchTool
   *
   * @param filename input file path
   */
  public TsFileSplitTool(String filename) {
    super();
    this.filename = filename;
  }

  /** entry of tool */
  public void run() throws IOException, IllegalPathException, PageException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(filename)) {
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
            tsFileIOWriter.startChunkGroup(deviceId);
          }

          List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(path);
          assert tsFileIOWriter != null;

          ChunkMetadata firstChunkMetadata = chunkMetadataList.get(0);
          reader.position(firstChunkMetadata.getOffsetOfChunkHeader());
          ChunkHeader chunkHeader = reader.readChunkHeader(reader.readMarker());
          MeasurementSchema measurementSchema =
              new MeasurementSchema(
                  chunkHeader.getMeasurementID(), chunkHeader.getDataType(),
                  chunkHeader.getEncodingType(), chunkHeader.getCompressionType());

          int numInChunk = 0;
          ChunkWriterImpl chunkWriter = new ChunkWriterImpl(measurementSchema);

          for (int i = 0; i < chunkMetadataList.size(); i++) {
            if (i != 0) {
              reader.position(chunkMetadataList.get(i).getOffsetOfChunkHeader());
              chunkHeader = reader.readChunkHeader(reader.readMarker());
            }
            TSDataType dataType = chunkHeader.getDataType();
            int dataSize = chunkHeader.getDataSize();
            Decoder valueDecoder =
                Decoder.getDecoderByType(chunkHeader.getEncodingType(), dataType);
            Decoder defaultTimeDecoder =
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64);

            while (dataSize > 0) {
              valueDecoder.reset();

              // a new Page
              PageHeader pageHeader =
                  reader.readPageHeader(
                      dataType,
                      ((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.CHUNK_HEADER);
              ByteBuffer pageData = reader.readPage(pageHeader, chunkHeader.getCompressionType());
              PageReader pageReader =
                  new PageReader(pageData, dataType, valueDecoder, defaultTimeDecoder, null);
              BatchData batchData = pageReader.getAllSatisfiedPageData();

              while (batchData.hasCurrent()) {
                writeToChunkWriter(
                    chunkWriter,
                    batchData.currentTime(),
                    batchData.currentValue(),
                    chunkHeader.getDataType());
                numInChunk++;
                if (numInChunk == chunkPointNumLowerBoundInCompaction) {
                  chunkWriter.writeToFileWriter(tsFileIOWriter);
                  numInChunk = 0;
                }
                batchData.next();
              }

              dataSize -= pageHeader.getSerializedPageSize();
            }
          }
          if (numInChunk != 0) {
            chunkWriter.writeToFileWriter(tsFileIOWriter);
          }
        }
      }

      logger.info("TsFile {} is split into {} new files.", filename, devices.size());
    }
  }

  private void writeToChunkWriter(
      ChunkWriterImpl chunkWriter, long time, Object value, TSDataType dataType) {
    switch (dataType) {
      case INT32:
        chunkWriter.write(time, (int) value);
        break;
      case INT64:
        chunkWriter.write(time, (long) value);
        break;
      case FLOAT:
        chunkWriter.write(time, (float) value);
        break;
      case DOUBLE:
        chunkWriter.write(time, (double) value);
        break;
      case BOOLEAN:
        chunkWriter.write(time, (boolean) value);
        break;
      case TEXT:
        chunkWriter.write(time, (Binary) value);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
  }
}
