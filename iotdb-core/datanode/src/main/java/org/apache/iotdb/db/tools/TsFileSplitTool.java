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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TsFileSplitTool {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileSplitTool.class);

  private final String filename;

  private static final String SIZE_PARAM = "-size";
  private static final String LEVEL_PARAM = "-level";

  /**
   * If the chunk point num is lower than this threshold, it will be deserialized into points,
   * default is 100
   */
  private final long chunkPointNumLowerBoundInCompaction =
      IoTDBDescriptor.getInstance().getConfig().getChunkPointNumLowerBoundInCompaction();

  private static long targetSplitFileSize =
      IoTDBDescriptor.getInstance().getConfig().getTargetCompactionFileSize();

  private static String levelNum = "10";

  private static final FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  // TODO maxPlanIndex and minPlanIndex should be modified after cluster is refactored
  // Maximum index of plans executed within this TsFile
  protected long maxPlanIndex = Long.MIN_VALUE;

  // Minimum index of plans executed within this TsFile.
  protected long minPlanIndex = Long.MAX_VALUE;

  public static void main(String[] args) throws IOException {
    checkArgs(args);
    String fileName = args[0];
    LOGGER.info("Splitting TsFile {} ...", fileName);
    new TsFileSplitTool(fileName).run();
  }

  /* construct TsFileSketchTool */
  public TsFileSplitTool(String filename) {
    this.filename = filename;
  }

  /* entry of tool */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void run() throws IOException {
    if (ModificationFile.getExclusiveMods(new File(filename)).exists()) {
      throw new IOException("Unsupported to split TsFile with modification currently.");
    }

    TsFileIOWriter writer = null;

    try (TsFileSequenceReader reader = new TsFileSequenceReader(filename)) {
      Iterator<List<Path>> pathIterator = reader.getPathsIterator();
      Set<IDeviceID> devices = new HashSet<>();
      String[] filePathSplit = filename.split(IoTDBConstant.FILE_NAME_SEPARATOR);
      int originVersionIndex = Integer.parseInt(filePathSplit[filePathSplit.length - 3]);
      int versionIndex = originVersionIndex + 1;
      filePathSplit[filePathSplit.length - 2] = levelNum;

      while (pathIterator.hasNext()) {
        for (Path path : pathIterator.next()) {
          IDeviceID deviceId = path.getIDeviceID();
          if (devices.add(deviceId)) {
            if (writer != null && writer.getPos() < targetSplitFileSize) {
              writer.endChunkGroup();
              writer.startChunkGroup(deviceId);
            } else {
              if (writer != null) {
                // seal last TsFile
                TsFileResource resource = endFileAndGenerateResource(writer);
                resource.close();
              }

              filePathSplit[filePathSplit.length - 3] = String.valueOf(versionIndex);
              StringBuilder sb = new StringBuilder();
              for (int i = 0; i < filePathSplit.length; i++) {
                sb.append(filePathSplit[i]);
                if (i != filePathSplit.length - 1) {
                  sb.append(IoTDBConstant.FILE_NAME_SEPARATOR);
                }
              }
              // open a new TsFile
              writer = new TsFileIOWriter(fsFactory.getFile(sb.toString()));
              versionIndex++;
              writer.startChunkGroup(deviceId);
            }
          }

          List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(path);
          assert writer != null;

          ChunkMetadata firstChunkMetadata = chunkMetadataList.get(0);
          reader.position(firstChunkMetadata.getOffsetOfChunkHeader());
          ChunkHeader chunkHeader = reader.readChunkHeader(reader.readMarker());
          if (chunkHeader.getChunkType()
                  == (byte) (MetaMarker.CHUNK_HEADER | TsFileConstant.TIME_COLUMN_MASK)
              || chunkHeader.getChunkType()
                  == (byte) (MetaMarker.CHUNK_HEADER | TsFileConstant.VALUE_COLUMN_MASK)
              || chunkHeader.getChunkType()
                  == (byte)
                      (MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | TsFileConstant.TIME_COLUMN_MASK)
              || chunkHeader.getChunkType()
                  == (byte)
                      (MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | TsFileConstant.VALUE_COLUMN_MASK)) {
            throw new IOException("Unsupported to split TsFile with aligned timeseries currently.");
          }

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
                  new PageReader(pageData, dataType, valueDecoder, defaultTimeDecoder);
              BatchData batchData = pageReader.getAllSatisfiedPageData();

              while (batchData.hasCurrent()) {
                writeToChunkWriter(
                    chunkWriter,
                    batchData.currentTime(),
                    batchData.currentValue(),
                    chunkHeader.getDataType());
                numInChunk++;
                if (numInChunk == chunkPointNumLowerBoundInCompaction) {
                  chunkWriter.writeToFileWriter(writer);
                  numInChunk = 0;
                }
                batchData.next();
              }

              dataSize -= pageHeader.getSerializedPageSize();
            }
          }
          if (numInChunk != 0) {
            chunkWriter.writeToFileWriter(writer);
          }
        }
      }

      if (writer != null) {
        // seal last TsFile
        TsFileResource resource = endFileAndGenerateResource(writer);
        resource.close();
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

  private void writeToChunkWriter(
      ChunkWriterImpl chunkWriter, long time, Object value, TSDataType dataType) {
    switch (dataType) {
      case INT32:
      case DATE:
        chunkWriter.write(time, (int) value);
        break;
      case INT64:
      case TIMESTAMP:
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
      case BLOB:
      case STRING:
        chunkWriter.write(time, (Binary) value);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
  }

  private TsFileResource endFileAndGenerateResource(TsFileIOWriter writer) throws IOException {
    writer.endChunkGroup();
    writer.endFile();

    TsFileResource tsFileResource = new TsFileResource(writer.getFile());
    Map<IDeviceID, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap =
        writer.getDeviceTimeseriesMetadataMap();
    for (Map.Entry<IDeviceID, List<TimeseriesMetadata>> entry :
        deviceTimeseriesMetadataMap.entrySet()) {
      IDeviceID device = entry.getKey();
      for (TimeseriesMetadata timeseriesMetaData : entry.getValue()) {
        tsFileResource.updateStartTime(device, timeseriesMetaData.getStatistics().getStartTime());
        tsFileResource.updateEndTime(device, timeseriesMetaData.getStatistics().getEndTime());
      }
    }
    tsFileResource.setMinPlanIndex(minPlanIndex);
    tsFileResource.setMaxPlanIndex(maxPlanIndex);
    tsFileResource.setStatus(TsFileResourceStatus.NORMAL);
    tsFileResource.serialize();

    return tsFileResource;
  }

  private static void checkArgs(String[] args) {
    if (args.length == 3) {
      if (args[1].equals(SIZE_PARAM)) {
        targetSplitFileSize = Long.parseLong(args[2]);
        return;
      } else if (args[1].equals(LEVEL_PARAM)) {
        levelNum = args[2];
        return;
      }
    } else if (args.length == 5) {
      if (args[1].equals(SIZE_PARAM) && args[3].equals(LEVEL_PARAM)) {
        targetSplitFileSize = Long.parseLong(args[2]);
        levelNum = args[4];
        return;
      } else if (args[1].equals(LEVEL_PARAM) && args[3].equals(SIZE_PARAM)) {
        levelNum = args[2];
        targetSplitFileSize = Long.parseLong(args[4]);
        return;
      }
    }
    throw new UnsupportedOperationException("Invalid param");
  }
}
