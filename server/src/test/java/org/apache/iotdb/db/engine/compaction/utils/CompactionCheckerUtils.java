package org.apache.iotdb.db.engine.compaction.utils;

import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.BatchDataIterator;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.iotdb.db.utils.QueryUtils.modifyChunkMetaData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CompactionCheckerUtils {

  /**
   * Use TsFileSequenceReader to read the file list from back to front
   *
   * @param tsFileResources The tsFileResources to be read
   * @return All time series and their corresponding data points
   */
  public static Map<String, List<TimeValuePair>> readFiles(List<TsFileResource> tsFileResources)
      throws IOException, IllegalPathException {
    Map<String, List<TimeValuePair>> result = new HashMap<>();
    for (TsFileResource tsFileResource : tsFileResources) {
      try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFileResource.getTsFilePath())) {
        List<Path> allPaths = reader.getAllPaths();
        for (Path path : allPaths) {
          List<TimeValuePair> timeValuePairs =
              result.computeIfAbsent(path.getFullPath(), k -> new ArrayList<>());
          List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(path);
          List<Modification> seriesModifications = new LinkedList<>();

          for (Modification modification : tsFileResource.getModFile().getModifications()) {
            if (modification.getPath().matchFullPath(new PartialPath(path.getFullPath()))) {
              seriesModifications.add(modification);
            }
          }
          modifyChunkMetaData(chunkMetadataList, seriesModifications);
          for (ChunkMetadata chunkMetadata : chunkMetadataList) {
            Chunk chunk = reader.readMemChunk(chunkMetadata);
            ChunkReader chunkReader = new ChunkReader(chunk, null);
            while (chunkReader.hasNextSatisfiedPage()) {
              BatchData batchData = chunkReader.nextPageData();
              BatchDataIterator batchDataIterator = batchData.getBatchDataIterator();
              while (batchDataIterator.hasNextTimeValuePair()) {
                timeValuePairs.add(batchDataIterator.nextTimeValuePair());
              }
            }
          }
        }
      }
    }
    return result;
  }

  /**
   * 1. Use TsFileSequenceReader to read the merged file from back to front, and compare it with the
   * data before the merge to ensure complete consistency. 2. Use TsFileSequenceReader to read the
   * merged file from the front to the back, and compare it with the data before the merge to ensure
   * complete consistency (including pointsInPage statistics in pageHeader). 3. Check whether the
   * statistical information of TsFileResource corresponds to the original data point one-to-one,
   * including startTime, endTime (device level)
   *
   * @param sourceData All time series before merging and their corresponding data points
   * @param mergedFile The merged File to be checked
   */
  public static void checkDataAndResource(
      Map<String, List<TimeValuePair>> sourceData, TsFileResource mergedFile)
      throws IOException, IllegalPathException {
    // read from back to front
    List<TsFileResource> tsFileResources = new ArrayList<>();
    tsFileResources.add(mergedFile);
    Map<String, List<TimeValuePair>> mergedData = readFiles(tsFileResources);
    compareSensorAndData(sourceData, mergedData);

    // read from front to back
    mergedData = new HashMap<>();
    Map<String, Long> fullPathPointNum = new HashMap<>();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(mergedFile.getTsFilePath())) {
      // Sequential reading of one ChunkGroup now follows this order:
      // first the CHUNK_GROUP_HEADER, then SeriesChunks (headers and data) in one ChunkGroup
      // Because we do not know how many chunks a ChunkGroup may have, we should read one byte (the
      // marker) ahead and judge accordingly.
      String device = "";
      String measurementID = "";
      List<TimeValuePair> currTimeValuePairs = new ArrayList<>();
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      byte marker;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case (byte) (MetaMarker.CHUNK_HEADER | TsFileConstant.TIME_COLUMN_MASK):
          case (byte) (MetaMarker.CHUNK_HEADER | TsFileConstant.VALUE_COLUMN_MASK):
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case (byte) (MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | TsFileConstant.TIME_COLUMN_MASK):
          case (byte) (MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | TsFileConstant.VALUE_COLUMN_MASK):
            ChunkHeader header = reader.readChunkHeader(marker);
            // read the next measurement and pack data of last measurement
            if (currTimeValuePairs.size() > 0) {
              PartialPath path = new PartialPath(device, measurementID);
              List<TimeValuePair> timeValuePairs =
                  mergedData.computeIfAbsent(path.getFullPath(), k -> new ArrayList<>());
              timeValuePairs.addAll(currTimeValuePairs);
            }
            measurementID = header.getMeasurementID();
            Decoder defaultTimeDecoder =
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64);
            Decoder valueDecoder =
                Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
            int dataSize = header.getDataSize();
            while (dataSize > 0) {
              valueDecoder.reset();
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(), header.getChunkType() == MetaMarker.CHUNK_HEADER);
              PartialPath path = new PartialPath(device, measurementID);
              long count = fullPathPointNum.getOrDefault(path.getFullPath(), 0L);
              count += pageHeader.getNumOfValues();
              fullPathPointNum.put(path.getFullPath(), count);
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
              PageReader reader1 =
                  new PageReader(
                      pageData, header.getDataType(), valueDecoder, defaultTimeDecoder, null);
              BatchData batchData = reader1.getAllSatisfiedPageData();
              BatchDataIterator batchDataIterator = batchData.getBatchDataIterator();
              while (batchDataIterator.hasNextTimeValuePair()) {
                currTimeValuePairs.add(batchDataIterator.nextTimeValuePair());
              }
              dataSize -= pageHeader.getSerializedPageSize();
            }
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            device = chunkGroupHeader.getDeviceID();
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
      // pack data of last measurement
      if (currTimeValuePairs.size() > 0) {
        PartialPath path = new PartialPath(device, measurementID);
        List<TimeValuePair> timeValuePairs =
            mergedData.computeIfAbsent(path.getFullPath(), k -> new ArrayList<>());
        timeValuePairs.addAll(currTimeValuePairs);
      }
    }
    compareSensorAndData(sourceData, mergedData);
    for (Entry<String, List<TimeValuePair>> sourceDataEntry : sourceData.entrySet()) {
      String fullPath = sourceDataEntry.getKey();
      long sourceDataNum = sourceDataEntry.getValue().size();
      long targetDataNum = fullPathPointNum.getOrDefault(fullPath, 0L);
      assertEquals(sourceDataNum, targetDataNum);
    }

    // check TsFileResource
    Map<String, long[]> devicePointNumMap = new HashMap<>();
    for (Entry<String, List<TimeValuePair>> dataEntry : sourceData.entrySet()) {
      PartialPath partialPath = new PartialPath(dataEntry.getKey());
      String device = partialPath.getDevice();
      long[] statistics =
          devicePointNumMap.computeIfAbsent(
              device, k -> new long[] {Long.MAX_VALUE, Long.MIN_VALUE});
      List<TimeValuePair> timeValuePairs = dataEntry.getValue();
      statistics[0] = Math.min(statistics[0], timeValuePairs.get(0).getTimestamp()); // start time
      statistics[1] =
          Math.max(
              statistics[1],
              timeValuePairs.get(timeValuePairs.size() - 1).getTimestamp()); // end time
    }
    for (Entry<String, long[]> deviceCountEntry : devicePointNumMap.entrySet()) {
      String device = deviceCountEntry.getKey();
      long[] statistics = deviceCountEntry.getValue();
      assertEquals(statistics[0], mergedFile.getStartTime(device));
      assertEquals(statistics[1], mergedFile.getEndTime(device));
    }
  }

  private static void compareSensorAndData(
      Map<String, List<TimeValuePair>> expectedData, Map<String, List<TimeValuePair>> targetData) {
    for (Entry<String, List<TimeValuePair>> sourceDataEntry : expectedData.entrySet()) {
      String fullPath = sourceDataEntry.getKey();
      List<TimeValuePair> sourceTimeValuePairs = sourceDataEntry.getValue();
      List<TimeValuePair> targetTimeValuePairs = targetData.get(fullPath);
      compareData(sourceTimeValuePairs, targetTimeValuePairs);
    }
  }

  private static void compareData(
      List<TimeValuePair> expectedData, List<TimeValuePair> targetData) {
    if (targetData.size() > expectedData.size()) {
      fail();
    }
    if (targetData.size() < expectedData.size()) {
      fail();
    }
    for (int i = 0; i < expectedData.size(); i++) {
      TimeValuePair expectedTimeValuePair = expectedData.get(i);
      TimeValuePair targetTimeValuePair = targetData.get(i);
      assertEquals(expectedTimeValuePair.getTimestamp(), targetTimeValuePair.getTimestamp());
      assertEquals(expectedTimeValuePair.getValue(), targetTimeValuePair.getValue());
    }
  }

  /**
   * Use TsFileSequenceReader to read the merged file from the front to the back, and compare it
   * with the data before the merge one by one to ensure that the order and size of chunk and page
   * are one-to-one correspondence
   *
   * @param chunkPagePointsNum Target chunk and page size
   * @param mergedFile The merged File to be checked
   */
  public static void checkChunkAndPage(
      List<List<Long>> chunkPagePointsNum, TsFileResource mergedFile) throws IOException {
    List<List<Long>> mergedChunkPagePointsNum = new ArrayList<>();
    List<Long> pagePointsNum = new ArrayList<>();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(mergedFile.getTsFilePath())) {
      // Sequential reading of one ChunkGroup now follows this order:
      // first the CHUNK_GROUP_HEADER, then SeriesChunks (headers and data) in one ChunkGroup
      // Because we do not know how many chunks a ChunkGroup may have, we should read one byte (the
      // marker) ahead and judge accordingly.
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      byte marker;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case (byte) (MetaMarker.CHUNK_HEADER | TsFileConstant.TIME_COLUMN_MASK):
          case (byte) (MetaMarker.CHUNK_HEADER | TsFileConstant.VALUE_COLUMN_MASK):
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case (byte) (MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | TsFileConstant.TIME_COLUMN_MASK):
          case (byte) (MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | TsFileConstant.VALUE_COLUMN_MASK):
            ChunkHeader header = reader.readChunkHeader(marker);
            // read the next measurement and pack data of last measurement
            if (pagePointsNum.size() > 0) {
              mergedChunkPagePointsNum.add(pagePointsNum);
              pagePointsNum = new ArrayList<>();
            }
            Decoder defaultTimeDecoder =
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64);
            Decoder valueDecoder =
                Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
            int dataSize = header.getDataSize();
            while (dataSize > 0) {
              valueDecoder.reset();
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(), header.getChunkType() == MetaMarker.CHUNK_HEADER);
              pageHeader.getNumOfValues();
              pagePointsNum.add(pageHeader.getNumOfValues());
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
              PageReader reader1 =
                  new PageReader(
                      pageData, header.getDataType(), valueDecoder, defaultTimeDecoder, null);
              BatchData batchData = reader1.getAllSatisfiedPageData();
              BatchDataIterator batchDataIterator = batchData.getBatchDataIterator();
              while (batchDataIterator.hasNextTimeValuePair()) {
                batchDataIterator.nextTimeValuePair();
              }
              dataSize -= pageHeader.getSerializedPageSize();
            }
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
      // pack data of last measurement
      if (pagePointsNum.size() > 0) {
        mergedChunkPagePointsNum.add(pagePointsNum);
      }

      for (int i = 0; i < chunkPagePointsNum.size(); i++) {
        for (int j = 0; j < chunkPagePointsNum.get(i).size(); j++) {
          assertEquals(chunkPagePointsNum.get(i).get(j), mergedChunkPagePointsNum.get(i).get(j));
        }
      }
    }
  }
}
