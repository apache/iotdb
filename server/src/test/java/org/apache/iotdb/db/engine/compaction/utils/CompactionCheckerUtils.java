package org.apache.iotdb.db.engine.compaction.utils;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.BatchDataIterator;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompactionCheckerUtils {

  /**
   * Use TsFileSequenceReader to read the file list from back to front
   *
   * @param tsFileResources The tsFileResources to be read
   * @return All time series and their corresponding data points
   */
  public static Map<String, List<TimeValuePair>> readFiles(List<TsFileResource> tsFileResources)
      throws IOException {
    Map<String, List<TimeValuePair>> result = new HashMap<>();
    for (TsFileResource tsFileResource : tsFileResources) {
      try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFileResource.getTsFilePath())) {
        List<Path> allPaths = reader.getAllPaths();
        for (Path path : allPaths) {
          List<TimeValuePair> timeValuePairs = new ArrayList<>();
          List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(path);
          for (ChunkMetadata chunkMetadata : chunkMetadataList) {
            Chunk chunk = reader.readMemChunk(chunkMetadata);
            ChunkReader chunkReader = new ChunkReader(chunk, null);
            while (chunkReader.hasNextSatisfiedPage()) {
              BatchData batchData = chunkReader.nextPageData();
              BatchDataIterator batchDataIterator = batchData.getBatchDataIterator();
              while (batchDataIterator.hasNextTimeValuePair()) {
                timeValuePairs.add(batchDataIterator.currentTimeValuePair());
              }
            }
          }
          result.put(path.getFullPath(), timeValuePairs);
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
      Map<String, List<TimeValuePair>> sourceData, TsFileResource mergedFile) {}

  /**
   * Use TsFileSequenceReader to read the merged file from the front to the back, and compare it
   * with the data before the merge one by one to ensure that the order and size of chunk and page
   * are one-to-one correspondence
   *
   * @param chunkPagePointsNum Target chunk and page size
   * @param mergedFile The merged File to be checked
   */
  public static void checkChunkAndPage(
      List<List<Long>> chunkPagePointsNum, TsFileResource mergedFile) {}
}
