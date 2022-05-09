package org.apache.iotdb.db.engine.compaction.cross.utils;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.RewriteCrossSpaceCompactionResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RewriteCompactionEstimator implements CompactionEstimator {

  private final RewriteCrossSpaceCompactionResource resource;

  private long timeLimit;

  private long maxCostOfReadingSeqFile;

  private Pair<Integer, Integer> maxUnseqChunkNumInDevice;

  private final List<Pair<Integer, Integer>> maxSeqChunkNumInDeviceList;

  // the number of timeseries being compacted at the same time
  private final int subCompactionTaskNum =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  public RewriteCompactionEstimator(RewriteCrossSpaceCompactionResource resource) {
    timeLimit =
        IoTDBDescriptor.getInstance().getConfig().getCrossCompactionFileSelectionTimeBudget();
    if (timeLimit < 0) {
      timeLimit = Long.MAX_VALUE;
    }
    this.resource = resource;
    this.maxCostOfReadingSeqFile = 0;
    this.maxSeqChunkNumInDeviceList = new ArrayList<>();
  }

  @Override
  public long estimateMemory(int unseqIndex, List<Integer> seqIndexes) throws IOException {
    long cost = 0;
    cost += calculateReadingUnseqFile(unseqIndex);
    cost += calculateReadingSeqFiles(seqIndexes);
    cost += calculatingWritingTargetFiles(unseqIndex, seqIndexes);
    maxSeqChunkNumInDeviceList.clear();
    return cost;
  }

  private long calculateReadingUnseqFile(int unseqIndex) throws IOException {
    TsFileResource unseqResource = resource.getUnseqFiles().get(unseqIndex);
    TsFileSequenceReader reader = resource.getFileReader(unseqResource);
    int[] fileInfo = getTotalAndLargestChunkNum(reader);
    // it is max aligned series num of one device when tsfile contains aligned series,
    // else is sub compaction task num.
    int concurrentSeriesNum = fileInfo[2] == -1 ? subCompactionTaskNum : fileInfo[2];
    maxUnseqChunkNumInDevice = new Pair<>(fileInfo[3], fileInfo[0]);
    return concurrentSeriesNum * (unseqResource.getTsFileSize() * fileInfo[1] / fileInfo[0]);
  }

  private long calculateReadingSeqFiles(List<Integer> seqIndexes) throws IOException {
    long cost = 0;
    for (int seqIndex : seqIndexes) {
      TsFileResource seqResource = resource.getSeqFiles().get(seqIndex);
      TsFileSequenceReader reader = resource.getFileReader(seqResource);
      int[] fileInfo = getTotalAndLargestChunkNum(reader);
      // it is max aligned series num of one device when tsfile contains aligned series,
      // else is sub compaction task num.
      int concurrentSeriesNum = fileInfo[2] == -1 ? subCompactionTaskNum : fileInfo[2];
      long seqFileCost =
          concurrentSeriesNum * (seqResource.getTsFileSize() * fileInfo[1] / fileInfo[0]);
      if (seqFileCost > maxCostOfReadingSeqFile) {
        cost -= maxCostOfReadingSeqFile;
        cost += seqFileCost;
        maxCostOfReadingSeqFile = seqFileCost;
      }
      maxSeqChunkNumInDeviceList.add(new Pair<>(fileInfo[3], fileInfo[0]));
    }
    return cost;
  }

  private long calculatingWritingTargetFiles(int unseqIndex, List<Integer> seqIndexes)
      throws IOException {
    long cost = 0;
    for (int seqIndex : seqIndexes) {
      TsFileResource seqResource = resource.getSeqFiles().get(seqIndex);
      TsFileSequenceReader reader = resource.getFileReader(seqResource);
      // add seq file metadata size
      cost += reader.getFileMetadataSize();
      // add max chunk group size of this seq tsfile
      cost +=
          seqResource.getTsFileSize()
              * maxSeqChunkNumInDeviceList.get(0).left
              / maxSeqChunkNumInDeviceList.get(0).right;
    }
    // add max chunk group size of overlapped unseq tsfile
    cost +=
        resource.getUnseqFiles().get(unseqIndex).getTsFileSize()
            * maxUnseqChunkNumInDevice.left
            / maxUnseqChunkNumInDevice.right;
    return cost;
  }

  /**
   * @param reader
   * @return
   * @throws IOException
   */
  private int[] getTotalAndLargestChunkNum(TsFileSequenceReader reader) throws IOException {
    int totalChunkNum = 0;
    int maxChunkNum = 0;
    int maxAlignedSeriesNumInDevice = -1;
    int maxDeviceChunkNum = 0;
    Map<String, List<TimeseriesMetadata>> deviceMetadata = reader.getAllTimeseriesMetadata(true);
    for (Map.Entry<String, List<TimeseriesMetadata>> entry : deviceMetadata.entrySet()) {
      int deviceChunkNum = 0;
      List<TimeseriesMetadata> deviceTimeseriesMetadata = entry.getValue();
      if (deviceTimeseriesMetadata.get(0).getMeasurementId().equals("")) {
        // aligned device
        maxAlignedSeriesNumInDevice =
            Math.max(maxAlignedSeriesNumInDevice, deviceTimeseriesMetadata.size());
      }
      for (TimeseriesMetadata timeseriesMetadata : deviceTimeseriesMetadata) {
        deviceChunkNum += timeseriesMetadata.getChunkMetadataList().size();
        totalChunkNum += timeseriesMetadata.getChunkMetadataList().size();
        maxChunkNum = Math.max(maxChunkNum, timeseriesMetadata.getChunkMetadataList().size());
      }
      maxDeviceChunkNum = Math.max(maxDeviceChunkNum, deviceChunkNum);
    }
    return new int[] {totalChunkNum, maxChunkNum, maxAlignedSeriesNumInDevice, maxDeviceChunkNum};
  }
}
