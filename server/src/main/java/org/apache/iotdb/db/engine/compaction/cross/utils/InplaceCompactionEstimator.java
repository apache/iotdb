package org.apache.iotdb.db.engine.compaction.cross.utils;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.RewriteCrossSpaceCompactionResource;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.selector.RewriteCompactionFileSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.MergeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InplaceCompactionEstimator implements CompactionEstimator {
    private static final Logger logger = LoggerFactory.getLogger(RewriteCompactionFileSelector.class);
    private static final String LOG_FILE_COST = "Memory cost of file {} is {}";

    private final boolean tightEstimate;
    private long tempMaxSeqFileCost;
    private long timeLimit;

    // the number of timeseries being compacted at the same time
    private final int concurrentSeriesNum = IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

    /** Total metadata size of each file. */
    private final Map<TsFileResource, Long> fileMetaSizeMap = new HashMap<>();

    /** Maximum memory cost of querying a timeseries in each file. */
    private final Map<TsFileResource, Long> maxSeriesQueryCostMap = new HashMap<>();

    private final RewriteCrossSpaceCompactionResource resource;

    public InplaceCompactionEstimator(boolean tightEstimate, RewriteCrossSpaceCompactionResource resource) {
        this.tightEstimate = tightEstimate;
        this.tempMaxSeqFileCost = 0;
        timeLimit =
                IoTDBDescriptor.getInstance().getConfig().getCrossCompactionFileSelectionTimeBudget();
        if (timeLimit < 0) {
            timeLimit = Long.MAX_VALUE;
        }
        this.resource=resource;
    }

    @Override
    public long estimateMemory(int unseqIndex, List<Integer> seqIndexes) throws IOException {
        if(tightEstimate){
            return calculateTightMemoryCost(unseqIndex,seqIndexes,System.currentTimeMillis(),timeLimit);
        }else{
            return calculateLooseMemoryCost(unseqIndex,seqIndexes,System.currentTimeMillis(),timeLimit);
        }
    }

    private long calculateMemoryCost(
            int unseqIndex,
            List<Integer> seqIndexes,
            IFileQueryMemMeasurement unseqMeasurement,
            IFileQueryMemMeasurement seqMeasurement,
            long startTime,
            long timeLimit)
            throws IOException {
        long cost = 0;
        Long fileCost = unseqMeasurement.measure(resource.getUnseqFiles().get(unseqIndex));
        cost += fileCost;

        for (int seqIndex : seqIndexes) {
            TsFileResource seqFile=resource.getSeqFiles().get(seqIndex);
            fileCost = seqMeasurement.measure(seqFile);
            if (fileCost > tempMaxSeqFileCost) {
                // only one file will be read at the same time, so only the largest one is recorded here
                cost -= tempMaxSeqFileCost;
                cost += fileCost;
                tempMaxSeqFileCost = fileCost;
            }
            // but writing data into a new file may generate the same amount of metadata in memory
            cost += calculateMetadataSize(seqFile);
            long timeConsumption = System.currentTimeMillis() - startTime;
            if (timeConsumption > timeLimit) {
                return Long.MAX_VALUE;
            }
        }
        return cost;
    }

    private long calculateLooseMemoryCost(
            int unseqIndex,
            List<Integer> seqIndexes,
            long startTime,
            long timeLimit)
            throws IOException {
        return calculateMemoryCost(
                unseqIndex,
                seqIndexes,
                TsFileResource::getTsFileSize,
                this::calculateMetadataSize,
                startTime,
                timeLimit);
    }

    private long calculateTightMemoryCost(
            int unseqIndex,
            List<Integer> seqIndexes,
            long startTime,
            long timeLimit)
            throws IOException {
        return calculateMemoryCost(
                unseqIndex,
                seqIndexes,
                this::calculateTightUnseqMemoryCost,
                this::calculateTightSeqMemoryCost,
                startTime,
                timeLimit);
    }

    private long calculateMetadataSize(TsFileResource seqFile) throws IOException {
        Long cost = fileMetaSizeMap.get(seqFile);
        if (cost == null) {
            cost = MergeUtils.getFileMetaSize(seqFile, resource.getFileReader(seqFile));
            fileMetaSizeMap.put(seqFile, cost);
            logger.debug(LOG_FILE_COST, seqFile, cost);
        }
        return cost;
    }

    private long calculateTightFileMemoryCost(
            TsFileResource seqFile, IFileQueryMemMeasurement measurement) throws IOException {
        Long cost = maxSeriesQueryCostMap.get(seqFile);
        if (cost == null) {
            long[] chunkNums =
                    MergeUtils.findTotalAndLargestSeriesChunkNum(seqFile, resource.getFileReader(seqFile));
            long totalChunkNum = chunkNums[0];
            long maxChunkNum = chunkNums[1];
            cost = measurement.measure(seqFile) * maxChunkNum / totalChunkNum;
            maxSeriesQueryCostMap.put(seqFile, cost);
            logger.debug(LOG_FILE_COST, seqFile, cost);
        }
        return cost;
    }

    // this method traverses all ChunkMetadata to find out which series has the most chunks and uses
    // its proportion to all series to get a maximum estimation
    private long calculateTightSeqMemoryCost(TsFileResource seqFile) throws IOException {
        long singleSeriesCost = calculateTightFileMemoryCost(seqFile, this::calculateMetadataSize);
        long multiSeriesCost = concurrentSeriesNum * singleSeriesCost;
        long maxCost = calculateMetadataSize(seqFile);
        return Math.min(multiSeriesCost, maxCost);
    }

    // this method traverses all ChunkMetadata to find out which series has the most chunks and uses
    // its proportion among all series to get a maximum estimation
    private long calculateTightUnseqMemoryCost(TsFileResource unseqFile) throws IOException {
        long singleSeriesCost = calculateTightFileMemoryCost(unseqFile, TsFileResource::getTsFileSize);
        long multiSeriesCost = concurrentSeriesNum * singleSeriesCost;
        long maxCost = unseqFile.getTsFileSize();
        return Math.min(multiSeriesCost, maxCost);
    }
}
