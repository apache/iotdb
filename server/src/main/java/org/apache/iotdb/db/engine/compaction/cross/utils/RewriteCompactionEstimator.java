package org.apache.iotdb.db.engine.compaction.cross.utils;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.RewriteCrossSpaceCompactionResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import java.io.IOException;
import java.util.List;

public class RewriteCompactionEstimator implements CompactionEstimator {

    private final RewriteCrossSpaceCompactionResource resource;

    private long timeLimit;

    // the number of timeseries being compacted at the same time
    private final int concurrentSeriesNum = IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

    public RewriteCompactionEstimator( RewriteCrossSpaceCompactionResource resource) {
        timeLimit =
                IoTDBDescriptor.getInstance().getConfig().getCrossCompactionFileSelectionTimeBudget();
        if (timeLimit < 0) {
            timeLimit = Long.MAX_VALUE;
        }
        this.resource=resource;
    }

    @Override
    public long estimateMemory(int unseqIndex, List<Integer> seqIndexes) throws IOException {
        return 0;
    }

    private long calculateReadingUnseqFile(int unseqIndex) throws IOException {
        TsFileResource unseqResource=resource.getUnseqFiles().get(unseqIndex);
        TsFileSequenceReader reader=resource.getFileReader(unseqResource);
        long size=0;
        unseqResource.getDevices()e
    }

    private

}
