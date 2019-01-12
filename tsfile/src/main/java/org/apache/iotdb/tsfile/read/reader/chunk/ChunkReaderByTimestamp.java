package org.apache.iotdb.tsfile.read.reader.chunk;

import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.file.header.PageHeader;


public class ChunkReaderByTimestamp extends ChunkReader {

    private long currentTimestamp;

    public ChunkReaderByTimestamp(Chunk chunk) {
        super(chunk);
    }

    @Override
    public boolean pageSatisfied(PageHeader pageHeader) {
        long maxTimestamp = pageHeader.getMax_timestamp();
        // if maxTimestamp > currentTimestamp, this page should NOT be skipped
        return maxTimestamp >= currentTimestamp && maxTimestamp >= getMaxTombstoneTime();
    }

    public void setCurrentTimestamp(long currentTimestamp) {
        this.currentTimestamp = currentTimestamp;
    }

}
