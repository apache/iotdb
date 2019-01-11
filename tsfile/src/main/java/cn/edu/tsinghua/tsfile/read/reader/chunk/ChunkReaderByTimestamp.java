package cn.edu.tsinghua.tsfile.read.reader.chunk;

import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.read.common.Chunk;


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
