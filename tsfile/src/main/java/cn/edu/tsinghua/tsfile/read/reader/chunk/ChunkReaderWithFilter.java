package cn.edu.tsinghua.tsfile.read.reader.chunk;

import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.read.filter.DigestForFilter;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.read.common.Chunk;


public class ChunkReaderWithFilter extends ChunkReader {

    private Filter filter;

    public ChunkReaderWithFilter(Chunk chunk, Filter filter) {
        super(chunk, filter);
        this.filter = filter;
    }

    @Override
    public boolean pageSatisfied(PageHeader pageHeader) {
        if (pageHeader.getMax_timestamp() < getMaxTombstoneTime())
            return false;
        DigestForFilter digest = new DigestForFilter(
                pageHeader.getMin_timestamp(),
                pageHeader.getMax_timestamp(),
                pageHeader.getStatistics().getMinBytebuffer(),
                pageHeader.getStatistics().getMaxBytebuffer(),
                chunkHeader.getDataType());
        return filter.satisfy(digest);
    }

}
