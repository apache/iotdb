package org.apache.iotdb.tsfile.read.reader.chunk;

import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.file.header.PageHeader;


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
