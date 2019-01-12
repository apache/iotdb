package org.apache.iotdb.tsfile.read.reader.chunk;

import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.file.header.PageHeader;


public class ChunkReaderWithoutFilter extends ChunkReader {

    public ChunkReaderWithoutFilter(Chunk chunk) {
        super(chunk);
    }

    @Override
    public boolean pageSatisfied(PageHeader pageHeader) {
        return pageHeader.getMax_timestamp() > getMaxTombstoneTime();
    }

}
