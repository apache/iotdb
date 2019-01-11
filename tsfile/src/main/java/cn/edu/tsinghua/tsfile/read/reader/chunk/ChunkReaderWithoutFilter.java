package cn.edu.tsinghua.tsfile.read.reader.chunk;

import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.read.common.Chunk;


public class ChunkReaderWithoutFilter extends ChunkReader {

    public ChunkReaderWithoutFilter(Chunk chunk) {
        super(chunk);
    }

    @Override
    public boolean pageSatisfied(PageHeader pageHeader) {
        return pageHeader.getMax_timestamp() > getMaxTombstoneTime();
    }

}
