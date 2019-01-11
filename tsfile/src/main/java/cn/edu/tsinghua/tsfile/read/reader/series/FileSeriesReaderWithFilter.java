package cn.edu.tsinghua.tsfile.read.reader.series;

import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.read.filter.DigestForFilter;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.read.common.Chunk;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.reader.chunk.ChunkReaderWithFilter;

import java.io.IOException;
import java.util.List;

/**
 * <p> Series reader is used to query one series of one tsfile,
 * this reader has a filter which has the same series as the querying series.
 */
public class FileSeriesReaderWithFilter extends FileSeriesReader {

    private Filter filter;

    public FileSeriesReaderWithFilter(ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList, Filter filter) {
        super(chunkLoader, chunkMetaDataList);
        this.filter = filter;
    }

    @Override
    protected void initChunkReader(ChunkMetaData chunkMetaData) throws IOException {
        Chunk chunk = chunkLoader.getChunk(chunkMetaData);
        this.chunkReader = new ChunkReaderWithFilter(chunk, filter);
        this.chunkReader.setMaxTombstoneTime(chunkMetaData.getMaxTombstoneTime());
    }

    @Override
    protected boolean chunkSatisfied(ChunkMetaData chunkMetaData) {
        DigestForFilter digest = new DigestForFilter(
                chunkMetaData.getStartTime(),
                chunkMetaData.getEndTime(),
                chunkMetaData.getDigest().getStatistics().get(StatisticConstant.MIN_VALUE),
                chunkMetaData.getDigest().getStatistics().get(StatisticConstant.MAX_VALUE),
                chunkMetaData.getTsDataType());
        return filter.satisfy(digest);
    }

}
