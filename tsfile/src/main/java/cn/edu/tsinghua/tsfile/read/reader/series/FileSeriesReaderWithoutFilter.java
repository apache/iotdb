package cn.edu.tsinghua.tsfile.read.reader.series;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.read.common.Chunk;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.reader.chunk.ChunkReaderWithoutFilter;

import java.io.IOException;
import java.util.List;

/**
 * <p> Series reader is used to query one series of one tsfile,
 * this reader has no filter.
 */
public class FileSeriesReaderWithoutFilter extends FileSeriesReader {

    public FileSeriesReaderWithoutFilter(ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
        super(chunkLoader, chunkMetaDataList);
    }

    @Override
    protected void initChunkReader(ChunkMetaData chunkMetaData) throws IOException {
        Chunk chunk = chunkLoader.getChunk(chunkMetaData);
        this.chunkReader = new ChunkReaderWithoutFilter(chunk);
        this.chunkReader.setMaxTombstoneTime(chunkMetaData.getMaxTombstoneTime());
    }

    @Override
    protected boolean chunkSatisfied(ChunkMetaData chunkMetaData) {
        return true;
    }

}
