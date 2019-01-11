package cn.edu.tsinghua.tsfile.read.reader.series;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.List;

/**
 * <p> Series reader is used to query one series of one tsfile.
 */
public abstract class FileSeriesReader {

    protected ChunkLoader chunkLoader;
    protected List<ChunkMetaData> chunkMetaDataList;
    protected ChunkReader chunkReader;
    private int chunkToRead;

    private BatchData data;

    public FileSeriesReader(ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
        this.chunkLoader = chunkLoader;
        this.chunkMetaDataList = chunkMetaDataList;
        this.chunkToRead = 0;
    }

    public boolean hasNextBatch() {

        // current chunk has data
        if (chunkReader != null && chunkReader.hasNextBatch()) {
            return true;

            // has additional chunk to read
        } else {
            return chunkToRead < chunkMetaDataList.size();
        }

    }

    public BatchData nextBatch() throws IOException {

        // current chunk has additional batch
        if (chunkReader != null && chunkReader.hasNextBatch()) {
            data = chunkReader.nextBatch();
            return data;
        }

        // current chunk does not have additional batch, init new chunk reader
        while (chunkToRead < chunkMetaDataList.size()) {

            ChunkMetaData chunkMetaData = chunkMetaDataList.get(chunkToRead++);
            if (chunkSatisfied(chunkMetaData)) {
                // chunk metadata satisfy the condition
                initChunkReader(chunkMetaData);

                if (chunkReader.hasNextBatch()) {
                    data = chunkReader.nextBatch();
                    return data;
                }
            }
        }

        if (data == null)
            return new BatchData();
        return data;
    }

    public BatchData currentBatch() {
        return data;
    }

    protected abstract void initChunkReader(ChunkMetaData chunkMetaData) throws IOException;

    protected abstract boolean chunkSatisfied(ChunkMetaData chunkMetaData);

    public void close() throws IOException {
        chunkLoader.close();
    }

}
