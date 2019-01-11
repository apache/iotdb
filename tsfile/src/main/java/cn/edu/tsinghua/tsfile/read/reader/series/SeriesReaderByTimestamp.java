package cn.edu.tsinghua.tsfile.read.reader.series;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.common.Chunk;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.reader.chunk.ChunkReader;
import cn.edu.tsinghua.tsfile.read.reader.chunk.ChunkReaderByTimestamp;

import java.io.IOException;
import java.util.List;

/**
 * <p> Series reader is used to query one series of one tsfile,
 * using this reader to query the value of a series with given timestamps.
 */
public class SeriesReaderByTimestamp {
    protected ChunkLoader chunkLoader;
    protected List<ChunkMetaData> chunkMetaDataList;
    private int currentChunkIndex = 0;

    private ChunkReader chunkReader;
    private long currentTimestamp;
    private BatchData data = null; // current batch data

    public SeriesReaderByTimestamp(ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
        this.chunkLoader = chunkLoader;
        this.chunkMetaDataList = chunkMetaDataList;
        currentTimestamp = Long.MIN_VALUE;
    }

    public TSDataType getDataType() {
        return chunkMetaDataList.get(0).getTsDataType();
    }

    public Object getValueInTimestamp(long timestamp) throws IOException {
        this.currentTimestamp = timestamp;

        // first initialization, only invoked in the first time
        if (chunkReader == null) {
            if (!constructNextSatisfiedChunkReader())
                return null;

            if (chunkReader.hasNextBatch())
                data = chunkReader.nextBatch();
            else
                return null;
        }

        while (data != null) {
            while (data.hasNext()) {
                if (data.currentTime() < timestamp)
                    data.next();
                else
                    break;
            }

            if (data.hasNext()) {
                if (data.currentTime() == timestamp)
                    return data.currentValue();
                return null;
            } else {
                if (chunkReader.hasNextBatch()) { // data does not has next
                    data = chunkReader.nextBatch();
                } else if (!constructNextSatisfiedChunkReader()) {
                    return null;
                }
            }
        }

        return null;
    }

    private boolean constructNextSatisfiedChunkReader() throws IOException {
        while (currentChunkIndex < chunkMetaDataList.size()) {
            ChunkMetaData chunkMetaData = chunkMetaDataList.get(currentChunkIndex++);
            if (chunkSatisfied(chunkMetaData)) {
                initChunkReader(chunkMetaData);
                ((ChunkReaderByTimestamp) chunkReader).setCurrentTimestamp(currentTimestamp);
                return true;
            }
        }
        return false;
    }

    private void initChunkReader(ChunkMetaData chunkMetaData) throws IOException {
        Chunk chunk = chunkLoader.getChunk(chunkMetaData);
        this.chunkReader = new ChunkReaderByTimestamp(chunk);
        this.chunkReader.setMaxTombstoneTime(chunkMetaData.getMaxTombstoneTime());
    }


    private boolean chunkSatisfied(ChunkMetaData chunkMetaData) {
        return chunkMetaData.getEndTime() >= currentTimestamp;
    }

}
