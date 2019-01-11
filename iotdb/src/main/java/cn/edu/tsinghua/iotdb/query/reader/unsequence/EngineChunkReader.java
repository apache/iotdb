package cn.edu.tsinghua.iotdb.query.reader.unsequence;

import cn.edu.tsinghua.iotdb.query.reader.IReader;
import cn.edu.tsinghua.iotdb.utils.TimeValuePairUtils;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.read.UnClosedTsFileReader;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;

public class EngineChunkReader implements IReader {

    private ChunkReader chunkReader;
    private BatchData data;

    /**
     * Each EngineChunkReader has a corresponding UnClosedTsFileReader, when EngineChunkReader is closed,
     * UnClosedTsFileReader also should be closed in meanwhile.
     */
    private TsFileSequenceReader unClosedTsFileReader;

    public EngineChunkReader(ChunkReader chunkReader, TsFileSequenceReader unClosedTsFileReader) {
        this.chunkReader = chunkReader;
        this.unClosedTsFileReader = unClosedTsFileReader;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (data == null || !data.hasNext()) {
            if (chunkReader.hasNextBatch()) {
                data = chunkReader.nextBatch();
            } else {
                return false;
            }
        }

        return data.hasNext();
    }

    @Override
    public TimeValuePair next() {
        TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(data);
        data.next();
        return timeValuePair;
    }

    @Override
    public void skipCurrentTimeValuePair() {
        next();
    }

    @Override
    public void close() throws IOException {
        this.chunkReader.close();
        this.unClosedTsFileReader.close();
    }

    // TODO
    @Override public boolean hasNextBatch() {
        return false;
    }

    // TODO
    @Override public BatchData nextBatch() {
        return null;
    }

    // TODO
    @Override public BatchData currentBatch() {
        return null;
    }
}
