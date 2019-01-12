package org.apache.iotdb.db.query.reader.unsequence;

import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

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
