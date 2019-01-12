package org.apache.iotdb.db.query.reader.sequence;

import org.apache.iotdb.db.engine.querycontext.UnsealedTsFile;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;
import org.apache.iotdb.db.engine.querycontext.UnsealedTsFile;
import org.apache.iotdb.db.query.control.FileReaderManager;

import java.io.IOException;

public class UnSealedTsFileReader implements IReader {

    protected Path seriesPath;
    private FileSeriesReader unSealedTsFileReader;
    private BatchData data;

    public UnSealedTsFileReader(UnsealedTsFile unsealedTsFile, Filter filter) throws IOException {

        TsFileSequenceReader unClosedTsFileReader = FileReaderManager.getInstance().get(unsealedTsFile.getFilePath(), true);
        ChunkLoader chunkLoader = new ChunkLoaderImpl(unClosedTsFileReader);

        if (filter == null) {
            unSealedTsFileReader = new FileSeriesReaderWithoutFilter(chunkLoader, unsealedTsFile.getChunkMetaDataList());
        } else {
            unSealedTsFileReader = new FileSeriesReaderWithFilter(chunkLoader, unsealedTsFile.getChunkMetaDataList(), filter);
        }

    }

    @Override
    public boolean hasNext() throws IOException {
        if (data == null || !data.hasNext()) {
            if (!unSealedTsFileReader.hasNextBatch()) {
                return false;
            }
            data = unSealedTsFileReader.nextBatch();
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
        data.next();
    }

    @Override
    public void close() throws IOException {
        if (unSealedTsFileReader != null) {
            unSealedTsFileReader.close();
        }
    }

    @Override
    public boolean hasNextBatch() {
        return false;
    }

    @Override
    public BatchData nextBatch() {
        return null;
    }

    @Override
    public BatchData currentBatch() {
        return null;
    }
}
