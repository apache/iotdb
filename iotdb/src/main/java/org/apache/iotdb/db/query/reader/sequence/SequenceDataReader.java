package org.apache.iotdb.db.query.reader.sequence;

import org.apache.iotdb.db.engine.querycontext.GlobalSortedSeriesDataSource;
import org.apache.iotdb.db.query.reader.mem.MemChunkReaderWithFilter;
import org.apache.iotdb.db.query.reader.mem.MemChunkReaderWithoutFilter;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.db.engine.querycontext.GlobalSortedSeriesDataSource;
import org.apache.iotdb.db.query.reader.mem.MemChunkReaderWithFilter;
import org.apache.iotdb.db.query.reader.mem.MemChunkReaderWithoutFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <p> A reader for sequentially inserts data，including a list of sealedTsFile, unSealedTsFile
 * and data in MemTable.
 */
public class SequenceDataReader implements IReader {

    private List<IReader> seriesReaders;
    private boolean curReaderInitialized;
    private int nextSeriesReaderIndex;
    private IReader currentSeriesReader;

    public SequenceDataReader(GlobalSortedSeriesDataSource sources, Filter filter) throws IOException {
        seriesReaders = new ArrayList<>();

        curReaderInitialized = false;
        nextSeriesReaderIndex = 0;

        // add reader for sealed TsFiles
        if (sources.hasSealedTsFiles()) {
            seriesReaders.add(new SealedTsFilesReader(sources.getSeriesPath(), sources.getSealedTsFiles(), filter));
        }

        // add reader for unSealed TsFile
        if (sources.hasUnsealedTsFile()) {
            seriesReaders.add(new UnSealedTsFileReader(sources.getUnsealedTsFile(), filter));
        }

        // add data in memTable
        if (sources.hasRawSeriesChunk()) {
            if (filter == null) {
                seriesReaders.add(new MemChunkReaderWithoutFilter(sources.getReadableChunk()));
            } else {
                seriesReaders.add(new MemChunkReaderWithFilter(sources.getReadableChunk(), filter));
            }
        }

    }

    @Override
    public boolean hasNext() throws IOException {
        if (curReaderInitialized && currentSeriesReader.hasNext()) {
            return true;
        } else {
            curReaderInitialized = false;
        }

        while (nextSeriesReaderIndex < seriesReaders.size()) {
            currentSeriesReader = seriesReaders.get(nextSeriesReaderIndex++);
            if (currentSeriesReader.hasNext()) {
                curReaderInitialized = true;
                return true;
            } else {
                curReaderInitialized = false;
            }
        }
        return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
        return currentSeriesReader.next();
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    @Override
    public void close() throws IOException {
        for (IReader seriesReader : seriesReaders) {
            seriesReader.close();
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
