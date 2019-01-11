package cn.edu.tsinghua.iotdb.query.reader.sequence;

import cn.edu.tsinghua.iotdb.engine.querycontext.UnsealedTsFile;
import cn.edu.tsinghua.iotdb.query.control.FileReaderManager;
import cn.edu.tsinghua.iotdb.query.reader.IReader;
import cn.edu.tsinghua.iotdb.utils.TimeValuePairUtils;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReader;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;

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
