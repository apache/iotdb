package cn.edu.tsinghua.tsfile.read;

import cn.edu.tsinghua.tsfile.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.read.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.query.executor.TsFileExecutor;

import java.io.IOException;


public class ReadOnlyTsFile {

    private TsFileSequenceReader fileReader;
    private MetadataQuerier metadataQuerier;
    private ChunkLoader chunkLoader;
    private TsFileExecutor tsFileExecutor;

    public ReadOnlyTsFile(TsFileSequenceReader fileReader) throws IOException {
        this.fileReader = fileReader;
        this.metadataQuerier = new MetadataQuerierByFileImpl(fileReader);
        this.chunkLoader = new ChunkLoaderImpl(fileReader);
        tsFileExecutor = new TsFileExecutor(metadataQuerier, chunkLoader);
    }

    public QueryDataSet query(QueryExpression queryExpression) throws IOException {
        return tsFileExecutor.execute(queryExpression);
    }

    public void close() throws IOException {
        fileReader.close();
    }
}
