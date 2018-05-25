package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/18.
 */
public class GlobalSortedSeriesDataSource {
    private Path seriesPath;
    private List<IntervalFileNode> sealedTsFiles;
    private UnsealedTsFile unsealedTsFile;
    private RawSeriesChunk rawSeriesChunk;

    public GlobalSortedSeriesDataSource(Path seriesPath, List<IntervalFileNode> sealedTsFiles,
                                        UnsealedTsFile unsealedTsFile, RawSeriesChunk rawSeriesChunk) {
        this.seriesPath = seriesPath;
        this.sealedTsFiles = sealedTsFiles;
        this.unsealedTsFile = unsealedTsFile;
        this.rawSeriesChunk = rawSeriesChunk;
    }

    public boolean hasUnsealedTsFile() {
        return unsealedTsFile != null;
    }

    public boolean hasRawSeriesChunk() {
        return rawSeriesChunk != null;
    }

    public List<IntervalFileNode> getSealedTsFiles() {
        return sealedTsFiles;
    }

    public void setSealedTsFiles(List<IntervalFileNode> sealedTsFiles) {
        this.sealedTsFiles = sealedTsFiles;
    }

    public UnsealedTsFile getUnsealedTsFile() {
        return unsealedTsFile;
    }

    public void setUnsealedTsFile(UnsealedTsFile unsealedTsFile) {
        this.unsealedTsFile = unsealedTsFile;
    }

    public RawSeriesChunk getRawSeriesChunk() {
        return rawSeriesChunk;
    }

    public void setRawSeriesChunk(RawSeriesChunk rawSeriesChunk) {
        this.rawSeriesChunk = rawSeriesChunk;
    }

    public Path getSeriesPath() {
        return seriesPath;
    }

    public void setSeriesPath(Path seriesPath) {
        this.seriesPath = seriesPath;
    }
}
