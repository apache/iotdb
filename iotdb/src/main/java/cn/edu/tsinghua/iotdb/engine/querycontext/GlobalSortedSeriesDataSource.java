package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.memtable.TimeValuePairSorter;
import cn.edu.tsinghua.tsfile.read.common.Path;

import java.util.List;


public class GlobalSortedSeriesDataSource {
    private Path seriesPath;

    // sealed tsfile
    private List<IntervalFileNode> sealedTsFiles;

    // unsealed tsfile
    private UnsealedTsFile unsealedTsFile;

    // seq mem-table
    private TimeValuePairSorter readableChunk;

    public GlobalSortedSeriesDataSource(Path seriesPath, List<IntervalFileNode> sealedTsFiles,
                                        UnsealedTsFile unsealedTsFile, TimeValuePairSorter readableChunk) {
        this.seriesPath = seriesPath;
        this.sealedTsFiles = sealedTsFiles;
        this.unsealedTsFile = unsealedTsFile;

        this.readableChunk = readableChunk;
    }

    public boolean hasSealedTsFiles() {
        return sealedTsFiles != null && sealedTsFiles.size() > 0;
    }

    public List<IntervalFileNode> getSealedTsFiles() {
        return sealedTsFiles;
    }

    public boolean hasUnsealedTsFile() {
        return unsealedTsFile != null;
    }

    public UnsealedTsFile getUnsealedTsFile() {
        return unsealedTsFile;
    }

    public boolean hasRawSeriesChunk() {
        return readableChunk != null;
    }

    public TimeValuePairSorter getReadableChunk() {
        return readableChunk;
    }


    public void setSealedTsFiles(List<IntervalFileNode> sealedTsFiles) {
        this.sealedTsFiles = sealedTsFiles;
    }

    public void setUnsealedTsFile(UnsealedTsFile unsealedTsFile) {
        this.unsealedTsFile = unsealedTsFile;
    }

    public void setReadableChunk(TimeValuePairSorter readableChunk) {
        this.readableChunk = readableChunk;
    }

    public void setSeriesPath(Path seriesPath) {
        this.seriesPath = seriesPath;
    }

    public Path getSeriesPath() {
        return seriesPath;
    }

}
