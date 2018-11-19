package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/18.
 */
public class OverflowSeriesDataSource {
    private Path seriesPath;
    private TSDataType dataType;
    private List<OverflowInsertFile> overflowInsertFileList;
    private RawSeriesChunk rawSeriesChunk;
    private UpdateDeleteInfoOfOneSeries updateDeleteInfoOfOneSeries;

    public OverflowSeriesDataSource(Path seriesPath) {
        this.seriesPath = seriesPath;
    }

    public OverflowSeriesDataSource(Path seriesPath, TSDataType dataType, List<OverflowInsertFile> overflowInsertFileList, RawSeriesChunk rawSeriesChunk, UpdateDeleteInfoOfOneSeries updateDeleteInfoOfOneSeries) {
        this.seriesPath = seriesPath;
        this.dataType = dataType;
        this.overflowInsertFileList = overflowInsertFileList;
        this.rawSeriesChunk = rawSeriesChunk;
        this.updateDeleteInfoOfOneSeries = updateDeleteInfoOfOneSeries;
    }

    public List<OverflowInsertFile> getOverflowInsertFileList() {
        return overflowInsertFileList;
    }

    public void setOverflowInsertFileList(List<OverflowInsertFile> overflowInsertFileList) {
        this.overflowInsertFileList = overflowInsertFileList;
    }

    public UpdateDeleteInfoOfOneSeries getUpdateDeleteInfoOfOneSeries() {
        return updateDeleteInfoOfOneSeries;
    }

    public void setUpdateDeleteInfoOfOneSeries(UpdateDeleteInfoOfOneSeries updateDeleteInfoOfOneSeries) {
        this.updateDeleteInfoOfOneSeries = updateDeleteInfoOfOneSeries;
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

    public TSDataType getDataType() {
        return dataType;
    }

    public boolean hasRawSeriesChunk() {
        return rawSeriesChunk != null && !rawSeriesChunk.isEmpty();
    }
}
