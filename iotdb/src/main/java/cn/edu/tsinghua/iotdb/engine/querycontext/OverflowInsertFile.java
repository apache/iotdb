package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;

import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/18.
 */
public class OverflowInsertFile {
    private String path;  //Full path of current OverflowInsertFile
    private List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDatas; //seriesChunkMetadata of selected series

    public OverflowInsertFile() {

    }

    public OverflowInsertFile(String path, List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDatas) {
        this.path = path;
        this.timeSeriesChunkMetaDatas = timeSeriesChunkMetaDatas;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public List<TimeSeriesChunkMetaData> getTimeSeriesChunkMetaDatas() {
        return timeSeriesChunkMetaDatas;
    }

    public void setTimeSeriesChunkMetaDatas(List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDatas) {
        this.timeSeriesChunkMetaDatas = timeSeriesChunkMetaDatas;
    }
}
