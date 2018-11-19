package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;

import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/18.
 */
public class UnsealedTsFile {
    private String filePath;
    private List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDatas;

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public List<TimeSeriesChunkMetaData> getTimeSeriesChunkMetaDatas() {
        return timeSeriesChunkMetaDatas;
    }

    public void setTimeSeriesChunkMetaDatas(List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDatas) {
        this.timeSeriesChunkMetaDatas = timeSeriesChunkMetaDatas;
    }
}
