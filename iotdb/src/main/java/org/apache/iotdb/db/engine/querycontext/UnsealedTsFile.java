package org.apache.iotdb.db.engine.querycontext;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;

import java.util.List;


public class UnsealedTsFile {
    private String filePath;
    private List<ChunkMetaData> timeSeriesChunkMetaDatas;

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public List<ChunkMetaData> getChunkMetaDataList() {
        return timeSeriesChunkMetaDatas;
    }

    public void setTimeSeriesChunkMetaDatas(List<ChunkMetaData> timeSeriesChunkMetaDatas) {
        this.timeSeriesChunkMetaDatas = timeSeriesChunkMetaDatas;
    }
}
