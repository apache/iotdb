package org.apache.iotdb.db.engine.querycontext;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;

import java.util.List;


public class OverflowInsertFile {

    private String filePath;

    // seriesChunkMetadata of selected series
    private List<ChunkMetaData> timeSeriesChunkMetaData;

    public OverflowInsertFile() {
    }

    public OverflowInsertFile(String path, List<ChunkMetaData> timeSeriesChunkMetaData) {
        this.filePath = path;
        this.timeSeriesChunkMetaData = timeSeriesChunkMetaData;
    }

    public String getFilePath() {
        return filePath;
    }

    public List<ChunkMetaData> getChunkMetaDataList() {
        return timeSeriesChunkMetaData;
    }

    public void setTimeSeriesChunkMetaData(List<ChunkMetaData> timeSeriesChunkMetaData) {
        this.timeSeriesChunkMetaData = timeSeriesChunkMetaData;
    }
}
