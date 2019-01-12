package org.apache.iotdb.db.engine.querycontext;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;

import java.util.List;


public class OverflowUpdateDeleteFile {
    private String filePath;
    private List<ChunkMetaData> timeSeriesChunkMetaDataList;

    public OverflowUpdateDeleteFile(String filePath, List<ChunkMetaData> timeSeriesChunkMetaDataList) {
        this.filePath = filePath;
        this.timeSeriesChunkMetaDataList = timeSeriesChunkMetaDataList;
    }

    public String getFilePath() {
        return filePath;
    }

    public List<ChunkMetaData> getTimeSeriesChunkMetaDataList() {
        return timeSeriesChunkMetaDataList;
    }
}
