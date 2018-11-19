package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSChunkType;

/**
 * store required members in TimeSeriesChunkMetaData
 */
public class TimeSeriesChunkProperties {
    private String measurementUID;

    /**
     * Type of this time series
     */
    @Deprecated
    private TSChunkType tsChunkType;

    /**
     * Byte offset in file_path to the RowGroupMetaData
     */
    private long fileOffset;
    private CompressionTypeName compression;

    public TimeSeriesChunkProperties() {
    }

    public TimeSeriesChunkProperties(String measurementUID, TSChunkType tsChunkType, long fileOffset,
                                     CompressionTypeName compression) {
        this.measurementUID = measurementUID;
        this.tsChunkType = tsChunkType;
        this.fileOffset = fileOffset;
        this.compression = compression;
    }

    public TSChunkType getTsChunkType() {
        return tsChunkType;
    }

    public long getFileOffset() {
        return fileOffset;
    }

    public CompressionTypeName getCompression() {
        return compression;
    }

    public String getMeasurementUID() {
        return measurementUID;
    }

    @Override
    public String toString() {
        return String.format("measurementUID %s, TSChunkType %s, fileOffset %d, CompressionTypeName %s",
                measurementUID, tsChunkType, fileOffset, compression);
    }
}
