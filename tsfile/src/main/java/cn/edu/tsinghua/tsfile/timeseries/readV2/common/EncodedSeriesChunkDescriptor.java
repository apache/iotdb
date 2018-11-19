package cn.edu.tsinghua.tsfile.timeseries.readV2.common;

import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class EncodedSeriesChunkDescriptor implements SeriesChunkDescriptor {
    public static final char UUID_SPLITER = '.';
    private String filePath;
    private long offsetInFile;
    private long lengthOfBytes;
    private CompressionTypeName compressionTypeName;
    private TSDataType dataType;
    private TsDigest valueDigest;
    private long minTimestamp;
    private long maxTimestamp;
    private long countOfPoints;
    private List<String> enumValueList;
    private long maxTombstoneTime;

    public EncodedSeriesChunkDescriptor(long offsetInFile, long lengthOfBytes, CompressionTypeName compressionTypeName,
                                        TSDataType dataType, TsDigest valueDigest, long minTimestamp, long maxTimestamp, long countOfPoints) {
        this.offsetInFile = offsetInFile;
        this.lengthOfBytes = lengthOfBytes;
        this.compressionTypeName = compressionTypeName;
        this.dataType = dataType;
        this.valueDigest = valueDigest;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.countOfPoints = countOfPoints;
    }

    public EncodedSeriesChunkDescriptor(long offsetInFile, long lengthOfBytes, CompressionTypeName compressionTypeName,
                                        TSDataType dataType, TsDigest valueDigest, long minTimestamp, long maxTimestamp, long countOfPoints, List<String> enumValueList) {
        this(offsetInFile, lengthOfBytes, compressionTypeName, dataType, valueDigest, minTimestamp, maxTimestamp, countOfPoints);
        this.enumValueList = enumValueList;
    }

    public EncodedSeriesChunkDescriptor(String filePath, long offsetInFile, long lengthOfBytes, CompressionTypeName compressionTypeName,
                                        TSDataType dataType, TsDigest valueDigest, long minTimestamp, long maxTimestamp, long countOfPoints) {
        this(offsetInFile, lengthOfBytes, compressionTypeName, dataType, valueDigest, minTimestamp, maxTimestamp, countOfPoints);
        this.filePath = filePath;
    }

    public EncodedSeriesChunkDescriptor(String filePath, long offsetInFile, long lengthOfBytes, CompressionTypeName compressionTypeName,
                                        TSDataType dataType, TsDigest valueDigest, long minTimestamp, long maxTimestamp, long countOfPoints, List<String> enumValueList) {
        this(filePath, offsetInFile, lengthOfBytes, compressionTypeName, dataType, valueDigest, minTimestamp, maxTimestamp, countOfPoints);
        this.enumValueList = enumValueList;
    }

    public boolean equals(Object object) {
        if (!(object instanceof EncodedSeriesChunkDescriptor)) {
            return false;
        }
        return getUUID().equals(((EncodedSeriesChunkDescriptor) object).getUUID());
    }

    public int hashCode() {
        return getUUID().hashCode();
    }

    private String getUUID() {
        return new StringBuilder().append(filePath).append(UUID_SPLITER).append(offsetInFile)
                .append(UUID_SPLITER).append(lengthOfBytes).toString();
    }

    public String getFilePath() {
        return filePath;
    }

    public long getOffsetInFile() {
        return offsetInFile;
    }

    public long getLengthOfBytes() {
        return lengthOfBytes;
    }

    public CompressionTypeName getCompressionTypeName() {
        return compressionTypeName;
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public TsDigest getValueDigest() {
        return valueDigest;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getCountOfPoints() {
        return countOfPoints;
    }

    public List<String> getEnumValueList() {
        return enumValueList;
    }

    @Override
    public String toString() {
        return "EncodedSeriesChunkDescriptor{" +
                "filePath='" + filePath + '\'' +
                ", offsetInFile=" + offsetInFile +
                ", lengthOfBytes=" + lengthOfBytes +
                ", compressionTypeName=" + compressionTypeName +
                ", dataType=" + dataType +
                ", valueDigest=" + valueDigest +
                ", minTimestamp=" + minTimestamp +
                ", maxTimestamp=" + maxTimestamp +
                ", countOfPoints=" + countOfPoints +
                ", enumValueList=" + enumValueList +
                '}';
    }

    public long getMaxTombstoneTime() {
        return maxTombstoneTime;
    }

    public void setMaxTombstoneTime(long maxTombstoneTime) {
        this.maxTombstoneTime = maxTombstoneTime;
    }
}
