package cn.edu.tsinghua.iotdb.queryV2.engine.reader.component;

import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunkDescriptor;

/**
 * Created by zhangjinrui on 2018/1/18.
 */
public class RawSeriesChunkDescriptor implements SeriesChunkDescriptor {

    private TSDataType dataType;
    private TsDigest valueDigest;
    private long minTimestamp;
    private long maxTimestamp;
    private int count;

    public RawSeriesChunkDescriptor(TSDataType dataType, TsDigest valueDigest, long minTimestamp, long maxTimestamp, int count) {
        this.dataType = dataType;
        this.valueDigest = valueDigest;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.count = count;
    }

    @Override
    public TSDataType getDataType() {
        return dataType;
    }

    @Override
    public TsDigest getValueDigest() {
        return valueDigest;
    }

    @Override
    public long getMinTimestamp() {
        return minTimestamp;
    }

    @Override
    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    @Override
    public long getCountOfPoints() {
        return count;
    }

    @Override
    public CompressionTypeName getCompressionTypeName() {
        throw new UnsupportedOperationException("RawSeriesChunkDescriptor has no compressionTypeName");
    }
}
