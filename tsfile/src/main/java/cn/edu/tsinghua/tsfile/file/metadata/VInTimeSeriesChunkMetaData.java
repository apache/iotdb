package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.DataType;
import cn.edu.tsinghua.tsfile.format.ValueInTimeSeriesChunkMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * For more information, see ValueInTimeSeriesChunkMetaData
 * in cn.edu.thu.tsfile.format package
 */
public class VInTimeSeriesChunkMetaData implements IConverter<ValueInTimeSeriesChunkMetaData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(VInTimeSeriesChunkMetaData.class);

    private TSDataType dataType;

    private TsDigest digest;
    private int maxError;

    /**
     * If values for data consist of enum values, metadata will store all possible values in time
     * series
     */
    private List<String> enumValues;

    public VInTimeSeriesChunkMetaData() {
    }

    public VInTimeSeriesChunkMetaData(TSDataType dataType) {
        this.dataType = dataType;
    }

    @Override
    public ValueInTimeSeriesChunkMetaData convertToThrift() {
        try {
            ValueInTimeSeriesChunkMetaData vTimeSeriesChunkMetaDataInThrift = new ValueInTimeSeriesChunkMetaData(
                    dataType == null ? null : DataType.valueOf(dataType.toString()));
            vTimeSeriesChunkMetaDataInThrift.setMax_error(maxError);
            vTimeSeriesChunkMetaDataInThrift.setEnum_values(enumValues);
            vTimeSeriesChunkMetaDataInThrift.setDigest(digest == null ? null : digest.convertToThrift());
            return vTimeSeriesChunkMetaDataInThrift;
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error(
                        "tsfile-file VInTimeSeriesChunkMetaData: failed to convert ValueInTimeSeriesChunkMetaData from TSFile to thrift, content is {}",
                        this, e);
            throw e;
        }
    }

    @Override
    public void convertToTSF(ValueInTimeSeriesChunkMetaData vTimeSeriesChunkMetaDataInThrift) {
        try {
            this.dataType = vTimeSeriesChunkMetaDataInThrift.getData_type() == null ? null : TSDataType.valueOf(vTimeSeriesChunkMetaDataInThrift.getData_type().toString());
            this.maxError = vTimeSeriesChunkMetaDataInThrift.getMax_error();
            this.enumValues = vTimeSeriesChunkMetaDataInThrift.getEnum_values();
            if (vTimeSeriesChunkMetaDataInThrift.getDigest() == null) {
                this.digest = null;
            } else {
                this.digest = new TsDigest();
                this.digest.convertToTSF(vTimeSeriesChunkMetaDataInThrift.getDigest());
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error(
                        "tsfile-file VInTimeSeriesChunkMetaData: failed to convert ValueInTimeSeriesChunkMetaData from thrift to TSFile, content is {}",
                        vTimeSeriesChunkMetaDataInThrift, e);
            throw e;
        }
    }

    @Override
    public String toString() {
        return String.format("VInTimeSeriesChunkMetaData{ TSDataType %s, TSDigest %s, maxError %d, enumValues %s }", dataType, digest,
                maxError, enumValues);
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public void setDataType(TSDataType dataType) {
        this.dataType = dataType;
    }

    public TsDigest getDigest() {
        return digest;
    }

    public void setDigest(TsDigest digest) {
        this.digest = digest;
    }

    public int getMaxError() {
        return maxError;
    }

    public void setMaxError(int maxError) {
        this.maxError = maxError;
    }

    public List<String> getEnumValues() {
        return enumValues;
    }

    public void setEnumValues(List<String> enumValues) {
        this.enumValues = enumValues;
    }
}
