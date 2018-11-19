package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSFreqType;
import cn.edu.tsinghua.tsfile.format.DataType;
import cn.edu.tsinghua.tsfile.format.FreqType;
import cn.edu.tsinghua.tsfile.format.TimeInTimeSeriesChunkMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * For more information, see TimeInTimeSeriesChunkMetaData
 * in cn.edu.thu.tsfile.format package
 */
public class TInTimeSeriesChunkMetaData implements IConverter<TimeInTimeSeriesChunkMetaData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TInTimeSeriesChunkMetaData.class);

    private TSDataType dataType;
    private long startTime;
    private long endTime;

    private TSFreqType freqType;
    private List<Integer> frequencies;

    /**
     * If values for data consist of enum values, metadata will store all possible values in time
     * series
     */
    private List<String> enumValues;

    public TInTimeSeriesChunkMetaData() {
    }

    public TInTimeSeriesChunkMetaData(TSDataType dataType, long startTime, long endTime) {
        this.dataType = dataType;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public TimeInTimeSeriesChunkMetaData convertToThrift() {
        try {
            TimeInTimeSeriesChunkMetaData tTimeSeriesChunkMetaDataInThrift =
                    new TimeInTimeSeriesChunkMetaData(
                            dataType == null ? null : DataType.valueOf(dataType.toString()), startTime, endTime);
            tTimeSeriesChunkMetaDataInThrift.setFreq_type(freqType == null ? null : FreqType.valueOf(freqType.toString()));
            tTimeSeriesChunkMetaDataInThrift.setFrequencies(frequencies);
            tTimeSeriesChunkMetaDataInThrift.setEnum_values(enumValues);
            return tTimeSeriesChunkMetaDataInThrift;
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error(
                        "tsfile-file TInTimeSeriesChunkMetaData: failed to convert TimeInTimeSeriesChunkMetaData from TSFile to thrift, content is {}",
                        this, e);
            throw e;
        }
    }

    @Override
    public void convertToTSF(TimeInTimeSeriesChunkMetaData tTimeSeriesChunkMetaDataInThrift) {
        try {
            dataType = tTimeSeriesChunkMetaDataInThrift.getData_type() == null ? null : TSDataType.valueOf(tTimeSeriesChunkMetaDataInThrift.getData_type().toString());
            freqType = tTimeSeriesChunkMetaDataInThrift.getFreq_type() == null ? null : TSFreqType.valueOf(tTimeSeriesChunkMetaDataInThrift.getFreq_type().toString());
            frequencies = tTimeSeriesChunkMetaDataInThrift.getFrequencies();
            startTime = tTimeSeriesChunkMetaDataInThrift.getStartime();
            endTime = tTimeSeriesChunkMetaDataInThrift.getEndtime();
            enumValues = tTimeSeriesChunkMetaDataInThrift.getEnum_values();
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error(
                        "tsfile-file TInTimeSeriesChunkMetaData: failed to convert TimeInTimeSeriesChunkMetaData from thrift to TSFile, content is {}",
                        tTimeSeriesChunkMetaDataInThrift, e);
            throw e;
        }
    }

    @Override
    public String toString() {
        return String.format(
                "TInTimeSeriesChunkMetaData{ TSDataType %s, TSFreqType %s, frequencies %s, starttime %d, endtime %d, enumValues %s }",
                dataType, freqType, frequencies, startTime, endTime, enumValues);
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public void setDataType(TSDataType dataType) {
        this.dataType = dataType;
    }

    public TSFreqType getFreqType() {
        return freqType;
    }

    public void setFreqType(TSFreqType freqType) {
        this.freqType = freqType;
    }

    public List<Integer> getFrequencies() {
        return frequencies;
    }

    public void setFrequencies(List<Integer> frequencies) {
        this.frequencies = frequencies;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public List<String> getEnumValues() {
        return enumValues;
    }

    public void setEnumValues(List<String> enumValues) {
        this.enumValues = enumValues;
    }
}
