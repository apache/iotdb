package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSFreqType;
import cn.edu.tsinghua.tsfile.format.DataType;
import cn.edu.tsinghua.tsfile.format.FreqType;
import cn.edu.tsinghua.tsfile.format.TimeSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * For more information, see TimeSeries in cn.edu.thu.tsfile.format package
 */
public class TimeSeriesMetadata implements IConverter<TimeSeries> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesMetadata.class);

    private String measurementUID;

    private TSDataType type;

    /**
     * If type is FIXED_LEN_BYTE_ARRAY, this is the byte length of the values. Otherwise, if
     * specified, this is the maximum bit length to store any of the values. (e.g. a low cardinality
     * INT timeseries could have this set to 32). Note that this is in the schema, and therefore fixed
     * for the entire file.
     */
    private int typeLength;

    private TSFreqType freqType;
    private List<Integer> frequencies;

    /**
     * If values for data consist of enum values, metadata will store all possible values in time
     * series
     */
    private List<String> enumValues;

    public TimeSeriesMetadata() {
    }

    public TimeSeriesMetadata(String measurementUID, TSDataType dataType) {
        this.measurementUID = measurementUID;
        this.type = dataType;
    }

    @Override
    public TimeSeries convertToThrift() {
        try {
            TimeSeries timeSeriesInThrift = new TimeSeries(measurementUID,
                    type == null ? null : DataType.valueOf(type.toString()), "");//FIXME remove deltaType from TimeSeries.java
            timeSeriesInThrift.setType_length(typeLength);
            timeSeriesInThrift.setFreq_type(freqType == null ? null : FreqType.valueOf(freqType.toString()));
            timeSeriesInThrift.setFrequencies(frequencies);
            timeSeriesInThrift.setEnum_values(enumValues);
            return timeSeriesInThrift;
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error(
                        "tsfile-file TimeSeriesMetadata: failed to convert TimeSeriesMetadata from TSFile to thrift, content is {}",
                        this, e);
            throw e;
        }
    }

    @Override
    public void convertToTSF(TimeSeries timeSeriesInThrift) {
        try {
            measurementUID = timeSeriesInThrift.getMeasurement_uid();
            type = timeSeriesInThrift.getType() == null ? null
                    : TSDataType.valueOf(timeSeriesInThrift.getType().toString());
            typeLength = timeSeriesInThrift.getType_length();
            freqType = timeSeriesInThrift.getFreq_type() == null ? null
                    : TSFreqType.valueOf(timeSeriesInThrift.getFreq_type().toString());
            frequencies = timeSeriesInThrift.getFrequencies();
            enumValues = timeSeriesInThrift.getEnum_values();
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error(
                        "tsfile-file TimeSeriesMetadata: failed to convert TimeSeriesMetadata from TSFile to thrift, content is {}",
                        timeSeriesInThrift, e);
        }
    }

    public String getMeasurementUID() {
        return measurementUID;
    }

    public void setMeasurementUID(String measurementUID) {
        this.measurementUID = measurementUID;
    }

    public int getTypeLength() {
        return typeLength;
    }

    public void setTypeLength(int typeLength) {
        this.typeLength = typeLength;
    }

    public TSDataType getType() {
        return type;
    }

    public void setType(TSDataType type) {
        this.type = type;
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

    public List<String> getEnumValues() {
        return enumValues;
    }

    public void setEnumValues(List<String> enumValues) {
        this.enumValues = enumValues;
    }

    @Override
    public String toString() {
        return String.format(
                "TimeSeriesMetadata: measurementUID %s, type length %d, DataType %s, FreqType %s,frequencies %s",
                measurementUID, typeLength, type, freqType, frequencies);
    }
}
