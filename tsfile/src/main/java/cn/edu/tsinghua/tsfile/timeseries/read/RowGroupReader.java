package cn.edu.tsinghua.tsfile.timeseries.read;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jinrui Zhang
 * This class is used to read one RowGroup.
 */
public class RowGroupReader {

    protected static final Logger logger = LoggerFactory.getLogger(RowGroupReader.class);
    public Map<String, TSDataType> seriesDataTypeMap;
    protected Map<String, ValueReader> valueReaders = new HashMap<>();
    protected String deltaObjectUID;

    protected List<String> measurementIds;
    protected long totalByteSize;

    protected ITsRandomAccessFileReader raf;

    public RowGroupReader() {

    }

    public RowGroupReader(RowGroupMetaData rowGroupMetaData, ITsRandomAccessFileReader raf) {
        logger.debug(String.format("init a new RowGroupReader, the deltaObjectId is %s", rowGroupMetaData.getDeltaObjectID()));
        seriesDataTypeMap = new HashMap<>();
        deltaObjectUID = rowGroupMetaData.getDeltaObjectID();
        measurementIds = new ArrayList<>();
        this.totalByteSize = rowGroupMetaData.getTotalByteSize();
        this.raf = raf;

        initValueReaders(rowGroupMetaData);
    }

    public List<Object> getTimeByRet(List<Object> timeRet, HashMap<Integer, Object> retMap) {
        List<Object> timeRes = new ArrayList<Object>();
        for (Integer i : retMap.keySet()) {
            timeRes.add(timeRet.get(i));
        }
        return timeRes;
    }

    public TSDataType getDataTypeBySeriesName(String name) {
        return this.seriesDataTypeMap.get(name);
    }

    public String getDeltaObjectUID() {
        return this.deltaObjectUID;
    }

    /**
     * Read time-value pairs whose time is be included in timeRet. WARNING: this
     * function is only for "time" Series
     *
     * @param measurementId measurement's id
     * @param timeRet       Array of the time.
     * @return DynamicOneColumnData
     * @throws IOException exception in IO
     */
    public DynamicOneColumnData readValueUseTimestamps(String measurementId, long[] timeRet) throws IOException {
        logger.debug("query {}.{} using common time, time length : {}", deltaObjectUID, measurementId, timeRet.length);
        return valueReaders.get(measurementId).getValuesForGivenValues(timeRet);
    }

    public DynamicOneColumnData readOneColumnUseFilter(String sid, DynamicOneColumnData res, int fetchSize
            , SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) throws IOException {
        ValueReader valueReader = valueReaders.get(sid);
        return valueReader.readOneColumnUseFilter(res, fetchSize, timeFilter, freqFilter, valueFilter);
    }

    public DynamicOneColumnData readOneColumn(String sid, DynamicOneColumnData res, int fetchSize) throws IOException {
        ValueReader valueReader = valueReaders.get(sid);
        return valueReader.readOneColumn(res, fetchSize);
    }

    public ValueReader getValueReaderForSpecificMeasurement(String sid) {
        return getValueReaders().get(sid);
    }

    public long getTotalByteSize() {
        return totalByteSize;
    }

    public void setTotalByteSize(long totalByteSize) {
        this.totalByteSize = totalByteSize;
    }

    public Map<String, ValueReader> getValueReaders() {
        return valueReaders;
    }

    public void setValueReaders(HashMap<String, ValueReader> valueReaders) {
        this.valueReaders = valueReaders;
    }

    public ITsRandomAccessFileReader getRaf() {
        return raf;
    }

    public void setRaf(ITsRandomAccessFileReader raf) {
        this.raf = raf;
    }

    public boolean containsMeasurement(String measurementID) {
        return this.valueReaders.containsKey(measurementID);
    }

    public void close() throws IOException {
        this.raf.close();
    }

    public void initValueReaders(RowGroupMetaData rowGroupMetaData) {
        for (TimeSeriesChunkMetaData tscMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()) {
            if (tscMetaData.getVInTimeSeriesChunkMetaData() != null) {
                measurementIds.add(tscMetaData.getProperties().getMeasurementUID());
                seriesDataTypeMap.put(tscMetaData.getProperties().getMeasurementUID(),
                        tscMetaData.getVInTimeSeriesChunkMetaData().getDataType());

                ValueReader si = new ValueReader(tscMetaData.getProperties().getFileOffset(),
                        tscMetaData.getTotalByteSize(),
                        tscMetaData.getVInTimeSeriesChunkMetaData().getDataType(),
                        tscMetaData.getVInTimeSeriesChunkMetaData().getDigest(), this.raf,
                        tscMetaData.getVInTimeSeriesChunkMetaData().getEnumValues(),
                        tscMetaData.getProperties().getCompression(), tscMetaData.getNumRows(),
                        tscMetaData.getTInTimeSeriesChunkMetaData().getStartTime(), tscMetaData.getTInTimeSeriesChunkMetaData().getEndTime());
                valueReaders.put(tscMetaData.getProperties().getMeasurementUID(), si);
            }
        }
    }
}