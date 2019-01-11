package cn.edu.tsinghua.tsfile.timeseries.read;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.read.management.SeriesSchema;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * This class implements several read methods which can read data in different ways.<br>
 * This class provides some APIs for reading.
 */
public class RecordReader {

    private static final Logger logger = LoggerFactory.getLogger(RecordReader.class);
    private FileReader fileReader;
    private Map<String, Map<String, SeriesSchema>> seriesSchemaMap;

    public RecordReader(ITsRandomAccessFileReader raf) throws IOException {
        this.fileReader = new FileReader(raf);
    }

    //for hadoop-connector
    public RecordReader(ITsRandomAccessFileReader raf, List<RowGroupMetaData> rowGroupMetaDataList) throws IOException {
        this.fileReader = new FileReader(raf, rowGroupMetaDataList);
    }

    /**
     * Read one path without filter.
     *
     * @param res the iterative result
     * @param fetchSize fetch size
     * @param deltaObjectUID delta object id
     * @param measurementUID  measurement Id
     * @return the result in means of DynamicOneColumnData
     * @throws IOException TsFile read error
     */
    public DynamicOneColumnData getValueInOneColumn(DynamicOneColumnData res, int fetchSize
            , String deltaObjectUID, String measurementUID) throws IOException {

        checkSeries(deltaObjectUID, measurementUID);

        List<RowGroupReader> rowGroupReaderList = fileReader.getRowGroupReaderListByDeltaObject(deltaObjectUID);
        int i = 0;
        if (res != null) {
            i = res.getRowGroupIndex();
        }
        for (; i < rowGroupReaderList.size(); i++) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(i);

            if(rowGroupReader.getValueReaders().get(measurementUID) == null) {
                return alignColumn(measurementUID);
            }

            res = getValueInOneColumn(res, fetchSize, rowGroupReader, measurementUID);
            if (res.valueLength >= fetchSize) {
                res.hasReadAll = false;
                break;
            }
        }
        return res;
    }

    /**
     * Read one path without filter and do not throw exceptino. Used by hadoop.
     *
     * @param res the iterative result
     * @param fetchSize fetch size
     * @param deltaObjectUID delta object id
     * @param measurementUID  measurement Id
     * @return the result in means of DynamicOneColumnData
     * @throws IOException TsFile read error
     */
    public DynamicOneColumnData getValueInOneColumnWithoutException(DynamicOneColumnData res, int fetchSize
            , String deltaObjectUID, String measurementUID) throws IOException {
        try {
            checkSeriesByHadoop(deltaObjectUID, measurementUID);
        }catch(IOException ex){
            if(res == null)res = new DynamicOneColumnData();
            res.dataType = fileReader.getRowGroupReaderListByDeltaObject(deltaObjectUID).get(0).getDataTypeBySeriesName(measurementUID);
            return res;
        }
        List<RowGroupReader> rowGroupReaderList = fileReader.getRowGroupReaderListByDeltaObjectByHadoop(deltaObjectUID);
        int i = 0;
        if (res != null) {
            i = res.getRowGroupIndex();
        }
        for (; i < rowGroupReaderList.size(); i++) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(i);
            res = getValueInOneColumn(res, fetchSize, rowGroupReader, measurementUID);
            if (res.valueLength >= fetchSize) {
                res.hasReadAll = false;
                break;
            }
        }
        return res;
    }

    private DynamicOneColumnData getValueInOneColumn(DynamicOneColumnData res, int fetchSize,
                                                     RowGroupReader rowGroupReader, String measurementId) throws IOException {
        return rowGroupReader.getValueReaders().get(measurementId).readOneColumn(res, fetchSize);
    }


    /**
     * Read one path without filter from one specific
     * <code>RowGroupReader</code> according to the indexList
     * @param res result
     * @param fetchSize fetch size
     * @param deltaObjectUID delta object id
     * @param measurementId  measurement Id
     * @param idxes index list of the RowGroupReader
     * @return DynamicOneColumnData
     * @throws IOException failed to get value
     */
    public DynamicOneColumnData getValueInOneColumn(DynamicOneColumnData res, int fetchSize, String deltaObjectUID,
                                                    String measurementId, ArrayList<Integer> idxes) throws IOException {
        checkSeries(deltaObjectUID, measurementId);
        int rowGroupSkipCount = 0;

        List<RowGroupReader> rowGroupReaderList = fileReader.getRowGroupReaderList();
        int i = 0;
        if (res != null) {
            i = res.getRowGroupIndex();
        }
        for (; i < idxes.size(); i++) {
            int idx = idxes.get(i);
            RowGroupReader rowGroupReader = rowGroupReaderList.get(idx);
            if (!deltaObjectUID.equals(rowGroupReader.getDeltaObjectUID())) {
                rowGroupSkipCount++;
                continue;
            }

            if(rowGroupReader.getValueReaders().get(measurementId) == null) {
                return alignColumn(measurementId);
            }

            res = rowGroupReader.getValueReaders().get(measurementId).readOneColumn(res, fetchSize);
            for (int k = 0; k < rowGroupSkipCount; k++) {
                res.plusRowGroupIndexAndInitPageOffset();
            }
            if (res.valueLength >= fetchSize) {
                res.hasReadAll = false;
                break;
            }
        }
        return res;
    }

    public DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize
            , SingleSeriesFilterExpression valueFilter) throws IOException {
        String deltaObjectUID = valueFilter.getFilterSeries().getDeltaObjectUID();
        String measurementUID = valueFilter.getFilterSeries().getMeasurementUID();
        return getValuesUseFilter(res, fetchSize, deltaObjectUID, measurementUID, null, null, valueFilter);
    }

    public DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize, String deltaObjectUID,
                                                   String measurementId, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
                                                   SingleSeriesFilterExpression valueFilter) throws IOException {
        checkSeries(deltaObjectUID, measurementId);
        int i = 0;
        if (res != null) {
            i = res.getRowGroupIndex();
        }

        List<RowGroupReader> rowGroupReaderList = fileReader.getRowGroupReaderListByDeltaObject(deltaObjectUID);
        for (; i < rowGroupReaderList.size(); i++) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(i);

            if(rowGroupReader.getValueReaders().get(measurementId) == null) {
                return alignColumn(measurementId);
            }

            res = getValuesUseFilter(res, fetchSize, rowGroupReader, measurementId, timeFilter, freqFilter, valueFilter);
            if (res.valueLength >= fetchSize) {
                res.hasReadAll = false;
                break;
            }
        }
        return res;
    }

    public DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize
            , SingleSeriesFilterExpression valueFilter, ArrayList<Integer> idxs) throws IOException {
        String deltaObjectUID = valueFilter.getFilterSeries().getDeltaObjectUID();
        String measurementUID = valueFilter.getFilterSeries().getMeasurementUID();
        return getValuesUseFilter(res, fetchSize, deltaObjectUID, measurementUID, null, null, valueFilter, idxs);
    }

    public DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize, String deltaObjectUID,
                                                   String measurementId, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
                                                   SingleSeriesFilterExpression valueFilter, ArrayList<Integer> idxs) throws IOException {
        checkSeries(deltaObjectUID, measurementId);
        int rowGroupSkipCount = 0;

        List<RowGroupReader> rowGroupReaderList = fileReader.getRowGroupReaderList();
        int i = 0;
        if (res != null) {
            i = res.getRowGroupIndex();
        }
        for (; i < idxs.size(); i++) {
            logger.info("GetValuesUseFilter and timeIdxs. RowGroupIndex is :" + idxs.get(i));
            int idx = idxs.get(i);
            RowGroupReader rowGroupReader = rowGroupReaderList.get(idx);
            if (!deltaObjectUID.equals(rowGroupReader.getDeltaObjectUID())) {
                rowGroupSkipCount++;
                continue;
            }

            if(rowGroupReader.getValueReaders().get(measurementId) == null) {
                return alignColumn(measurementId);
            }

            res = getValuesUseFilter(res, fetchSize, rowGroupReader, measurementId, timeFilter, freqFilter, valueFilter);
            for (int k = 0; k < rowGroupSkipCount; k++) {
                res.plusRowGroupIndexAndInitPageOffset();
            }
            if (res.valueLength >= fetchSize) {
                res.hasReadAll = false;
                break;
            }
        }
        return res;
    }

    private DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize,
                                                    RowGroupReader rowGroupReader, String measurementId, SingleSeriesFilterExpression timeFilter,
                                                    SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) throws IOException {

        res = rowGroupReader.getValueReaders().get(measurementId).readOneColumnUseFilter(res, fetchSize, timeFilter,
                freqFilter, valueFilter);
        return res;
    }

    public DynamicOneColumnData getValuesUseTimestamps(String deltaObjectUID, String measurementId, long[] timestamps)
            throws IOException {
        checkSeries(deltaObjectUID, measurementId);
        DynamicOneColumnData res = null;
        List<RowGroupReader> rowGroupReaderList = fileReader.getRowGroupReaderListByDeltaObject(deltaObjectUID);
        for (int i = 0; i < rowGroupReaderList.size(); i++) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(i);

            if(rowGroupReader.getValueReaders().get(measurementId) == null) {
                return alignColumn(measurementId);
            }

            if (i == 0) {
                res = getValuesUseTimestamps(rowGroupReader, measurementId, timestamps);
            } else {
                DynamicOneColumnData tmpRes = getValuesUseTimestamps(rowGroupReader, measurementId, timestamps);
                res.mergeRecord(tmpRes);
            }
        }
        return res;
    }

    public DynamicOneColumnData getValuesUseTimestamps(String deltaObjectUID, String measurementId, long[] timeRet,
                                                       ArrayList<Integer> idxs) throws IOException {
        checkSeries(deltaObjectUID, measurementId);
        DynamicOneColumnData res = null;
        List<RowGroupReader> rowGroupReaderList = fileReader.getRowGroupReaderList();

        boolean init = false;
        for (int i = 0; i < idxs.size(); i++) {
            int idx = idxs.get(i);
            RowGroupReader rowGroupReader = rowGroupReaderList.get(idx);
            if (!deltaObjectUID.equals(rowGroupReader.getDeltaObjectUID())) {
                continue;
            }

            if(rowGroupReader.getValueReaders().get(measurementId) == null) {
                return alignColumn(measurementId);
            }

            if (!init) {
                res = getValuesUseTimestamps(rowGroupReader, measurementId, timeRet);
                init = true;
            } else {
                DynamicOneColumnData tmpRes = getValuesUseTimestamps(rowGroupReader, measurementId, timeRet);
                res.mergeRecord(tmpRes);
            }
        }
        return res;
    }

    private DynamicOneColumnData getValuesUseTimestamps(RowGroupReader rowGroupReader, String measurementId, long[] timeRet)
            throws IOException {
        return rowGroupReader.getValueReaders().get(measurementId).getValuesForGivenValues(timeRet);
    }

    public boolean isEnumsColumn(String deltaObjectUID, String sid) throws IOException {
        List<RowGroupReader> rowGroupReaderList = fileReader.getRowGroupReaderListByDeltaObject(deltaObjectUID);
        for (RowGroupReader rowGroupReader : rowGroupReaderList) {
            if (rowGroupReader.getValueReaderForSpecificMeasurement(sid) == null) {
                continue;
            }
            if (rowGroupReader.getValueReaders().get(sid).getDataType() == TSDataType.ENUMS) {
                return true;
            }
        }
        return false;
    }

    //For Tsfile-Spark-Connector
    public List<SeriesSchema> getAllSeriesSchema() throws IOException {
        List<TimeSeriesMetadata> tslist = this.fileReader.getFileMetaData().getTimeSeriesList();
        List<SeriesSchema> seriesSchemas = new ArrayList<>();
        for(TimeSeriesMetadata ts: tslist ) {
            seriesSchemas.add(new SeriesSchema(ts.getMeasurementUID(), ts.getType(), null));
        }
        return seriesSchemas;
    }

    public ArrayList<String> getAllDeltaObjects() throws IOException {
        ArrayList<String> res = new ArrayList<>();
        HashMap<String, Integer> deltaObjectMap = new HashMap<>();
        List<RowGroupReader> rowGroupReaders = fileReader.getRowGroupReaderList();
        for (RowGroupReader rgr : rowGroupReaders) {
            String deltaObjectUID = rgr.getDeltaObjectUID();
            if (!deltaObjectMap.containsKey(deltaObjectUID)) {
                res.add(deltaObjectUID);
                deltaObjectMap.put(deltaObjectUID, 1);
            }
        }
        return res;
    }

    public Map<String, ArrayList<SeriesSchema>> getAllSeriesSchemasGroupByDeltaObject() {
        Map<String, ArrayList<SeriesSchema>> res = new HashMap<>();
        Map<String, List<RowGroupReader>> rowGroupReaders = fileReader.getRowGroupReaderMap();
        for (String deltaObjectUID : rowGroupReaders.keySet()) {
            HashMap<String, Integer> measurementMap = new HashMap<>();
            ArrayList<SeriesSchema> cols = new ArrayList<>();
            for (RowGroupReader rgr : rowGroupReaders.get(deltaObjectUID)) {
                for (String measurement : rgr.seriesDataTypeMap.keySet()) {
                    if (!measurementMap.containsKey(measurement)) {
                        cols.add(new SeriesSchema(measurement, rgr.seriesDataTypeMap.get(measurement), null));
                        measurementMap.put(measurement, 1);
                    }
                }
            }
            res.put(deltaObjectUID, cols);
        }
        return res;
    }

    public Map<String, Integer> getDeltaObjectRowGroupCounts() {
        Map<String, Integer> res = new HashMap<>();
        Map<String, List<RowGroupReader>> rowGroupReaders = fileReader.getRowGroupReaderMap();
        for (String deltaObjectUID : rowGroupReaders.keySet()) {
            res.put(deltaObjectUID, rowGroupReaders.get(deltaObjectUID).size());
        }
        return res;
    }

    public Map<String, String> getDeltaObjectTypes() {
        Map<String, String> res = new HashMap<>();
        Map<String, List<RowGroupReader>> rowGroupReaders = fileReader.getRowGroupReaderMap();
        for (String deltaObjectUID : rowGroupReaders.keySet()) {

            RowGroupReader rgr = rowGroupReaders.get(deltaObjectUID).get(0);
        }
        return res;
    }

    public ArrayList<Long> getRowGroupPosList() throws IOException {
        ArrayList<Long> res = new ArrayList<>();
        long startPos = 0;
        for (RowGroupReader rowGroupReader : fileReader.getRowGroupReaderList()) {
            long currentEndPos = rowGroupReader.getTotalByteSize() + startPos;
            res.add(currentEndPos);
            startPos = currentEndPos;
        }
        return res;
    }

    public FilterSeries<?> getColumnByMeasurementName(String deltaObject, String measurement) throws IOException {
        TSDataType type = null;

        //modified for Tsfile-Spark-Connector
        type = this.fileReader.getFileMetaData().getType(measurement);

        if (type == TSDataType.INT32) {
            return FilterFactory.intFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER);
        } else if (type == TSDataType.INT64) {
            return FilterFactory.longFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER);
        } else if (type == TSDataType.FLOAT) {
            return FilterFactory.floatFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER);
        } else if (type == TSDataType.DOUBLE) {
            return FilterFactory.doubleFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER);
        } else if (type == TSDataType.BOOLEAN) {
            return FilterFactory.booleanFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER);
        } else if (type == TSDataType.ENUMS || type == TSDataType.TEXT) {
            return FilterFactory.stringFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER);
        } else {
            throw new UnSupportedDataTypeException(String.valueOf(type));
        }
    }

    // modified for Tsfile-Spark-Connector
    private void checkSeries(String deltaObject, String measurement) throws IOException {
        this.fileReader.loadDeltaObj(deltaObject);
        if(!fileReader.containsDeltaObj(deltaObject) || !fileReader.getFileMetaData().containsMeasurement(measurement)) {
            throw new IOException("Series "+ deltaObject + "#" + measurement + " does not exist in the current file.");
        }
    }


    // corresponding with the modification of method 'checkSeries'
    private DynamicOneColumnData alignColumn(String measurementId) throws IOException{
        TSDataType type = fileReader.getFileMetaData().getType(measurementId);
        return new DynamicOneColumnData(type);
    }

    private void checkSeriesByHadoop(String deltaObject, String measurement) throws IOException {
        if (seriesSchemaMap == null) {
            seriesSchemaMap = new HashMap<>();
            Map<String, ArrayList<SeriesSchema>> seriesSchemaListMap = getAllSeriesSchemasGroupByDeltaObject();
            for (String key : seriesSchemaListMap.keySet()) {
                HashMap<String, SeriesSchema> tmap = new HashMap<>();
                for (SeriesSchema ss : seriesSchemaListMap.get(key)) {
                    tmap.put(ss.name, ss);
                }
                seriesSchemaMap.put(key, tmap);
            }
        }
        if (seriesSchemaMap.containsKey(deltaObject)) {
            if (seriesSchemaMap.get(deltaObject).containsKey(measurement)) {
                return;
            }
        }
        throw new IOException("Series is not exist in current file: " + deltaObject + "#" + measurement);
    }

    public List<RowGroupReader> getAllRowGroupReaders() throws IOException {
        return fileReader.getRowGroupReaderList();
    }

    public Map<String, String> getProps() {
        return fileReader.getProps();
    }

    public String getProp(String key) {
        return fileReader.getProp(key);
    }

    public void close() throws IOException {
        fileReader.close();
    }


}
