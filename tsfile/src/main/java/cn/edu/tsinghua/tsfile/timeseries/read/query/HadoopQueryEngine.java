package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.CrossSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HadoopQueryEngine extends QueryEngine {

    private static final String SEPARATOR_DEVIDE_SERIES = ".";
    private List<RowGroupMetaData> rowGroupMetaDataList;

    public HadoopQueryEngine(ITsRandomAccessFileReader raf, List<RowGroupMetaData> rowGroupMetaDataList) throws IOException {
        super(raf, rowGroupMetaDataList);
        this.rowGroupMetaDataList = rowGroupMetaDataList;
    }

    private List<String> initDeviceIdList() {
        Set<String> deviceIdSet = new HashSet<>();
        for (RowGroupMetaData rowGroupMetaData : rowGroupMetaDataList) {
            deviceIdSet.add(rowGroupMetaData.getDeltaObjectID());
        }
        return new ArrayList<>(deviceIdSet);
    }

    private List<String> initSensorIdList(){
        Set<String> sensorIdSet = new HashSet<>();
        for(RowGroupMetaData rowGroupMetaData : rowGroupMetaDataList) {
            for(TimeSeriesChunkMetaData timeSeriesChunkMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()){
                sensorIdSet.add(timeSeriesChunkMetaData.getProperties().getMeasurementUID());
            }
        }
        return new ArrayList<>(sensorIdSet);
    }

    public OnePassQueryDataSet queryWithSpecificRowGroups(List<String> deviceIdList, List<String> sensorIdList, FilterExpression timeFilter, FilterExpression freqFilter, FilterExpression valueFilter) throws IOException{
        if(deviceIdList == null)deviceIdList = initDeviceIdList();
        if(sensorIdList == null)sensorIdList = initSensorIdList();

        List<Path> paths = new ArrayList<>();
        for(String deviceId : deviceIdList){
            for(String sensorId: sensorIdList){
                paths.add(new Path(deviceId + SEPARATOR_DEVIDE_SERIES + sensorId));
            }
        }

        if (timeFilter == null && freqFilter == null && valueFilter == null) {
            return queryWithoutFilter(paths);
        } else if (valueFilter instanceof SingleSeriesFilterExpression || (timeFilter != null && valueFilter == null)) {
            return readOneColumnValueUseFilter(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                    (SingleSeriesFilterExpression) valueFilter);
        } else if (valueFilter instanceof CrossSeriesFilterExpression) {
            return crossColumnQuery(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                    (CrossSeriesFilterExpression) valueFilter);
        }
        throw new IOException("Query Not Support Exception");
    }

    private OnePassQueryDataSet queryWithoutFilter(List<Path> paths) throws IOException {
        return new IteratorOnePassQueryDataSet(paths) {
            @Override
            public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws IOException {
                return recordReader.getValueInOneColumnWithoutException(res, FETCH_SIZE, p.getDeltaObjectToString(), p.getMeasurementToString());
            }
        };
    }

    private OnePassQueryDataSet readOneColumnValueUseFilter(List<Path> paths, SingleSeriesFilterExpression timeFilter,
                                                            SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) throws IOException {
        logger.debug("start read one column data with filter");

        return new IteratorOnePassQueryDataSet(paths) {
            @Override
            public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws IOException {
                return recordReader.getValuesUseFilter(res, FETCH_SIZE, p.getDeltaObjectToString(), p.getMeasurementToString()
                        , timeFilter, freqFilter, valueFilter);
            }
        };
    }

    private OnePassQueryDataSet crossColumnQuery(List<Path> paths, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
                                                 CrossSeriesFilterExpression valueFilter) throws IOException {
        CrossQueryTimeGenerator timeQueryDataSet = new CrossQueryTimeGenerator(timeFilter, freqFilter, valueFilter, FETCH_SIZE) {
            @Override
            public DynamicOneColumnData getDataInNextBatch(DynamicOneColumnData res, int fetchSize,
                                                           SingleSeriesFilterExpression valueFilter, int valueFilterNumber) throws ProcessorException, IOException {
                return recordReader.getValuesUseFilter(res, fetchSize, valueFilter);
            }
        };

        return new CrossOnePassQueryIteratorDataSet(timeQueryDataSet) {
            @Override
            public boolean getMoreRecords() throws IOException {
                try {
                    long[] timeRet = crossQueryTimeGenerator.generateTimes();
                    if (timeRet.length == 0) {
                        return true;
                    }
                    for (Path p : paths) {
                        String deltaObjectUID = p.getDeltaObjectToString();
                        String measurementUID = p.getMeasurementToString();
                        DynamicOneColumnData oneColDataList = recordReader.getValuesUseTimestamps(deltaObjectUID, measurementUID, timeRet);
                        mapRet.put(p.getFullPath(), oneColDataList);
                    }

                } catch (ProcessorException e) {
                    throw new IOException(e.getMessage());
                }
                return false;
            }
        };
    }
}