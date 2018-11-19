package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.common.constant.QueryConstant;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.CrossSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.FilterUtils;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.RecordReader;
import cn.edu.tsinghua.tsfile.timeseries.read.RowGroupReader;
import cn.edu.tsinghua.tsfile.timeseries.read.management.SeriesSchema;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryEngine {
    protected static final Logger logger = LoggerFactory.getLogger(QueryEngine.class);
    protected static int FETCH_SIZE = 20000;
    protected RecordReader recordReader;

    public QueryEngine(ITsRandomAccessFileReader raf) throws IOException {
        recordReader = new RecordReader(raf);
    }

    public QueryEngine(ITsRandomAccessFileReader raf, int fetchSize) throws IOException {
        recordReader = new RecordReader(raf);
        FETCH_SIZE = fetchSize;
    }

    //for hadoop-connector
    public QueryEngine(ITsRandomAccessFileReader raf, List<RowGroupMetaData> rowGroupMetaDataList) throws IOException {
        recordReader = new RecordReader(raf, rowGroupMetaDataList);
    }

    public static OnePassQueryDataSet query(QueryConfig config, String fileName) throws IOException {
        TsRandomAccessLocalFileReader raf = new TsRandomAccessLocalFileReader(fileName);
        QueryEngine queryEngine = new QueryEngine(raf);
        OnePassQueryDataSet onePassQueryDataSet = queryEngine.query(config);
        raf.close();
        return onePassQueryDataSet;
    }

    public OnePassQueryDataSet query(QueryConfig config) throws IOException {
        if (config.getQueryType() == QueryType.QUERY_WITHOUT_FILTER) {
            return queryWithoutFilter(config);
        } else if (config.getQueryType() == QueryType.SELECT_ONE_COL_WITH_FILTER) {
            return readOneColumnValueUseFilter(config);
        } else if (config.getQueryType() == QueryType.CROSS_QUERY) {
            return crossColumnQuery(config);
        }
        return null;
    }

    /**
     * One of the basic query methods, return <code>OnePassQueryDataSet</code> which contains
     * the query result.
     * <p>
     *
     * @param paths query paths
     * @param timeFilter filter for time
     * @param freqFilter filter for frequency
     * @param valueFilter filter for value
     * @return query result
     * @throws IOException TsFile read error
     */
    public OnePassQueryDataSet query(List<Path> paths, FilterExpression timeFilter, FilterExpression freqFilter,
                                     FilterExpression valueFilter) throws IOException {

        if (timeFilter == null && freqFilter == null && valueFilter == null) {
            return queryWithoutFilter(paths);
        } else if (valueFilter instanceof SingleSeriesFilterExpression || (timeFilter != null && valueFilter == null)) {
            return readOneColumnValueUseFilter(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                    (SingleSeriesFilterExpression) valueFilter);
        } else if (valueFilter instanceof CrossSeriesFilterExpression) {
            return crossColumnQuery(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                    (CrossSeriesFilterExpression) valueFilter);
        }
        return null;
    }

    public OnePassQueryDataSet query(QueryConfig config, Map<String, Long> params) throws IOException {
        List<Path> paths = getPathsFromSelectedPaths(config.getSelectColumns());

        SingleSeriesFilterExpression timeFilter = FilterUtils.construct(config.getTimeFilter(), null);
        SingleSeriesFilterExpression freqFilter = FilterUtils.construct(config.getFreqFilter(), null);
        FilterExpression valueFilter;
        if (config.getQueryType() == QueryType.CROSS_QUERY) {
            valueFilter = FilterUtils.constructCrossFilter(config.getValueFilter(), recordReader);
        } else {
            valueFilter = FilterUtils.construct(config.getValueFilter(), recordReader);
        }
        return query(paths, timeFilter, freqFilter, valueFilter, params);
    }

    public OnePassQueryDataSet query(List<Path> paths, FilterExpression timeFilter, FilterExpression freqFilter,
                                     FilterExpression valueFilter, Map<String, Long> params) throws IOException {

        long startOffset = params.get(QueryConstant.PARTITION_START_OFFSET);
        long endOffset = params.get(QueryConstant.PARTITION_END_OFFSET);

        ArrayList<Integer> idxs = calSpecificRowGroupByPartition(startOffset, endOffset);

        if (logger.isDebugEnabled()) {
            logger.debug(startOffset + "|" + endOffset + "|" + idxs);
        }
        return queryWithSpecificRowGroups(paths, timeFilter, freqFilter, valueFilter, idxs);
    }

    private OnePassQueryDataSet queryWithSpecificRowGroups(List<Path> paths, FilterExpression timeFilter, FilterExpression freqFilter
            , FilterExpression valueFilter, ArrayList<Integer> rowGroupIndexList) throws IOException {
        if (timeFilter == null && freqFilter == null && valueFilter == null) {
            return queryWithoutFilter(paths, rowGroupIndexList);
        } else if (valueFilter instanceof SingleSeriesFilterExpression || (timeFilter != null && valueFilter == null)) {
            return readOneColumnValueUseFilter(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                    (SingleSeriesFilterExpression) valueFilter, rowGroupIndexList);
        } else if (valueFilter instanceof CrossSeriesFilterExpression) {
            return crossColumnQuery(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                    (CrossSeriesFilterExpression) valueFilter, rowGroupIndexList);
        }
        throw new IOException("Query Not Support Exception");
    }

    private OnePassQueryDataSet queryWithoutFilter(QueryConfig config) throws IOException {
        List<Path> paths = getPathsFromSelectedPaths(config.getSelectColumns());
        return queryWithoutFilter(paths);
    }

    private OnePassQueryDataSet queryWithoutFilter(List<Path> paths) throws IOException {
        return new IteratorOnePassQueryDataSet(paths) {
            @Override
            public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws IOException {
                return recordReader.getValueInOneColumn(res, FETCH_SIZE, p.getDeltaObjectToString(), p.getMeasurementToString());
            }
        };
    }

    private OnePassQueryDataSet queryWithoutFilter(List<Path> paths, ArrayList<Integer> RowGroupIdxList) throws IOException {
        return new IteratorOnePassQueryDataSet(paths) {
            @Override
            public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws IOException {
                return recordReader.getValueInOneColumn(res, FETCH_SIZE, p.getDeltaObjectToString(), p.getMeasurementToString(), RowGroupIdxList);
            }
        };
    }

    private OnePassQueryDataSet readOneColumnValueUseFilter(QueryConfig config) throws IOException {
        SingleSeriesFilterExpression timeFilter = FilterUtils.construct(config.getTimeFilter(), null);
        SingleSeriesFilterExpression freqFilter = FilterUtils.construct(config.getFreqFilter(), null);
        SingleSeriesFilterExpression valueFilter = FilterUtils.construct(config.getValueFilter(), recordReader);
        List<Path> paths = getPathsFromSelectedPaths(config.getSelectColumns());
        return readOneColumnValueUseFilter(paths, timeFilter, freqFilter, valueFilter);
    }

    private OnePassQueryDataSet readOneColumnValueUseFilter(List<Path> paths, SingleSeriesFilterExpression timeFilter,
                                                            SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) throws IOException {
        logger.debug("start read one column data with filter...");
        return new IteratorOnePassQueryDataSet(paths) {
            @Override
            public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws IOException {
                return recordReader.getValuesUseFilter(res, FETCH_SIZE, p.getDeltaObjectToString(), p.getMeasurementToString()
                        , timeFilter, freqFilter, valueFilter);
            }
        };
    }

    private OnePassQueryDataSet readOneColumnValueUseFilter(List<Path> paths, SingleSeriesFilterExpression timeFilter,
                                                            SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter, ArrayList<Integer> rowGroupIndexList) throws IOException {
        logger.debug("start read one column data with filter according to specific RowGroup Index List {}", rowGroupIndexList);

        return new IteratorOnePassQueryDataSet(paths) {
            @Override
            public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws IOException {
                return recordReader.getValuesUseFilter(res, FETCH_SIZE, p.getDeltaObjectToString(), p.getMeasurementToString()
                        , timeFilter, freqFilter, valueFilter, rowGroupIndexList);
            }
        };
    }

    private OnePassQueryDataSet crossColumnQuery(QueryConfig config) throws IOException {
        logger.info("start cross columns getIndex...");
        SingleSeriesFilterExpression timeFilter = FilterUtils.construct(config.getTimeFilter(), null);
        SingleSeriesFilterExpression freqFilter = FilterUtils.construct(config.getFreqFilter(), null);
        CrossSeriesFilterExpression valueFilter = (CrossSeriesFilterExpression) FilterUtils.constructCrossFilter(config.getValueFilter(),
                recordReader);
        List<Path> paths = getPathsFromSelectedPaths(config.getSelectColumns());
        return crossColumnQuery(paths, timeFilter, freqFilter, valueFilter);
    }

    private OnePassQueryDataSet crossColumnQuery(List<Path> paths, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
                                                 CrossSeriesFilterExpression valueFilter) throws IOException {

        CrossQueryTimeGenerator timeGenerator = new CrossQueryTimeGenerator(timeFilter, freqFilter, valueFilter, FETCH_SIZE) {
            @Override
            public DynamicOneColumnData getDataInNextBatch(DynamicOneColumnData res, int fetchSize,
                                                           SingleSeriesFilterExpression valueFilter, int valueFilterNumber) throws ProcessorException, IOException {
                return recordReader.getValuesUseFilter(res, fetchSize, valueFilter);
            }
        };

        return new CrossOnePassQueryIteratorDataSet(timeGenerator) {
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

    private OnePassQueryDataSet crossColumnQuery(List<Path> paths, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
                                                 CrossSeriesFilterExpression valueFilter, ArrayList<Integer> RowGroupIdxList) throws IOException {
        CrossQueryTimeGenerator timeQueryDataSet = new CrossQueryTimeGenerator(timeFilter, freqFilter, valueFilter, FETCH_SIZE) {
            @Override
            public DynamicOneColumnData getDataInNextBatch(DynamicOneColumnData res, int fetchSize,
                                                           SingleSeriesFilterExpression valueFilter, int valueFilterNumber) throws ProcessorException, IOException {
                return recordReader.getValuesUseFilter(res, fetchSize, valueFilter, RowGroupIdxList);
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
                        DynamicOneColumnData oneColDataList = recordReader.getValuesUseTimestamps(deltaObjectUID, measurementUID, timeRet, RowGroupIdxList);
                        mapRet.put(p.getFullPath(), oneColDataList);
                    }

                } catch (ProcessorException e) {
                    throw new IOException(e.getMessage());
                }
                return false;
            }
        };
    }

    private List<Path> getPathsFromSelectedPaths(List<String> selectedPaths) {
        List<Path> paths = new ArrayList<>();
        for (String path : selectedPaths) {
            Path p = new Path(path);
            paths.add(p);
        }
        return paths;
    }

    public Map<String, ArrayList<SeriesSchema>> getAllSeriesSchemasGroupByDeltaObject() throws IOException {
        return recordReader.getAllSeriesSchemasGroupByDeltaObject();
    }

    public Map<String, Integer> getDeltaObjectRowGroupCount() throws IOException {
        return recordReader.getDeltaObjectRowGroupCounts();
    }

    public Map<String, String> getDeltaObjectTypes() throws IOException {
        return recordReader.getDeltaObjectTypes();
    }

    public boolean pathExist(Path path) throws IOException{
        FilterSeries<?> col = recordReader.getColumnByMeasurementName(path.getDeltaObjectToString(), path.getMeasurementToString());

        return col != null;
    }

    public ArrayList<String> getAllDeltaObject() throws IOException {
        return recordReader.getAllDeltaObjects();
    }

    public List<SeriesSchema> getAllSeriesSchema() throws IOException {
        return recordReader.getAllSeriesSchema();
    }

    // Start - Methods for spark reading
    public ArrayList<Long> getRowGroupPosList() throws IOException {
        return recordReader.getRowGroupPosList();
    }

    public ArrayList<Integer> calSpecificRowGroupByPartition(long start, long end) throws IOException {
        ArrayList<Long> rowGroupsPosList = getRowGroupPosList();
        ArrayList<Integer> res = new ArrayList<>();
        long curStartPos = 0L;
        for (int i = 0; i < rowGroupsPosList.size(); i++) {
            long curEndPos = rowGroupsPosList.get(i);
            long midPos = curStartPos + (curEndPos - curStartPos) / 2;
            if (start < midPos && midPos <= end) {
                res.add(i);
            }
            curStartPos = curEndPos;
        }
        return res;
    }

    public ArrayList<String> getAllDeltaObjectUIDByPartition(long start, long end) throws IOException {
        ArrayList<Long> rowGroupsPosList = getRowGroupPosList();
        List<RowGroupReader> rgrs = recordReader.getAllRowGroupReaders();
        ArrayList<String> res = new ArrayList<>();
        long curStartPos = 0L;
        for (int i = 0; i < rowGroupsPosList.size(); i++) {
            long curEndPos = rowGroupsPosList.get(i);
            long midPos = curStartPos + (curEndPos - curStartPos) / 2;
            if (start < midPos && midPos <= end) {
                res.add(rgrs.get(i).getDeltaObjectUID());
            }
            curStartPos = curEndPos;
        }
        return res;
    }

    public Map<String, String> getProps() {
        return recordReader.getProps();
    }

    public String getProp(String key) {
        return recordReader.getProp(key);
    }

    public void close() throws IOException{
        recordReader.close();
    }
}
