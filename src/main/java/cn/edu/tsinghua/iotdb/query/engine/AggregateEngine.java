package cn.edu.tsinghua.iotdb.query.engine;

import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.management.FilterStructure;
import cn.edu.tsinghua.iotdb.query.management.ReadCachePrefix;
import cn.edu.tsinghua.iotdb.query.reader.AggregateRecordReader;
import cn.edu.tsinghua.iotdb.query.reader.QueryRecordReader;
import cn.edu.tsinghua.iotdb.query.reader.ReaderType;
import cn.edu.tsinghua.iotdb.query.reader.RecordReaderFactory;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.CrossQueryTimeGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static cn.edu.tsinghua.iotdb.query.engine.EngineUtils.noFilterOrOnlyHasTimeFilter;

public class AggregateEngine {

    private static final Logger logger = LoggerFactory.getLogger(AggregateEngine.class);

    /** aggregation batch calculation size **/
    private int aggregateFetchSize =
            TsfileDBDescriptor.getInstance().getConfig().fetchSize;

    /** cross read query fetch size **/
    private int crossQueryFetchSize =
            TsfileDBDescriptor.getInstance().getConfig().fetchSize;

    /**
     * <p>Public invoking method of multiple aggregation.
     *
     * @param aggregations           aggregation pairs
     * @param filterStructures list of <code>FilterStructure</code>
     * @throws ProcessorException read or write lock error etc
     * @throws IOException        read TsFile error
     * @throws PathErrorException path resolving error
     */
    public void multiAggregate(List<Pair<Path, AggregateFunction>> aggregations, List<FilterStructure> filterStructures)
            throws IOException, PathErrorException, ProcessorException {
        if (noFilterOrOnlyHasTimeFilter(filterStructures)) {
            if (filterStructures != null && filterStructures.size() == 1 && filterStructures.get(0).onlyHasTimeFilter()) {
                multiAggregateWithoutFilter(aggregations, filterStructures.get(0).getTimeFilter());
            } else {
                multiAggregateWithoutFilter(aggregations, null);
            }
        } else {
            multiAggregateWithFilter(aggregations, filterStructures);
        }
    }

    private void multiAggregateWithFilter(List<Pair<Path, AggregateFunction>> aggregations, List<FilterStructure> filterStructures)
            throws IOException, PathErrorException, ProcessorException {

        // stores the query QueryDataSet of each FilterStructure in filterStructures
        List<QueryDataSet> fsDataSets = new ArrayList<>();

        // stores calculated common timestamps of each FilterStructure answer
        List<long[]> fsTimeList = new ArrayList<>();

        // stores used index of each fsTimeList
        List<Integer> fsTimeIndexList = new ArrayList<>();

        // represents whether this FilterStructure answer still has unread data
        List<Boolean> fsHasUnReadDataList = new ArrayList<>();

        // represents that whether the 'key' ordinal aggregation still has unread data
        Map<Integer, Boolean> aggregationHasUnReadDataMap = new HashMap<>();

        for (int idx = 0; idx < filterStructures.size(); idx++) {
            FilterStructure filterStructure = filterStructures.get(idx);
            QueryDataSet queryDataSet = new QueryDataSet();

            queryDataSet.crossQueryTimeGenerator = new CrossQueryTimeGenerator(filterStructure.getTimeFilter(),
                    filterStructure.getFrequencyFilter(), filterStructure.getValueFilter(), crossQueryFetchSize) {
                @Override
                public DynamicOneColumnData getDataInNextBatch(DynamicOneColumnData res, int fetchSize,
                                                               SingleSeriesFilterExpression valueFilter, int valueFilterNumber)
                        throws ProcessorException, IOException {
                    try {
                        return getDataUseSingleValueFilter(valueFilter, res, fetchSize, valueFilterNumber);
                    } catch (PathErrorException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            };
            fsDataSets.add(queryDataSet);
            long[] curCommonTimestamps = queryDataSet.crossQueryTimeGenerator.generateTimes();
            fsTimeList.add(curCommonTimestamps);
            fsTimeIndexList.add(0);
            if (curCommonTimestamps.length > 0) {
                fsHasUnReadDataList.add(true);
            } else {
                fsHasUnReadDataList.add(false);
            }
        }

        // the aggregate timestamps calculated by filterStructures
        List<Long> aggregateTimestamps = new ArrayList<>();
        PriorityQueue<Long> priorityQueue = new PriorityQueue<>();

        for (int i = 0; i < fsTimeList.size(); i++) {
            boolean flag = fsHasUnReadDataList.get(i);
            if (flag) {
                priorityQueue.add(fsTimeList.get(i)[fsTimeIndexList.get(i)]);
            }
        }

        // if there still has any uncompleted read data, hasAnyUnReadDataFlag is true
        boolean hasAnyUnReadDataFlag = true;

        while (true) {
            while (aggregateTimestamps.size() < aggregateFetchSize && !priorityQueue.isEmpty() && hasAnyUnReadDataFlag) {
                // add the minimum timestamp and remove others in timeArray
                long minTime = priorityQueue.poll();
                aggregateTimestamps.add(minTime);
                while (!priorityQueue.isEmpty() && minTime == priorityQueue.peek())
                    priorityQueue.poll();

                for (int i = 0; i < fsTimeList.size(); i++) {
                    boolean flag = fsHasUnReadDataList.get(i);
                    if (flag) {
                        int curTimeIdx = fsTimeIndexList.get(i);
                        long[] curTimestamps = fsTimeList.get(i);
                        // remove all timestamps equal to min time in all series
                        while (curTimeIdx < curTimestamps.length && curTimestamps[curTimeIdx] == minTime) {
                            curTimeIdx++;
                        }
                        if (curTimeIdx < curTimestamps.length) {
                            fsTimeIndexList.set(i, curTimeIdx);
                            priorityQueue.add(curTimestamps[curTimeIdx]);
                        } else {
                            long[] newTimeStamps = fsDataSets.get(i).crossQueryTimeGenerator.generateTimes();
                            if (newTimeStamps.length > 0) {
                                priorityQueue.add(newTimeStamps[0]);
                                fsTimeList.set(i, newTimeStamps);
                                fsTimeIndexList.set(i, 1);
                            } else {
                                fsHasUnReadDataList.set(i, false);
                            }
                        }
                    }
                }
            }

            logger.debug(String.format("common timestamps in multiple aggregation process, timestamps size : %s, timestamps: %s",
                    String.valueOf(aggregateTimestamps.size()), aggregateTimestamps.toString()));

            if (aggregateTimestamps.size() == 0)
                break;

            //TODO optimize it using multi process

            hasAnyUnReadDataFlag = false;
            int aggregationPathOrdinal = 0;
            for (Pair<Path, AggregateFunction> pair : aggregations) {
                Path path = pair.left;
                AggregateFunction aggregateFunction = pair.right;
                String deltaObjectUID = path.getDeltaObjectToString();
                String measurementUID = path.getMeasurementToString();
                TSDataType dataType = MManager.getInstance().getSeriesType(path.getFullPath());
                aggregationPathOrdinal++;

                // current aggregation has no un read data
                if (aggregationHasUnReadDataMap.containsKey(aggregationPathOrdinal) && !aggregationHasUnReadDataMap.get(aggregationPathOrdinal)) {
                    continue;
                }

                // the query prefix here must not be conflict with method querySeriesForCross()
                AggregateRecordReader recordReader = (AggregateRecordReader)
                        RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measurementUID,
                        null,  null, null,
                        ReadCachePrefix.addQueryPrefix("AggQuery", aggregationPathOrdinal), ReaderType.AGGREGATE);

                if (recordReader.getInsertMemoryData() == null) {
                    Pair<AggregateFunction, Boolean> aggrPair = recordReader.aggregateUsingTimestamps(aggregateFunction, aggregateTimestamps);

                    boolean hasUnReadDataFlag = aggrPair.right;
                    aggregationHasUnReadDataMap.put(aggregationPathOrdinal, hasUnReadDataFlag);
                    if (hasUnReadDataFlag) {
                        hasAnyUnReadDataFlag = true;
                    }

                } else {
                    Pair<AggregateFunction, Boolean> aggrPair = recordReader.aggregateUsingTimestamps(aggregateFunction, aggregateTimestamps);
                    boolean hasUnReadDataFlag = aggrPair.right;
                    aggregationHasUnReadDataMap.put(aggregationPathOrdinal, hasUnReadDataFlag);
                    if (hasUnReadDataFlag) {
                        hasAnyUnReadDataFlag = true;
                    }
                }
            }

            // current batch timestamps has been used all
            aggregateTimestamps.clear();
        }
    }

    /**
     * Calculate the aggregation without filter is easy,
     * we don't need to consider the common aggregation timestamps.
     *
     * @param aggres
     * @return
     * @throws PathErrorException
     * @throws ProcessorException
     * @throws IOException
     */
    private void multiAggregateWithoutFilter(List<Pair<Path, AggregateFunction>> aggres, SingleSeriesFilterExpression queryTimeFilter)
            throws PathErrorException, ProcessorException, IOException {

        int aggreNumber = 0;
        for (Pair<Path, AggregateFunction> pair : aggres) {
            aggreNumber++;
            Path path = pair.left;
            AggregateFunction aggregateFunction = pair.right;
            String deltaObjectUID = path.getDeltaObjectToString();
            String measurementUID = path.getMeasurementToString();

            AggregateRecordReader recordReader = (AggregateRecordReader)
                    RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measurementUID,
                    queryTimeFilter, null, null, ReadCachePrefix.addQueryPrefix(aggreNumber), ReaderType.AGGREGATE);

            recordReader.aggregate(aggregateFunction);
        }
    }

    /**
     * This function is only used for CrossQueryTimeGenerator.
     * A CrossSeriesFilterExpression is consist of many SingleSeriesFilterExpression.
     * e.g. CSAnd(d1.s1, d2.s1) is consist of d1.s1 and d2.s1, so this method would be invoked twice,
     * once for querying d1.s1, once for querying d2.s1.
     * <p>
     * When this method is invoked, need add the filter index as a new parameter, for the reason of exist of
     * <code>RecordReaderCache</code>, if the composition of CrossFilterExpression exist same SingleFilterExpression,
     * we must guarantee that the <code>RecordReaderCache</code> doesn't cause conflict to the same SingleFilterExpression.
     */
    private DynamicOneColumnData getDataUseSingleValueFilter(SingleSeriesFilterExpression queryValueFilter,
                                                             DynamicOneColumnData res, int fetchSize, int valueFilterNumber)
            throws ProcessorException, IOException, PathErrorException {

        String deltaObjectUID = ((SingleSeriesFilterExpression) queryValueFilter).getFilterSeries().getDeltaObjectUID();
        String measurementUID = ((SingleSeriesFilterExpression) queryValueFilter).getFilterSeries().getMeasurementUID();

        // query prefix here must not be conflict with query in multiAggregate method
        String valueFilterPrefix = ReadCachePrefix.addFilterPrefix("AggFilterStructure", valueFilterNumber);

        QueryRecordReader recordReader = (QueryRecordReader)
                RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measurementUID,
                null, queryValueFilter, null, valueFilterPrefix, ReaderType.QUERY);

        if (res == null) {
            res = recordReader.queryOneSeries(null, queryValueFilter, null, fetchSize);
        } else {
            res = recordReader.queryOneSeries(null, queryValueFilter, res, fetchSize);
        }

        return res;
    }


}
