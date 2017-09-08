package cn.edu.thu.tsfiledb.query.engine;


import java.io.IOException;
import java.util.*;

import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.query.aggregation.AggregateFunction;
import cn.edu.thu.tsfiledb.query.aggregation.AggregationResult;
import cn.edu.thu.tsfiledb.query.dataset.InsertDynamicData;
import cn.edu.thu.tsfiledb.query.management.RecordReaderFactory;
import cn.edu.thu.tsfiledb.query.reader.RecordReader;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.CrossQueryTimeGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregateEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateEngine.class);

    public static QueryDataSet multiAggregate(List<Pair<Path, AggregateFunction>> aggres, List<FilterStructure> filterStructures) throws ProcessorException, IOException, PathErrorException {

        if (filterStructures == null || filterStructures.size() == 0 || (filterStructures.size()==1 && filterStructures.get(0).noFilter()) ) {
            return multiAggregateWithoutFilter(aggres, filterStructures);
        } else {
            throw new ProcessorException("Multi aggregation doesn't support filter currently.");
        }
    }

    private static QueryDataSet multiAggregateWithoutFilter(List<Pair<Path, AggregateFunction>> aggres, List<FilterStructure> filterStructures) throws PathErrorException, ProcessorException, IOException {

        QueryDataSet ansQueryDataSet = new QueryDataSet();

        int aggreNumber = 0;
        for (Pair<Path, AggregateFunction> pair : aggres) {
            aggreNumber ++;
            Path path = pair.left;
            AggregateFunction aggregateFunction = pair.right;
            String deltaObjectUID = path.getDeltaObjectToString();
            String measurementUID = path.getMeasurementToString();
            TSDataType dataType = MManager.getInstance().getSeriesType(path.getFullPath());
            String aggregationKey = aggregateFunction.name + "(" + path.getFullPath() + ")";
            if (ansQueryDataSet.mapRet.size() > 0 && ansQueryDataSet.mapRet.containsKey(aggregationKey)) {
                continue;
            }

            RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measurementUID,
                    null, null, null, null, ReadCachePrefix.addQueryPrefix(aggreNumber));

            if (recordReader.insertAllData == null) {
                // get overflow params merged with bufferwrite insert data
                List<Object> params = EngineUtils.getOverflowInfoAndFilterDataInMem(null, null, null, null, recordReader.insertPageInMemory, recordReader.overflowInfo);
                DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
                DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
                DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
                SingleSeriesFilterExpression newTimeFilter = (SingleSeriesFilterExpression) params.get(3);

                recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                        insertTrue, updateTrue, updateFalse,
                        newTimeFilter, null, null, dataType);

                AggregationResult aggrRet = recordReader.aggregate(deltaObjectUID, measurementUID, aggregateFunction,
                        updateTrue, updateFalse, recordReader.insertAllData, newTimeFilter, null, null);
                ansQueryDataSet.mapRet.put(aggregationKey, aggrRet.data);
            } else {
                DynamicOneColumnData aggData = ansQueryDataSet.mapRet.get(aggregationKey);
                if (aggData != null) {
                    aggData.clearData();
                }
                ansQueryDataSet.mapRet.put(aggregationKey, aggData);
            }
        }

        return ansQueryDataSet;
    }

    /**
     *  This function is only used for CrossQueryTimeGenerator.
     *  A CrossSeriesFilterExpression is consist of many SingleSeriesFilterExpression.
     *  e.g. CSAnd(d1.s1, d2.s1) is consist of d1.s1 and d2.s1, so this method would be invoked twice,
     *  once for querying d1.s1, once for querying d2.s1.
     *  <p>
     *  When this method is invoked, need add the filter index as a new parameter, for the reason of exist of
     *  <code>RecordReaderCache</code>, if the composition of CrossFilterExpression exist same SingleFilterExpression,
     *  we must guarantee that the <code>RecordReaderCache</code> doesn't cause conflict to the same SingleFilterExpression.
     */
    private static DynamicOneColumnData getDataUseSingleValueFilter(SingleSeriesFilterExpression valueFilter, SingleSeriesFilterExpression freqFilter,
                                                            DynamicOneColumnData res, int fetchSize, int valueFilterNumber)
            throws ProcessorException, IOException, PathErrorException {

        String deltaObjectUID = ((SingleSeriesFilterExpression) valueFilter).getFilterSeries().getDeltaObjectUID();
        String measurementUID = ((SingleSeriesFilterExpression) valueFilter).getFilterSeries().getMeasurementUID();
        String valueFilterPrefix = ReadCachePrefix.addFilterPrefix(valueFilterNumber);

        RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measurementUID,
                null, freqFilter, valueFilter, null, valueFilterPrefix);

        if (res == null) {
            // get four overflow params
            List<Object> params = EngineUtils.getOverflowInfoAndFilterDataInMem(null, freqFilter, valueFilter,
                    res, recordReader.insertPageInMemory, recordReader.overflowInfo);

            DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
            DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
            DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
            SingleSeriesFilterExpression newTimeFilter = (SingleSeriesFilterExpression) params.get(3);

            recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                    insertTrue, updateTrue, updateFalse,
                    newTimeFilter, valueFilter, null, MManager.getInstance().getSeriesType(deltaObjectUID+"."+measurementUID));
            res = recordReader.getValueInOneColumnWithOverflow(deltaObjectUID, measurementUID,
                    updateTrue, updateFalse, recordReader.insertAllData, newTimeFilter, valueFilter, res, fetchSize);
            res.putOverflowInfo(insertTrue, updateTrue, updateFalse, newTimeFilter);
        } else {
            res = recordReader.getValueInOneColumnWithOverflow(deltaObjectUID, measurementUID,
                    res.updateTrue, res.updateFalse, recordReader.insertAllData, res.timeFilter, valueFilter, res, fetchSize);
        }

        return res;
    }
}
