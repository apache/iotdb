package cn.edu.thu.tsfiledb.query.engine;


import java.io.IOException;
import java.util.List;

import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.query.aggregation.AggregateFunction;
import cn.edu.thu.tsfiledb.query.aggregation.AggregationResult;
import cn.edu.thu.tsfiledb.query.dataset.InsertDynamicData;
import cn.edu.thu.tsfiledb.query.management.RecordReaderFactory;
import cn.edu.thu.tsfiledb.query.reader.RecordReader;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;

public class AggregateEngine {

    public static QueryDataSet aggregate(Path path, AggregateFunction func,
                                         FilterExpression timeFilter, FilterExpression freqFilter, FilterExpression valueFilter) throws ProcessorException, IOException, PathErrorException {
        if (!(timeFilter == null || timeFilter instanceof SingleSeriesFilterExpression) ||
                !(freqFilter == null || freqFilter instanceof SingleSeriesFilterExpression) ||
                !(valueFilter == null || valueFilter instanceof SingleSeriesFilterExpression)) {
            throw new ProcessorException("Filter must be SingleSeriesFilterExpression");
        }

        QueryDataSet queryDataSet = new QueryDataSet();
        String deltaObjectUID = path.getDeltaObjectToString();
        String measurementUID = path.getMeasurementToString();

        RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectUID, measurementUID,
                (SingleSeriesFilterExpression) timeFilter,
                (SingleSeriesFilterExpression) freqFilter,
                (SingleSeriesFilterExpression) valueFilter, null, "");

        // get overflow params merged with bufferwrite insert data.
        List<Object> params = EngineUtils.getOverflowInfoAndFilterDataInMem((SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                (SingleSeriesFilterExpression) valueFilter, null, recordReader.insertPageInMemory, recordReader.overflowInfo);
        DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
        DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
        DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
        SingleSeriesFilterExpression deleteFilter = (SingleSeriesFilterExpression) params.get(3);

        if (recordReader.insertAllData == null) {
            recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                    insertTrue, updateTrue, updateFalse,
                    deleteFilter, (SingleSeriesFilterExpression)valueFilter, (SingleSeriesFilterExpression)freqFilter,
                    MManager.getInstance().getSeriesType(path.getFullPath()));
        } else {
            recordReader.insertAllData.readStatusReset();
            recordReader.insertAllData.setBufferWritePageList(recordReader.bufferWritePageList);
            recordReader.insertAllData.setCurrentPageBuffer(insertTrue);
        }

        AggregationResult aggrRet = recordReader.aggregate(deltaObjectUID, measurementUID, func,
                updateTrue, updateFalse, recordReader.insertAllData
                , deleteFilter, (SingleSeriesFilterExpression) freqFilter, (SingleSeriesFilterExpression) valueFilter);

        queryDataSet.mapRet.put(func.name + "(" + path.getFullPath() + ")", aggrRet.data);
        // TODO close current recordReader, need close file stream?
        // recordReader.closeFromFactory();
        return queryDataSet;
    }

}
