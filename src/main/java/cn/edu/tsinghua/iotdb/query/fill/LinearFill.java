package cn.edu.tsinghua.iotdb.query.fill;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.iotdb.query.engine.EngineUtils;
import cn.edu.tsinghua.iotdb.query.engine.ReadCachePrefix;
import cn.edu.tsinghua.iotdb.query.management.RecordReaderFactory;
import cn.edu.tsinghua.iotdb.query.reader.RecordReader;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.io.IOException;

import static cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory.*;


public class LinearFill extends IFill{

    private long beforeRange, afterRange;

    private Path path;

    private DynamicOneColumnData result;

    public LinearFill(long beforeRange, long afterRange) {
        this.beforeRange = beforeRange;
        this.afterRange = afterRange;
    }

    public LinearFill(Path path, TSDataType dataType, long queryTime, long beforeRange, long afterRange) {
        super(dataType, queryTime);
        this.path = path;
        this.beforeRange = beforeRange;
        this.afterRange = afterRange;
        result = new DynamicOneColumnData(dataType, true, true);
    }

    public long getBeforeRange() {
        return beforeRange;
    }

    public void setBeforeRange(long beforeRange) {
        this.beforeRange = beforeRange;
    }

    public long getAfterRange() {
        return afterRange;
    }

    public void setAfterRange(long afterRange) {
        this.afterRange = afterRange;
    }

    @Override
    public IFill copy(Path path) {
        return new LinearFill(path, dataType, queryTime, beforeRange, afterRange);
    }

    @Override
    public DynamicOneColumnData getFillResult() throws ProcessorException, IOException, PathErrorException {
        long beforeTime, afterTime;
        if (beforeRange == -1) {
            beforeTime = 0;
        } else {
            beforeTime = queryTime - beforeRange;
        }
        if (afterRange == -1) {
            afterTime = Long.MAX_VALUE;
        } else {
            afterTime = queryTime + afterRange;
        }

        SingleSeriesFilterExpression leftFilter = gtEq(timeFilterSeries(), beforeTime, true);
        SingleSeriesFilterExpression rightFilter = ltEq(timeFilterSeries(), afterTime, true);
        SingleSeriesFilterExpression fillTimeFilter = (SingleSeriesFilterExpression) and(leftFilter, rightFilter);

        String deltaObjectId = path.getDeltaObjectToString();
        String measurementId = path.getMeasurementToString();
        String recordReaderPrefix = ReadCachePrefix.addQueryPrefix("LinearFill", -1);

        RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectId, measurementId,
                fillTimeFilter, null, null, null, recordReaderPrefix);

        recordReader.buildInsertMemoryData(fillTimeFilter, null);

        // updateFalse is useless because there is no value filter
        recordReader.getLinearFillResult(result, deltaObjectId, measurementId, fillTimeFilter, beforeTime, queryTime, afterTime);

        return result;
    }
}
