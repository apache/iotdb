package cn.edu.tsinghua.iotdb.query.engine;

import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitorFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;

import static cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory.and;

/**
 * Take out some common methods used for QueryEngine.
 */
public class EngineUtils {

    /**
     * QueryDataSet.BatchReadGenerator has calculated and removed the common RowRecord timestamps in the top of heap.
     * For the reason of that the RowRecord number is greater than fetch size,
     * so there may be remaining data in QueryDataSet.BatchReadGenerator,
     */
    public static void putRecordFromBatchReadGenerator(QueryDataSet dataSet) {
        for (Path path : dataSet.getBatchReadGenerator().retMap.keySet()) {
            DynamicOneColumnData batchReadData = dataSet.getBatchReadGenerator().retMap.get(path);
            DynamicOneColumnData leftData = batchReadData.sub(batchReadData.curIdx);

            // copy batch read info from oneColRet to leftRet
            batchReadData.copyFetchInfoTo(leftData);
            dataSet.getBatchReadGenerator().retMap.put(path, leftData);
            batchReadData.rollBack(batchReadData.valueLength - batchReadData.curIdx);
            dataSet.mapRet.put(path.getFullPath(), batchReadData);
        }
    }

    public static String aggregationKey(AggregateFunction aggregateFunction, Path path) {
        return aggregateFunction.name + "(" + path.getFullPath() + ")";
    }

    public static boolean noFilterOrOnlyHasTimeFilter(List<FilterStructure> filterStructures) {
        if (filterStructures == null || filterStructures.size() == 0
                || (filterStructures.size() == 1 && filterStructures.get(0).noFilter())
                || (filterStructures.size() == 1 && filterStructures.get(0).onlyHasTimeFilter())) {
            return true;
        }
        return false;
    }

    public static DynamicOneColumnData copy(DynamicOneColumnData data) {
        if (data == null) {
            // if data is null, return a DynamicOneColumnData which has no value
            return new DynamicOneColumnData(TSDataType.INT64, true);
        }

        DynamicOneColumnData ans = new DynamicOneColumnData(data.dataType, true);
        for (int i = 0;i < data.timeLength;i ++) {
            ans.putTime(data.getTime(i));
        }
        switch (data.dataType) {
            case INT32:
                for (int i = 0;i < data.valueLength;i++) {
                    ans.putInt(data.getInt(i));
                }
                break;
            case INT64:
                for (int i = 0;i < data.valueLength;i++) {
                    ans.putLong(data.getLong(i));
                }
                break;
            case FLOAT:
                for (int i = 0;i < data.valueLength;i++) {
                    ans.putFloat(data.getFloat(i));
                }
                break;
            case DOUBLE:
                for (int i = 0;i < data.valueLength;i++) {
                    ans.putDouble(data.getDouble(i));
                }
                break;
            case BOOLEAN:
                for (int i = 0;i < data.valueLength;i++) {
                    ans.putBoolean(data.getBoolean(i));
                }
                break;
            case TEXT:
                for (int i = 0;i < data.valueLength;i++) {
                    ans.putBinary(data.getBinary(i));
                }
                break;
            default:
                break;
        }

        return ans;
    }
}