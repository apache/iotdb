package cn.edu.tsinghua.iotdb.query.engine;

import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitorFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /**
     *  Merge the overflow insert data with the bufferwrite insert data.
     */
    public static List<Object> getOverflowInfoAndFilterDataInMem(SingleSeriesFilterExpression timeFilter,
                  SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter,
                  DynamicOneColumnData res, DynamicOneColumnData insertDataInMemory, List<Object> overflowParams) {

        List<Object> paramList = new ArrayList<>();

        if (res == null) {
            // time filter of overflow is not null, time filter should be as same as time filter of overflow.
            if (overflowParams.get(3) != null) {
                timeFilter = (SingleSeriesFilterExpression) overflowParams.get(3);
            }

            DynamicOneColumnData updateTrue = (DynamicOneColumnData) overflowParams.get(1);
            insertDataInMemory = getSatisfiedData(updateTrue, timeFilter, freqFilter, valueFilter, insertDataInMemory);

            DynamicOneColumnData overflowInsertTrue = (DynamicOneColumnData) overflowParams.get(0);
            // add insert records from memory in BufferWriter stage
            if (overflowInsertTrue == null) {
                overflowInsertTrue = insertDataInMemory;
            } else {
                overflowInsertTrue = mergeOverflowAndMemory(overflowInsertTrue, insertDataInMemory);
            }
            paramList.add(overflowInsertTrue);
            paramList.add(overflowParams.get(1));
            paramList.add(overflowParams.get(2));
            paramList.add(timeFilter);
        } else {
            paramList.add(res.insertTrue);
            paramList.add(res.updateTrue);
            paramList.add(res.updateFalse);
            paramList.add(res.timeFilter);
        }

        return paramList;
    }

    /**
     * Get satisfied values from a DynamicOneColumnData.
     *
     */
    private static DynamicOneColumnData getSatisfiedData(DynamicOneColumnData updateTrue, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter
            , SingleSeriesFilterExpression valueFilter, DynamicOneColumnData oneColData) {
        if (oneColData == null) {
            return null;
        }
        if (oneColData.valueLength == 0) {
            return oneColData;
        }

        // update the value in oneColData according to updateTrue
        oneColData = updateValueAccordingToUpdateTrue(updateTrue, oneColData);
        DynamicOneColumnData res = new DynamicOneColumnData(oneColData.dataType, true);
        SingleValueVisitor<?> timeVisitor = null;
        if (timeFilter != null) {
            timeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
        }
        SingleValueVisitor<?> valueVisitor = null;
        if (valueFilter != null) {
            valueVisitor = getSingleValueVisitorByDataType(oneColData.dataType, valueFilter);
        }

        switch (oneColData.dataType) {
            case BOOLEAN:
                for (int i = 0; i < oneColData.valueLength; i++) {
                    boolean v = oneColData.getBoolean(i);
                    if ((valueFilter == null && timeFilter == null)
                            || (valueFilter != null && timeFilter == null && valueVisitor.satisfyObject(v, valueFilter))
                            || (valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i)))
                            || (valueFilter != null && timeFilter != null && valueVisitor.satisfyObject(v, valueFilter)
                            && timeVisitor.verify(oneColData.getTime(i)))) {
                        res.putBoolean(v);
                        res.putTime(oneColData.getTime(i));
                    }
                }
                break;
            case DOUBLE:
                for (int i = 0; i < oneColData.valueLength; i++) {
                    double v = oneColData.getDouble(i);
                    if ((valueFilter == null && timeFilter == null)
                            || (valueFilter != null && timeFilter == null && valueVisitor.verify(v))
                            || (valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i)))
                            || (valueFilter != null && timeFilter != null && valueVisitor.verify(v)
                            && timeVisitor.verify(oneColData.getTime(i)))) {
                        res.putDouble(v);
                        res.putTime(oneColData.getTime(i));
                    }
                }
                break;
            case FLOAT:
                for (int i = 0; i < oneColData.valueLength; i++) {
                    float v = oneColData.getFloat(i);
                    if ((valueFilter == null && timeFilter == null)
                            || (valueFilter != null && timeFilter == null && valueVisitor.verify(v))
                            || (valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i)))
                            || (valueFilter != null && timeFilter != null && valueVisitor.verify(v)
                            && timeVisitor.verify(oneColData.getTime(i)))) {
                        res.putFloat(v);
                        res.putTime(oneColData.getTime(i));
                    }
                }
                break;
            case INT32:
                for (int i = 0; i < oneColData.valueLength; i++) {
                    int v = oneColData.getInt(i);
                    if ((valueFilter == null && timeFilter == null)
                            || (valueFilter != null && timeFilter == null && valueVisitor.verify(v))
                            || (valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i)))
                            || (valueFilter != null && timeFilter != null && valueVisitor.verify(v)
                            && timeVisitor.verify(oneColData.getTime(i)))) {
                        res.putInt(v);
                        res.putTime(oneColData.getTime(i));
                    }
                }
                break;
            case INT64:
                for (int i = 0; i < oneColData.valueLength; i++) {
                    long v = oneColData.getLong(i);
                    if ((valueFilter == null && timeFilter == null)
                            || (valueFilter != null && timeFilter == null && valueVisitor.verify(v))
                            || (valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i)))
                            || (valueFilter != null && timeFilter != null && valueVisitor.verify(v)
                            && timeVisitor.verify(oneColData.getTime(i)))) {
                        res.putLong(v);
                        res.putTime(oneColData.getTime(i));
                    }
                }
                break;
            case TEXT:
                for (int i = 0; i < oneColData.valueLength; i++) {
                    Binary v = oneColData.getBinary(i);
                    if ((valueFilter == null && timeFilter == null)
                            || (valueFilter != null && timeFilter == null && valueVisitor.satisfyObject(v, valueFilter))
                            || (valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i)))
                            || (valueFilter != null && timeFilter != null && valueVisitor.satisfyObject(v, valueFilter)
                            && timeVisitor.verify(oneColData.getTime(i)))) {
                        res.putBinary(v);
                        res.putTime(oneColData.getTime(i));
                    }
                }
                break;
            default:
                throw new UnSupportedDataTypeException("UnSupported data type for read: " + oneColData.dataType);
        }

        return res;
    }

    //    private boolean mayHasSatisfiedValue(SingleSeriesFilterExpression timeFilter, SingleValueVisitor<?> timeVisitor,
//                                         SingleSeriesFilterExpression valueFilter, SingleValueVisitor<?> valueVisitor) {
//        if ((valueFilter == null && timeFilter == null) ||
//                (valueFilter != null && timeFilter == null && valueVisitor.verify(v)) ||
//                (valueFilter == null && timeFilter != null && timeVisitor.verify(oneColData.getTime(i))) ||
//                (valueFilter != null && timeFilter != null && valueVisitor.verify(v) && timeVisitor.verify(oneColData.getTime(i)))) {
//            return true;
//        }
//        return false;
//    }

    private static DynamicOneColumnData updateValueAccordingToUpdateTrue(DynamicOneColumnData updateTrue
            , DynamicOneColumnData oneColData) {
        if (updateTrue == null) {
            return oneColData;
        }
        if (oneColData == null) {
            return null;
        }
        int idx = 0;
        for (int i = 0; i < updateTrue.valueLength; i++) {
            while (idx < oneColData.valueLength && updateTrue.getTime(i * 2 + 1) >= oneColData.getTime(idx)) {
                if (updateTrue.getTime(i * 2) <= oneColData.getTime(idx)) {
                    // oneColData.updateAValueFromDynamicOneColumnData(updateTrue, i, idx);
                    switch (oneColData.dataType) {
                        case BOOLEAN:
                            oneColData.setBoolean(idx, updateTrue.getBoolean(i));
                            break;
                        case INT32:
                            oneColData.setInt(idx, updateTrue.getInt(i));
                            break;
                        case INT64:
                            oneColData.setLong(idx, updateTrue.getLong(i));
                            break;
                        case FLOAT:
                            oneColData.setFloat(idx, updateTrue.getFloat(i));
                            break;
                        case DOUBLE:
                            oneColData.setDouble(idx, updateTrue.getDouble(i));
                            break;
                        case TEXT:
                            oneColData.setBinary(idx, updateTrue.getBinary(i));
                            break;
                        case ENUMS:
                        default:
                            throw new UnSupportedDataTypeException(String.valueOf(oneColData.dataType));
                    }
                }
                idx++;
            }
            if (idx >= oneColData.valueLength) {
                break;
            }
        }

        return oneColData;
    }

    private static SingleValueVisitor<?> getSingleValueVisitorByDataType(TSDataType type, SingleSeriesFilterExpression filter) {
        switch (type) {
            case INT32:
                return new SingleValueVisitor<Integer>(filter);
            case INT64:
                return new SingleValueVisitor<Long>(filter);
            case FLOAT:
                return new SingleValueVisitor<Float>(filter);
            case DOUBLE:
                return new SingleValueVisitor<Double>(filter);
            default:
                return SingleValueVisitorFactory.getSingleValueVisitor(type);
        }
    }


    /**
     * Merge insert data in overflow and buffer writer memory.<br>
     * Important: If there is two fields whose timestamp are equal, use the value
     * from overflow.
     *
     * @param overflowData data in overflow insert
     * @param memoryData data in buffer write insert
     * @return merge result of the overflow and memory insert
     */
    private static DynamicOneColumnData mergeOverflowAndMemory(
            DynamicOneColumnData overflowData, DynamicOneColumnData memoryData) {
        if (overflowData == null && memoryData == null) {
            return null;
        } else if (overflowData != null && memoryData == null) {
            return overflowData;
        } else if (overflowData == null) {
            return memoryData;
        }

        DynamicOneColumnData res = new DynamicOneColumnData(overflowData.dataType, true);
        int overflowIdx = 0;
        int memoryIdx = 0;
        while (overflowIdx < overflowData.valueLength || memoryIdx < memoryData.valueLength) {
            while (overflowIdx < overflowData.valueLength && (memoryIdx >= memoryData.valueLength ||
                    memoryData.getTime(memoryIdx) >= overflowData.getTime(overflowIdx))) {
                res.putTime(overflowData.getTime(overflowIdx));
                res.putAValueFromDynamicOneColumnData(overflowData, overflowIdx);
                if (memoryIdx < memoryData.valueLength && memoryData.getTime(memoryIdx) == overflowData.getTime(overflowIdx)) {
                    memoryIdx++;
                }
                overflowIdx++;
            }

            while (memoryIdx < memoryData.valueLength && (overflowIdx >= overflowData.valueLength ||
                    overflowData.getTime(overflowIdx) > memoryData.getTime(memoryIdx))) {
                res.putTime(memoryData.getTime(memoryIdx));
                res.putAValueFromDynamicOneColumnData(memoryData, memoryIdx);
                memoryIdx++;
            }
        }

        return res;
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
}