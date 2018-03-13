package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperation;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.OverflowInsertDataReader;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.DigestVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.IntervalTimeVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

import static cn.edu.tsinghua.iotdb.query.reader.ReaderUtils.getSingleValueVisitorByDataType;

/**
 * <p>
 * InsertDynamicData is encapsulating class for memSeriesChunkIterator, overflowSeriesInsertReader, overflowOperationReader.
 * A hasNext and removeCurrentValue method is recommended.
 * </p>
 *
 * @author CGF
 */
public class InsertDynamicData {

    private static final Logger logger = LoggerFactory.getLogger(InsertDynamicData.class);

    private TSDataType dataType;
    private boolean hasNext = false;

    /** memtable data in memory iterator **/
    private Iterator<TimeValuePair> memSeriesChunkIterator;

    /** current TimeValuePair for memtable data **/
    private TimeValuePair currentTimeValuePair;

    /** overflow insert data reader **/
    private OverflowInsertDataReader overflowInsertDataReader;

    /** overflow update data reader **/
    private OverflowOperationReader overflowOperationReader;

    /** time filter for this series **/
    public SingleSeriesFilterExpression timeFilter;

    /** value filter for this series **/
    public SingleSeriesFilterExpression valueFilter;

    /** current satisfied time **/
    private long currentSatisfiedTime = -1;

    private int curSatisfiedIntValue;
    private boolean curSatisfiedBooleanValue;
    private long curSatisfiedLongValue;
    private float curSatisfiedFloatValue;
    private double curSatisfiedDoubleValue;
    private Binary curSatisfiedBinaryValue;

    private DigestVisitor digestVisitor = new DigestVisitor();
    private SingleValueVisitor singleValueVisitor;
    private SingleValueVisitor singleTimeVisitor;

    public InsertDynamicData(TSDataType dataType, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter,
                             RawSeriesChunk memRawSeriesChunk, OverflowInsertDataReader overflowInsertDataReader, OverflowOperationReader overflowOperationReader) {
        this.dataType = dataType;
        this.timeFilter = timeFilter;
        this.valueFilter = valueFilter;
        this.singleTimeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
        this.singleValueVisitor = getSingleValueVisitorByDataType(dataType, valueFilter);

        this.overflowInsertDataReader = overflowInsertDataReader;
        this.overflowOperationReader = overflowOperationReader;

        IntervalTimeVisitor intervalTimeVisitor = new IntervalTimeVisitor();
        if (memRawSeriesChunk != null) {
            if (!memRawSeriesChunk.isEmpty()) {
                if (intervalTimeVisitor.satisfy(timeFilter, memRawSeriesChunk.getMinTimestamp(), memRawSeriesChunk.getMaxTimestamp())) {
                    memSeriesChunkIterator = memRawSeriesChunk.getIterator();
                }
            }

            // TODO add value filter examination and overflow update operation optimization
        }
    }

    public TSDataType getDataType() {
        return this.dataType;
    }

    public long getCurrentMinTime() {
        return currentSatisfiedTime;
    }

    public int getCurrentIntValue() {
        return curSatisfiedIntValue;
    }

    public boolean getCurrentBooleanValue() {
        return curSatisfiedBooleanValue;
    }

    public long getCurrentLongValue() {
        return curSatisfiedLongValue;
    }

    public float getCurrentFloatValue() {
        return curSatisfiedFloatValue;
    }

    public double getCurrentDoubleValue() {
        return curSatisfiedDoubleValue;
    }

    public Binary getCurrentBinaryValue() {
        return curSatisfiedBinaryValue;
    }

    public Object getCurrentObjectValue() {
        switch (dataType) {
            case INT32:
                return getCurrentIntValue();
            case INT64:
                return getCurrentLongValue();
            case BOOLEAN:
                return getCurrentBooleanValue();
            case FLOAT:
                return getCurrentFloatValue();
            case DOUBLE:
                return getCurrentDoubleValue();
            case TEXT:
                return getCurrentBinaryValue();
            default:
                throw new UnSupportedDataTypeException("UnSupported aggregation datatype: " + dataType);
        }
    }

    public void removeCurrentValue() {
        hasNext = false;
    }

    public boolean hasNext() throws IOException {
        if (hasNext)
            return true;

        if (currentTimeValuePair == null && memSeriesChunkIterator != null && memSeriesChunkIterator.hasNext()) {
            currentTimeValuePair = memSeriesChunkIterator.next();
        }

        while (currentTimeValuePair != null) {
            if (overflowInsertDataReader.hasNext() && overflowInsertDataReader.peek().getTimestamp() <= currentTimeValuePair.getTimestamp()) {
                if (examineOverflowInsertValue()) {
                    if (overflowInsertDataReader.peek().getTimestamp() == currentTimeValuePair.getTimestamp()) {
                        overflowInsertDataReader.next();
                        getNextMemTimeValuePair();
                    } else {
                        overflowInsertDataReader.next();
                    }
                    hasNext = true;
                    return true;
                } else {
                    if (overflowInsertDataReader.peek().getTimestamp() == currentTimeValuePair.getTimestamp()) {
                        overflowInsertDataReader.next();
                        getNextMemTimeValuePair();
                    } else {
                        overflowInsertDataReader.next();
                    }
                }
            } else {
                if (examineCurrentTimeValuePair()) {
                    getNextMemTimeValuePair();
                    hasNext = true;
                    return true;
                } else {
                    getNextMemTimeValuePair();
                }
            }
        }

        while (overflowInsertDataReader.hasNext()) {

            //logger.debug("there exist overflow insert value, but not exist memtable value");

            if (examineOverflowInsertValue()) {
                overflowInsertDataReader.next();
                hasNext = true;
                return true;
            } else {
                overflowInsertDataReader.next();
            }
        }

        // TODO close file stream

        return false;
    }

    private boolean examineOverflowInsertValue() throws IOException {

        while (overflowOperationReader.hasNext() &&
                overflowOperationReader.getCurrentOperation().getRightBound() < overflowInsertDataReader.peek().getTimestamp()) {
            overflowOperationReader.next();
        }

        long time = overflowInsertDataReader.peek().getTimestamp();

        // TODO this could be removed if there exist timeFilter examination in overflowInsertDataReader
        if (!singleTimeVisitor.satisfyObject(time, timeFilter)) {
            return false;
        }

        switch (dataType) {
            case INT32:
                if (overflowOperationReader.hasNext() && overflowOperationReader.getCurrentOperation().verifyTime(time)) {
                    if (overflowOperationReader.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE
                            || !singleValueVisitor.satisfyObject(overflowOperationReader.getCurrentOperation().getValue().getInt(), valueFilter)) {
                        return false;
                    } else {
                        currentSatisfiedTime = time;
                        curSatisfiedIntValue = overflowOperationReader.getCurrentOperation().getValue().getInt();
                        return true;
                    }
                } else {
                    if (singleValueVisitor.satisfyObject(overflowInsertDataReader.peek().getValue().getInt(), valueFilter)) {
                        currentSatisfiedTime = time;
                        curSatisfiedIntValue = overflowInsertDataReader.peek().getValue().getInt();
                        return true;
                    }
                    return false;
                }
            case INT64:
                if (overflowOperationReader.hasNext() && overflowOperationReader.getCurrentOperation().verifyTime(time)) {
                    if (overflowOperationReader.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE
                            || !singleValueVisitor.satisfyObject(overflowOperationReader.getCurrentOperation().getValue().getLong(), valueFilter)) {
                        return false;
                    } else {
                        currentSatisfiedTime = time;
                        curSatisfiedLongValue = overflowOperationReader.getCurrentOperation().getValue().getLong();
                        return true;
                    }
                } else {
                    if (singleValueVisitor.satisfyObject(overflowInsertDataReader.peek().getValue().getLong(), valueFilter)) {
                        currentSatisfiedTime = time;
                        curSatisfiedLongValue = overflowInsertDataReader.peek().getValue().getLong();
                        return true;
                    }
                    return false;
                }
            case FLOAT:
                if (overflowOperationReader.hasNext() && overflowOperationReader.getCurrentOperation().verifyTime(time)) {
                    if (overflowOperationReader.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE
                            || !singleValueVisitor.satisfyObject(overflowOperationReader.getCurrentOperation().getValue().getFloat(), valueFilter)) {
                        return false;
                    } else {
                        currentSatisfiedTime = time;
                        curSatisfiedFloatValue = overflowOperationReader.getCurrentOperation().getValue().getFloat();
                        return true;
                    }
                } else {
                    if (singleValueVisitor.satisfyObject(overflowInsertDataReader.peek().getValue().getFloat(), valueFilter)) {
                        currentSatisfiedTime = time;
                        curSatisfiedFloatValue = overflowInsertDataReader.peek().getValue().getFloat();
                        return true;
                    }
                    return false;
                }
            case DOUBLE:
                if (overflowOperationReader.hasNext() && overflowOperationReader.getCurrentOperation().verifyTime(time)) {
                    if (overflowOperationReader.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE
                            || !singleValueVisitor.satisfyObject(overflowOperationReader.getCurrentOperation().getValue().getDouble(), valueFilter)) {
                        return false;
                    } else {
                        currentSatisfiedTime = time;
                        curSatisfiedDoubleValue = overflowOperationReader.getCurrentOperation().getValue().getDouble();
                        return true;
                    }
                } else {
                    if (singleValueVisitor.satisfyObject(overflowInsertDataReader.peek().getValue().getDouble(), valueFilter)) {
                        currentSatisfiedTime = time;
                        curSatisfiedDoubleValue = overflowInsertDataReader.peek().getValue().getDouble();
                        return true;
                    }
                    return false;
                }
            case BOOLEAN:
                if (overflowOperationReader.hasNext() && overflowOperationReader.getCurrentOperation().verifyTime(time)) {
                    if (overflowOperationReader.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE
                            || !singleValueVisitor.satisfyObject(overflowOperationReader.getCurrentOperation().getValue().getBoolean(), valueFilter)) {
                        return false;
                    } else {
                        currentSatisfiedTime = time;
                        curSatisfiedBooleanValue = overflowOperationReader.getCurrentOperation().getValue().getBoolean();
                        return true;
                    }
                } else {
                    if (singleValueVisitor.satisfyObject(overflowInsertDataReader.peek().getValue().getBoolean(), valueFilter)) {
                        currentSatisfiedTime = time;
                        curSatisfiedBooleanValue = overflowInsertDataReader.peek().getValue().getBoolean();
                        return true;
                    }
                    return false;
                }
            case TEXT:
                if (overflowOperationReader.hasNext() && overflowOperationReader.getCurrentOperation().verifyTime(time)) {
                    if (overflowOperationReader.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE
                            || !singleValueVisitor.satisfyObject(overflowOperationReader.getCurrentOperation().getValue().getBinary(), valueFilter)) {
                        return false;
                    } else {
                        currentSatisfiedTime = time;
                        curSatisfiedBinaryValue = overflowOperationReader.getCurrentOperation().getValue().getBinary();
                        return true;
                    }
                } else {
                    if (singleValueVisitor.satisfyObject(overflowInsertDataReader.peek().getValue().getBinary(), valueFilter)) {
                        currentSatisfiedTime = time;
                        curSatisfiedBinaryValue = overflowInsertDataReader.peek().getValue().getBinary();
                        return true;
                    }
                    return false;
                }
        }

        return false;
    }

    private boolean examineCurrentTimeValuePair() {

        while (overflowOperationReader.hasNext() && overflowOperationReader.getCurrentOperation().getRightBound() < currentTimeValuePair.getTimestamp()) {
            overflowOperationReader.next();
        }

        long time = currentTimeValuePair.getTimestamp();
        if (!singleTimeVisitor.satisfyObject(time, timeFilter)) {
            return false;
        }

        switch (dataType) {
            case INT32:
                if (overflowOperationReader.hasNext() && overflowOperationReader.getCurrentOperation().verifyTime(time)) {
                    if (overflowOperationReader.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE
                            || !singleValueVisitor.satisfyObject(overflowOperationReader.getCurrentOperation().getValue().getInt(), valueFilter)) {
                        return false;
                    } else {
                        currentSatisfiedTime = time;
                        curSatisfiedIntValue = overflowOperationReader.getCurrentOperation().getValue().getInt();
                        return true;
                    }
                } else {
                    if (singleValueVisitor.satisfyObject(currentTimeValuePair.getValue().getInt(), valueFilter)) {
                        currentSatisfiedTime = time;
                        curSatisfiedIntValue = currentTimeValuePair.getValue().getInt();
                        return true;
                    }
                    return false;
                }
            case INT64:
                if (overflowOperationReader.hasNext() && overflowOperationReader.getCurrentOperation().verifyTime(time)) {
                    if (overflowOperationReader.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE
                            || !singleValueVisitor.satisfyObject(overflowOperationReader.getCurrentOperation().getValue().getLong(), valueFilter)) {
                        return false;
                    } else {
                        currentSatisfiedTime = time;
                        curSatisfiedLongValue = overflowOperationReader.getCurrentOperation().getValue().getLong();
                        return true;
                    }
                } else {
                    if (singleValueVisitor.satisfyObject(currentTimeValuePair.getValue().getLong(), valueFilter)) {
                        currentSatisfiedTime = time;
                        curSatisfiedLongValue = currentTimeValuePair.getValue().getLong();
                        return true;
                    }
                    return false;
                }
            case FLOAT:
                if (overflowOperationReader.hasNext() && overflowOperationReader.getCurrentOperation().verifyTime(time)) {
                    if (overflowOperationReader.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE
                            || !singleValueVisitor.satisfyObject(overflowOperationReader.getCurrentOperation().getValue().getFloat(), valueFilter)) {
                        return false;
                    } else {
                        currentSatisfiedTime = time;
                        curSatisfiedFloatValue = overflowOperationReader.getCurrentOperation().getValue().getFloat();
                        return true;
                    }
                } else {
                    if (singleValueVisitor.satisfyObject(currentTimeValuePair.getValue().getFloat(), valueFilter)) {
                        currentSatisfiedTime = time;
                        curSatisfiedFloatValue = currentTimeValuePair.getValue().getFloat();
                        return true;
                    }
                    return false;
                }
            case DOUBLE:
                if (overflowOperationReader.hasNext() && overflowOperationReader.getCurrentOperation().verifyTime(time)) {
                    if (overflowOperationReader.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE
                            || !singleValueVisitor.satisfyObject(overflowOperationReader.getCurrentOperation().getValue().getDouble(), valueFilter)) {
                        return false;
                    } else {
                        currentSatisfiedTime = time;
                        curSatisfiedDoubleValue = overflowOperationReader.getCurrentOperation().getValue().getDouble();
                        return true;
                    }
                } else {
                    if (singleValueVisitor.satisfyObject(currentTimeValuePair.getValue().getDouble(), valueFilter)) {
                        currentSatisfiedTime = time;
                        curSatisfiedDoubleValue = currentTimeValuePair.getValue().getDouble();
                        return true;
                    }
                    return false;
                }
            case BOOLEAN:
                if (overflowOperationReader.hasNext() && overflowOperationReader.getCurrentOperation().verifyTime(time)) {
                    if (overflowOperationReader.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE
                            || !singleValueVisitor.satisfyObject(overflowOperationReader.getCurrentOperation().getValue().getBoolean(), valueFilter)) {
                        return false;
                    } else {
                        currentSatisfiedTime = time;
                        curSatisfiedBooleanValue = overflowOperationReader.getCurrentOperation().getValue().getBoolean();
                        return true;
                    }
                } else {
                    if (singleValueVisitor.satisfyObject(currentTimeValuePair.getValue().getBoolean(), valueFilter)) {
                        currentSatisfiedTime = time;
                        curSatisfiedBooleanValue = currentTimeValuePair.getValue().getBoolean();
                        return true;
                    }
                    return false;
                }
            case TEXT:
                if (overflowOperationReader.hasNext() && overflowOperationReader.getCurrentOperation().verifyTime(time)) {
                    if (overflowOperationReader.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE
                            || !singleValueVisitor.satisfyObject(overflowOperationReader.getCurrentOperation().getValue().getBinary(), valueFilter)) {
                        return false;
                    } else {
                        currentSatisfiedTime = time;
                        curSatisfiedBinaryValue = overflowOperationReader.getCurrentOperation().getValue().getBinary();
                        return true;
                    }
                } else {
                    if (singleValueVisitor.satisfyObject(currentTimeValuePair.getValue().getBinary(), valueFilter)) {
                        currentSatisfiedTime = time;
                        curSatisfiedBinaryValue = currentTimeValuePair.getValue().getBinary();
                        return true;
                    }
                    return false;
                }

        }

        return false;
    }

    private void getNextMemTimeValuePair() {
        if (memSeriesChunkIterator.hasNext()) {
            currentTimeValuePair = memSeriesChunkIterator.next();
        } else {
            currentTimeValuePair = null;
        }
    }
}
