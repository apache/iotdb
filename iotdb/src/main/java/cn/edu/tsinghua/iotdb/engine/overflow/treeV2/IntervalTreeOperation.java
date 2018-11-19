package cn.edu.tsinghua.iotdb.engine.overflow.treeV2;

import cn.edu.tsinghua.iotdb.engine.overflow.index.CrossRelation;
import cn.edu.tsinghua.iotdb.engine.overflow.index.IntervalRelation;
import cn.edu.tsinghua.iotdb.engine.overflow.treeV2.IntervalTree;
import cn.edu.tsinghua.iotdb.engine.overflow.utils.MergeStatus;
import cn.edu.tsinghua.iotdb.engine.overflow.utils.OverflowOpType;
import cn.edu.tsinghua.iotdb.engine.overflow.utils.TimePair;
import cn.edu.tsinghua.iotdb.exception.UnSupportedOverflowOpTypeException;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowDeleteOperation;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperation;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowUpdateOperation;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.And;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.GtEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.LongInterval;
import cn.edu.tsinghua.tsfile.timeseries.filter.verifier.FilterVerifier;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static cn.edu.tsinghua.tsfile.common.utils.ReadWriteStreamUtils.readUnsignedVarInt;


/**
 * @author CGF
 */
public class IntervalTreeOperation implements IIntervalTreeOperator {

    private static final Logger LOG = LoggerFactory.getLogger(IntervalTreeOperation.class);

    private IntervalTree index;

    private TSDataType dataType; // All operations data type in IntervalTreeOperation.

    private int valueSize; // byte occupation of data type.

    private long maxTimestamps;

    public IntervalTreeOperation(TSDataType dataType) {
        index = new IntervalTree(dataType);
        this.dataType = dataType;
        switch (dataType) {
            case INT32:
                this.valueSize = 4;
                break;
            case INT64:
                this.valueSize = 8;
                break;
            case FLOAT:
                this.valueSize = 4;
                break;
            case DOUBLE:
                this.valueSize = 8;
                break;
            case BOOLEAN:
                this.valueSize = 1;
                break;
            default:
                this.valueSize = -1;
        }
    }

    @Override
    public void insert(long t, byte[] value) {
        if (t > maxTimestamps) {
            return;
        }

        index.update(new TimePair(t, t, value, OverflowOpType.INSERT, dataType));
    }

    @Override
    public void update(long s, long e, byte[] value) {
        // s must >= e
        index.update(new TimePair(s, e, value, OverflowOpType.UPDATE, dataType));

        maxTimestamps = Math.max(maxTimestamps, e);
    }

    @Override
    public void delete(long timestamp) {
        // timestamp-1 for TSFile sql expression
        // in TSFile sql expression, there only exists "DELETE X where Y < Z"
        if (timestamp > 0)
            index.update(new TimePair(0, timestamp, null, OverflowOpType.DELETE, dataType));
        else
            // for "DELETE X where Y = 0"
            index.update(new TimePair(0, timestamp, null, OverflowOpType.DELETE, dataType));

        maxTimestamps = Math.max(maxTimestamps, timestamp);
    }

    /**
     * Notice that serialization file doesn't store the value when DELETE operation.
     *
     * @param out - serialization output stream.
     * @throws IOException - serialization problem
     */
    @Override
    public void toBytes(OutputStream out) throws IOException {
        index.midOrderSerialize(out);
        index = new IntervalTree();
    }

    /**
     * used for queryFileBlock()
     *
     * @param doc            - DynamicOneColumn to be put
     * @param s              - start time
     * @param e              - end time
     * @param value          - overflow value
     * @param overflowOpType - OverflowOpType
     * @param dataType       - TSDataType
     */
    private void putValueUsingFileData(DynamicOneColumnData doc, long s, long e,
                                       byte[] value, OverflowOpType overflowOpType, TSDataType dataType) {
        switch (overflowOpType) {
            case INSERT:
                putTimePair(doc, s, -e);
                break;
            case UPDATE:
                putTimePair(doc, s, e);
                break;
            case DELETE:
                putTimePair(doc, -s, -e);
                break;
            default:
                throw new UnSupportedOverflowOpTypeException("Unsupported Overflow operation type.");
        }

        switch (dataType) {
            case INT32:
                if (overflowOpType != OverflowOpType.DELETE)
                    doc.putInt(BytesUtils.bytesToInt(value));
                else
                    doc.putInt(0);
                break;
            case INT64:
                if (overflowOpType != OverflowOpType.DELETE)
                    doc.putLong(BytesUtils.bytesToLong(value));
                else
                    doc.putLong(0L);
                break;
            case FLOAT:
                if (overflowOpType != OverflowOpType.DELETE)
                    doc.putFloat(BytesUtils.bytesToFloat(value));
                else
                    doc.putFloat(0.0f);
                break;
            case DOUBLE:
                if (overflowOpType != OverflowOpType.DELETE)
                    doc.putDouble(BytesUtils.bytesToDouble(value));
                else
                    doc.putDouble(0);
                break;  
            case BOOLEAN:
                if (overflowOpType != OverflowOpType.DELETE)
                    doc.putBoolean(BytesUtils.bytesToBool(value));
                else
                    doc.putBoolean(false);
                break;
            case TEXT:
                if (overflowOpType != OverflowOpType.DELETE)
                    doc.putBinary(Binary.valueOf(BytesUtils.bytesToString(value)));
                else
                    doc.putBinary(new Binary(""));
                break;
            default:
                throw new UnSupportedDataTypeException("Unsupported TSFile data type.");
        }
    }

    /**
     * Put value into newer DynamicOneColumnData.
     *
     * @param ansData   new DynamicOneColumn
     * @param startTime - start time
     * @param endTime   - end time
     * @param doc       - previous DynamicOneColumn
     * @param i         - index
     * @param dataType  - TSDataType
     */
    private void putValueUseDynamic(DynamicOneColumnData ansData, long startTime, long endTime,
                                    DynamicOneColumnData doc, int i, OverflowOpType overflowOpType, TSDataType dataType) {

        // don't care the plus or minus of the value of start time or end time.
        switch (overflowOpType) {
            case INSERT:
                putTimePair(ansData, Math.abs(startTime), -Math.abs(endTime));
                break;
            case UPDATE:
                putTimePair(ansData, Math.abs(startTime), Math.abs(endTime));
                break;
            case DELETE:
                putTimePair(ansData, -Math.abs(startTime), -Math.abs(endTime));
                break;
            default:
                throw new UnSupportedOverflowOpTypeException("Unsupported Overflow operation type.");
        }

        switch (dataType) {
            case INT32:
                ansData.putInt(doc.getInt(i));
                break;
            case INT64:
                ansData.putLong(doc.getLong(i));
                break;
            case FLOAT:
                ansData.putFloat(doc.getFloat(i));
                break;
            case DOUBLE:
                ansData.putDouble(doc.getDouble(i));
                break;
            case BOOLEAN:
            	ansData.putBoolean(doc.getBoolean(i));
                break;
            case TEXT:
                ansData.putBinary(doc.getBinary(i));
                break;
            default:
                throw new UnSupportedDataTypeException("Unsupported TSFile data type.");
        }
    }

    /**
     * Notice that both s and e are >= 0. </br>
     *
     * @param s      - start time
     * @param e      - end time
     * @param value  - time pair value
     * @param status - time pair merge status
     * @return - TimePair tp has both positive start time and end time values.
     */
    private TimePair constructTimePair(long s, long e, byte[] value, MergeStatus status) {
        if (s <= 0 && e <= 0) // s<=0 && e<=0 for delete operation
            return new TimePair(-s, -e, value, OverflowOpType.DELETE, status);
        else if (s > 0 && e < 0) {
            return new TimePair(s, -e, value, OverflowOpType.INSERT, status);
        } else {
            return new TimePair(s, e, value, OverflowOpType.UPDATE, status);
        }
    }

    /**
     * Notice that both s and e are >= 0. </br>
     * <p>
     * Return the time pair using given start time value and end time value.
     *
     * @param startTime - start time
     * @param endTime   - end time
     * @param status    - time pair merge status
     * @return - TimePair tp has both positive start time and end time values.
     */
    private TimePair constructTimePair(long startTime, long endTime, MergeStatus status) {
        if (startTime <= 0 && endTime <= 0) // s<=0 && e<=0 for delete operation
            return new TimePair(-startTime, -endTime, null, OverflowOpType.DELETE, status);
        else if (startTime > 0 && endTime < 0) {
            return new TimePair(startTime, -endTime, null, OverflowOpType.INSERT, status);
        } else {
            return new TimePair(startTime, endTime, null, OverflowOpType.UPDATE, status);
        }
    }

    /**
     * Read data from overflow file which has been serialized,
     * return the correspond time pair structure. </br>
     *
     * @param inputStream - InputStream
     * @param valueSize   - value byte size
     * @return - TimePair
     * @throws IOException - IOException
     */
    private TimePair readTimePairFromOldOverflow(InputStream inputStream, int valueSize) throws IOException {
        long s = BytesUtils.readLong(inputStream);
        long e = BytesUtils.readLong(inputStream);
        if (s <= 0 && e < 0) {  // DELETE OPERATION. s may < 0.
            return constructTimePair(s, e, null, MergeStatus.MERGING);
        } else { // INSERT or UPDATE OPERATION
            if (valueSize == -1) { // var length read method
                int len = readUnsignedVarInt(inputStream);
                byte[] stringBytes = new byte[len];
                inputStream.read(stringBytes);
                return constructTimePair(s, e, stringBytes, MergeStatus.MERGING);
            }
            return constructTimePair(s, e, BytesUtils.safeReadInputStreamToBytes(valueSize, inputStream), MergeStatus.MERGING);
        }
    }

    @Override
    public DynamicOneColumnData queryFileBlock(SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter,
                                               InputStream inputStream, DynamicOneColumnData newData) throws IOException {

        DynamicOneColumnData ans = new DynamicOneColumnData(dataType, true); // merge answer
        int i = 0;
        TimePair oldTimePair = new TimePair(-1, -1, MergeStatus.DONE);
        TimePair newTimePair = constructTimePair(-1, -1, MergeStatus.DONE);
        long L = Long.MIN_VALUE;
        long R = Long.MIN_VALUE;

        while (newData != null && i < newData.valueLength) {

            if (newTimePair.mergestatus == MergeStatus.DONE) {
                // (L,R) represent new time pair range.
                L = newData.getTime(i * 2);
                R = newData.getTime(i * 2 + 1);
                newTimePair = constructTimePair(newData.getTime(i * 2), newData.getTime(i * 2 + 1), MergeStatus.MERGING);
            }

            if (inputStream.available() == 0 && oldTimePair.mergestatus == MergeStatus.DONE) {  // old overflow file is empty, but newData is not empty
                putValueUseDynamic(ans, L, R, newData, i++, newTimePair.opType, dataType);
                newTimePair.reset();
                continue;
            }

            if (L <= 0 && R < 0) {  // DELETE OPERATION
                long rightTime = -R;
                while (inputStream.available() != 0 || oldTimePair.mergestatus == MergeStatus.MERGING) {
                    if (oldTimePair.mergestatus == MergeStatus.DONE) {
                        oldTimePair = readTimePairFromOldOverflow(inputStream, valueSize);
                    }
                    if (rightTime >= oldTimePair.e) {        // e.g. [0, 12]  [3, 10]
                        oldTimePair.reset();
                    } else if (rightTime < oldTimePair.s) {  // e.g. [0, 12] [14, 16]
                        putValueUseDynamic(ans, L, R, newData, i++, OverflowOpType.DELETE, dataType);
                        newTimePair.reset();
                        break;
                    } else {                // e.g. [0, 12]  [10, 20]
                        oldTimePair.s = rightTime + 1;
                        putValueUseDynamic(ans, L, R, newData, i++, OverflowOpType.DELETE, dataType);
                        newTimePair.reset();
                        break;
                    }
                }
            } else if (L > 0 && R < 0) {   // INSERT OPERATION
                R = -R;
                while (inputStream.available() != 0 || oldTimePair.mergestatus == MergeStatus.MERGING) {
                    if (oldTimePair.mergestatus == MergeStatus.DONE) {
                        oldTimePair = readTimePairFromOldOverflow(inputStream, valueSize);
                    }
                    if (oldTimePair.mergestatus == MergeStatus.MERGING) {
                        CrossRelation relation = IntervalRelation.getRelation(newTimePair, oldTimePair);
                        if (relation == CrossRelation.RCOVERSL) {
                            if (oldTimePair.s == L && oldTimePair.e == R) { // newTimePair equals oldTimePair
                                putValueUseDynamic(ans, L, -R, newData, i++, newTimePair.opType, dataType);
                                newTimePair.reset();
                                oldTimePair.reset();
                                break;
                            } else if (oldTimePair.s == L) {
                                putValueUseDynamic(ans, L, -R, newData, i++, newTimePair.opType, dataType);
                                newTimePair.reset();
                                oldTimePair.s = R + 1;
                                break;
                            } else if (oldTimePair.e == R) {
                                putValueUsingFileData(ans, oldTimePair.s, L - 1, oldTimePair.v, oldTimePair.opType, dataType);
                                putValueUseDynamic(ans, L, -R, newData, i++, newTimePair.opType, dataType);
                                newTimePair.reset();
                                oldTimePair.reset();
                                break;
                            } else {
                                putValueUsingFileData(ans, oldTimePair.s, L - 1, oldTimePair.v, oldTimePair.opType, dataType);
                                putValueUseDynamic(ans, L, -R, newData, i++, newTimePair.opType, dataType);
                                newTimePair.reset();
                                oldTimePair.s = R + 1;
                                break;
                            }
                        } else if (relation == CrossRelation.LFIRST) { // newTimePair first
                            putValueUseDynamic(ans, L, -R, newData, i++, newTimePair.opType, dataType);
                            newTimePair.reset();
                            break;
                        } else if (relation == CrossRelation.RFIRST) { // oldTimePair first
                            putValueUsingFileData(ans, oldTimePair.s, oldTimePair.e, oldTimePair.v, oldTimePair.opType, dataType);
                            oldTimePair.reset();
                        } else {
                            // relation == CrossRelation.LCOVERSR) : newerTimePair covers oldTimePair, newTimePair width must > 1, impossible
                            // relation == CrossRelation.LFIRSTCROSS) :  newTimePair first cross, impossible
                            // relation == CrossRelation.RFIRSTCROSS) :  oldTimePair first cross, impossible

                            LOG.error("unreachable method, new time pair:" + newTimePair.toString() + ". old time pair"
                            + oldTimePair.toString());
                        }
                    }
                }
            } else {  // UPDATE OPERATION
                while (inputStream.available() != 0 || oldTimePair.mergestatus == MergeStatus.MERGING) {
                    if (oldTimePair.mergestatus == MergeStatus.DONE) {
                        oldTimePair = readTimePairFromOldOverflow(inputStream, valueSize);
                    }
                    if (oldTimePair.mergestatus == MergeStatus.MERGING) {
                        CrossRelation relation = IntervalRelation.getRelation(newTimePair, oldTimePair);
                        if (relation == CrossRelation.LCOVERSR) { // newTimePair covers oldTimePair
                            if (oldTimePair.opType == OverflowOpType.INSERT) {
                                if (oldTimePair.s == L) {
                                    putValueUseDynamic(ans, oldTimePair.s, -oldTimePair.e, newData, i, OverflowOpType.INSERT, dataType);
                                    L = oldTimePair.s + 1;
                                    oldTimePair.reset();
                                } else if (oldTimePair.e == R) {
                                    putValueUseDynamic(ans, L, oldTimePair.s - 1, newData, i, OverflowOpType.UPDATE, dataType);
                                    putValueUseDynamic(ans, oldTimePair.s, -oldTimePair.e, newData, i, OverflowOpType.INSERT, dataType);
                                    i++;
                                    newTimePair.reset();
                                    oldTimePair.reset();
                                } else {
                                    putValueUseDynamic(ans, L, oldTimePair.s - 1, newData, i, OverflowOpType.UPDATE, dataType);
                                    putValueUseDynamic(ans, oldTimePair.s, -oldTimePair.e, newData, i, OverflowOpType.INSERT, dataType);
                                    L = oldTimePair.e + 1;
                                    oldTimePair.reset();
                                }
                            } else if (oldTimePair.opType == OverflowOpType.DELETE) {
                                if (oldTimePair.s == L) {
                                    putValueUseDynamic(ans, -oldTimePair.s, -oldTimePair.e, newData, i, OverflowOpType.DELETE, dataType);
                                    L = oldTimePair.e + 1;
                                    oldTimePair.reset();
                                } else if (oldTimePair.e == R) {
                                    // the start time of DELETE time pair > 0
                                    putValueUseDynamic(ans, L, oldTimePair.s - 1, newData, i, OverflowOpType.UPDATE, dataType);
                                    i++;
                                    newTimePair.reset();
                                    oldTimePair.reset();
                                } else {
                                    // the start time of DELETE time pair > 0
                                    putValueUseDynamic(ans, L, oldTimePair.s - 1, newData, i, OverflowOpType.UPDATE, dataType);
                                    L = oldTimePair.e + 1;
                                    oldTimePair.reset();
                                }
                            } else {
                                oldTimePair.reset();
                            }
                        } else if (relation == CrossRelation.RCOVERSL) { // oldTimePair covers newTimePair
                            if (oldTimePair.s == L && oldTimePair.e == R) { // newTimePair equals oldTimePair
                                if (oldTimePair.opType == OverflowOpType.DELETE) {
                                    putValueUsingFileData(ans, oldTimePair.s, oldTimePair.e, oldTimePair.v, oldTimePair.opType, dataType);
                                    i++;
                                    oldTimePair.reset();
                                    break;
                                } else if (oldTimePair.opType == OverflowOpType.UPDATE) {
                                    putValueUseDynamic(ans, L, R, newData, i++, OverflowOpType.UPDATE, dataType);
                                    newTimePair.reset();
                                    oldTimePair.reset();
                                    break;
                                } else if (oldTimePair.opType == OverflowOpType.INSERT) {
                                    putValueUseDynamic(ans, L, -R, newData, i++, OverflowOpType.INSERT, dataType);
                                    newTimePair.reset();
                                    oldTimePair.reset();
                                    break;
                                }
                            } else if (oldTimePair.s == L) {
                                if (oldTimePair.opType == OverflowOpType.DELETE) {
                                    putValueUsingFileData(ans, oldTimePair.s, R, oldTimePair.v, oldTimePair.opType, dataType);
                                    oldTimePair.s = R + 1;
                                    i++;
                                    newTimePair.reset();
                                    break;
                                } else if (oldTimePair.opType == OverflowOpType.UPDATE) {
                                    putValueUseDynamic(ans, L, R, newData, i++, OverflowOpType.UPDATE, dataType);
                                    newTimePair.reset();
                                    oldTimePair.s = R + 1;
                                    break;
                                } else {
                                    // oldTimePair.opType == OverflowOpType.INSERT
                                    // oldTimePair covers newTimePair, but oldTimePair is INSERT operation. impossible
                                    LOG.error("unreachable method, new time pair : {}, old time pair : {}, ",
                                            newTimePair.toString(),
                                            oldTimePair.toString());
                                }
                            } else if (oldTimePair.e == R) {
                                if (oldTimePair.opType == OverflowOpType.DELETE) {
                                    putValueUsingFileData(ans, oldTimePair.s, oldTimePair.e, oldTimePair.v, oldTimePair.opType, dataType);
                                    oldTimePair.reset();
                                    i++;
                                    newTimePair.reset();
                                    break;
                                } else if (oldTimePair.opType == OverflowOpType.UPDATE) {
                                    putValueUsingFileData(ans, oldTimePair.s, L - 1, oldTimePair.v, oldTimePair.opType, dataType);
                                    putValueUseDynamic(ans, L, R, newData, i++, OverflowOpType.UPDATE, dataType);
                                    newTimePair.reset();
                                    oldTimePair.reset();
                                    break;
                                } else if (oldTimePair.opType == OverflowOpType.INSERT) {
                                    // oldTimePair covers newTimePair, but oldTimePair is INSERT operation. impossible
                                    LOG.error("unreachable method, new time pair : {}, old time pair : {}, ",
                                            newTimePair.toString(),
                                            oldTimePair.toString());
                                }
                            } else {
                                if (oldTimePair.opType == OverflowOpType.DELETE) {
                                    i++;
                                    newTimePair.reset();
                                    break;
                                } else if (oldTimePair.opType == OverflowOpType.UPDATE) {
                                    putValueUsingFileData(ans, oldTimePair.s, L - 1, oldTimePair.v, oldTimePair.opType, dataType);
                                    putValueUseDynamic(ans, L, R, newData, i++, OverflowOpType.UPDATE, dataType);
                                    newTimePair.reset();
                                    oldTimePair.s = R + 1;
                                    break;
                                } else if (oldTimePair.opType == OverflowOpType.INSERT) {
                                    // oldTimePair covers newTimePair, but oldTimePair is INSERT operation. impossible
                                    LOG.error("unreachable method, new time pair : {}, old time pair : {}, ",
                                            newTimePair.toString(),
                                            oldTimePair.toString());
                                }
                            }
                        } else if (relation == CrossRelation.LFIRSTCROSS) {  // newTimePair first cross
                            // old TimePair could not be INSERT, DELETE
                            putValueUseDynamic(ans, L, R, newData, i++, OverflowOpType.UPDATE, dataType);
                            newTimePair.reset();
                            oldTimePair.s = R + 1;
                            break;
                        } else if (relation == CrossRelation.RFIRSTCROSS) { // oldTimePair first cross
                            if (oldTimePair.opType == OverflowOpType.DELETE) {
                                // delete operation need to be added to ans
                                putValueUsingFileData(ans, oldTimePair.s, L - 1, oldTimePair.v, oldTimePair.opType, dataType);
                                L = oldTimePair.e + 1;
                                oldTimePair.reset();
                            } else if (oldTimePair.opType == OverflowOpType.UPDATE) { // ??
                                putValueUsingFileData(ans, oldTimePair.s, L - 1, oldTimePair.v, oldTimePair.opType, dataType);
                                oldTimePair.reset();
                            }
                        } else if (relation == CrossRelation.LFIRST) { // newTimePair first
                            putValueUseDynamic(ans, L, R, newData, i++, OverflowOpType.UPDATE, dataType);
                            newTimePair.reset();
                            break;
                        } else if (relation == CrossRelation.RFIRST) { // oldTimePair first
                            putValueUsingFileData(ans, oldTimePair.s, oldTimePair.e, oldTimePair.v, oldTimePair.opType, dataType);
                            oldTimePair.reset();
                        }
                    }
                }
            }
        }

        // newData is empty, but overflow file still has data.
        while (inputStream.available() != 0 || oldTimePair.mergestatus == MergeStatus.MERGING) {
            if (oldTimePair.mergestatus == MergeStatus.DONE) {
                oldTimePair = readTimePairFromOldOverflow(inputStream, valueSize);
            }

            putValueUsingFileData(ans, oldTimePair.s, oldTimePair.e, oldTimePair.v, oldTimePair.opType, dataType);
            oldTimePair.reset();
        }

        return ans;
    }

    @Override
    public DynamicOneColumnData queryMemory(SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter,
            DynamicOneColumnData newerMemoryData) {
        if (newerMemoryData == null) {
            return index.dynamicQuery(timeFilter, dataType);
        }
        DynamicOneColumnData ans = new DynamicOneColumnData(dataType, true);
        DynamicOneColumnData oldData = index.dynamicQuery(timeFilter, dataType);

        int i = 0, j = 0; // i represents newMemoryData, j represents oldMemoryData
        TimePair oldTimePair = new TimePair(-1, -1, MergeStatus.DONE);
        TimePair newTimePair = new TimePair(-1, -1, MergeStatus.DONE);
        long L = Long.MIN_VALUE;
        long R = Long.MIN_VALUE;

        while (i < newerMemoryData.valueLength) {

            if (newTimePair.mergestatus == MergeStatus.DONE) {
                // (L,R) represent new time pair range.
                L = newerMemoryData.getTime(i * 2);
                R = newerMemoryData.getTime(i * 2 + 1);
                newTimePair = constructTimePair(newerMemoryData.getTime(i * 2), newerMemoryData.getTime(i * 2 + 1), MergeStatus.MERGING);
            }

            if (oldTimePair.mergestatus == MergeStatus.DONE && j >= oldData.valueLength) {  // old overflow file is empty, but newData is not empty
                putValueUseDynamic(ans, L, R, newerMemoryData, i++, newTimePair.opType, dataType);
                newTimePair.reset();
                continue;
            }

            if (L <= 0 && R < 0) {  // DELETE OPERATION
                long rightTime = -R;
                while (j < oldData.valueLength || oldTimePair.mergestatus == MergeStatus.MERGING) {
                    if (oldTimePair.mergestatus == MergeStatus.DONE) {
                        oldTimePair = constructTimePair(oldData.getTime(j * 2), oldData.getTime(j * 2 + 1), null, MergeStatus.MERGING);
                    }
                    if (rightTime >= oldTimePair.e) {        // e.g. [0, 12]  [3, 10]
                        oldTimePair.reset();
                        j++;
                    } else if (rightTime < oldTimePair.s) {  // e.g. [0, 12] [14, 16]
                        putValueUseDynamic(ans, L, R, newerMemoryData, i++, OverflowOpType.DELETE, dataType);
                        newTimePair.reset();
                        break;
                    } else {                // e.g. [0, 12]  [10, 20]
                        oldTimePair.s = rightTime + 1;
                        putValueUseDynamic(ans, L, R, newerMemoryData, i++, OverflowOpType.DELETE, dataType);
                        newTimePair.reset();
                        break;
                    }
                }
            } else if (L > 0 && R < 0) {   // INSERT OPERATION
                R = -R;
                while (j < oldData.valueLength || oldTimePair.mergestatus == MergeStatus.MERGING) {
                    if (oldTimePair.mergestatus == MergeStatus.DONE) {
                        oldTimePair = constructTimePair(oldData.getTime(j * 2), oldData.getTime(j * 2 + 1), null, MergeStatus.MERGING);
                    }
                    if (oldTimePair.mergestatus == MergeStatus.MERGING) {
                        CrossRelation relation = IntervalRelation.getRelation(newTimePair, oldTimePair);
                        if (relation == CrossRelation.RCOVERSL) {
                            if (oldTimePair.s == L && oldTimePair.e == R) { // newTimePair equals oldTimePair
                                putValueUseDynamic(ans, L, -R, newerMemoryData, i++, OverflowOpType.INSERT, dataType);
                                newTimePair.reset();
                                oldTimePair.reset();
                                j++;
                                break;
                            } else if (oldTimePair.s == L) {
                                putValueUseDynamic(ans, L, -R, newerMemoryData, i++, OverflowOpType.INSERT, dataType);
                                newTimePair.reset();
                                oldTimePair.s = R + 1;
                                break;
                            } else if (oldTimePair.e == R) {
                                putValueUseDynamic(ans, oldTimePair.s, L - 1, oldData, j++, oldTimePair.opType, dataType);
                                putValueUseDynamic(ans, L, -R, newerMemoryData, i++, OverflowOpType.INSERT, dataType);
                                newTimePair.reset();
                                oldTimePair.reset();
                                break;
                            } else {
                                putValueUseDynamic(ans, oldTimePair.s, L - 1, oldData, j, oldTimePair.opType, dataType);
                                putValueUseDynamic(ans, L, -R, newerMemoryData, i++, OverflowOpType.INSERT, dataType);
                                newTimePair.reset();
                                oldTimePair.s = R + 1;
                                break;
                            }
                        } else if (relation == CrossRelation.LFIRST) { // newTimePair first
                            putValueUseDynamic(ans, L, -R, newerMemoryData, i++, OverflowOpType.INSERT, dataType);
                            newTimePair.reset();
                            break;
                        } else if (relation == CrossRelation.RFIRST) { // oldTimePair first
                            putValueUseDynamic(ans, oldTimePair.s, oldTimePair.e, oldData, j, oldTimePair.opType, dataType);
                            oldTimePair.reset();
                            j++;
                        } else {
                            // relation == CrossRelation.LCOVERSR) : newerTimePair covers oldTimePair, newTimePair width must > 1, impossible
                            // relation == CrossRelation.LFIRSTCROSS) :  newTimePair first cross, impossible
                            // relation == CrossRelation.RFIRSTCROSS) :  oldTimePair first cross, impossible
                            LOG.error("unreachable method, new time pair : {}, old time pair : {}, ",
                                    newTimePair.toString(),
                                    oldTimePair.toString());
                        }
                    }
                }
            } else {  // UPDATE OPERATION
                while (j < oldData.valueLength || oldTimePair.mergestatus == MergeStatus.MERGING) {
                    if (oldTimePair.mergestatus == MergeStatus.DONE) {
                        oldTimePair = constructTimePair(oldData.getTime(j * 2), oldData.getTime(j * 2 + 1), null, MergeStatus.MERGING);
                    }
                    if (oldTimePair.mergestatus == MergeStatus.MERGING) {
                        CrossRelation relation = IntervalRelation.getRelation(newTimePair, oldTimePair);
                        if (relation == CrossRelation.LCOVERSR) { // newTimePair covers oldTimePair
                            if (oldTimePair.opType == OverflowOpType.INSERT) {
                                if (oldTimePair.s == L) {
                                    putValueUseDynamic(ans, oldTimePair.s, -oldTimePair.e, newerMemoryData, i, OverflowOpType.INSERT, dataType);
                                    L = oldTimePair.s + 1;
                                    oldTimePair.reset();
                                    j++;
                                } else if (oldTimePair.e == R) {
                                    putValueUseDynamic(ans, L, oldTimePair.s - 1, newerMemoryData, i, OverflowOpType.UPDATE, dataType);
                                    putValueUseDynamic(ans, oldTimePair.s, -oldTimePair.e, newerMemoryData, i, OverflowOpType.INSERT, dataType);
                                    i++;
                                    newTimePair.reset();
                                    oldTimePair.reset();
                                    j++;
                                } else {
                                    putValueUseDynamic(ans, L, oldTimePair.s - 1, newerMemoryData, i, OverflowOpType.UPDATE, dataType);
                                    putValueUseDynamic(ans, oldTimePair.s, -oldTimePair.e, newerMemoryData, i, OverflowOpType.INSERT, dataType);
                                    L = oldTimePair.e + 1;
                                    oldTimePair.reset();
                                    j++;
                                }
                            } else if (oldTimePair.opType == OverflowOpType.DELETE) {
                                if (oldTimePair.s == L) {
                                    putValueUseDynamic(ans, -oldTimePair.s, -oldTimePair.e, newerMemoryData, i, OverflowOpType.DELETE, dataType);
                                    L = oldTimePair.e + 1;
                                    oldTimePair.reset();
                                    j++;
                                } else if (oldTimePair.e == R) {
                                    // the start time of DELETE time pair > 0
                                    putValueUseDynamic(ans, L, oldTimePair.s - 1, newerMemoryData, i, OverflowOpType.UPDATE, dataType);
                                    i++;
                                    newTimePair.reset();
                                    oldTimePair.reset();
                                    j++;
                                } else {
                                    // the start time of DELETE time pair > 0
                                    putValueUseDynamic(ans, L, oldTimePair.s - 1, newerMemoryData, i, OverflowOpType.UPDATE, dataType);
                                    L = oldTimePair.e + 1;
                                    oldTimePair.reset();
                                    j++;
                                }
                            } else {
                                oldTimePair.reset();
                                j++;
                            }
                        } else if (relation == CrossRelation.RCOVERSL) { // oldTimePair covers newTimePair
                            if (oldTimePair.s == L && oldTimePair.e == R) { // newTimePair equals oldTimePair
                                if (oldTimePair.opType == OverflowOpType.DELETE) {
                                    putValueUseDynamic(ans, oldTimePair.s, oldTimePair.e, oldData, j, oldTimePair.opType, dataType);
                                    i++;
                                    oldTimePair.reset();
                                    j++;
                                    break;
                                } else if (oldTimePair.opType == OverflowOpType.UPDATE) {
                                    putValueUseDynamic(ans, L, R, newerMemoryData, i++, OverflowOpType.UPDATE, dataType);
                                    newTimePair.reset();
                                    oldTimePair.reset();
                                    j++;
                                    break;
                                } else if (oldTimePair.opType == OverflowOpType.INSERT) {
                                    putValueUseDynamic(ans, L, -R, newerMemoryData, i++, OverflowOpType.INSERT, dataType);
                                    newTimePair.reset();
                                    oldTimePair.reset();
                                    j++;
                                    break;
                                }
                            } else if (oldTimePair.s == L) {
                                if (oldTimePair.opType == OverflowOpType.DELETE) {
                                    putValueUseDynamic(ans, oldTimePair.s, R, oldData, j, oldTimePair.opType, dataType);
                                    oldTimePair.s = R + 1;
                                    i++;
                                    newTimePair.reset();
                                    break;
                                } else if (oldTimePair.opType == OverflowOpType.UPDATE) {
                                    putValueUseDynamic(ans, L, R, newerMemoryData, i++, OverflowOpType.UPDATE, dataType);
                                    newTimePair.reset();
                                    oldTimePair.s = R + 1;
                                    break;
                                } else {
                                    // oldTimePair.opType == OverflowOpType.INSERT
                                    // oldTimePair covers newTimePair, but oldTimePair is INSERT operation. impossible
                                    LOG.error("unreachable method, new time pair : {}, old time pair : {}, ",
                                            newTimePair.toString(),
                                            oldTimePair.toString());
                                }
                            } else if (oldTimePair.e == R) {
                                if (oldTimePair.opType == OverflowOpType.DELETE) {
                                    putValueUseDynamic(ans, oldTimePair.s, oldTimePair.e, oldData, j, oldTimePair.opType, dataType);
                                    oldTimePair.reset();
                                    j++;
                                    newTimePair.reset();
                                    i++;
                                    break;
                                } else if (oldTimePair.opType == OverflowOpType.UPDATE) {
                                    putValueUseDynamic(ans, oldTimePair.s, L - 1, oldData, j, oldTimePair.opType, dataType);
                                    putValueUseDynamic(ans, L, R, newerMemoryData, i++, OverflowOpType.UPDATE, dataType);
                                    newTimePair.reset();
                                    i++;
                                    oldTimePair.reset();
                                    j++;
                                    break;
                                } else if (oldTimePair.opType == OverflowOpType.INSERT) {
                                    // oldTimePair covers newTimePair, but oldTimePair is INSERT operation. impossible
                                    LOG.error("unreachable method, new time pair : {}, old time pair : {}, ",
                                            newTimePair.toString(),
                                            oldTimePair.toString());
                                }
                            } else {
                                if (oldTimePair.opType == OverflowOpType.DELETE) {
                                    i++;
                                    newTimePair.reset();
                                    break;
                                } else if (oldTimePair.opType == OverflowOpType.UPDATE) {
                                    putValueUseDynamic(ans, oldTimePair.s, L - 1, oldData, j, oldTimePair.opType, dataType);
                                    putValueUseDynamic(ans, L, R, newerMemoryData, i++, OverflowOpType.UPDATE, dataType);
                                    newTimePair.reset();
                                    oldTimePair.s = R + 1;
                                    break;
                                } else if (oldTimePair.opType == OverflowOpType.INSERT) {
                                    // oldTimePair covers newTimePair, but oldTimePair is INSERT operation. impossible
                                    LOG.error("unreachable method, new time pair : {}, old time pair : {}, ",
                                            newTimePair.toString(),
                                            oldTimePair.toString());
                                }
                            }
                        } else if (relation == CrossRelation.LFIRSTCROSS) {  // newTimePair first cross
                            // old TimePair could not be INSERT, DELETE
                            putValueUseDynamic(ans, L, R, newerMemoryData, i++, OverflowOpType.UPDATE, dataType);
                            newTimePair.reset();
                            oldTimePair.s = R + 1;
                            break;
                        } else if (relation == CrossRelation.RFIRSTCROSS) { // oldTimePair first cross
                            if (oldTimePair.opType == OverflowOpType.DELETE) {
                                // delete operation need to be added to ans
                                putValueUseDynamic(ans, oldTimePair.s, L - 1, oldData, j, oldTimePair.opType, dataType);
                                L = oldTimePair.e + 1;
                                oldTimePair.reset();
                                j++;
                            } else if (oldTimePair.opType == OverflowOpType.UPDATE) { // ??
                                putValueUseDynamic(ans, oldTimePair.s, L - 1, oldData, j, oldTimePair.opType, dataType);
                                oldTimePair.reset();
                                j++;
                            }
                        } else if (relation == CrossRelation.LFIRST) { // newTimePair first
                            putValueUseDynamic(ans, L, R, newerMemoryData, i++, OverflowOpType.UPDATE, dataType);
                            newTimePair.reset();
                            break;
                        } else if (relation == CrossRelation.RFIRST) { // oldTimePair first
                            putValueUseDynamic(ans, oldTimePair.s, oldTimePair.e, oldData, j, oldTimePair.opType, dataType);
                            oldTimePair.reset();
                            j++;
                        }
                    }
                }
            }
        }

        // newData is empty, but old memory still has data.
        while (j < oldData.valueLength || oldTimePair.mergestatus == MergeStatus.MERGING) {
            if (oldTimePair.mergestatus == MergeStatus.DONE) {
                oldTimePair = constructTimePair(oldData.getTime(j * 2), oldData.getTime(j * 2 + 1), null, MergeStatus.MERGING);
            }

            putValueUseDynamic(ans, oldTimePair.s, oldTimePair.e, oldData, j, oldTimePair.opType, dataType);
            oldTimePair.reset();
            j++;
        }

        return ans;
    }

    /**c
     * Given time filter, value filter and frequency filter,
     * return the correspond data which meet all the filters expression. </br>
     * List<Object> stores three DynamicOneColumnData structures, insertAdopt, updateAdopt
     * and updateNotAdopt.
     *
     * @param overflowData - overflow data
     * @return - List<OverflowOperation>
     */
    @Override
    public List<OverflowOperation> getDynamicList(DynamicOneColumnData overflowData) {

        List<OverflowOperation> overflowOperations = new ArrayList<>();

        for (int i = 0; i < overflowData.valueLength; i++) {
            long L = overflowData.getTime(i * 2);
            long R = overflowData.getTime(i * 2 + 1);

            if (L > 0 && R > 0) { // UPDATE
                switch (dataType) {
                    case INT32:
                        overflowOperations.add(new OverflowUpdateOperation(L, R, new TsPrimitiveType.TsInt(overflowData.getInt(i))));
                        break;
                    case INT64:
                        overflowOperations.add(new OverflowUpdateOperation(L, R, new TsPrimitiveType.TsLong(overflowData.getLong(i))));
                        break;
                    case FLOAT:
                        overflowOperations.add(new OverflowUpdateOperation(L, R, new TsPrimitiveType.TsFloat(overflowData.getFloat(i))));
                        break;
                    case DOUBLE:
                        overflowOperations.add(new OverflowUpdateOperation(L, R, new TsPrimitiveType.TsDouble(overflowData.getDouble(i))));
                        break;
                    case BOOLEAN:
                        overflowOperations.add(new OverflowUpdateOperation(L, R, new TsPrimitiveType.TsBoolean(overflowData.getBoolean(i))));
                        break;
                    case TEXT:
                        overflowOperations.add(new OverflowUpdateOperation(L, R, new TsPrimitiveType.TsBinary(overflowData.getBinary(i))));
                        break;
                    default:
                        throw new UnSupportedDataTypeException("Unsupported tsfile data type.");
                }
            } else if (L <= 0 && R <= 0) {    // DELETE
                overflowOperations.add(new OverflowDeleteOperation(-L, -R));
            }
        }

        return overflowOperations;
    }

    /**
     * both start time and end time are "long" datatype and occupy 16 bytes.
     *
     * @return - memory occupation
     */
    @Override
    public long calcMemSize() {
        return index.getTotalMemory();
    }

    /**
     * reset the status of IntervalTreeOperation.
     */
    public void reset() {
        index = new IntervalTree();
    }

    public boolean isEmpty() {
        return index.isEmpty();
    }

    private void putTimePair(DynamicOneColumnData data, long s, long e) {
        data.putTime(s);
        data.putTime(e);
    }

}
