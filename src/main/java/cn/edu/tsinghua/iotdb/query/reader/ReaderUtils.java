package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitorFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;


/**
 * Move some methods which has long code in <code>OverflowValueReader</code>
 * to this as utils method.
 *
 * @author CGF
 */
public class ReaderUtils {

    /**
     * -1: no updateTrue data, no updateFalse data.
     * 0: updateTrue data < updateFalse data.
     * 1: updateFalse data < updateTrue data.
     *
     * @param updateTrueIdx  index of updateTrue DynamicOneColumn
     * @param updateFalseIdx index of updateFalse DynamicOneColumn
     * @param updateTrue     updateTrue DynamicOneColumn
     * @param updateFalse    updateFalse DynamicOneColumn
     * @return the mode
     */
    public static int getNextMode(int updateTrueIdx, int updateFalseIdx, DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse) {
        if (updateTrueIdx > updateTrue.timeLength - 2 && updateFalseIdx > updateFalse.timeLength - 2) {
            return -1;
        } else if (updateTrueIdx <= updateTrue.timeLength - 2 && updateFalseIdx > updateFalse.timeLength - 2) {
            return 0;
        } else if (updateTrueIdx > updateTrue.timeLength - 2 && updateFalseIdx <= updateFalse.timeLength - 2) {
            return 1;
        } else {
            long t0 = updateTrue.getTime(updateTrueIdx);
            long t1 = updateFalse.getTime(updateFalseIdx);
            return t0 < t1 ? 0 : 1;
        }
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
     * <p>
     * Read one page data,
     * this page data may be changed by overflow operation, so the overflow parameter is required.
     * </p>
     *
     * @param dataType the <code>DataType</code> of the read page
     * @param pageTimeValues the decompressed timestamps of this page
     * @param decoder the <code>Decoder</code> of current page
     * @param page Page data
     * @param res same as result data, we need pass it many times
     * @param timeFilter time filter
     * @param freqFilter frequency filter
     * @param valueFilter value filter
     * @param insertMemoryData the memory data(bufferwrite along with overflow)
     * @param update update operation array, update[0] means updateTrue data, update[1] means updateFalse data
     * @return DynamicOneColumnData of the read result
     * @throws IOException TsFile read error
     * @param idx the read index of timeValues
     */
    public static DynamicOneColumnData readOnePage(TSDataType dataType, long[] pageTimeValues,
                                                   Decoder decoder, InputStream page, DynamicOneColumnData res,
                                                   SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter,
                                                   InsertDynamicData insertMemoryData, DynamicOneColumnData[] update, int[] idx) throws IOException {
        // This method is only used for aggregation function.

        // calculate current mode
        int mode = getNextMode(idx[0], idx[1], update[0], update[1]);

        try {
            SingleValueVisitor<?> timeVisitor = null;
            if (timeFilter != null) {
                timeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
            }
            SingleValueVisitor<?> valueVisitor = null;
            if (valueFilter != null) {
                valueVisitor = getSingleValueVisitorByDataType(dataType, valueFilter);
            }

            int timeIdx = 0;
            switch (dataType) {
                case INT32:
                    while (decoder.hasNext(page)) {
                        // put insert points that less than or equals to current
                        // Timestamp in page.
                        while (insertMemoryData.hasInsertData() && timeIdx < pageTimeValues.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimeValues[timeIdx]) {
                            res.putTime(insertMemoryData.getCurrentMinTime());
                            res.putInt(insertMemoryData.getCurrentIntValue());
                            res.insertTrueIndex++;

                            if (insertMemoryData.getCurrentMinTime() == pageTimeValues[timeIdx]) {
                                insertMemoryData.removeCurrentValue();
                                timeIdx++;
                                decoder.readInt(page);
                                if (!decoder.hasNext(page)) {
                                    break;
                                }
                            } else {
                                insertMemoryData.removeCurrentValue();
                            }
                        }

                        if (!decoder.hasNext(page)) {
                            break;
                        }
                        int v = decoder.readInt(page);
                        if (mode == -1) {

                            if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))) {
                                res.putInt(v);
                                res.putTime(pageTimeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 0) {
                            if (update[0].getTime(idx[0]) <= pageTimeValues[timeIdx]
                                    && pageTimeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
                                // update the value
                                if (timeFilter == null
                                        || timeVisitor.verify(pageTimeValues[timeIdx])) {
                                    res.putInt(update[0].getInt(idx[0] / 2));
                                    res.putTime(pageTimeValues[timeIdx]);
                                }
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))) {
                                res.putInt(v);
                                res.putTime(pageTimeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 1) {
                            if (update[1].getTime(idx[1]) <= pageTimeValues[timeIdx]
                                    && pageTimeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
                                // do nothing
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))) {
                                res.putInt(v);
                                res.putTime(pageTimeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        // Set the interval to next position that current time
                        // in page maybe be included.
                        while (mode != -1 && timeIdx < pageTimeValues.length
                                && pageTimeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
                            idx[mode] += 2;
                            mode = getNextMode(idx[0], idx[1], update[0], update[1]);
                        }
                    }
                    break;
                case BOOLEAN:
                    while (decoder.hasNext(page)) {
                        // put insert points
                        while (insertMemoryData.curIdx < insertMemoryData.valueLength && timeIdx < pageTimeValues.length
                                && insertMemoryData.getTime(insertMemoryData.curIdx) <= pageTimeValues[timeIdx]) {
                            res.putTime(insertMemoryData.getTime(insertMemoryData.curIdx));
                            res.putBoolean(insertMemoryData.getBoolean(insertMemoryData.curIdx));
                            insertMemoryData.curIdx++;
                            res.insertTrueIndex++;
                            // if equal, take value from insertTrue and skip one
                            // value from page
                            if (insertMemoryData.getTime(insertMemoryData.curIdx - 1) == pageTimeValues[timeIdx]) {
                                timeIdx++;
                                decoder.readBoolean(page);
                                if (!decoder.hasNext(page)) {
                                    break;
                                }
                            }
                        }

                        if (mode == -1) {
                            boolean v = decoder.readBoolean(page);
                            if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.satisfyObject(v, valueFilter))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.satisfyObject(v, valueFilter)
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))) {
                                res.putBoolean(v);
                                res.putTime(pageTimeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 0) {
                            boolean v = decoder.readBoolean(page);
                            if (update[0].getTime(idx[0]) <= pageTimeValues[timeIdx]
                                    && pageTimeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
                                // update the value
                                if (timeFilter == null
                                        || timeVisitor.verify(pageTimeValues[timeIdx])) {
                                    res.putBoolean(update[0].getBoolean(idx[0] / 2));
                                    res.putTime(pageTimeValues[timeIdx]);
                                }
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.satisfyObject(v, valueFilter))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.satisfyObject(v, valueFilter)
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))) {
                                res.putBoolean(v);
                                res.putTime(pageTimeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 1) {
                            boolean v = decoder.readBoolean(page);
                            if (update[1].getTime(idx[1]) <= pageTimeValues[timeIdx]
                                    && pageTimeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
                                // do nothing
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.satisfyObject(v, valueFilter))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.satisfyObject(v, valueFilter)
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))) {
                                res.putBoolean(v);
                                res.putTime(pageTimeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        while (mode != -1 && timeIdx < pageTimeValues.length
                                && pageTimeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
                            idx[mode] += 2;
                            mode = getNextMode(idx[0], idx[1], update[0], update[1]);
                        }
                    }
                    break;
                case INT64:
                    while (decoder.hasNext(page)) {
                        // put insert points
                        while (insertMemoryData.curIdx < insertMemoryData.valueLength && timeIdx < pageTimeValues.length
                                && insertMemoryData.getTime(insertMemoryData.curIdx) <= pageTimeValues[timeIdx]) {
                            res.putTime(insertMemoryData.getTime(insertMemoryData.curIdx));
                            res.putLong(insertMemoryData.getLong(insertMemoryData.curIdx));
                            insertMemoryData.curIdx++;
                            res.insertTrueIndex++;
                            // if equal, take value from insertTrue and skip one
                            // value from page
                            if (insertMemoryData.getTime(insertMemoryData.curIdx - 1) == pageTimeValues[timeIdx]) {
                                timeIdx++;
                                decoder.readLong(page);
                                if (!decoder.hasNext(page)) {
                                    break;
                                }
                            }
                        }
                        if (!decoder.hasNext(page)) {
                            break;
                        }
                        long v = decoder.readLong(page);
                        if (mode == -1) {
                            if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))) {
                                res.putLong(v);
                                res.putTime(pageTimeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 0) {
                            if (update[0].getTime(idx[0]) <= pageTimeValues[timeIdx]
                                    && pageTimeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
                                // update the value,需要和高飞再商量一下这个逻辑
                                if (timeFilter == null
                                        || timeVisitor.verify(pageTimeValues[timeIdx])) {
                                    res.putLong(update[0].getLong(idx[0] / 2));
                                    res.putTime(pageTimeValues[timeIdx]);
                                }
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))) {
                                res.putLong(v);
                                res.putTime(pageTimeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 1) {
                            if (update[1].getTime(idx[1]) <= pageTimeValues[timeIdx]
                                    && pageTimeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
                                // do nothing
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))) {
                                res.putLong(v);
                                res.putTime(pageTimeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        while (mode != -1 && timeIdx < pageTimeValues.length
                                && pageTimeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
                            idx[mode] += 2;
                            mode = getNextMode(idx[0], idx[1], update[0], update[1]);
                        }
                    }
                    break;
                case FLOAT:
                    while (decoder.hasNext(page)) {
                        // put insert points
                        while (insertMemoryData.curIdx < insertMemoryData.valueLength && timeIdx < pageTimeValues.length
                                && insertMemoryData.getTime(insertMemoryData.curIdx) <= pageTimeValues[timeIdx]) {
                            res.putTime(insertMemoryData.getTime(insertMemoryData.curIdx));
                            res.putFloat(insertMemoryData.getFloat(insertMemoryData.curIdx));
                            insertMemoryData.curIdx++;
                            res.insertTrueIndex++;
                            // if equal, take value from insertTrue and skip one
                            // value from page
                            if (insertMemoryData.getTime(insertMemoryData.curIdx - 1) == pageTimeValues[timeIdx]) {
                                timeIdx++;
                                decoder.readFloat(page);
                                if (!decoder.hasNext(page)) {
                                    break;
                                }
                            }
                        }
                        if (!decoder.hasNext(page)) {
                            break;
                        }
                        float v = decoder.readFloat(page);
                        if (mode == -1) {
                            if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))) {
                                res.putFloat(v);
                                res.putTime(pageTimeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 0) {
                            if (update[0].getTime(idx[0]) <= pageTimeValues[timeIdx]
                                    && pageTimeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
                                // update the value
                                if (timeFilter == null
                                        || timeVisitor.verify(pageTimeValues[timeIdx])) {
                                    res.putFloat(update[0].getFloat(idx[0] / 2));
                                    res.putTime(pageTimeValues[timeIdx]);
                                }
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))) {
                                res.putFloat(v);
                                res.putTime(pageTimeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 1) {
                            if (update[1].getTime(idx[1]) <= pageTimeValues[timeIdx]
                                    && pageTimeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
                                // do nothing
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))) {
                                res.putFloat(v);
                                res.putTime(pageTimeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        while (mode != -1 && timeIdx < pageTimeValues.length
                                && pageTimeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
                            idx[mode] += 2;
                            mode = getNextMode(idx[0], idx[1], update[0], update[1]);
                        }
                    }
                    break;
                case DOUBLE:
                    while (decoder.hasNext(page)) {
                        // put insert points
                        while (insertMemoryData.curIdx < insertMemoryData.valueLength && timeIdx < pageTimeValues.length
                                && insertMemoryData.getTime(insertMemoryData.curIdx) <= pageTimeValues[timeIdx]) {
                            res.putTime(insertMemoryData.getTime(insertMemoryData.curIdx));
                            res.putDouble(insertMemoryData.getDouble(insertMemoryData.curIdx));
                            insertMemoryData.curIdx++;
                            res.insertTrueIndex++;
                            // if equal, take value from insertTrue and skip one
                            // value from page
                            if (insertMemoryData.getTime(insertMemoryData.curIdx - 1) == pageTimeValues[timeIdx]) {
                                timeIdx++;
                                decoder.readDouble(page);
                                if (!decoder.hasNext(page)) {
                                    break;
                                }
                            }
                        }
                        if (!decoder.hasNext(page)) {
                            break;
                        }
                        double v = decoder.readDouble(page);
                        if (mode == -1) {
                            if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))) {
                                res.putDouble(v);
                                res.putTime(pageTimeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 0) {
                            if (update[0].getTime(idx[0]) <= pageTimeValues[timeIdx]
                                    && pageTimeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
                                // update the value
                                if (timeFilter == null
                                        || timeVisitor.verify(pageTimeValues[timeIdx])) {
                                    res.putDouble(update[0].getDouble(idx[0] / 2));
                                    res.putTime(pageTimeValues[timeIdx]);
                                }
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))) {
                                res.putDouble(v);
                                res.putTime(pageTimeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 1) {
                            if (update[1].getTime(idx[1]) <= pageTimeValues[timeIdx]
                                    && pageTimeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
                                // do nothing
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))) {
                                res.putDouble(v);
                                res.putTime(pageTimeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        while (mode != -1 && timeIdx < pageTimeValues.length
                                && pageTimeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
                            idx[mode] += 2;
                            mode = getNextMode(idx[0], idx[1], update[0], update[1]);
                        }
                    }
                    break;
                case TEXT:
                    while (decoder.hasNext(page)) {
                        // put insert points
                        while (insertMemoryData.curIdx < insertMemoryData.valueLength && timeIdx < pageTimeValues.length
                                && insertMemoryData.getTime(insertMemoryData.curIdx) <= pageTimeValues[timeIdx]) {
                            res.putTime(insertMemoryData.getTime(insertMemoryData.curIdx));
                            res.putBinary(insertMemoryData.getBinary(insertMemoryData.curIdx));
                            insertMemoryData.curIdx++;
                            res.insertTrueIndex++;
                            // if equal, take value from insertTrue and skip one
                            // value from page
                            if (insertMemoryData.getTime(insertMemoryData.curIdx - 1) == pageTimeValues[timeIdx]) {
                                timeIdx++;
                                decoder.readBinary(page);
                                if (!decoder.hasNext(page)) {
                                    break;
                                }
                            }
                        }
                        if (!decoder.hasNext(page)) {
                            break;
                        }
                        Binary v = decoder.readBinary(page);
                        if (mode == -1) {
                            if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.satisfyObject(v, valueFilter))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.satisfyObject(v, valueFilter)
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))) {
                                res.putBinary(v);
                                res.putTime(pageTimeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 0) {
                            if (update[0].getTime(idx[0]) <= pageTimeValues[timeIdx]
                                    && pageTimeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
                                // update the value
                                if (timeFilter == null
                                        || timeVisitor.verify(pageTimeValues[timeIdx])) {
                                    res.putBinary(update[0].getBinary(idx[0] / 2));
                                    res.putTime(pageTimeValues[timeIdx]);
                                }
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.satisfyObject(v, valueFilter))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.satisfyObject(v, valueFilter)
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))) {
                                res.putBinary(v);
                                res.putTime(pageTimeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 1) {
                            if (update[1].getTime(idx[1]) <= pageTimeValues[timeIdx]
                                    && pageTimeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
                                // do nothing
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.satisfyObject(v, valueFilter))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.satisfyObject(v, valueFilter)
                                    && timeVisitor.verify(pageTimeValues[timeIdx]))) {
                                res.putBinary(v);
                                res.putTime(pageTimeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        while (mode != -1 && timeIdx < pageTimeValues.length
                                && pageTimeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
                            idx[mode] += 2;
                            mode = getNextMode(idx[0], idx[1], update[0], update[1]);
                        }
                    }
                    break;
                default:
                    throw new IOException("Data type not support : " + dataType);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Don't forget to update the curIdx in updateTrue and updateFalse
        update[0].curIdx = idx[0];
        update[1].curIdx = idx[1];
        return res;
    }


    public static Pair<DynamicOneColumnData, Integer> readOnePage(TSDataType dataType, long[] pageTimeValues, int pageTimeIndex,
                                                                  Decoder decoder, InputStream page,
                                                                  SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
                                                                  List<Long> commonTimestamps, int commonTimestampsIndex,
                                                                  InsertDynamicData insertMemoryData, DynamicOneColumnData[] update, int[] updateIdx,
                                                                  DynamicOneColumnData lastAggreData) throws IOException {
        // This method is only used for aggregation function.

        // calculate current mode
        int mode = getNextMode(updateIdx[0], updateIdx[1], update[0], update[1]);

        try {
            SingleValueVisitor<?> timeVisitor = null;
            if (timeFilter != null) {
                timeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
            }

            switch (dataType) {
                case INT32:
                    while (decoder.hasNext(page)) {
                        long timestamp = commonTimestamps.get(commonTimestampsIndex);

                        while (insertMemoryData.hasInsertData() && pageTimeIndex < pageTimeValues.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimeValues[pageTimeIndex]) {

                            if (insertMemoryData.getCurrentMinTime() == timestamp) {
                                lastAggreData.putTime(insertMemoryData.getCurrentMinTime());
                                lastAggreData.putInt(insertMemoryData.getCurrentIntValue());
                                lastAggreData.insertTrueIndex++;
                                commonTimestampsIndex += 1;

                                if (insertMemoryData.getCurrentMinTime() == pageTimeValues[pageTimeIndex]) {
                                    insertMemoryData.removeCurrentValue();
                                    pageTimeIndex++;
                                    decoder.readInt(page);
                                    if (!decoder.hasNext(page)) {
                                        break;
                                    }
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }
                        }

                        if (!decoder.hasNext(page) || pageTimeIndex >= pageTimeValues.length) {
                            break;
                        }
                        if (commonTimestampsIndex >= commonTimestamps.size()) {
                            break;
                        }

                        int v = decoder.readInt(page);

                        // compare with commonTimestamps firstly
                        // then compare with time filter
                        // lastly compare with update operation
                        if (mode == -1) {
                            if (pageTimeValues[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if ((timeFilter == null || timeVisitor.verify(pageTimeValues[pageTimeIndex]))) {
                                    lastAggreData.putInt(v);
                                    lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                } else {
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeValues[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 0) {
                            if (pageTimeValues[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if (timeFilter == null || timeVisitor.verify(pageTimeValues[pageTimeIndex])) {
                                    if (update[0].getTime(updateIdx[0]) <= pageTimeValues[pageTimeIndex]
                                            && pageTimeValues[pageTimeIndex] <= update[0].getTime(updateIdx[0] + 1)) {
                                        lastAggreData.putInt(update[0].getInt(updateIdx[0] / 2));
                                        lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                    } else {
                                        lastAggreData.putInt(v);
                                        lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                    }
                                } else {
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeValues[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 1) {
                            if (pageTimeValues[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if (timeFilter == null || timeVisitor.verify(pageTimeValues[pageTimeIndex])) {
                                    if (update[1].getTime(updateIdx[1]) <= pageTimeValues[pageTimeIndex]
                                            && pageTimeValues[pageTimeIndex] <= update[1].getTime(updateIdx[1] + 1)) {
                                        // never reach there
                                    } else {
                                        lastAggreData.putInt(v);
                                        lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                    }
                                } else {
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeValues[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        }

                        // Set the interval to next position that current time
                        // in page maybe be included.
                        while (mode != -1 && pageTimeIndex < pageTimeValues.length
                                && pageTimeValues[pageTimeIndex] > update[mode].getTime(updateIdx[mode] + 1)) {
                            updateIdx[mode] += 2;
                            mode = getNextMode(updateIdx[0], updateIdx[1], update[0], update[1]);
                        }
                    }

                    // still has page data, no common timestamps
                    if (decoder.hasNext(page) && commonTimestampsIndex >= commonTimestamps.size()) {

                    }

                    // still has common timestamps, no page data
                    if (!decoder.hasNext(page) && commonTimestampsIndex < commonTimestamps.size()) {

                    }

                    break;
                case BOOLEAN:
                    while (decoder.hasNext(page)) {
                        long timestamp = commonTimestamps.get(commonTimestampsIndex);

                        while (insertMemoryData.hasInsertData() && pageTimeIndex < pageTimeValues.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimeValues[pageTimeIndex]) {

                            if (insertMemoryData.getCurrentMinTime() == timestamp) {
                                lastAggreData.putTime(insertMemoryData.getCurrentMinTime());
                                lastAggreData.putInt(insertMemoryData.getCurrentIntValue());
                                lastAggreData.insertTrueIndex++;
                                commonTimestampsIndex += 1;

                                if (insertMemoryData.getCurrentMinTime() == pageTimeValues[pageTimeIndex]) {
                                    insertMemoryData.removeCurrentValue();
                                    pageTimeIndex++;
                                    decoder.readInt(page);
                                    if (!decoder.hasNext(page)) {
                                        break;
                                    }
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }
                        }

                        if (!decoder.hasNext(page) || pageTimeIndex >= pageTimeValues.length) {
                            break;
                        }
                        if (commonTimestampsIndex >= commonTimestamps.size()) {
                            break;
                        }

                        int v = decoder.readInt(page);

                        // compare with commonTimestamps firstly
                        // then compare with time filter
                        // lastly compare with update operation
                        if (mode == -1) {
                            if (pageTimeValues[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if ((timeFilter == null || timeVisitor.verify(pageTimeValues[pageTimeIndex]))) {
                                    lastAggreData.putInt(v);
                                    lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                } else {
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeValues[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 0) {
                            if (pageTimeValues[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if (timeFilter == null || timeVisitor.verify(pageTimeValues[pageTimeIndex])) {
                                    if (update[0].getTime(updateIdx[0]) <= pageTimeValues[pageTimeIndex]
                                            && pageTimeValues[pageTimeIndex] <= update[0].getTime(updateIdx[0] + 1)) {
                                        lastAggreData.putInt(update[0].getInt(updateIdx[0] / 2));
                                        lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                    } else {
                                        lastAggreData.putInt(v);
                                        lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                    }
                                } else {
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeValues[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 1) {
                            if (pageTimeValues[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if (timeFilter == null || timeVisitor.verify(pageTimeValues[pageTimeIndex])) {
                                    if (update[1].getTime(updateIdx[1]) <= pageTimeValues[pageTimeIndex]
                                            && pageTimeValues[pageTimeIndex] <= update[1].getTime(updateIdx[1] + 1)) {
                                        // never reach there
                                    } else {
                                        lastAggreData.putInt(v);
                                        lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                    }
                                } else {
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeValues[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        }

                        // Set the interval to next position that current time
                        // in page maybe be included.
                        while (mode != -1 && pageTimeIndex < pageTimeValues.length
                                && pageTimeValues[pageTimeIndex] > update[mode].getTime(updateIdx[mode] + 1)) {
                            updateIdx[mode] += 2;
                            mode = getNextMode(updateIdx[0], updateIdx[1], update[0], update[1]);
                        }
                    }

                    // still has page data, no common timestamps
                    if (decoder.hasNext(page) && commonTimestampsIndex >= commonTimestamps.size()) {

                    }

                    // still has common timestamps, no page data
                    if (!decoder.hasNext(page) && commonTimestampsIndex < commonTimestamps.size()) {

                    }

                    break;
                case INT64:
                    while (decoder.hasNext(page)) {
                        long timestamp = commonTimestamps.get(commonTimestampsIndex);

                        while (insertMemoryData.hasInsertData() && pageTimeIndex < pageTimeValues.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimeValues[pageTimeIndex]) {

                            if (insertMemoryData.getCurrentMinTime() == timestamp) {
                                lastAggreData.putTime(insertMemoryData.getCurrentMinTime());
                                lastAggreData.putInt(insertMemoryData.getCurrentIntValue());
                                lastAggreData.insertTrueIndex++;
                                commonTimestampsIndex += 1;

                                if (insertMemoryData.getCurrentMinTime() == pageTimeValues[pageTimeIndex]) {
                                    insertMemoryData.removeCurrentValue();
                                    pageTimeIndex++;
                                    decoder.readInt(page);
                                    if (!decoder.hasNext(page)) {
                                        break;
                                    }
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }
                        }

                        if (!decoder.hasNext(page) || pageTimeIndex >= pageTimeValues.length) {
                            break;
                        }
                        if (commonTimestampsIndex >= commonTimestamps.size()) {
                            break;
                        }

                        int v = decoder.readInt(page);

                        // compare with commonTimestamps firstly
                        // then compare with time filter
                        // lastly compare with update operation
                        if (mode == -1) {
                            if (pageTimeValues[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if ((timeFilter == null || timeVisitor.verify(pageTimeValues[pageTimeIndex]))) {
                                    lastAggreData.putInt(v);
                                    lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                } else {
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeValues[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 0) {
                            if (pageTimeValues[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if (timeFilter == null || timeVisitor.verify(pageTimeValues[pageTimeIndex])) {
                                    if (update[0].getTime(updateIdx[0]) <= pageTimeValues[pageTimeIndex]
                                            && pageTimeValues[pageTimeIndex] <= update[0].getTime(updateIdx[0] + 1)) {
                                        lastAggreData.putInt(update[0].getInt(updateIdx[0] / 2));
                                        lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                    } else {
                                        lastAggreData.putInt(v);
                                        lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                    }
                                } else {
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeValues[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 1) {
                            if (pageTimeValues[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if (timeFilter == null || timeVisitor.verify(pageTimeValues[pageTimeIndex])) {
                                    if (update[1].getTime(updateIdx[1]) <= pageTimeValues[pageTimeIndex]
                                            && pageTimeValues[pageTimeIndex] <= update[1].getTime(updateIdx[1] + 1)) {
                                        // never reach there
                                    } else {
                                        lastAggreData.putInt(v);
                                        lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                    }
                                } else {
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeValues[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        }

                        // Set the interval to next position that current time
                        // in page maybe be included.
                        while (mode != -1 && pageTimeIndex < pageTimeValues.length
                                && pageTimeValues[pageTimeIndex] > update[mode].getTime(updateIdx[mode] + 1)) {
                            updateIdx[mode] += 2;
                            mode = getNextMode(updateIdx[0], updateIdx[1], update[0], update[1]);
                        }
                    }

                    // still has page data, no common timestamps
                    if (decoder.hasNext(page) && commonTimestampsIndex >= commonTimestamps.size()) {

                    }

                    // still has common timestamps, no page data
                    if (!decoder.hasNext(page) && commonTimestampsIndex < commonTimestamps.size()) {

                    }

                    break;
                case FLOAT:
                    while (decoder.hasNext(page)) {
                        long timestamp = commonTimestamps.get(commonTimestampsIndex);

                        while (insertMemoryData.hasInsertData() && pageTimeIndex < pageTimeValues.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimeValues[pageTimeIndex]) {

                            if (insertMemoryData.getCurrentMinTime() == timestamp) {
                                lastAggreData.putTime(insertMemoryData.getCurrentMinTime());
                                lastAggreData.putInt(insertMemoryData.getCurrentIntValue());
                                lastAggreData.insertTrueIndex++;
                                commonTimestampsIndex += 1;

                                if (insertMemoryData.getCurrentMinTime() == pageTimeValues[pageTimeIndex]) {
                                    insertMemoryData.removeCurrentValue();
                                    pageTimeIndex++;
                                    decoder.readInt(page);
                                    if (!decoder.hasNext(page)) {
                                        break;
                                    }
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }
                        }

                        if (!decoder.hasNext(page) || pageTimeIndex >= pageTimeValues.length) {
                            break;
                        }
                        if (commonTimestampsIndex >= commonTimestamps.size()) {
                            break;
                        }

                        int v = decoder.readInt(page);

                        // compare with commonTimestamps firstly
                        // then compare with time filter
                        // lastly compare with update operation
                        if (mode == -1) {
                            if (pageTimeValues[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if ((timeFilter == null || timeVisitor.verify(pageTimeValues[pageTimeIndex]))) {
                                    lastAggreData.putInt(v);
                                    lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                } else {
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeValues[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 0) {
                            if (pageTimeValues[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if (timeFilter == null || timeVisitor.verify(pageTimeValues[pageTimeIndex])) {
                                    if (update[0].getTime(updateIdx[0]) <= pageTimeValues[pageTimeIndex]
                                            && pageTimeValues[pageTimeIndex] <= update[0].getTime(updateIdx[0] + 1)) {
                                        lastAggreData.putInt(update[0].getInt(updateIdx[0] / 2));
                                        lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                    } else {
                                        lastAggreData.putInt(v);
                                        lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                    }
                                } else {
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeValues[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 1) {
                            if (pageTimeValues[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if (timeFilter == null || timeVisitor.verify(pageTimeValues[pageTimeIndex])) {
                                    if (update[1].getTime(updateIdx[1]) <= pageTimeValues[pageTimeIndex]
                                            && pageTimeValues[pageTimeIndex] <= update[1].getTime(updateIdx[1] + 1)) {
                                        // never reach there
                                    } else {
                                        lastAggreData.putInt(v);
                                        lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                    }
                                } else {
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeValues[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        }

                        // Set the interval to next position that current time
                        // in page maybe be included.
                        while (mode != -1 && pageTimeIndex < pageTimeValues.length
                                && pageTimeValues[pageTimeIndex] > update[mode].getTime(updateIdx[mode] + 1)) {
                            updateIdx[mode] += 2;
                            mode = getNextMode(updateIdx[0], updateIdx[1], update[0], update[1]);
                        }
                    }

                    // still has page data, no common timestamps
                    if (decoder.hasNext(page) && commonTimestampsIndex >= commonTimestamps.size()) {

                    }

                    // still has common timestamps, no page data
                    if (!decoder.hasNext(page) && commonTimestampsIndex < commonTimestamps.size()) {

                    }

                    break;
                case DOUBLE:
                    while (decoder.hasNext(page)) {
                        long timestamp = commonTimestamps.get(commonTimestampsIndex);

                        while (insertMemoryData.hasInsertData() && pageTimeIndex < pageTimeValues.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimeValues[pageTimeIndex]) {

                            if (insertMemoryData.getCurrentMinTime() == timestamp) {
                                lastAggreData.putTime(insertMemoryData.getCurrentMinTime());
                                lastAggreData.putInt(insertMemoryData.getCurrentIntValue());
                                lastAggreData.insertTrueIndex++;
                                commonTimestampsIndex += 1;

                                if (insertMemoryData.getCurrentMinTime() == pageTimeValues[pageTimeIndex]) {
                                    insertMemoryData.removeCurrentValue();
                                    pageTimeIndex++;
                                    decoder.readInt(page);
                                    if (!decoder.hasNext(page)) {
                                        break;
                                    }
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }
                        }

                        if (!decoder.hasNext(page) || pageTimeIndex >= pageTimeValues.length) {
                            break;
                        }
                        if (commonTimestampsIndex >= commonTimestamps.size()) {
                            break;
                        }

                        int v = decoder.readInt(page);

                        // compare with commonTimestamps firstly
                        // then compare with time filter
                        // lastly compare with update operation
                        if (mode == -1) {
                            if (pageTimeValues[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if ((timeFilter == null || timeVisitor.verify(pageTimeValues[pageTimeIndex]))) {
                                    lastAggreData.putInt(v);
                                    lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                } else {
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeValues[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 0) {
                            if (pageTimeValues[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if (timeFilter == null || timeVisitor.verify(pageTimeValues[pageTimeIndex])) {
                                    if (update[0].getTime(updateIdx[0]) <= pageTimeValues[pageTimeIndex]
                                            && pageTimeValues[pageTimeIndex] <= update[0].getTime(updateIdx[0] + 1)) {
                                        lastAggreData.putInt(update[0].getInt(updateIdx[0] / 2));
                                        lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                    } else {
                                        lastAggreData.putInt(v);
                                        lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                    }
                                } else {
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeValues[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 1) {
                            if (pageTimeValues[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if (timeFilter == null || timeVisitor.verify(pageTimeValues[pageTimeIndex])) {
                                    if (update[1].getTime(updateIdx[1]) <= pageTimeValues[pageTimeIndex]
                                            && pageTimeValues[pageTimeIndex] <= update[1].getTime(updateIdx[1] + 1)) {
                                        // never reach there
                                    } else {
                                        lastAggreData.putInt(v);
                                        lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                    }
                                } else {
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeValues[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        }

                        // Set the interval to next position that current time
                        // in page maybe be included.
                        while (mode != -1 && pageTimeIndex < pageTimeValues.length
                                && pageTimeValues[pageTimeIndex] > update[mode].getTime(updateIdx[mode] + 1)) {
                            updateIdx[mode] += 2;
                            mode = getNextMode(updateIdx[0], updateIdx[1], update[0], update[1]);
                        }
                    }

                    // still has page data, no common timestamps
                    if (decoder.hasNext(page) && commonTimestampsIndex >= commonTimestamps.size()) {

                    }

                    // still has common timestamps, no page data
                    if (!decoder.hasNext(page) && commonTimestampsIndex < commonTimestamps.size()) {

                    }

                    break;
                case TEXT:
                    while (decoder.hasNext(page)) {
                        long timestamp = commonTimestamps.get(commonTimestampsIndex);

                        while (insertMemoryData.hasInsertData() && pageTimeIndex < pageTimeValues.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimeValues[pageTimeIndex]) {

                            if (insertMemoryData.getCurrentMinTime() == timestamp) {
                                lastAggreData.putTime(insertMemoryData.getCurrentMinTime());
                                lastAggreData.putInt(insertMemoryData.getCurrentIntValue());
                                lastAggreData.insertTrueIndex++;
                                commonTimestampsIndex += 1;

                                if (insertMemoryData.getCurrentMinTime() == pageTimeValues[pageTimeIndex]) {
                                    insertMemoryData.removeCurrentValue();
                                    pageTimeIndex++;
                                    decoder.readInt(page);
                                    if (!decoder.hasNext(page)) {
                                        break;
                                    }
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }
                        }

                        if (!decoder.hasNext(page) || pageTimeIndex >= pageTimeValues.length) {
                            break;
                        }
                        if (commonTimestampsIndex >= commonTimestamps.size()) {
                            break;
                        }

                        int v = decoder.readInt(page);

                        // compare with commonTimestamps firstly
                        // then compare with time filter
                        // lastly compare with update operation
                        if (mode == -1) {
                            if (pageTimeValues[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if ((timeFilter == null || timeVisitor.verify(pageTimeValues[pageTimeIndex]))) {
                                    lastAggreData.putInt(v);
                                    lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                } else {
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeValues[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 0) {
                            if (pageTimeValues[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if (timeFilter == null || timeVisitor.verify(pageTimeValues[pageTimeIndex])) {
                                    if (update[0].getTime(updateIdx[0]) <= pageTimeValues[pageTimeIndex]
                                            && pageTimeValues[pageTimeIndex] <= update[0].getTime(updateIdx[0] + 1)) {
                                        lastAggreData.putInt(update[0].getInt(updateIdx[0] / 2));
                                        lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                    } else {
                                        lastAggreData.putInt(v);
                                        lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                    }
                                } else {
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeValues[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 1) {
                            if (pageTimeValues[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if (timeFilter == null || timeVisitor.verify(pageTimeValues[pageTimeIndex])) {
                                    if (update[1].getTime(updateIdx[1]) <= pageTimeValues[pageTimeIndex]
                                            && pageTimeValues[pageTimeIndex] <= update[1].getTime(updateIdx[1] + 1)) {
                                        // never reach there
                                    } else {
                                        lastAggreData.putInt(v);
                                        lastAggreData.putTime(pageTimeValues[pageTimeIndex]);
                                    }
                                } else {
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeValues[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        }

                        // Set the interval to next position that current time
                        // in page maybe be included.
                        while (mode != -1 && pageTimeIndex < pageTimeValues.length
                                && pageTimeValues[pageTimeIndex] > update[mode].getTime(updateIdx[mode] + 1)) {
                            updateIdx[mode] += 2;
                            mode = getNextMode(updateIdx[0], updateIdx[1], update[0], update[1]);
                        }
                    }

                    // still has page data, no common timestamps
                    if (decoder.hasNext(page) && commonTimestampsIndex >= commonTimestamps.size()) {

                    }

                    // still has common timestamps, no page data
                    if (!decoder.hasNext(page) && commonTimestampsIndex < commonTimestamps.size()) {

                    }

                    break;
                default:
                    throw new IOException("Data type not support : " + dataType);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Don't forget to update the curIdx in updateTrue and updateFalse
        update[0].curIdx = updateIdx[0];
        update[1].curIdx = updateIdx[1];
        return new Pair<>(lastAggreData, 0);
    }
}
