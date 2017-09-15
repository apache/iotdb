package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitorFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

import java.io.IOException;
import java.io.InputStream;


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
     * Read one page data using overflow operation.
     *
     * @param dataType
     * @param idx
     * @param timeValues
     * @param decoder
     * @param page
     * @param pageHeader
     * @param res
     * @param timeFilter
     * @param freqFilter
     * @param valueFilter
     * @param insertMemoryData
     * @param update
     * @return
     * @throws IOException
     */
    public static DynamicOneColumnData readOnePageWithOverflow(TSDataType dataType, int[] idx, long[] timeValues,
                                                               Decoder decoder, InputStream page, PageHeader pageHeader, DynamicOneColumnData res,
                                                               SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter,
                                                               InsertDynamicData insertMemoryData, DynamicOneColumnData[] update) throws IOException {
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
                        while (insertMemoryData.hasInsertData() && timeIdx < timeValues.length
                                && insertMemoryData.getCurrentMinTime() <= timeValues[timeIdx]) {
                            res.putTime(insertMemoryData.getCurrentMinTime());
                            res.putInt(insertMemoryData.getCurrentIntValue());
                            res.insertTrueIndex++;

                            if (insertMemoryData.getCurrentMinTime() == timeValues[timeIdx]) {
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
                                    && timeVisitor.verify(timeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(timeValues[timeIdx]))) {
                                res.putInt(v);
                                res.putTime(timeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 0) {
                            if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
                                // update the value
                                if (timeFilter == null
                                        || timeVisitor.verify(timeValues[timeIdx])) {
                                    res.putInt(update[0].getInt(idx[0] / 2));
                                    res.putTime(timeValues[timeIdx]);
                                }
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(timeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(timeValues[timeIdx]))) {
                                res.putInt(v);
                                res.putTime(timeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 1) {
                            if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
                                // do nothing
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(timeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(timeValues[timeIdx]))) {
                                res.putInt(v);
                                res.putTime(timeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        // Set the interval to next position that current time
                        // in page maybe be included.
                        while (mode != -1 && timeIdx < timeValues.length
                                && timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
                            idx[mode] += 2;
                            mode = getNextMode(idx[0], idx[1], update[0], update[1]);
                        }
                    }
                    break;
                case BOOLEAN:
                    while (decoder.hasNext(page)) {
                        // put insert points
                        while (insertMemoryData.curIdx < insertMemoryData.valueLength && timeIdx < timeValues.length
                                && insertMemoryData.getTime(insertMemoryData.curIdx) <= timeValues[timeIdx]) {
                            res.putTime(insertMemoryData.getTime(insertMemoryData.curIdx));
                            res.putBoolean(insertMemoryData.getBoolean(insertMemoryData.curIdx));
                            insertMemoryData.curIdx++;
                            res.insertTrueIndex++;
                            // if equal, take value from insertTrue and skip one
                            // value from page
                            if (insertMemoryData.getTime(insertMemoryData.curIdx - 1) == timeValues[timeIdx]) {
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
                                    && timeVisitor.verify(timeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.satisfyObject(v, valueFilter)
                                    && timeVisitor.verify(timeValues[timeIdx]))) {
                                res.putBoolean(v);
                                res.putTime(timeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 0) {
                            boolean v = decoder.readBoolean(page);
                            if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
                                // update the value
                                if (timeFilter == null
                                        || timeVisitor.verify(timeValues[timeIdx])) {
                                    res.putBoolean(update[0].getBoolean(idx[0] / 2));
                                    res.putTime(timeValues[timeIdx]);
                                }
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.satisfyObject(v, valueFilter))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(timeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.satisfyObject(v, valueFilter)
                                    && timeVisitor.verify(timeValues[timeIdx]))) {
                                res.putBoolean(v);
                                res.putTime(timeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 1) {
                            boolean v = decoder.readBoolean(page);
                            if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
                                // do nothing
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.satisfyObject(v, valueFilter))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(timeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.satisfyObject(v, valueFilter)
                                    && timeVisitor.verify(timeValues[timeIdx]))) {
                                res.putBoolean(v);
                                res.putTime(timeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        while (mode != -1 && timeIdx < timeValues.length
                                && timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
                            idx[mode] += 2;
                            mode = getNextMode(idx[0], idx[1], update[0], update[1]);
                        }
                    }
                    break;
                case INT64:
                    while (decoder.hasNext(page)) {
                        // put insert points
                        while (insertMemoryData.curIdx < insertMemoryData.valueLength && timeIdx < timeValues.length
                                && insertMemoryData.getTime(insertMemoryData.curIdx) <= timeValues[timeIdx]) {
                            res.putTime(insertMemoryData.getTime(insertMemoryData.curIdx));
                            res.putLong(insertMemoryData.getLong(insertMemoryData.curIdx));
                            insertMemoryData.curIdx++;
                            res.insertTrueIndex++;
                            // if equal, take value from insertTrue and skip one
                            // value from page
                            if (insertMemoryData.getTime(insertMemoryData.curIdx - 1) == timeValues[timeIdx]) {
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
                                    && timeVisitor.verify(timeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(timeValues[timeIdx]))) {
                                res.putLong(v);
                                res.putTime(timeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 0) {
                            if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
                                // update the value,需要和高飞再商量一下这个逻辑
                                if (timeFilter == null
                                        || timeVisitor.verify(timeValues[timeIdx])) {
                                    res.putLong(update[0].getLong(idx[0] / 2));
                                    res.putTime(timeValues[timeIdx]);
                                }
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(timeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(timeValues[timeIdx]))) {
                                res.putLong(v);
                                res.putTime(timeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 1) {
                            if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
                                // do nothing
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(timeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(timeValues[timeIdx]))) {
                                res.putLong(v);
                                res.putTime(timeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        while (mode != -1 && timeIdx < timeValues.length
                                && timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
                            idx[mode] += 2;
                            mode = getNextMode(idx[0], idx[1], update[0], update[1]);
                        }
                    }
                    break;
                case FLOAT:
                    while (decoder.hasNext(page)) {
                        // put insert points
                        while (insertMemoryData.curIdx < insertMemoryData.valueLength && timeIdx < timeValues.length
                                && insertMemoryData.getTime(insertMemoryData.curIdx) <= timeValues[timeIdx]) {
                            res.putTime(insertMemoryData.getTime(insertMemoryData.curIdx));
                            res.putFloat(insertMemoryData.getFloat(insertMemoryData.curIdx));
                            insertMemoryData.curIdx++;
                            res.insertTrueIndex++;
                            // if equal, take value from insertTrue and skip one
                            // value from page
                            if (insertMemoryData.getTime(insertMemoryData.curIdx - 1) == timeValues[timeIdx]) {
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
                                    && timeVisitor.verify(timeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(timeValues[timeIdx]))) {
                                res.putFloat(v);
                                res.putTime(timeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 0) {
                            if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
                                // update the value
                                if (timeFilter == null
                                        || timeVisitor.verify(timeValues[timeIdx])) {
                                    res.putFloat(update[0].getFloat(idx[0] / 2));
                                    res.putTime(timeValues[timeIdx]);
                                }
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(timeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(timeValues[timeIdx]))) {
                                res.putFloat(v);
                                res.putTime(timeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 1) {
                            if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
                                // do nothing
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(timeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(timeValues[timeIdx]))) {
                                res.putFloat(v);
                                res.putTime(timeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        while (mode != -1 && timeIdx < timeValues.length
                                && timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
                            idx[mode] += 2;
                            mode = getNextMode(idx[0], idx[1], update[0], update[1]);
                        }
                    }
                    break;
                case DOUBLE:
                    while (decoder.hasNext(page)) {
                        // put insert points
                        while (insertMemoryData.curIdx < insertMemoryData.valueLength && timeIdx < timeValues.length
                                && insertMemoryData.getTime(insertMemoryData.curIdx) <= timeValues[timeIdx]) {
                            res.putTime(insertMemoryData.getTime(insertMemoryData.curIdx));
                            res.putDouble(insertMemoryData.getDouble(insertMemoryData.curIdx));
                            insertMemoryData.curIdx++;
                            res.insertTrueIndex++;
                            // if equal, take value from insertTrue and skip one
                            // value from page
                            if (insertMemoryData.getTime(insertMemoryData.curIdx - 1) == timeValues[timeIdx]) {
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
                                    && timeVisitor.verify(timeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(timeValues[timeIdx]))) {
                                res.putDouble(v);
                                res.putTime(timeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 0) {
                            if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
                                // update the value
                                if (timeFilter == null
                                        || timeVisitor.verify(timeValues[timeIdx])) {
                                    res.putDouble(update[0].getDouble(idx[0] / 2));
                                    res.putTime(timeValues[timeIdx]);
                                }
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(timeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(timeValues[timeIdx]))) {
                                res.putDouble(v);
                                res.putTime(timeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 1) {
                            if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
                                // do nothing
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.verify(v))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(timeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.verify(v)
                                    && timeVisitor.verify(timeValues[timeIdx]))) {
                                res.putDouble(v);
                                res.putTime(timeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        while (mode != -1 && timeIdx < timeValues.length
                                && timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
                            idx[mode] += 2;
                            mode = getNextMode(idx[0], idx[1], update[0], update[1]);
                        }
                    }
                    break;
                case TEXT:
                    while (decoder.hasNext(page)) {
                        // put insert points
                        while (insertMemoryData.curIdx < insertMemoryData.valueLength && timeIdx < timeValues.length
                                && insertMemoryData.getTime(insertMemoryData.curIdx) <= timeValues[timeIdx]) {
                            res.putTime(insertMemoryData.getTime(insertMemoryData.curIdx));
                            res.putBinary(insertMemoryData.getBinary(insertMemoryData.curIdx));
                            insertMemoryData.curIdx++;
                            res.insertTrueIndex++;
                            // if equal, take value from insertTrue and skip one
                            // value from page
                            if (insertMemoryData.getTime(insertMemoryData.curIdx - 1) == timeValues[timeIdx]) {
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
                                    && timeVisitor.verify(timeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.satisfyObject(v, valueFilter)
                                    && timeVisitor.verify(timeValues[timeIdx]))) {
                                res.putBinary(v);
                                res.putTime(timeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 0) {
                            if (update[0].getTime(idx[0]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[0].getTime(idx[0] + 1)) {
                                // update the value
                                if (timeFilter == null
                                        || timeVisitor.verify(timeValues[timeIdx])) {
                                    res.putBinary(update[0].getBinary(idx[0] / 2));
                                    res.putTime(timeValues[timeIdx]);
                                }
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.satisfyObject(v, valueFilter))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(timeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.satisfyObject(v, valueFilter)
                                    && timeVisitor.verify(timeValues[timeIdx]))) {
                                res.putBinary(v);
                                res.putTime(timeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        if (mode == 1) {
                            if (update[1].getTime(idx[1]) <= timeValues[timeIdx]
                                    && timeValues[timeIdx] <= update[1].getTime(idx[1] + 1)) {
                                // do nothing
                            } else if ((valueFilter == null && timeFilter == null)
                                    || (valueFilter != null && timeFilter == null
                                    && valueVisitor.satisfyObject(v, valueFilter))
                                    || (valueFilter == null && timeFilter != null
                                    && timeVisitor.verify(timeValues[timeIdx]))
                                    || (valueFilter != null && timeFilter != null
                                    && valueVisitor.satisfyObject(v, valueFilter)
                                    && timeVisitor.verify(timeValues[timeIdx]))) {
                                res.putBinary(v);
                                res.putTime(timeValues[timeIdx]);
                            }
                            timeIdx++;
                        }

                        while (mode != -1 && timeIdx < timeValues.length
                                && timeValues[timeIdx] > update[mode].getTime(idx[mode] + 1)) {
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
}
