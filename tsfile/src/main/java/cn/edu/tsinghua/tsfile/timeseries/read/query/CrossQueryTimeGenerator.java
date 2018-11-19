package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.CSAnd;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.CSOr;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * This class is used in batch query for Cross Query.
 *
 * @author ZJR, CGF
 */
public abstract class CrossQueryTimeGenerator {

    public ArrayList<DynamicOneColumnData> retMap; // represent the single valueFilter and its' data
    public ArrayList<Boolean> hasReadAllList; // represent whether the data has been read all
    protected ArrayList<Long> lastValueList; // represent the value stored in CSOr relation
    protected ArrayList<Integer> idxCount; // represent the dfsCnt and the sum node number of its' subtree
    protected int dfsCnt; // to record which single valueFilter is used

    protected SingleSeriesFilterExpression timeFilter;
    protected SingleSeriesFilterExpression freqFilter;
    protected FilterExpression valueFilter;
    protected int fetchSize;

    public CrossQueryTimeGenerator(SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
                                   FilterExpression valueFilter, int fetchSize) {
        retMap = new ArrayList<>();
        hasReadAllList = new ArrayList<>();
        lastValueList = new ArrayList<>();
        idxCount = new ArrayList<>();
        this.valueFilter = valueFilter;
        this.timeFilter = timeFilter;
        this.fetchSize = fetchSize;
        dfsCnt = -1;
        initRetMapAndFilterMap(valueFilter);
    }

    private int initRetMapAndFilterMap(FilterExpression valueFilter) {
        dfsCnt++;
        int tmpIdx = dfsCnt;
        retMap.add(null);
        hasReadAllList.add(false);
        lastValueList.add(-1L);
        idxCount.add(-1);

        if (valueFilter instanceof SingleSeriesFilterExpression) {
            idxCount.set(tmpIdx, 1);
            return 1;
        } else if (valueFilter instanceof CSAnd) {
            FilterExpression left = ((CSAnd) valueFilter).getLeft();
            FilterExpression right = ((CSAnd) valueFilter).getRight();
            int l = initRetMapAndFilterMap(left);
            int r = initRetMapAndFilterMap(right);
            idxCount.set(tmpIdx, l + r + 1);
            return l + r + 1;
        } else {
            FilterExpression left = ((CSOr) valueFilter).getLeft();
            FilterExpression right = ((CSOr) valueFilter).getRight();
            int l = initRetMapAndFilterMap(left);
            int r = initRetMapAndFilterMap(right);
            idxCount.set(tmpIdx, l + r + 1);
            return l + r + 1;
        }
    }

    /**
     * Calculate common time using FilterExpression.
     * @return common time
     * @throws ProcessorException exception in query processor
     * @throws IOException exception in IO
     */
    public long[] generateTimes() throws ProcessorException, IOException {
        long[] res = new long[fetchSize];

        int cnt = 0;
        SingleValueVisitor<Long> timeFilterVisitor = new SingleValueVisitor<>();
        while (cnt < fetchSize) {
            // init dfsCnt=-1 before calculateOneTime
            dfsCnt = -1;
            long v = calculateOneTime(valueFilter);
            if (v == -1) {
                break;
            }
            if ((timeFilter == null) || (timeFilter != null && timeFilterVisitor.satisfyObject(v, timeFilter))) {
                res[cnt] = v;
                cnt++;
            }
        }
        if (cnt < fetchSize) {
            return Arrays.copyOfRange(res, 0, cnt);
        }
        return res;
    }

    private long calculateOneTime(FilterExpression valueFilter) throws ProcessorException, IOException {
        // first check whether the value is used in CSOr relation
        dfsCnt++;
        if (lastValueList.get(dfsCnt) != -1L) {
            long v = lastValueList.get(dfsCnt);
            lastValueList.set(dfsCnt, -1L);
            // this current valueFilter is a branch of CSOr relation, and has been calculated before
            // return the value directly and no need to calculate again
            dfsCnt += (idxCount.get(dfsCnt) - 1);
            return v;
        }
        if (valueFilter instanceof SingleSeriesFilterExpression) {
            DynamicOneColumnData res = retMap.get(dfsCnt);

            // res is null or res has no data.
            if ((res == null) || (res.curIdx == res.valueLength && !hasReadAllList.get(dfsCnt))) {
                res = getMoreRecordForOneCol(dfsCnt, (SingleSeriesFilterExpression) valueFilter);
            }

            if (res == null || res.curIdx == res.valueLength) {
                //represent this col has no more value
                return -1;
            }
            return res.getTime(res.curIdx++);
        } else if (valueFilter instanceof CSAnd) {
            FilterExpression left = ((CSAnd) valueFilter).getLeft();
            FilterExpression right = ((CSAnd) valueFilter).getRight();
            int leftPreIndex = dfsCnt;
            long l = calculateOneTime(left);
            int rightPreIndex = dfsCnt;
            long r = calculateOneTime(right);
            while (l != -1 && r != -1) {
                while (l < r && l != -1) {
                    dfsCnt = leftPreIndex;
                    l = calculateOneTime(left);
                }
                if (l == r) {
                    break;
                }
                dfsCnt = rightPreIndex;
                r = calculateOneTime(right);
            }
            if (l == -1 || r == -1) {
                return -1;
            }
            return l;
        } else if (valueFilter instanceof CSOr) {
            FilterExpression left = ((CSOr) valueFilter).getLeft();
            FilterExpression right = ((CSOr) valueFilter).getRight();
            int lidx = dfsCnt + 1;
            long l = calculateOneTime(left);
            // dfsCnt has changed when above calculateOneTime(left) is over
            int ridx = dfsCnt + 1;
            long r = calculateOneTime(right);

            if (l == -1 && r != -1) {
                return r;
            } else if (l != -1 && r == -1) {
                return l;
            } else if (l == -1 && r == -1) {
                return -1;
            } else {
                if (l < r) {
                    lastValueList.set(ridx, r);
                    return l;
                } else if (l > r) {
                    lastValueList.set(lidx, l);
                    return r;
                } else {
                    return l;
                }
            }
        }
        return -1;
    }

    public DynamicOneColumnData getMoreRecordForOneCol(int idx, SingleSeriesFilterExpression valueFilter)
            throws ProcessorException, IOException {
        DynamicOneColumnData res = retMap.get(idx);
        if (res != null) {
            // rowGroupIdx will not change
            res.clearData();
        }
        res = getDataInNextBatch(res, fetchSize, valueFilter, idx);
        retMap.set(idx, res);
        if (res == null || res.valueLength == 0) {
            hasReadAllList.set(idx, true);
        }
        return res;
    }

    /**
     * valueFilterNumber parameter is mainly used for IoTDB.
     * Because of the exist of <code>RecordReaderCache</code>,
     * we must know the occur position of the SingleSeriesFilter in CrossSeriesFilterExpression.
     *
     * @param res result set
     * @param fetchSize fetch size of this query
     * @param valueFilter filter of value
     * @param valueFilterNumber the position number of SingleValueFilter in CrossValueFilter
     * @return the query data of next read
     * @throws ProcessorException exception in query process
     * @throws IOException exception in IO
     */
    public abstract DynamicOneColumnData getDataInNextBatch(DynamicOneColumnData res, int fetchSize,
                                                            SingleSeriesFilterExpression valueFilter, int valueFilterNumber)
            throws ProcessorException, IOException;
}