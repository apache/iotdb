package cn.edu.tsinghua.tsfile.timeseries.filterV2;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.factory.FilterType;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.operator.*;

/**
 * Created by zhangjinrui on 2017/12/15.
 */
public class TimeFilter {

    public static class TimeEq extends Eq {
        private TimeEq(Long value) {
            super(value, FilterType.TIME_FILTER);
        }
    }

    public static class TimeNotEq extends NotEq {
        private TimeNotEq(Long value) {
            super(value, FilterType.TIME_FILTER);
        }
    }

    public static class TimeGt extends Gt {
        private TimeGt(Long value) {
            super(value, FilterType.TIME_FILTER);
        }
    }

    public static class TimeGtEq extends GtEq {
        private TimeGtEq(Long value) {
            super(value, FilterType.TIME_FILTER);
        }
    }

    public static class TimeLt extends Lt {
        private TimeLt(Long value) {
            super(value, FilterType.TIME_FILTER);
        }
    }

    public static class TimeLtEq extends LtEq {
        private TimeLtEq(Long value) {
            super(value, FilterType.TIME_FILTER);
        }
    }

    public static class TimeNoRestriction extends NoRestriction {
        public String toString() {
            return FilterType.TIME_FILTER + super.toString();
        }
    }

    public static class TimeNot extends Not<Long> {
        private TimeNot(Filter<Long> filter) {
            super(filter);
        }
    }

    public static TimeEq eq(Long value) {
        return new TimeEq(value);
    }

    public static TimeGt gt(Long value) {
        return new TimeGt(value);
    }

    public static TimeGtEq gtEq(Long value) {
        return new TimeGtEq(value);
    }

    public static TimeLt lt(Long value) {
        return new TimeLt(value);
    }

    public static TimeLtEq ltEq(Long value) {
        return new TimeLtEq(value);
    }

    public static TimeNoRestriction noRestriction() {
        return new TimeNoRestriction();
    }

    public static TimeNot not(Filter<Long> filter) {
        return new TimeNot(filter);
    }

    public static TimeNotEq notEq(Long value) {
        return new TimeNotEq(value);
    }

}
