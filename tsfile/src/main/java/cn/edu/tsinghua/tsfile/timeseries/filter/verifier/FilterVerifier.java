package cn.edu.tsinghua.tsfile.timeseries.filter.verifier;

import cn.edu.tsinghua.tsfile.common.exception.filter.UnSupportFilterDataTypeException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * optimizing of filter, transfer SingleSensorFilter to interval comparison
 * see {@link Interval}
 *
 * @author CGF
 */
public abstract class FilterVerifier {

    private static final Logger LOG = LoggerFactory.getLogger(FilterVerifier.class);

    public static FilterVerifier create(TSDataType dataType) {
        switch (dataType) {
            case INT32:
                return new IntFilterVerifier();
            case INT64:
                return new LongFilterVerifier();
            case FLOAT:
                return new FloatFilterVerifier();
            case DOUBLE:
                return new DoubleFilterVerifier();
            default:
                LOG.error("wrong filter verifier invoke, FilterVerifier only support INT32,INT64,FLOAT and DOUBLE.");
                throw new UnSupportFilterDataTypeException("wrong filter verifier invoke");
        }
    }

    public abstract Interval getInterval(SingleSeriesFilterExpression filter);
}
