package cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To generate a SingleValueVisitor using TSDataType.
 *
 * @author CGF
 */
public class SingleValueVisitorFactory {

    static final Logger LOG = LoggerFactory.getLogger(SingleValueVisitorFactory.class);

    private static final SingleValueVisitor<Integer> intVisitor = new SingleValueVisitor<Integer>();
    private static final SingleValueVisitor<Long> longVisitor = new SingleValueVisitor<Long>();
    private static final SingleValueVisitor<Float> floatVisitor = new SingleValueVisitor<Float>();
    private static final SingleValueVisitor<Double> doubleVisitor = new SingleValueVisitor<Double>();
    private static final SingleValueVisitor<Boolean> booleanVisitor = new SingleValueVisitor<Boolean>();
    private static final SingleValueVisitor<String> stringVisitor = new SingleValueVisitor<String>();

    /**
     * get SingleValueVisitor using TSDataType
     *
     * @param type data type of TsFile
     * @return single value visitor
     */
    public static SingleValueVisitor<?> getSingleValueVisitor(TSDataType type) {
        switch (type) {
            case INT64:
                return longVisitor;
            case INT32:
                return intVisitor;
            case FLOAT:
                return floatVisitor;
            case DOUBLE:
                return doubleVisitor;
            case BOOLEAN:
                return booleanVisitor;
            case TEXT:
                return stringVisitor;
            default:
                LOG.error("Unsupported tsfile data type.");
                throw new UnSupportedDataTypeException("Unsupported tsfile data type.");
        }
    }
}

