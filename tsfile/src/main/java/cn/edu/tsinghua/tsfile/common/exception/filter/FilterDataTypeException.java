package cn.edu.tsinghua.tsfile.common.exception.filter;

/**
 * This Exception is used while filter data type is not consistent with the series type. <br>
 * e.g. you want to get result from a Double series used Integer filter.<br>
 * This Exception extends super class
 * {@link FilterDataTypeException}
 *
 * @author CGF
 */
public class FilterDataTypeException extends RuntimeException {

    private static final long serialVersionUID = 1888878519023495363L;

    public FilterDataTypeException(String message, Throwable cause) {
        super(message, cause);
    }

    public FilterDataTypeException(String message) {
        super(message);
    }

    public FilterDataTypeException(Throwable cause) {
        super(cause);
    }
}
