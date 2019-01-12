package org.apache.iotdb.tsfile.exception.filter;

/**
 * This Exception is used while invoke UnarySeriesFilter's accept method. <br>
 * This Exception extends super class
 * {@link FilterInvokeException}
 *
 * @author CGF
 */
public class FilterInvokeException extends RuntimeException {

    private static final long serialVersionUID = 1888878519023495363L;

    public FilterInvokeException(String message, Throwable cause) {
        super(message, cause);
    }

    public FilterInvokeException(String message) {
        super(message);
    }

    public FilterInvokeException(Throwable cause) {
        super(cause);
    }
}
