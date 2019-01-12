package org.apache.iotdb.tsfile.exception;

/**
 * This Exception is the parent class for all runtime exceptions.<br>
 * This Exception extends super class {@link java.lang.RuntimeException}
 *
 * @author kangrong
 */
abstract public class TSFileRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 6455048223316780984L;

    public TSFileRuntimeException() {
        super();
    }

    public TSFileRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public TSFileRuntimeException(String message) {
        super(message);
    }

    public TSFileRuntimeException(Throwable cause) {
        super(cause);
    }
}
