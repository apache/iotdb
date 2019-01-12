package org.apache.iotdb.tsfile.qp.exception;

/**
 * This exception is threw whiling meeting error in logical optimizer process
 *
 */
public class LogicalOptimizeException extends QueryProcessorException {

    private static final long serialVersionUID = -7098092782689670064L;

    public LogicalOptimizeException(String msg) {
        super(msg);
    }

}
