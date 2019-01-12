package org.apache.iotdb.tsfile.hadoop;

/**
 * @author liukun
 */
public class TSFHadoopException extends Exception {


    private static final long serialVersionUID = 9206686224701568169L;

    public TSFHadoopException() {
        super();
    }

    public TSFHadoopException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public TSFHadoopException(String message, Throwable cause) {
        super(message, cause);
    }

    public TSFHadoopException(String message) {
        super(message);
    }

    public TSFHadoopException(Throwable cause) {
        super(cause);
    }

}
