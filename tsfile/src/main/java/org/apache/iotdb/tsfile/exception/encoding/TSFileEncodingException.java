package org.apache.iotdb.tsfile.exception.encoding;

import org.apache.iotdb.tsfile.exception.TSFileRuntimeException;

/**
 * This Exception is used while encoding failed. <br>
 * This Exception extends super class
 * {@link TSFileRuntimeException}
 *
 * @author kangrong
 */
public class TSFileEncodingException extends TSFileRuntimeException {
    private static final long serialVersionUID = -7225811149696714845L;

    public TSFileEncodingException() {
    }

    public TSFileEncodingException(String message, Throwable cause) {
        super(message, cause);
    }

    public TSFileEncodingException(String message) {
        super(message);
    }

    public TSFileEncodingException(Throwable cause) {
        super(cause);
    }
}
