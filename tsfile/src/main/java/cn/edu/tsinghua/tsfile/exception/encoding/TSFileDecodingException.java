package cn.edu.tsinghua.tsfile.exception.encoding;

import cn.edu.tsinghua.tsfile.exception.TSFileRuntimeException;

/**
 * This Exception is used while decoding failed. <br>
 * This Exception extends super class
 * {@link TSFileRuntimeException}
 *
 * @author kangrong
 */
public class TSFileDecodingException extends TSFileRuntimeException {
    private static final long serialVersionUID = -8632392900655017028L;

    public TSFileDecodingException() {
    }

    public TSFileDecodingException(String message, Throwable cause) {
        super(message, cause);
    }

    public TSFileDecodingException(String message) {
        super(message);
    }

    public TSFileDecodingException(Throwable cause) {
        super(cause);
    }
}
