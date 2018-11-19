package cn.edu.tsinghua.tsfile.common.exception;

/**
 * This Exception is used in that specified decoder doesn't exist. <br>
 * This Exception extends super class {@link java.lang.Exception}
 *
 * @author kangrong
 */
public class DecoderNotFoundException extends Exception {
    private static final long serialVersionUID = -310868735953605021L;

    public DecoderNotFoundException(String message) {
        super(message);
    }
}
