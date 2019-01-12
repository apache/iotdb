package org.apache.iotdb.tsfile.exception.write;

/**
 * Exception occurs when writing a page
 */
public class PageException extends WriteProcessException {

    private static final long serialVersionUID = 7385627296529388683L;

    public PageException(String msg) {
        super(msg);
    }
}
