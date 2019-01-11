package cn.edu.tsinghua.tsfile.exception.write;

/**
 * This exception is threw while meeting error in writing procession.
 *
 * @author kangrong
 */
public class WriteProcessException extends Exception {
    private static final long serialVersionUID = -2664638061585302767L;
    protected String errMsg;

    public WriteProcessException(String msg) {
        super(msg);
        this.errMsg = msg;
    }

    @Override
    public String getMessage() {
        return errMsg;
    }
}
