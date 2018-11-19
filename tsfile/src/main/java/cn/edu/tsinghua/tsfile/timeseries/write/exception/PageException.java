package cn.edu.tsinghua.tsfile.timeseries.write.exception;

/**
 * This exception means the the page size exceeding threshold what user setting.
 *
 * @author kangrong
 */
public class PageException extends WriteProcessException {

    private static final long serialVersionUID = 7385627296529388683L;

    public PageException(String msg) {
        super(msg);
    }
}
