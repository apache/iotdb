package cn.edu.tsinghua.tsfile.qp.exception;



/**
 * This exception is threw whiling meeting error in
 *
 */
public class RemoveNotException extends LogicalOptimizeException {

    private static final long serialVersionUID = -772591029262375715L;

    public RemoveNotException(String msg) {
        super(msg);
    }

}
