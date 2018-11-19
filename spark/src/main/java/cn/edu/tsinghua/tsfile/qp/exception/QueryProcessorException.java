package cn.edu.tsinghua.tsfile.qp.exception;

/**
 * This exception is threw whiling meeting error in query processor
 *
 */
public class QueryProcessorException extends Exception {
    private static final long serialVersionUID = -8987915921329335088L;

    private String errMsg;
    QueryProcessorException(String msg) {
        super(msg);
        this.errMsg = msg;
    }
    
    @Override
    public String getMessage() {
        return errMsg;
    }

}
