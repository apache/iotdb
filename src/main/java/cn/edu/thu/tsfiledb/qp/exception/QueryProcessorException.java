package cn.edu.thu.tsfiledb.qp.exception;

/**
 * This exception is the basic exception of query process.
 * It's thrown when meeting any error in query process.
 *
 * @author qiaojialin
 *
 */
public class QueryProcessorException extends Exception {
    private static final long serialVersionUID = -8987915921329335088L;

    private String errMsg;
    public QueryProcessorException(String msg) {
        super(msg);
        this.errMsg = msg;
    }
    
    @Override
    public String getMessage() {
        return errMsg;
    }

}
