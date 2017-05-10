package cn.edu.thu.tsfiledb.qp.exception;

public class QueryProcessorException extends Exception {
    private static final long serialVersionUID = -8987915921329335088L;

    protected String errMsg;
    public QueryProcessorException(String msg) {
        super(msg);
        this.errMsg = msg;
    }
    
    @Override
    public String getMessage() {
        return errMsg;
    }

}
