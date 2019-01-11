package cn.edu.tsinghua.iotdb.exception.qp;

/**
 * This exception is the basic exception of query process.
 * It's thrown when meeting any error in query process.
 */
public class QueryProcessorException extends Exception {
    private static final long serialVersionUID = -8987915921329335088L;

    public QueryProcessorException(String msg) {
        super(msg);
    }

    public QueryProcessorException(Throwable e) {
        super(e);
    }
}
