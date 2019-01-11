package cn.edu.tsinghua.iotdb.exception.qp;


/**
 * This exception is thrown while meeting error in transforming
 * logical operator to physical plan.
 */
public class LogicalOperatorException extends QueryProcessorException {

    private static final long serialVersionUID = 7573857366601268706L;

    public LogicalOperatorException(String msg) {
        super(msg);
    }

}
