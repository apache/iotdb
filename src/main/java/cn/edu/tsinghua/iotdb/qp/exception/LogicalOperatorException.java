package cn.edu.tsinghua.iotdb.qp.exception;


/**
 * This exception is thrown while meeting error in transforming
 * logical operator to physical plan.
 * 
 * @author kangrong
 *
 */
public class LogicalOperatorException extends QueryProcessorException {

    private static final long serialVersionUID = 7573857366601268706L;

    public LogicalOperatorException(String msg) {
        super(msg);
    }

}
