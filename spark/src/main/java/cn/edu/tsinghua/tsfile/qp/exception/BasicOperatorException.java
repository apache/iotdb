package cn.edu.tsinghua.tsfile.qp.exception;


/**
 * This exception is threw whiling meeting error in BasicOperator
 *
 */
public class BasicOperatorException extends QueryProcessorException {

    private static final long serialVersionUID = -2163809754074237707L;

    public BasicOperatorException(String msg) {
        super(msg);
    }

}
