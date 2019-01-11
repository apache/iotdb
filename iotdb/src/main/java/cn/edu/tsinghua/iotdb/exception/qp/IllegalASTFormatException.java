package cn.edu.tsinghua.iotdb.exception.qp;

/**
 * This exception is thrown while meeting error in parsing ast tree
 * to generate logical operator
 */
public class IllegalASTFormatException extends QueryProcessorException {
    private static final long serialVersionUID = -8987915911329315588L;

    public IllegalASTFormatException(String msg) {
        super(msg);
    }

}
