package cn.edu.thu.tsfiledb.qp.exception.logical.operator;

public class SeriesNotExistException extends QpSelectFromException {
    private static final long serialVersionUID = -5838261873402011565L;

    public SeriesNotExistException(String msg) {
        super(msg);
    }

}
