package cn.edu.tsinghua.iotdb.exception;

import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;

public class PathErrorException extends QueryProcessorException {

    private static final long serialVersionUID = 2141197032898163234L;

    public PathErrorException(String msg) {
        super(msg);
    }

    public PathErrorException(Throwable e) {
        super(e);
    }
}
