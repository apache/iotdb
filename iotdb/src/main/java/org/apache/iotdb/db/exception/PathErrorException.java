package org.apache.iotdb.db.exception;

import org.apache.iotdb.db.exception.qp.QueryProcessorException;

public class PathErrorException extends QueryProcessorException {

    private static final long serialVersionUID = 2141197032898163234L;

    public PathErrorException(String msg) {
        super(msg);
    }

    public PathErrorException(Throwable e) {
        super(e);
    }
}
