package org.apache.iotdb.tsfile.qp.exception;


/**
 * This exception is threw whiling meeting error in
 *
 */
public class DNFOptimizeException extends LogicalOptimizeException {

    private static final long serialVersionUID = 807384397361662482L;

    public DNFOptimizeException(String msg) {
        super(msg);
    }

}
