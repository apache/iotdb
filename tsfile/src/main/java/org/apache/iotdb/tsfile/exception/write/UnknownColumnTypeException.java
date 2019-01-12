package org.apache.iotdb.tsfile.exception.write;

import org.apache.iotdb.tsfile.exception.TSFileRuntimeException;

/**
 * This Exception is used while getting an unknown column type. <br>
 * This Exception extends super class
 * {@link TSFileRuntimeException}
 *
 * @author kangrong
 */
public class UnknownColumnTypeException extends TSFileRuntimeException {
    private static final long serialVersionUID = -4003170165687174659L;
    public String type;

    public UnknownColumnTypeException(String type) {
        super("Column type not found: " + type);
        this.type = type;
    }
}
