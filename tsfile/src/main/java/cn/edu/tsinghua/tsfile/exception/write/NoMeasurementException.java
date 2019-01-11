package cn.edu.tsinghua.tsfile.exception.write;

/**
 * This exception means it can not find the measurement while writing a TSRecord
 */
public class NoMeasurementException extends WriteProcessException {

    private static final long serialVersionUID = -5599767368831572747L;

    public NoMeasurementException(String msg) {
        super(msg);
    }
}
