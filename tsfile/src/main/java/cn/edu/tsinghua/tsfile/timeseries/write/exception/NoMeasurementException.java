package cn.edu.tsinghua.tsfile.timeseries.write.exception;

/**
 * This exception means it finds a data point's measurement doesn't exist while writing a TSRecord
 *
 * @author kangrong
 */
public class NoMeasurementException extends WriteProcessException {

    private static final long serialVersionUID = -5599767368831572747L;

    public NoMeasurementException(String msg) {
        super(msg);
    }
}
