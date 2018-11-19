package cn.edu.tsinghua.tsfile.common.exception;

/**
 * Created by kangrong on 17/2/20.
 */
public class UnSupportedDataTypeException extends RuntimeException {

    private static final long serialVersionUID = 6399248887091915203L;

    public UnSupportedDataTypeException(String dataTypeName) {
        super("UnSupported dataType: " + dataTypeName);
    }
}
