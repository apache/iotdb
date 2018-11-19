package cn.edu.tsinghua.tsfile.timeseries.write.exception;

/**
 * This exception is throw if the json of schema in writing process is invalid, like missing necessary fields.
 *
 * @author kangrong
 */
public class InvalidJsonSchemaException extends WriteProcessException {
    private static final long serialVersionUID = -4469810656988557000L;

    public InvalidJsonSchemaException(String msg) {
        super(msg);
    }
}
