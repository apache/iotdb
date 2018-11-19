package cn.edu.tsinghua.tsfile.common.exception;

/**
 * Thrown when the input/output formats are not properly configured
 */
public class BadConfigurationException extends TSFileRuntimeException {
    private static final long serialVersionUID = -5342992200738090898L;

    public BadConfigurationException() {
    }

    public BadConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public BadConfigurationException(String message) {
        super(message);
    }

    public BadConfigurationException(Throwable cause) {
        super(cause);
    }
}
