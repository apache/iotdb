package cn.edu.tsinghua.tsfile.exception.compress;

/**
 * This exception will be thrown when the codec is not supported by tsfile, meaning there is no
 * matching type defined in CompressionCodecName
 */
public class CompressionTypeNotSupportedException extends RuntimeException {
    private static final long serialVersionUID = -2244072267816916609L;
    private final Class<?> codecClass;

    public CompressionTypeNotSupportedException(Class<?> codecClass) {
        super("codec not supported: " + codecClass.getName());
        this.codecClass = codecClass;
    }

    public CompressionTypeNotSupportedException(String codecType) {
        super("codec not supported: " + codecType);
        this.codecClass = null;
    }

    public Class<?> getCodecClass() {
        return codecClass;
    }
}
