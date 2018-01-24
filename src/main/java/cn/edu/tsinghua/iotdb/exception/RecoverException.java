package cn.edu.tsinghua.iotdb.exception;

public class RecoverException extends Exception{
    private static final long serialVersionUID = 3591716406230730147L;

    public RecoverException(String message) {
        super(message);
    }

    public RecoverException(String message, Throwable cause) {
        super(message, cause);
    }

    public RecoverException(Throwable e) {
        super(e);
    }
}
