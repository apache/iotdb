package cn.edu.thu.tsfiledb.auth.model;

/**
 * The exception for authority model
 * Created by liukun on 17/1/4.
 */
public class AuthException extends Exception {
    public AuthException(String format, String userName, String roleName) {
        super();
    }

    public AuthException(String message) {
        super(message);
    }

    public AuthException(String message, Throwable cause) {
        super(message, cause);
    }

    public AuthException(Throwable cause) {
        super(cause);
    }

    protected AuthException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
