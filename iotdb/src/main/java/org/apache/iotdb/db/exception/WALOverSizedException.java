package org.apache.iotdb.db.exception;

import java.io.IOException;

public class WALOverSizedException extends IOException {
    private static final long serialVersionUID = -3145068900134508628L;

    public WALOverSizedException() {
        super();
    }

    public WALOverSizedException(String message) {
        super(message);
    }

    public WALOverSizedException(Throwable cause) {
        super(cause);
    }
}
