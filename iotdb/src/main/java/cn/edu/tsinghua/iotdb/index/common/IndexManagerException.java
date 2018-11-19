package cn.edu.tsinghua.iotdb.index.common;

public class IndexManagerException extends Exception {

    private static final long serialVersionUID = 6261687971768311032L;

    public IndexManagerException() {
        super();
    }

    public IndexManagerException(String message) {
        super(message);
    }

    public IndexManagerException(Throwable cause) {
        super(cause);
    }
}
