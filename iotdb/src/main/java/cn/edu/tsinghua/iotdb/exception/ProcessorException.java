package cn.edu.tsinghua.iotdb.exception;

/**
 * Processor Exception, the top-level exception in IoTDB.
 */
public class ProcessorException extends Exception {

    private static final long serialVersionUID = 4137638418544201605L;

    public ProcessorException(String msg) {
        super(msg);
    }

    public ProcessorException(Throwable e) {
        super(e);
    }

    public ProcessorException(Exception e) {
        super(e);
    }

    public ProcessorException() {
        super();
    }
}