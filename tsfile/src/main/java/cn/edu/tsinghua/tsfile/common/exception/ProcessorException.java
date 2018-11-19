package cn.edu.tsinghua.tsfile.common.exception;

/**
 * This Exception is used to handle some error in query processing
 *
 * @author Jinrui Zhang
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
