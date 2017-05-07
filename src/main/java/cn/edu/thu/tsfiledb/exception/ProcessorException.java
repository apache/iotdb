package cn.edu.thu.tsfiledb.exception;

/**
 * This exception is used for some error in processor
 * 
 * @author kangrong
 *
 */
public class ProcessorException extends Exception {

	private static final long serialVersionUID = 4137638418544201605L;

	public ProcessorException(String msg) {
		super(msg);
	}

	public ProcessorException(PathErrorException pathExcp) {
		super("PathErrorException: " + pathExcp.getMessage());
	}

	public ProcessorException() {
		super();
	}
}
