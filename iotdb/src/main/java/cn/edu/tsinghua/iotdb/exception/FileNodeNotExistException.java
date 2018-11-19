package cn.edu.tsinghua.iotdb.exception;

/**
 * Throw this exception when the file node processor is not exists
 * 
 * @author kangrong
 *
 */
public class FileNodeNotExistException extends RuntimeException {

	private static final long serialVersionUID = -4334041411884083545L;

	public FileNodeNotExistException(String msg) {
		super(msg);
	}
}
