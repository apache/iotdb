package org.apache.iotdb.db.exception;

/**
 * @author kangrong
 *
 */
public class ErrorDebugException extends RuntimeException {

	private static final long serialVersionUID = -1123099620556170447L;

	public ErrorDebugException(String msg) {
		super(msg);
	}

	public ErrorDebugException(Exception e) {
		super(e);
	}
}
