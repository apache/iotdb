package org.apache.iotdb.cluster.exception;

public class ErrorConfigureExecption extends RuntimeException {
	private static final long serialVersionUID = 5530077196040763508L;

	public ErrorConfigureExecption() {
		super();
	}

	public ErrorConfigureExecption(Exception pathExcp) {
		super(pathExcp.getMessage());
	}

	public ErrorConfigureExecption(String msg) {
		super(msg);
	}

	public ErrorConfigureExecption(Throwable throwable) {
		super(throwable.getMessage());
	}
}
