package cn.edu.thu.tsfiledb.sys.writeLog;

import java.io.IOException;

/**
 * Define a interface which can persist logs.
 *
 */
public interface WriteLogPersistable {
	/**
	 * Write an operator to aim OutputStream.
	 * The format for an operator is as follow:
	 * (operatorType)(operatorContent)(length)
	 * operatorType : operator type which uses one byte
	 * operatorContent: bytes which encode by the operator
	 * length: the length of operatorContent which uses two bytes
	 * @param operator (operatorType)(operatorContent)
	 * @throws IOException
	 */
	public void write(byte[] operator) throws IOException;

	public void flush() throws IOException;
	
	/**
	 * close the file stream and write all the contents to the file.
	 */
	public void close();
}
