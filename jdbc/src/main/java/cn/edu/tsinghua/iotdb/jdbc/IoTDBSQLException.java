package cn.edu.tsinghua.iotdb.jdbc;

import java.sql.SQLException;

public class IoTDBSQLException extends SQLException{
	private String errorMessage;
	
	private static final long serialVersionUID = -3306001287342258977L;

	public IoTDBSQLException(String reason){
		super(reason);
	}
	
	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
}
