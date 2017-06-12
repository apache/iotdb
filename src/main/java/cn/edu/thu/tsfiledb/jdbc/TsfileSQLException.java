package cn.edu.thu.tsfiledb.jdbc;

import java.sql.SQLException;

public class TsfileSQLException extends SQLException{
	private String errorMessage;
	
	private static final long serialVersionUID = -3306001287342258977L;

	public TsfileSQLException(String reason){
		super(reason);
	}
	
	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
}
