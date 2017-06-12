package cn.edu.thu.tsfiledb.jdbc;

import java.sql.SQLException;

public class TsfileURLException extends SQLException{
	private static final long serialVersionUID = -5071922897222027267L;
	
	public TsfileURLException(String reason){
		super(reason);
	}
}
