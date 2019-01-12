package org.apache.iotdb.jdbc;

import java.sql.SQLException;

public class IoTDBURLException extends SQLException{
	private static final long serialVersionUID = -5071922897222027267L;
	
	public IoTDBURLException(String reason){
		super(reason);
	}
}
