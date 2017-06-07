package cn.edu.thu.tsfiledb.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.util.Iterator;
import java.util.List;

import cn.edu.thu.tsfiledb.metadata.ColumnSchema;



public class TsfileMetadataResultSet extends TsfileQueryResultSet {

	private Iterator<?> columnItr;
	private ColumnSchema currentColumn;
	private String currentDeltaObject;
	private MetadataType type;

	public TsfileMetadataResultSet(List<ColumnSchema> columnSchemas, List<String> deltaObjectList) {
		if (columnSchemas != null) {
			columnItr = columnSchemas.iterator();
			type = MetadataType.COLUMN;
		} else if (deltaObjectList != null) {
			columnItr = deltaObjectList.iterator();
			type = MetadataType.DELTA_OBJECT;
		}
	}

	@Override
	public void close() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int findColumn(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public BigDecimal getBigDecimal(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean getBoolean(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public byte getByte(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public byte[] getBytes(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getConcurrency() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Date getDate(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public double getDouble(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getFetchDirection() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public float getFloat(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getInt(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public long getLong(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean next() throws SQLException {
		boolean hasNext = columnItr.hasNext();
		switch (type) {
		case COLUMN:
			if (hasNext) {
				currentColumn = (ColumnSchema)columnItr.next();
			}
			return hasNext;			
		case DELTA_OBJECT:
			if (hasNext) {
				currentDeltaObject = (String)columnItr.next();
			}
			return hasNext;		
		default:
			break;
		}
		return false;
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Object getObject(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public short getShort(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Statement getStatement() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public String getString(String columnName) throws SQLException {
	    	// use special key word to judge return content
		switch (columnName) {
		case "COLUMN_NAME":
			return currentColumn.name;
		case "COLUMN_TYPE":
			return currentColumn.dataType.toString();
		case "DELTA_OBJECT":
			return currentDeltaObject;
		default:
			break;
		}
		return null;
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Time getTime(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getType() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean isClosed() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean wasNull() throws SQLException {
		throw new SQLException("Method not supported");
	}

	private enum MetadataType{
		DELTA_OBJECT, COLUMN
	}
}
