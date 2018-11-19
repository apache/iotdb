package cn.edu.tsinghua.iotdb.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import cn.edu.tsinghua.iotdb.jdbc.thrift.TSIService.Iface;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TS_SessionHandle;

public class TsfilePrepareStatement extends TsfileStatement implements PreparedStatement {
	private final String sql;
	/**
	 * save the SQL parameters as (paramLoc,paramValue) pair
	 */
	private final Map<Integer, String> parameters=new HashMap<Integer, String>();
	
	public TsfilePrepareStatement(TsfileConnection connection, Iface client, TS_SessionHandle sessionHandle, String sql) {
		super(connection, client, sessionHandle);
		this.sql = sql;
	}

	@Override
	public void addBatch() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void clearParameters() throws SQLException {
		this.parameters.clear();
	}

	@Override
	public boolean execute() throws SQLException {
		return super.execute(createCompleteSql(sql, parameters));
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		return super.executeQuery(createCompleteSql(sql, parameters));
	}

	@Override
	public int executeUpdate() throws SQLException {
		return super.executeUpdate(createCompleteSql(sql, parameters));
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		this.parameters.put(parameterIndex,""+x);
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		this.parameters.put(parameterIndex,""+x);
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		this.parameters.put(parameterIndex,""+x);
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		this.parameters.put(parameterIndex,""+x);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		this.parameters.put(parameterIndex,""+x);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		if (x instanceof String) {
			setString(parameterIndex, (String) x);
		} else if (x instanceof Integer) {
			setInt(parameterIndex, ((Integer) x).intValue());
		} else if (x instanceof Long) {
			setLong(parameterIndex, ((Long) x).longValue());
		} else if (x instanceof Float) {
			setFloat(parameterIndex, ((Float) x).floatValue());
		} else if (x instanceof Double) {
			setDouble(parameterIndex, ((Double) x).doubleValue());
		} else if (x instanceof Boolean) {
			setBoolean(parameterIndex, ((Boolean) x).booleanValue());
		} else if (x instanceof Timestamp) {
			setTimestamp(parameterIndex, (Timestamp) x);
		} else {
			// Can't infer a type.
			throw new SQLException(String.format("Can''t infer the SQL type to use for an instance of %s. Use setObject() with an explicit Types value to specify the type to use.", x.getClass().getName()));
	    }
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		x = x.replace("'", "\\'");
		this.parameters.put(parameterIndex, "'" + x + "'");
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		DateTime dt = new DateTime(x.getTime());
		this.parameters.put(parameterIndex, dt.toString("yyyy-MM-dd HH:mm:ss.SSS"));
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLException("Method not supported");
	}

	private String createCompleteSql(final String sql, Map<Integer, String> parameters) throws SQLException {
		List<String> parts = splitSqlStatement(sql);

		StringBuilder newSql = new StringBuilder(parts.get(0));
		for (int i = 1; i < parts.size(); i++) {
			if (!parameters.containsKey(i)) {
				throw new SQLException("Parameter #" + i + " is unset");
			}
			newSql.append(parameters.get(i));
			newSql.append(parts.get(i));
		}
		return newSql.toString();

	}
	
	private List<String> splitSqlStatement(final String sql) {
		List<String> parts = new ArrayList<>();
		int apCount = 0;
		int off = 0;
		boolean skip = false;

		for (int i = 0; i < sql.length(); i++) {
			char c = sql.charAt(i);
			if (skip) {
				skip = false;
				continue;
			}
			switch (c) {
				case '\'':
					// skip something like 'xxxxx'
					apCount++;
					break;
				case '\\':
					// skip something like \r\n
					skip = true;
					break;
				case '?':
					// for input like: select a from 'bc' where d, 'bc' will be skipped
					if ((apCount & 1) == 0) {
						parts.add(sql.substring(off, i));
						off = i + 1;
					}
					break;
				default:
					break;
			}
		}
		parts.add(sql.substring(off, sql.length()));
		return parts;

	}

}
