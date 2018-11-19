package cn.edu.tsinghua.iotdb.jdbc;

import cn.edu.tsinghua.iotdb.jdbc.thrift.TSCloseOperationReq;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSCloseOperationResp;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSFetchResultsReq;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSFetchResultsResp;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSIService;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSOperationHandle;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSQueryDataSet;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TS_SessionHandle;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;

import org.apache.thrift.TException;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.Map.Entry;

public class TsfileQueryResultSet implements ResultSet {

	private Statement statement = null;
	private String sql;
	private SQLWarning warningChain = null;
	private boolean wasNull = false;
	private boolean isClosed = false;
	private TSIService.Iface client = null;
	private TSOperationHandle operationHandle = null;
	private List<String> columnInfoList;
	private List<String> columnTypeList;
	private Map<String, Integer> columnInfoMap;
	private RowRecord record;
	private Iterator<RowRecord> recordItr;
	private int rowsFetched = 0;
	private int maxRows;
	private int fetchSize;
	private boolean emptyResultSet = false;
	private String operationType;
	private final String TIMESTAMP_STR = "Time";
    private final String LIMIT_STR = "LIMIT";
    private final String OFFSET_STR = "OFFSET";
    private final String SLIMIT_STR = "SLIMIT";
    private final String SOFFSET_STR = "SOFFSET";

    private int rowsCount = 0;
    private int rowsOffset = -1;
    private int rowsLimit = -1;

    private int seriesOffset = -1;
    private int seriesLimit = -1;

	public TsfileQueryResultSet() {

	}

	public TsfileQueryResultSet(Statement statement, List<String> columnName, TSIService.Iface client,
								TS_SessionHandle sessionHandle, TSOperationHandle operationHandle, String sql,
								String aggregations, List<String> columnTypeList)
			throws SQLException {

        // first try to retrieve limit&offset&slimit&soffset parameters from sql
        String[] splited = sql.toUpperCase().split("\\s+");
        List<String> arraySplited = Arrays.asList(splited);
        try {
            int posLimit = arraySplited.indexOf(LIMIT_STR);
            if (posLimit != -1) {
                rowsLimit = Integer.parseInt(splited[posLimit + 1]);
                int posOffset = arraySplited.indexOf(OFFSET_STR);
                if (posOffset != -1) {
                    rowsOffset = Integer.parseInt(splited[posOffset + 1]);
                }
            }
            int posSLimit = arraySplited.indexOf(SLIMIT_STR);
            if (posSLimit != -1) {
                seriesLimit = Integer.parseInt(splited[posSLimit + 1]);
                int posSOffset = arraySplited.indexOf(SOFFSET_STR);
                if (posSOffset != -1) {
                    seriesOffset = Integer.parseInt(splited[posSOffset + 1]);
                }
            }
        } catch (NumberFormatException e) {
            throw new TsfileSQLException("Out of range: LIMIT&SLIMIT parameter data type should be Int32.");
        }

        this.sql = sql;
        this.statement = statement;
        this.maxRows = statement.getMaxRows();
        this.fetchSize = statement.getFetchSize();
        this.client = client;
        this.operationHandle = operationHandle;
        this.operationType = aggregations;
        this.columnInfoList = new ArrayList<>();
        this.columnInfoMap = new HashMap<>();
        this.columnTypeList = new ArrayList<>();

        this.columnInfoList.add(TIMESTAMP_STR);
        this.columnInfoMap.put(TIMESTAMP_STR, 1);
        int index = 2;
        int colCount = columnName.size();

        if (seriesLimit == -1) { // if slimit is unset
            seriesLimit = colCount;
            seriesOffset = 0;
        } else if (seriesOffset == -1) {// if slimit is set and soffset is unset
            seriesOffset = 0;
        } else if (seriesOffset >= colCount) { // if slimit and soffset are set, but soffset exceeds the upper boundary 'colCount'-1
            // assign 0 to seriesLimit so next() will return 'false' instantly without needing to fetch data
            // and the 'FOR' loop below will be skipped because seriesOffset equals 'seriesEnd' then.
            seriesLimit = 0;
        }
        // else slimit and soffset are set and soffset is less than 'colCount',
        // so there is no need to modify slimit or soffset.

		// assign columnInfoMap
		// Note: columnInfoMap must not be affected by slimit or soffset.
		for (int i = 0; i < colCount; i++) {
			String name = columnName.get(i);
			if (!columnInfoMap.containsKey(name)) {
				columnInfoMap.put(name, index++);
			}
		}

		// assign columnInfoList and columnTypeList under the effect of slimit and soffset
		int seriesEnd = seriesOffset + seriesLimit;
		for (int i = seriesOffset; i < colCount && i < seriesEnd; i++) {
			String name = columnName.get(i);
			columnInfoList.add(name);
			this.columnTypeList.add(columnTypeList.get(i));
		}

	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean absolute(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void afterLast() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void beforeFirst() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void clearWarnings() throws SQLException {
		warningChain = null;
	}

	@Override
	public void close() throws SQLException {
		if (isClosed)
			return;

		closeOperationHandle();
		client = null;
		isClosed = true;
	}

	private void closeOperationHandle() throws SQLException {
		try {
			if (operationHandle != null) {
				TSCloseOperationReq closeReq = new TSCloseOperationReq(operationHandle);
				TSCloseOperationResp closeResp = client.closeOperation(closeReq);
				Utils.verifySuccess(closeResp.getStatus());
			}
		} catch (SQLException e) {
			throw new SQLException("Error occurs for close opeation in server side becasuse " + e.getMessage());
		} catch (TException e) {
			throw new SQLException(
					"Error occurs when connecting to server for close operation, becasue: " + e.getMessage());
		}
	}

	@Override
	public void deleteRow() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int findColumn(String columnName) throws SQLException {
		return columnInfoMap.get(columnName);
	}

	@Override
	public boolean first() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Array getArray(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Array getArray(String arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public InputStream getAsciiStream(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public InputStream getAsciiStream(String arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return getBigDecimal(findColumnNameByIndex(columnIndex));
	}

	@Override
	public BigDecimal getBigDecimal(String columnName) throws SQLException {
		return new BigDecimal(getValueByName(columnName));
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		MathContext mc = new MathContext(scale);
		return getBigDecimal(columnIndex).round(mc);
	}

	@Override
	public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
		return getBigDecimal(findColumn(columnName), scale);
	}

	@Override
	public InputStream getBinaryStream(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public InputStream getBinaryStream(String arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Blob getBlob(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Blob getBlob(String arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		return getBoolean(findColumnNameByIndex(columnIndex));
	}

	@Override
	public boolean getBoolean(String columnName) throws SQLException {
		String b = getValueByName(columnName);
		if (b.trim().equalsIgnoreCase("0"))
			return false;
		if (b.trim().equalsIgnoreCase("1"))
			return true;
		return Boolean.parseBoolean(b);
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte getByte(String columnName) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getBytes(String columnName) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getCharacterStream(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Reader getCharacterStream(String arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Clob getClob(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Clob getClob(String arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getConcurrency() throws SQLException {
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public String getCursorName() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		return new Date(getLong(columnIndex));
	}

	@Override
	public Date getDate(String columnName) throws SQLException {
		return getDate(findColumn(columnName));
	}

	@Override
	public Date getDate(int arg0, Calendar arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Date getDate(String arg0, Calendar arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		return getDouble(findColumnNameByIndex(columnIndex));
	}

	@Override
	public double getDouble(String columnName) throws SQLException {
		return Double.parseDouble(getValueByName(columnName));
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public int getFetchSize() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		return getFloat(findColumnNameByIndex(columnIndex));
	}

	@Override
	public float getFloat(String columnName) throws SQLException {
		return Float.parseFloat(getValueByName(columnName));
	}

	@Override
	public int getHoldability() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		return getInt(findColumnNameByIndex(columnIndex));
	}

	@Override
	public int getInt(String columnName) throws SQLException {
		return Integer.parseInt(getValueByName(columnName));
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		return getLong(findColumnNameByIndex(columnIndex));
	}

	@Override
	public long getLong(String columnName) throws SQLException {
		return Long.parseLong(getValueByName(columnName));
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return new TsfileResultMetadata(columnInfoList, operationType, columnTypeList);
	}

	@Override
	public Reader getNCharacterStream(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Reader getNCharacterStream(String arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public NClob getNClob(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public NClob getNClob(String arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public String getNString(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public String getNString(String arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		return getObject(findColumnNameByIndex(columnIndex));
	}

	@Override
	public Object getObject(String columnName) throws SQLException {
		return getValueByName(columnName);
	}

	@Override
	public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Ref getRef(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Ref getRef(String arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getRow() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public RowId getRowId(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public RowId getRowId(String arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public SQLXML getSQLXML(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public SQLXML getSQLXML(String arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		return getShort(findColumnNameByIndex(columnIndex));
	}

	@Override
	public short getShort(String columnName) throws SQLException {
		return Short.parseShort(getValueByName(columnName));
	}

	@Override
	public Statement getStatement() throws SQLException {
		return this.statement;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		return getString(findColumnNameByIndex(columnIndex));
	}

	@Override
	public String getString(String columnName) throws SQLException {
		return getValueByName(columnName);
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		return new Time(getLong(columnIndex));
	}

	@Override
	public Time getTime(String columnName) throws SQLException {
		return getTime(findColumn(columnName));
	}

	@Override
	public Time getTime(int arg0, Calendar arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Time getTime(String arg0, Calendar arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return new Timestamp(getLong(columnIndex));
	}

	@Override
	public Timestamp getTimestamp(String columnName) throws SQLException {
		return getTimestamp(findColumn(columnName));
	}

	@Override
	public Timestamp getTimestamp(int arg0, Calendar arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Timestamp getTimestamp(String arg0, Calendar arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getType() throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public URL getURL(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public URL getURL(String arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public InputStream getUnicodeStream(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public InputStream getUnicodeStream(String arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return warningChain;
	}

	@Override
	public void insertRow() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean isClosed() throws SQLException {
		return isClosed;
	}

	@Override
	public boolean isFirst() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean isLast() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean last() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throw new SQLException("Method not supported");
	}

    // the next record rule without considering the LIMIT&SLIMIT constraints
    private boolean nextWithoutLimit() throws SQLException {
		if (maxRows > 0 && rowsFetched >= maxRows) {
			System.out.println("Reach max rows " + maxRows);
			return false;
		}

		if ((recordItr == null || !recordItr.hasNext()) && !emptyResultSet) {
			TSFetchResultsReq req = new TSFetchResultsReq(sql, fetchSize);

			try {
				TSFetchResultsResp resp = client.fetchResults(req);
				Utils.verifySuccess(resp.getStatus());
				if (!resp.hasResultSet) {
					emptyResultSet = true;
				} else {
					TSQueryDataSet tsQueryDataSet = resp.getQueryDataSet();
					List<RowRecord> records = Utils.convertRowRecords(tsQueryDataSet);
					recordItr = records.iterator();
				}
			} catch (TException e) {
				throw new SQLException("Cannot fetch result from server, because of network connection");
			}

		}
		if (emptyResultSet) {
			return false;
		}
		record = recordItr.next();
		// if(record.getDeltaObjectType() != null &&
		// record.getDeltaObjectType().equals(AGGREGATION_STR)){
		// if(columnInfo.containsKey(TIMESTAMP_STR)){
		// columnInfo.remove(TIMESTAMP_STR);
		// }
		// }

		rowsFetched++;
        // maxRows is a constraint that exists in parallel with the LIMIT&SLIMIT constraints,
        // so rowsFetched will increase whenever the row is fetched,
        // regardless of whether the row satisfies the LIMIT&SLIMIT constraints or not.

		return true;
	}

	@Override
    // the next record rule with the LIMIT&SLIMIT constraints added
	public boolean next() throws SQLException {
        if (rowsLimit == 0 || seriesLimit == 0) {
            return false;// indicating immediately that there is no next record
        }

        if (rowsLimit != -1) { // if LIMIT is set
            if (rowsOffset != -1) { // if OFFSET is set and the initial offset move has not been done yet
                for (int i = 0; i < rowsOffset; i++) { // try to move to the the next record position where OFFSET indicates
                    if (!nextWithoutLimit()) {
                        return false;// cannot move to the next record position where OFFSET indicates
                    }
                }
                rowsOffset = -1; // indicating that the initial offset move has been finished
            }

            if (rowsCount >= rowsLimit) { // if the LIMIT constraint is met
                return false;
            }
        }

		boolean isNext = nextWithoutLimit();
		if (isNext && rowsLimit != -1) {
			rowsCount++;
		}
		return isNext;
}

	@Override
	public boolean previous() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void refreshRow() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean relative(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean rowInserted() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setFetchDirection(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void setFetchSize(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateArray(int arg0, Array arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateArray(String arg0, Array arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateAsciiStream(int arg0, InputStream arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateAsciiStream(String arg0, InputStream arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateAsciiStream(String arg0, InputStream arg1, int arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateAsciiStream(String arg0, InputStream arg1, long arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateBigDecimal(String arg0, BigDecimal arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateBinaryStream(int arg0, InputStream arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateBinaryStream(String arg0, InputStream arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateBinaryStream(String arg0, InputStream arg1, int arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateBinaryStream(String arg0, InputStream arg1, long arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateBlob(int arg0, Blob arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateBlob(String arg0, Blob arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateBlob(int arg0, InputStream arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateBlob(String arg0, InputStream arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateBlob(int arg0, InputStream arg1, long arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateBlob(String arg0, InputStream arg1, long arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateBoolean(int arg0, boolean arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateBoolean(String arg0, boolean arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateByte(int arg0, byte arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateByte(String arg0, byte arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateBytes(int arg0, byte[] arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateBytes(String arg0, byte[] arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateCharacterStream(int arg0, Reader arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateCharacterStream(String arg0, Reader arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateCharacterStream(String arg0, Reader arg1, int arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateClob(int arg0, Clob arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateClob(String arg0, Clob arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateClob(int arg0, Reader arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateClob(String arg0, Reader arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateClob(int arg0, Reader arg1, long arg2) throws SQLException {
		throw new SQLException("Method not supported");

	}

	@Override
	public void updateClob(String arg0, Reader arg1, long arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateDate(int arg0, Date arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateDate(String arg0, Date arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateDouble(int arg0, double arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateDouble(String arg0, double arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateFloat(int arg0, float arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateFloat(String arg0, float arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateInt(int arg0, int arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateInt(String arg0, int arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateLong(int arg0, long arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateLong(String arg0, long arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateNCharacterStream(int arg0, Reader arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateNCharacterStream(String arg0, Reader arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateNCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateNClob(int arg0, NClob arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateNClob(String arg0, NClob arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateNClob(int arg0, Reader arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateNClob(String arg0, Reader arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateNClob(int arg0, Reader arg1, long arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateNClob(String arg0, Reader arg1, long arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateNString(int arg0, String arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateNString(String arg0, String arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateNull(int arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateNull(String arg0) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateObject(int arg0, Object arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateObject(String arg0, Object arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateObject(int arg0, Object arg1, int arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateObject(String arg0, Object arg1, int arg2) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateRef(int arg0, Ref arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateRef(String arg0, Ref arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateRow() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateRowId(int arg0, RowId arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateRowId(String arg0, RowId arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateShort(int arg0, short arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateShort(String arg0, short arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateString(int arg0, String arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateString(String arg0, String arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateTime(int arg0, Time arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateTime(String arg0, Time arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateTimestamp(int arg0, Timestamp arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public void updateTimestamp(String arg0, Timestamp arg1) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean wasNull() throws SQLException {
		return wasNull;
	}

	private void checkRecord() throws SQLException {
		if (record == null) {
			throw new SQLException("No record remains");
		}
	}

	private String findColumnNameByIndex(int columnIndex) throws SQLException{
		if(columnIndex <= 0) {
			throw new SQLException(String.format("column index should start from 1"));
		}
		if(columnIndex > columnInfoList.size()){
			throw new SQLException(String.format("column index %d out of range %d", columnIndex, columnInfoList.size()));
		}
		return columnInfoList.get(columnIndex-1);
	}

	private String getValueByName(String columnName) throws SQLException {
		checkRecord();
		if (columnName.equals(TIMESTAMP_STR)) {
			return String.valueOf(record.getTimestamp());
		}
		int tmp = columnInfoMap.get(columnName);
		int i = 0;
		for(Entry<Path, TsPrimitiveType> entry : record.getFields().entrySet()){
			i++;
			if(i == tmp-1){
				if(entry.getValue() != null){
					return entry.getValue().getStringValue();
				} else {
					return null;
				}
			}
		}
		return null;
	}
}
