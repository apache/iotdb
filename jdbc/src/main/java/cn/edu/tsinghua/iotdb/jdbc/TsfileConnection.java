package cn.edu.tsinghua.iotdb.jdbc;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.joda.time.DateTimeZone;

import cn.edu.tsinghua.service.rpc.thrift.ServerProperties;
import cn.edu.tsinghua.service.rpc.thrift.TSCloseSessionReq;
import cn.edu.tsinghua.service.rpc.thrift.TSGetTimeZoneResp;
import cn.edu.tsinghua.service.rpc.thrift.TSIService;
import cn.edu.tsinghua.service.rpc.thrift.TSOpenSessionReq;
import cn.edu.tsinghua.service.rpc.thrift.TSOpenSessionResp;
import cn.edu.tsinghua.service.rpc.thrift.TSProtocolVersion;
import cn.edu.tsinghua.service.rpc.thrift.TSSetTimeZoneReq;
import cn.edu.tsinghua.service.rpc.thrift.TSSetTimeZoneResp;
import cn.edu.tsinghua.service.rpc.thrift.TS_SessionHandle;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.SocketException;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Array;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class TsfileConnection implements Connection {
    private TsfileConnectionParams params;
    private boolean isClosed = true;
    private SQLWarning warningChain = null;
    private TSocket transport;
    public TSIService.Iface client = null;
    public TS_SessionHandle sessionHandle = null;
    private final List<TSProtocolVersion> supportedProtocols = new LinkedList<TSProtocolVersion>();
    private TSProtocolVersion protocol;
    private DateTimeZone timeZone;
    private boolean autoCommit;

    public TsfileConnection(){    
    }
    
    public TsfileConnection(String url, Properties info) throws SQLException, TTransportException {
	if (url == null) {
	    throw new TsfileURLException("Input url cannot be null");
	}
	params = Utils.parseURL(url, info);

	supportedProtocols.add(TSProtocolVersion.TSFILE_SERVICE_PROTOCOL_V1);
	
	openTransport();
	client = new TSIService.Client(new TBinaryProtocol(transport));
	// open client session
	openSession();
	// Wrap the client with a thread-safe proxy to serialize the RPC calls
	client = newSynchronizedClient(client);
	autoCommit = false;
    }

    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public void abort(Executor arg0) throws SQLException {
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
	TSCloseSessionReq req = new TSCloseSessionReq(sessionHandle);
	try {
	    client.closeSession(req);
	} catch (TException e) {
	    throw new SQLException("Error occurs when closing session at server", e);
	} finally {
	    isClosed = true;
	    if (transport != null){
			transport.close();
	    }
	}
    }

    @Override
    public void commit() throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public Blob createBlob() throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public Clob createClob() throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public NClob createNClob() throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public Statement createStatement() throws SQLException {
	if (isClosed) {
	    throw new SQLException("Cannot create statement because connection is closed");
	}
	return new TsfileStatement(this, client, sessionHandle);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
	if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
	    throw new SQLException(
		    String.format("Statement with resultset concurrency %d is not supported", resultSetConcurrency));
	}
	if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
	    throw new SQLException(String.format("Statement with resultset type %d is not supported", resultSetType));
	}
	return new TsfileStatement(this, client, sessionHandle);
    }

    @Override
    public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
	return autoCommit;
    }

    @Override
    public String getCatalog() throws SQLException {
	return "no cata log";
    }

    @Override
    public Properties getClientInfo() throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public String getClientInfo(String arg0) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public int getHoldability() throws SQLException {
//	throw new SQLException("Method not supported");
    	return 0;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
	if (isClosed) {
	    throw new SQLException("Cannot create statement because connection is closed");
	}
	return new TsfileDatabaseMetadata(this, client);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
	return TsfileJDBCConfig.connectionTimeoutInMs;
    }

    @Override
    public String getSchema() throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
	return Connection.TRANSACTION_NONE;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
	return warningChain;
    }

    @Override
    public boolean isClosed() throws SQLException {
	return isClosed;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
	return false;
    }

    @Override
    public boolean isValid(int arg0) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public String nativeSQL(String arg0) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public CallableStatement prepareCall(String arg0) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public CallableStatement prepareCall(String arg0, int arg1, int arg2) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
		return new TsfilePrepareStatement(this, client, sessionHandle, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    	throw new SQLException("Method not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    	throw new SQLException("Method not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    	throw new SQLException("Method not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
	    throws SQLException {
    	throw new SQLException("Method not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
	    int resultSetHoldability) throws SQLException {
    	throw new SQLException("Method not supported");
    }

    @Override
    public void releaseSavepoint(Savepoint arg0) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public void rollback() throws SQLException {
	
    }

    @Override
    public void rollback(Savepoint arg0) throws SQLException {
	
    }

    @Override
    public void setAutoCommit(boolean arg0) throws SQLException {
	autoCommit = arg0;
    }

    @Override
    public void setCatalog(String arg0) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public void setClientInfo(Properties arg0) throws SQLClientInfoException {
	throw new SQLClientInfoException("Method not supported", null);
    }

    @Override
    public void setClientInfo(String arg0, String arg1) throws SQLClientInfoException {
	throw new SQLClientInfoException("Method not supported", null);
    }

    @Override
    public void setHoldability(int arg0) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public void setNetworkTimeout(Executor arg0, int arg1) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public void setReadOnly(boolean arg0) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public Savepoint setSavepoint(String arg0) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public void setSchema(String arg0) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public void setTransactionIsolation(int arg0) throws SQLException {
	throw new SQLException("Method not supported");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
	throw new SQLException("Method not supported");
    }

    private void openTransport() throws TTransportException {
		transport = new TSocket(params.getHost(), params.getPort(), TsfileJDBCConfig.connectionTimeoutInMs);
		try {
			transport.getSocket().setKeepAlive(true);
		} catch (SocketException e) {
		    System.out.println("Cannot set socket keep alive because: " + e.getMessage());
		}
		if (!transport.isOpen()) {
		    transport.open();
		}
    }

    private void openSession() throws SQLException {
	TSOpenSessionReq openReq = new TSOpenSessionReq(TSProtocolVersion.TSFILE_SERVICE_PROTOCOL_V1);

	openReq.setUsername(params.getUsername());
	openReq.setPassword(params.getPassword());

	try {
	    TSOpenSessionResp openResp = client.openSession(openReq);

	    // validate connection
	    Utils.verifySuccess(openResp.getStatus());
	    if (!supportedProtocols.contains(openResp.getServerProtocolVersion())) {
		throw new TException("Unsupported TsFile protocol");
	    }
	    setProtocol(openResp.getServerProtocolVersion());
	    sessionHandle = openResp.getSessionHandle();
	    
	    if(timeZone != null){
	    		setTimeZone(timeZone.getID());
	    } else {
	    		timeZone = DateTimeZone.forID(getTimeZone());
	    }
	    
	} catch (TException e) {
	    throw new SQLException(
		    String.format("Can not establish connection with %s. because %s", params.getJdbcUriString(), e.getMessage()));
	}
	isClosed = false;
    }

    public boolean reconnect() {
	boolean flag = false;
	for (int i = 1; i <= TsfileJDBCConfig.RETRY_NUM; i++) {
	    try {   	
		if (transport != null) {
			transport.close();
		    openTransport();
		    client = new TSIService.Client(new TBinaryProtocol(transport));
		    openSession();
		    client = newSynchronizedClient(client);
		    flag = true;
		    break;
		}
	    } catch (Exception e) {
		try {
		    Thread.sleep(TsfileJDBCConfig.RETRY_INTERVAL);
		} catch (InterruptedException e1) {
		    e.printStackTrace();
		}
	    }
	}
	return flag;
    }

    public void setTimeZone(String tz) throws TException, TsfileSQLException{
    	TSSetTimeZoneReq req = new TSSetTimeZoneReq(tz);
    	TSSetTimeZoneResp resp = client.setTimeZone(req);
    	Utils.verifySuccess(resp.getStatus());
    	
    	timeZone = DateTimeZone.forID(tz);
    }
    
    public String getTimeZone() throws TException, TsfileSQLException{
    	if(timeZone != null) return timeZone.getID();
    	
    	TSGetTimeZoneResp resp = client.getTimeZone();
    	Utils.verifySuccess(resp.getStatus());
    	return resp.getTimeZone();
    }
    
    public ServerProperties getServerProperties() throws TException {
    		return client.getProperties();
    }
    
    public static TSIService.Iface newSynchronizedClient(TSIService.Iface client) {
	return (TSIService.Iface) Proxy.newProxyInstance(TsfileConnection.class.getClassLoader(),
		new Class[] { TSIService.Iface.class }, new SynchronizedHandler(client));
    }

    public TSProtocolVersion getProtocol() {
	return protocol;
    }

    public void setProtocol(TSProtocolVersion protocol) {
	this.protocol = protocol;
    }

    private static class SynchronizedHandler implements InvocationHandler {
	private final TSIService.Iface client;

	SynchronizedHandler(TSIService.Iface client) {
	    this.client = client;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
	    try {
		synchronized (client) {
		    return method.invoke(client, args);
		}
	    } catch (InvocationTargetException e) {
		// all IFace APIs throw TException
		if (e.getTargetException() instanceof TException) {
		    throw e.getTargetException();
		} else {
		    // should not happen
		    throw new TException("Error in calling method " + method.getName(), e.getTargetException());
		}
	    } catch (Exception e) {
		throw new TException("Error in calling method " + method.getName(), e);
	    }
	}
    }
}
