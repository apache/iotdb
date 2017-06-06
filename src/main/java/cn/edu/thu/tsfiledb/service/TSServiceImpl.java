package cn.edu.thu.tsfiledb.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.auth.AuthorityChecker;
import cn.edu.thu.tsfiledb.auth.dao.Authorizer;
import cn.edu.thu.tsfiledb.auth.model.AuthException;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.exception.NotConsistentException;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.ColumnSchema;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.metadata.Metadata;
import cn.edu.thu.tsfiledb.qp.exception.IllegalASTFormatException;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exec.impl.OverflowQPExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.logical.operator.RootOperator;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;
import cn.edu.thu.tsfiledb.query.management.ReadLockManager;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSCancelOperationReq;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSCancelOperationResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSCloseOperationReq;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSCloseOperationResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSCloseSessionReq;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSCloseSessionResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSColumnSchema;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSExecuteBatchStatementReq;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSExecuteBatchStatementResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSExecuteStatementReq;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSExecuteStatementResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFetchMetadataReq;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFetchMetadataResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFetchResultsReq;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFetchResultsResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSHandleIdentifier;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSOpenSessionReq;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSOpenSessionResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSOperationHandle;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSProtocolVersion;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSQueryDataSet;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TS_SessionHandle;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TS_Status;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TS_StatusCode;
import cn.edu.thu.tsfiledb.sql.exec.TSqlParserV2;
import cn.edu.thu.tsfiledb.sys.writeLog.WriteLogManager;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSIService;

public class TSServiceImpl implements TSIService.Iface {

	private WriteLogManager writeLogManager;
	private OverflowQPExecutor exec = new OverflowQPExecutor();
	// Record the username for every rpc connection. Username.get() is null if
	// login is failed.
	private ThreadLocal<String> username = new ThreadLocal<>();
	private ThreadLocal<HashMap<String, RootOperator>> queryStatus = new ThreadLocal<>();
	private ThreadLocal<HashMap<String, Iterator<QueryDataSet>>> queryRet = new ThreadLocal<>();

	private static final Logger LOGGER = LoggerFactory.getLogger(TSServiceImpl.class);

	public TSServiceImpl() throws IOException {
		LOGGER.info("TsFileDB Server: start checking write log...");
		writeLogManager = WriteLogManager.getInstance();
		writeLogManager.recovery();
		long cnt = 0l;
		PhysicalPlan plan;
		WriteLogManager.isRecovering = true;
		while ((plan = writeLogManager.getPhysicalPlan()) != null) {
//			System.out.println(cnt + " : " + ((MultiInsertPlan)plan).getDeltaObject() + "| " + ((MultiInsertPlan)plan).getTime());
			try {
				plan.processNonQuery(exec);
				cnt++;
			} catch (ProcessorException e) {
				e.printStackTrace();
				throw new IOException("Error in recovery from write log");
			}
		}
		WriteLogManager.isRecovering = false;
		LOGGER.info("TsFileDB Server: Done. Recover operation count {}", cnt);
	}

	@Override
	public TSOpenSessionResp OpenSession(TSOpenSessionReq req) throws TException {

		LOGGER.info("TsFileDB Server: receive open session request from username {}", req.getUsername());

		boolean status = false;
		try {
			status = Authorizer.login(req.getUsername(), req.getPassword());
		} catch (AuthException e) {
			status = false;
		}
		// boolean status = true;
		TS_Status ts_status;
		if (status) {
			ts_status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
			ts_status.setErrorMessage("login successfully.");
			username.set(req.getUsername());
			initForOneSession();
		} else {
			ts_status = new TS_Status(TS_StatusCode.ERROR_STATUS);
			ts_status.setErrorMessage("login failed. Username or password is wrong.");
		}
		TSOpenSessionResp resp = new TSOpenSessionResp(ts_status, TSProtocolVersion.TSFILE_SERVICE_PROTOCOL_V1);
		resp.setSessionHandle(new TS_SessionHandle(new TSHandleIdentifier(ByteBuffer.wrap(req.getUsername().getBytes()),
				ByteBuffer.wrap((req.getPassword().getBytes())))));
		LOGGER.info("TsFileDB Server: Login status: {}. User : {}", ts_status.getErrorMessage(), req.getUsername());

		return resp;
	}

	private void initForOneSession() {
		queryStatus.set(new HashMap<String, RootOperator>());
		queryRet.set(new HashMap<String, Iterator<QueryDataSet>>());
	}

	@Override
	public TSCloseSessionResp CloseSession(TSCloseSessionReq req) throws TException {
		LOGGER.info("TsFileDB Server: receive close session");
		TS_Status ts_status;
		if (username.get() == null) {
			ts_status = new TS_Status(TS_StatusCode.ERROR_STATUS);
			ts_status.setErrorMessage("Has not logged in");
		} else {
			ts_status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
			username.remove();
		}
		return new TSCloseSessionResp(ts_status);
	}

	@Override
	public TSCancelOperationResp CancelOperation(TSCancelOperationReq req) throws TException {
		return new TSCancelOperationResp(new TS_Status(TS_StatusCode.SUCCESS_STATUS));
	}

	@Override
	public TSCloseOperationResp CloseOperation(TSCloseOperationReq req) throws TException {
		try {
			ReadLockManager.getInstance().unlockForOneRequest();
			clearAllStatusForCurrentRequest();
		} catch (NotConsistentException e) {
			LOGGER.warn("Warning in closeOperation : {}", e.getMessage());
		} catch (ProcessorException e) {
			LOGGER.error("Error in closeOperation : {}", e.getMessage());
		}
		return new TSCloseOperationResp(new TS_Status(TS_StatusCode.SUCCESS_STATUS));
	}

	public void clearAllStatusForCurrentRequest() {
		this.queryRet.get().clear();
		this.queryStatus.get().clear();
		// Clear all parameters in last request.
		exec.clearParamter();
	}

	@Override
	public TSFetchMetadataResp FetchMetadata(TSFetchMetadataReq req) throws TException {
		TS_Status status;
		if (!checkLogin()) {
			LOGGER.info("TsFileDB Server: Not login.");
			status = new TS_Status(TS_StatusCode.ERROR_STATUS);
			status.setErrorMessage("Not login");
			return new TSFetchMetadataResp(status);
		}
		TSFetchMetadataResp resp = new TSFetchMetadataResp();
		try {
			Metadata metadata = MManager.getInstance().getMetadata();
			String metadataInJson = MManager.getInstance().getMetadataInString();
			Map<String, List<ColumnSchema>> seriesMap = metadata.getSeriesMap();
			Map<String, List<TSColumnSchema>> tsSeriesMap = Utils.convertAllSchema(seriesMap);
			resp.setSeriesMap(tsSeriesMap);
			resp.setDeltaObjectMap(metadata.getDeltaObjectMap());
			resp.setMetadataInJson(metadataInJson);
			status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
			resp.setStatus(status);
		} catch (PathErrorException e) {
			LOGGER.error("TsFileDB Server: failed to get all schema", e);
			status = new TS_Status(TS_StatusCode.ERROR_STATUS);
			status.setErrorMessage(e.getMessage());
			resp.setStatus(status);
			resp.setSeriesMap(null);
			resp.setDeltaObjectMap(null);
			resp.setMetadataInJson(null);
		} catch (Exception e) {
			LOGGER.error("TsFileDB Server: failed to get all schema for unknown reason", e);
			status = new TS_Status(TS_StatusCode.ERROR_STATUS);
			status.setErrorMessage(e.getMessage());
			resp.setStatus(status);
			resp.setSeriesMap(null);
			resp.setDeltaObjectMap(null);
			resp.setMetadataInJson(null);
		}
		return resp;
	}

	/**
	 * Judge whether the statement is ADMIN COMMAND and if true, execute it.
	 * 
	 * @param statement
	 * @return true if the statement is ADMIN COMMAND
	 * @throws IOException
	 */
	public boolean execAdminCommand(String statement) throws IOException {
		if (!username.get().equals("root")) {
			return false;
		}
		if (statement == null) {
			return false;
		}
		statement = statement.toLowerCase();
		switch (statement) {
		case "close":
			try {
				FileNodeManager.getInstance().closeAll();
			} catch (FileNodeManagerException e) {
				e.printStackTrace();
				throw new IOException(e);
			}
			//writeLogManager.overflowFlush();
			//writeLogManager.bufferFlush();
			// MManager.getInstance().flushObjectToFile();
			return true;
		case "merge":
			try {
				FileNodeManager.getInstance().mergeAll();
			} catch (FileNodeManagerException e) {
				e.printStackTrace();
				throw new IOException(e);
			}
			return true;
		}
		return false;
	}

	@Override
	public TSExecuteBatchStatementResp ExecuteBatchStatement(TSExecuteBatchStatementReq req) throws TException {
		try {
			if (!checkLogin()) {
				LOGGER.info("TsFileDB Server: Not login.");
				return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Not login", null);
			}
			List<String> statements = req.getStatements();
			List<Integer> result = new ArrayList<>();

			TSqlParserV2 parser = new TSqlParserV2();
			ArrayList<RootOperator> opList = new ArrayList<>();
			for (String statement : statements) {
				RootOperator root = parser.parseSQLToOperator(statement);
				if (root.isQuery()) {
					return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
							"statement is query :" + statement, result);
				}
				opList.add(root);
			}
			for (RootOperator op: opList) {
				ExecuteUpdateStatement(op);
			}

			return getTSBathExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS, "Execute statements successfully",
					result);
		} catch (Exception e) {
			LOGGER.error("TsFileDB Server: error occurs when executing statements", e);
			return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage(), null);
		}

	}

	@Override
	public TSExecuteStatementResp ExecuteStatement(TSExecuteStatementReq req) throws TException {
		try {
			if (!checkLogin()) {
				LOGGER.info("TsFileDB Server: Not login.");
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Not login");
			}
			String statement = req.getStatement();

			try {
				if (execAdminCommand(statement)) {
					return getTSExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS, "ADMIN_COMMAND_SUCCESS");
				}
			} catch (Exception e) {
				e.printStackTrace();
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Server Internal Error");
			}

			TSqlParserV2 parser = new TSqlParserV2();
			RootOperator root;
			try {
				root = parser.parseSQLToOperator(statement);
			} catch (IllegalASTFormatException e) {
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
						"Statement is not right:" + e.getMessage());
			} catch (NullPointerException e) {
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Statement is not allowed");
			}
			if (root.isQuery()) {
				return ExecuteQueryStatement(req);
			} else {
				return ExecuteUpdateStatement(root);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
		}
	}

	@Override
	public TSExecuteStatementResp ExecuteQueryStatement(TSExecuteStatementReq req) throws TException {

		try {
			if (!checkLogin()) {
				LOGGER.info("TsFileDB Server: Not login.");
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Not login");
			}

			String statement = req.getStatement();
			TSqlParserV2 parser = new TSqlParserV2();
			RootOperator root = parser.parseSQLToOperator(statement);

			List<Path> paths = null;
			// paths = ((SFWOperator) root).getSelSeriesPaths(exec);
			PhysicalPlan plan = parser.parseSQLToPhysicalPlan(statement, exec);
			paths = plan.getInvolvedSeriesPaths();

			// check whether current statement is a query statement
			if (!root.isQuery()) {
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Statement Error: Not a query statement");
			}

			// check path exists
			if (paths.size() == 0) {
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Selected columns do NOT EXIST.");
			}

			// check file level set
			try {
				MManager.getInstance().checkFileLevel(paths);
			} catch (PathErrorException e) {
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
			}

			// check permissions
			if (!checkAuthorization(paths, root.getType())) {
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "No permissions for this query.");
			}

			TSExecuteStatementResp resp = getTSExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS, "");
			List<String> columns = new ArrayList<>();
			for (Path p : paths) {
				columns.add(p.getFullPath());
			}
			TSHandleIdentifier operationId = new TSHandleIdentifier(ByteBuffer.wrap(username.get().getBytes()),
					ByteBuffer.wrap(("PASS".getBytes())));
			TSOperationHandle operationHandle = null;
			resp.setColumns(columns);
			operationHandle = new TSOperationHandle(operationId, true);
			resp.setOperationHandle(operationHandle);

			recordANewQuery(statement, root);

			return resp;
		} catch (Exception e) {
			LOGGER.error("TsFileDB Server: server internal error: {}", e.getMessage());
			return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
		}
	}

	@Override
	public TSFetchResultsResp FetchResults(TSFetchResultsReq req) throws TException {
		try {
			if (!checkLogin()) {
				return getTSFetchResultsResp(TS_StatusCode.ERROR_STATUS, "Not login.");
			}
			String statement = req.getStatement();

			if (!queryStatus.get().containsKey(statement)) {
				return getTSFetchResultsResp(TS_StatusCode.ERROR_STATUS, "Has not executed statement");
			}

			int fetchSize = req.getFetch_size();
			Iterator<QueryDataSet> queryDataSetIterator;
			if (!queryRet.get().containsKey(statement)) {
				TSqlParserV2 parser = new TSqlParserV2();
				RootOperator operator = queryStatus.get().get(statement);
				exec.setFetchSize(fetchSize);
				queryDataSetIterator = parser.query(operator, exec);
				queryRet.get().put(statement, queryDataSetIterator);
			} else {
				queryDataSetIterator = queryRet.get().get(statement);
			}

			boolean hasResultSet = false;
			QueryDataSet res = new QueryDataSet();
			if (queryDataSetIterator.hasNext()) {
				res = queryDataSetIterator.next();
				hasResultSet = true;
			} else {
				hasResultSet = false;
				queryRet.get().remove(statement);
			}
			TSQueryDataSet tsQueryDataSet = Utils.convertQueryDataSet(res);

			TSFetchResultsResp resp = getTSFetchResultsResp(TS_StatusCode.SUCCESS_STATUS,
					"FetchResult successfully. Has more result: " + hasResultSet);
			resp.setHasResultSet(hasResultSet);
			resp.setQueryDataSet(tsQueryDataSet);
			return resp;
		} catch (Exception e) {
			LOGGER.error("TsFileDB Server: server Internal Error: {}", e.getMessage());
			return getTSFetchResultsResp(TS_StatusCode.ERROR_STATUS, "Server Internal Error");
		}

	}

	@Override
	public TSExecuteStatementResp ExecuteUpdateStatement(TSExecuteStatementReq req) throws TException {
		try {
			if (!checkLogin()) {
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Not login");
			}
			String statement = req.getStatement();
			return ExecuteUpdateStatement(statement);
		} catch (ProcessorException e) {
			return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
		} catch (Exception e) {
			LOGGER.error("TsFileDB Server: server Internal Error: {}", e.getMessage());
			return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
		}
	}

	private TSExecuteStatementResp ExecuteUpdateStatement(RootOperator root) throws TException {
		try {
			PhysicalPlan plan = root.transformToPhysicalPlan(exec);
			List<Path> paths = plan.getInvolvedSeriesPaths();

			if (!checkAuthorization(paths, root.getType())) {
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "No permissions for this operation");
			}
			// TODO
			// In current version, we only return OK/ERROR
			// Do we need to add extra information of executive condition  
			boolean execRet;
			try {
				execRet = plan.processNonQuery(exec);
			} catch (ProcessorException e) {
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
			}
			if (execRet && needToBeWritenToLog(plan)) {
				writeLogManager.write(plan);
			}
			TS_StatusCode statusCode = execRet ? TS_StatusCode.SUCCESS_STATUS : TS_StatusCode.ERROR_STATUS;
			String msg = execRet ? "Execute successfully" : "Execute statement error.";
			TSExecuteStatementResp resp = getTSExecuteStatementResp(statusCode, msg);
			TSHandleIdentifier operationId = new TSHandleIdentifier(ByteBuffer.wrap(username.get().getBytes()),
					ByteBuffer.wrap(("PASS".getBytes())));
			TSOperationHandle operationHandle = null;
			operationHandle = new TSOperationHandle(operationId, false);
			resp.setOperationHandle(operationHandle);
			return resp;
		} catch (QueryProcessorException | PathErrorException e) {
			LOGGER.error("TsFileDB Server: query error", e);
			return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
		} catch (IOException e) {
			LOGGER.error("TsFileDB Server: write preLog error", e);
			return getTSExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS, "Write log error");
		}
	}

	private TSExecuteStatementResp ExecuteUpdateStatement(String statement)
			throws TException, QueryProcessorException, IOException, ProcessorException {

		TSqlParserV2 parser = new TSqlParserV2();
		RootOperator root;

		try {
			root = parser.parseSQLToOperator(statement);
		} catch (QueryProcessorException e) {
			e.printStackTrace();
			return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
		}
		if (root.isQuery()) {
			return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Statement is a query statement.");
		}

		// if operation belongs to add/delete/update
		PhysicalPlan plan = parser.parseSQLToPhysicalPlan(statement, exec);
		List<Path> paths = plan.getInvolvedSeriesPaths();

		if (!checkAuthorization(paths, root.getType())) {
			return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "No permissions for this operation");
		}

		// TODO 
		// In current version, we only return OK/ERROR
		// Do we need to add extra information of executive condition  
		boolean execRet;
		try {
			execRet = parser.nonQuery(root, exec);
		} catch (ProcessorException e) {
			return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
		}
		if (execRet && needToBeWritenToLog(plan)) {
			try {
				writeLogManager.write(plan);
			} catch (PathErrorException e) {
				throw new ProcessorException(e);
			}
		}
		TS_StatusCode statusCode = execRet ? TS_StatusCode.SUCCESS_STATUS : TS_StatusCode.ERROR_STATUS;
		String msg = execRet ? "Execute successfully" : "Execute statement error.";
		TSExecuteStatementResp resp = getTSExecuteStatementResp(statusCode, msg);
		TSHandleIdentifier operationId = new TSHandleIdentifier(ByteBuffer.wrap(username.get().getBytes()),
				ByteBuffer.wrap(("PASS".getBytes())));
		TSOperationHandle operationHandle = null;
		operationHandle = new TSOperationHandle(operationId, false);
		resp.setOperationHandle(operationHandle);
		return resp;
	}

	private boolean needToBeWritenToLog(PhysicalPlan plan) {
		if (plan.getOperatorType() == OperatorType.INSERT) {
			return false;
		}
		if (plan.getOperatorType() == OperatorType.UPDATE) {
			return true;
		}
		if (plan.getOperatorType() == OperatorType.DELETE) {
			return true;
		}
		return false;
	}

	private void recordANewQuery(String statement, RootOperator op) {
		queryStatus.get().put(statement, op);
		// refresh current queryRet for statement
		if (queryRet.get().containsKey(statement)) {
			queryRet.get().remove(statement);
		}
	}

	/**
	 * Check whether current user has logined.
	 * 
	 * @return true: If logined; false: If not logined
	 */
	private boolean checkLogin() {
		if (username.get() == null) {
			return false;
		}
		return true;
	}

	private boolean checkAuthorization(List<Path> paths, OperatorType type) {
		return AuthorityChecker.check(username.get(), paths, type);
	}

	private TSExecuteStatementResp getTSExecuteStatementResp(TS_StatusCode code, String msg) {
		TSExecuteStatementResp resp = new TSExecuteStatementResp();
		TS_Status ts_status = new TS_Status(code);
		ts_status.setErrorMessage(msg);
		resp.setStatus(ts_status);
		TSHandleIdentifier operationId = new TSHandleIdentifier(ByteBuffer.wrap(username.get().getBytes()),
				ByteBuffer.wrap(("PASS".getBytes())));
		TSOperationHandle operationHandle = new TSOperationHandle(operationId, false);
		resp.setOperationHandle(operationHandle);
		return resp;
	}

	private TSExecuteBatchStatementResp getTSBathExecuteStatementResp(TS_StatusCode code, String msg,
			List<Integer> result) {
		TSExecuteBatchStatementResp resp = new TSExecuteBatchStatementResp();
		TS_Status ts_status = new TS_Status(code);
		ts_status.setErrorMessage(msg);
		resp.setStatus(ts_status);
		resp.setResult(result);
		return resp;
	}

	private TSFetchResultsResp getTSFetchResultsResp(TS_StatusCode code, String msg) {
		TSFetchResultsResp resp = new TSFetchResultsResp();
		TS_Status ts_status = new TS_Status(code);
		ts_status.setErrorMessage(msg);
		resp.setStatus(ts_status);
		return resp;
	}

}
