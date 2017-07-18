package cn.edu.thu.tsfiledb.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import org.apache.thrift.TException;
import org.apache.thrift.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.auth.AuthException;
import cn.edu.thu.tsfiledb.auth.AuthorityChecker;
import cn.edu.thu.tsfiledb.auth.dao.Authorizer;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import cn.edu.thu.tsfiledb.exception.NotConsistentException;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.ColumnSchema;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.metadata.Metadata;
import cn.edu.thu.tsfiledb.qp.QueryProcessor;
import cn.edu.thu.tsfiledb.qp.exception.IllegalASTFormatException;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.executor.OverflowQPExecutor;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
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
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSIService;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSOpenSessionReq;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSOpenSessionResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSOperationHandle;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSProtocolVersion;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSQueryDataSet;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TS_SessionHandle;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TS_Status;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TS_StatusCode;
import cn.edu.thu.tsfiledb.sys.writelog.WriteLogManager;

/**
 * Thrift RPC implementation at server side
 */

public class TSServiceImpl implements TSIService.Iface, ServerContext {

	private WriteLogManager writeLogManager;
	private QueryProcessor processor = new QueryProcessor(new OverflowQPExecutor());
	// Record the username for every rpc connection. Username.get() is null if
	// login is failed.
	private ThreadLocal<String> username = new ThreadLocal<>();
	private ThreadLocal<HashMap<String, PhysicalPlan>> queryStatus = new ThreadLocal<>();
	private ThreadLocal<HashMap<String, Iterator<QueryDataSet>>> queryRet = new ThreadLocal<>();

	private static final Logger LOGGER = LoggerFactory.getLogger(TSServiceImpl.class);

	public TSServiceImpl() throws IOException {
		if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
			writeLogManager = WriteLogManager.getInstance();
		}
	}

	@Override
	public TSOpenSessionResp openSession(TSOpenSessionReq req) throws TException {

		LOGGER.info("TsFileDB Server: receive open session request from username {}", req.getUsername());

		boolean status;
		try {
			status = Authorizer.login(req.getUsername(), req.getPassword());
		} catch (AuthException e) {
			status = false;
		}
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
		queryStatus.set(new HashMap<>());
		queryRet.set(new HashMap<>());
	}

	@Override
	public TSCloseSessionResp closeSession(TSCloseSessionReq req) throws TException {
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
	public TSCancelOperationResp cancelOperation(TSCancelOperationReq req) throws TException {
		return new TSCancelOperationResp(new TS_Status(TS_StatusCode.SUCCESS_STATUS));
	}

	@Override
	public TSCloseOperationResp closeOperation(TSCloseOperationReq req) throws TException {
		LOGGER.info("TsFileDB Server: receive close operation");
		try {
			ReadLockManager.getInstance().unlockForOneRequest();
			clearAllStatusForCurrentRequest();
		} catch (ProcessorException e) {
			LOGGER.error("Error in closeOperation : {}", e.getMessage());
		}
		return new TSCloseOperationResp(new TS_Status(TS_StatusCode.SUCCESS_STATUS));
	}

	private void clearAllStatusForCurrentRequest() {
		this.queryRet.get().clear();
		this.queryStatus.get().clear();
		// Clear all parameters in last request.
		processor.getExecutor().clearParameters();
	}

	@Override
	public TSFetchMetadataResp fetchMetadata(TSFetchMetadataReq req) throws TException {
		TS_Status status;
		if (!checkLogin()) {
			LOGGER.info("TsFileDB Server: Not login.");
			status = new TS_Status(TS_StatusCode.ERROR_STATUS);
			status.setErrorMessage("Not login");
			return new TSFetchMetadataResp(status);
		}
		TSFetchMetadataResp resp = new TSFetchMetadataResp();
		switch (req.getType()) {
		case METADATA_IN_JSON:
			String metadataInJson = MManager.getInstance().getMetadataInString();
			resp.setMetadataInJson(metadataInJson);
			status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
			break;
		case DELTAOBJECT:
			Metadata metadata;
			try {
				metadata = MManager.getInstance().getMetadata();
				Map<String, List<String>> deltaObjectMap = metadata.getDeltaObjectMap();
				resp.setDeltaObjectMap(deltaObjectMap);
			} catch (PathErrorException e) {
				//LOGGER.error("cannot get delta object map", e);
			}
			status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
			break;
		case COLUMN:
			try {
				resp.setDataType(MManager.getInstance().getSeriesType(req.getColumnPath()).toString());
			} catch (PathErrorException e) {
				//LOGGER.error("cannot get column {} data type", req.getColumnPath(), e);
			}
			status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
			break;
		default:
			status = new TS_Status(TS_StatusCode.ERROR_STATUS);
			status.setErrorMessage(String.format("Unsuport fetch metadata operation %s", req.getType()));
			break;
		}
		resp.setStatus(status);
		return resp;
	}

	/**
	 * Judge whether the statement is ADMIN COMMAND and if true, execute it.
	 *
	 * @param statement
	 *            command
	 * @return true if the statement is ADMIN COMMAND
	 * @throws IOException
	 *             exception
	 */
	private boolean execAdminCommand(String statement) throws IOException {
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
			// writeLogManager.overflowFlush();
			// writeLogManager.bufferFlush();
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
	public TSExecuteBatchStatementResp executeBatchStatement(TSExecuteBatchStatementReq req) throws TException {
		try {
			if (!checkLogin()) {
				LOGGER.info("TsFileDB Server: Not login.");
				return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Not login", null);
			}
			List<String> statements = req.getStatements();
			List<Integer> result = new ArrayList<>();

			for (String statement : statements) {
				PhysicalPlan physicalPlan = processor.parseSQLToPhysicalPlan(statement);
				if (physicalPlan.isQuery()) {
					return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "statement is query :" + statement,
							result);
				}
				ExecuteUpdateStatement(physicalPlan);
			}

			return getTSBathExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS, "Execute statements successfully",
					result);
		} catch (Exception e) {
			LOGGER.error("TsFileDB Server: error occurs when executing statements", e);
			return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage(), null);
		}

	}

	@Override
	public TSExecuteStatementResp executeStatement(TSExecuteStatementReq req) throws TException {
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
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
			}

			PhysicalPlan physicalPlan;
			try {
				physicalPlan = processor.parseSQLToPhysicalPlan(statement);
			} catch (IllegalASTFormatException e) {
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
						"Statement format is not right:" + e.getMessage());
			} catch (NullPointerException e) {
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Statement is not allowed");
			}
			if (physicalPlan.isQuery()) {
				return executeQueryStatement(req);
			} else {
				return ExecuteUpdateStatement(physicalPlan);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
		}
	}

	@Override
	public TSExecuteStatementResp executeQueryStatement(TSExecuteStatementReq req) throws TException {

		try {
			if (!checkLogin()) {
				LOGGER.info("TsFileDB Server: Not login.");
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Not login");
			}

			String statement = req.getStatement();
			PhysicalPlan plan = processor.parseSQLToPhysicalPlan(statement);

			List<Path> paths;
			paths = plan.getPaths();

			// check path exists
			if (paths.size() == 0) {
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Timeseries does not exist.");
			}

			// check file level set
			try {
				MManager.getInstance().checkFileLevel(paths);
			} catch (PathErrorException e) {
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
			}

			// check permissions
			if (!checkAuthorization(paths, plan.getOperatorType())) {
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "No permissions for this query.");
			}

			TSExecuteStatementResp resp = getTSExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS, "");
			List<String> columns = new ArrayList<>();
			// Restore column header of aggregation to func(column_name), only
			// support single aggregation function for now
			String aggregateFuncName = null;
			try {
				aggregateFuncName = (String) processor.getExecutor().getParameter(SQLConstant.IS_AGGREGATION);
			} catch (NullPointerException ignored) {
			}
			if (aggregateFuncName != null) {
				columns.add(aggregateFuncName + "(" + paths.get(0).getFullPath() + ")");
			} else {
				for (Path p : paths) {
					columns.add(p.getFullPath());
				}
			}
			TSHandleIdentifier operationId = new TSHandleIdentifier(ByteBuffer.wrap(username.get().getBytes()),
					ByteBuffer.wrap(("PASS".getBytes())));
			TSOperationHandle operationHandle;
			resp.setColumns(columns);
			operationHandle = new TSOperationHandle(operationId, true);
			resp.setOperationHandle(operationHandle);
			recordANewQuery(statement, plan);
			resp.setOperationType(aggregateFuncName);
			return resp;
		} catch (Exception e) {
			LOGGER.error("TsFileDB Server: server internal error: {}", e.getMessage());
			return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
		}
	}

	@Override
	public TSFetchResultsResp fetchResults(TSFetchResultsReq req) throws TException {
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
				PhysicalPlan physicalPlan = queryStatus.get().get(statement);
				processor.getExecutor().setFetchSize(fetchSize);
				queryDataSetIterator = processor.getExecutor().processQuery(physicalPlan);
				queryRet.get().put(statement, queryDataSetIterator);
			} else {
				queryDataSetIterator = queryRet.get().get(statement);
			}

			boolean hasResultSet;
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
	public TSExecuteStatementResp executeUpdateStatement(TSExecuteStatementReq req) throws TException {
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

	private TSExecuteStatementResp ExecuteUpdateStatement(PhysicalPlan plan) throws TException {
		try {
			List<Path> paths = plan.getPaths();

			if (!checkAuthorization(paths, plan.getOperatorType())) {
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "No permissions for this operation");
			}
			// TODO
			// In current version, we only return OK/ERROR
			// Do we need to add extra information of executive condition
			boolean execRet;
			try {
				execRet = processor.getExecutor().processNonQuery(plan);
			} catch (ProcessorException e) {
				return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
			}
			if (TsfileDBDescriptor.getInstance().getConfig().enableWal
					&& WriteLogManager.getInstance().isRecovering == false && execRet && needToBeWrittenToLog(plan)) {
				writeLogManager.write(plan);
			}
			TS_StatusCode statusCode = execRet ? TS_StatusCode.SUCCESS_STATUS : TS_StatusCode.ERROR_STATUS;
			String msg = execRet ? "Execute successfully" : "Execute statement error.";
			TSExecuteStatementResp resp = getTSExecuteStatementResp(statusCode, msg);
			TSHandleIdentifier operationId = new TSHandleIdentifier(ByteBuffer.wrap(username.get().getBytes()),
					ByteBuffer.wrap(("PASS".getBytes())));
			TSOperationHandle operationHandle;
			operationHandle = new TSOperationHandle(operationId, false);
			resp.setOperationHandle(operationHandle);
			return resp;
		} catch (QueryProcessorException e) {
			LOGGER.error(e.getMessage());
			return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
		} catch (IOException e) {
			LOGGER.error("TsFileDB Server: write preLog error", e);
			return getTSExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS, "Write log error");
		}
	}

	private TSExecuteStatementResp ExecuteUpdateStatement(String statement)
			throws TException, QueryProcessorException, IOException, ProcessorException {

		PhysicalPlan physicalPlan;
		try {
			physicalPlan = processor.parseSQLToPhysicalPlan(statement);
		} catch (QueryProcessorException | ArgsErrorException e) {
			e.printStackTrace();
			return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
		}

		if (physicalPlan.isQuery()) {
			return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Statement is a query statement.");
		}

		// if operation belongs to add/delete/update
		List<Path> paths = physicalPlan.getPaths();

		if (!checkAuthorization(paths, physicalPlan.getOperatorType())) {
			return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "No permissions for this operation");
		}

		// TODO
		// In current version, we only return OK/ERROR
		// Do we need to add extra information of executive condition
		boolean execRet;
		try {
			execRet = processor.getExecutor().processNonQuery(physicalPlan);
		} catch (ProcessorException e) {
			return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
		}
		if (WriteLogManager.isRecovering == false && execRet && needToBeWrittenToLog(physicalPlan)) {
			try {
				writeLogManager.write(physicalPlan);
			} catch (PathErrorException e) {
				throw new ProcessorException(e);
			}
		}
		TS_StatusCode statusCode = execRet ? TS_StatusCode.SUCCESS_STATUS : TS_StatusCode.ERROR_STATUS;
		String msg = execRet ? "Execute successfully" : "Execute statement error.";
		TSExecuteStatementResp resp = getTSExecuteStatementResp(statusCode, msg);
		TSHandleIdentifier operationId = new TSHandleIdentifier(ByteBuffer.wrap(username.get().getBytes()),
				ByteBuffer.wrap(("PASS".getBytes())));
		TSOperationHandle operationHandle;
		operationHandle = new TSOperationHandle(operationId, false);
		resp.setOperationHandle(operationHandle);
		return resp;
	}

	private boolean needToBeWrittenToLog(PhysicalPlan plan) {
		if (plan.getOperatorType() == OperatorType.UPDATE) {
			return true;
		}
		if (plan.getOperatorType() == OperatorType.DELETE) {
			return true;
		}
		return false;
	}

	private void recordANewQuery(String statement, PhysicalPlan physicalPlan) {
		queryStatus.get().put(statement, physicalPlan);
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
		return username.get() != null;
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

	public void handleClientExit() throws TException {
		closeOperation(null);
		closeSession(null);
	}
}
