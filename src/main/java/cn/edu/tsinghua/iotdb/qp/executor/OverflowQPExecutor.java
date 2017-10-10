package cn.edu.tsinghua.iotdb.qp.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.ArgsErrorException;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.ColumnSchema;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.qp.constant.SQLConstant;
import cn.edu.tsinghua.iotdb.qp.logical.sys.AuthorOperator;
import cn.edu.tsinghua.iotdb.qp.logical.sys.MetadataOperator;
import cn.edu.tsinghua.iotdb.qp.logical.sys.PropertyOperator;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.DeletePlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.InsertPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.UpdatePlan;
import cn.edu.tsinghua.iotdb.qp.physical.sys.AuthorPlan;
import cn.edu.tsinghua.iotdb.qp.physical.sys.LoadDataPlan;
import cn.edu.tsinghua.iotdb.qp.physical.sys.MetadataPlan;
import cn.edu.tsinghua.iotdb.qp.physical.sys.PropertyPlan;
import cn.edu.tsinghua.iotdb.query.engine.FilterStructure;
import cn.edu.tsinghua.iotdb.query.engine.OverflowQueryEngine;
import cn.edu.tsinghua.iotdb.utils.LoadDataUtils;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

public class OverflowQPExecutor extends QueryProcessExecutor {

	private OverflowQueryEngine queryEngine;
	private FileNodeManager fileNodeManager;
	private MManager mManager = MManager.getInstance();
	private final String separator = "\\.";

	public OverflowQPExecutor() {
		queryEngine = new OverflowQueryEngine();
		fileNodeManager = FileNodeManager.getInstance();
	}

	@Override
	public boolean processNonQuery(PhysicalPlan plan) throws ProcessorException {
		switch (plan.getOperatorType()) {
		case DELETE:
			DeletePlan delete = (DeletePlan) plan;
			return delete(delete.getPaths(), delete.getDeleteTime());
		case UPDATE:
			UpdatePlan update = (UpdatePlan) plan;
			boolean flag = true;
			for (Pair<Long, Long> timePair : update.getIntervals()) {
				flag &= update(update.getPath(), timePair.left, timePair.right, update.getValue());
			}
			return flag;
		case INSERT:
			InsertPlan insert = (InsertPlan) plan;
			int result = multiInsert(insert.getDeltaObject(), insert.getTime(), insert.getMeasurements(),
					insert.getValues());
			return result > 0;
		case AUTHOR:
			AuthorPlan author = (AuthorPlan) plan;
			return operateAuthor(author);
		case LOADDATA:
			LoadDataPlan loadData = (LoadDataPlan) plan;
			LoadDataUtils load = new LoadDataUtils();
			load.loadLocalDataMultiPass(loadData.getInputFilePath(), loadData.getMeasureType(), getMManager());
			return true;
		case METADATA:
			MetadataPlan metadata = (MetadataPlan) plan;
			return operateMetadata(metadata);
		case PROPERTY:
			PropertyPlan property = (PropertyPlan) plan;
			return operateProperty(property);
		default:
			throw new UnsupportedOperationException(
					String.format("operation %s does not support", plan.getOperatorType()));
		}
	}

	@Override
	protected TSDataType getNonReservedSeriesType(Path path) throws PathErrorException {
		return MManager.getInstance().getSeriesType(path.getFullPath());
	}

	@Override
	protected boolean judgeNonReservedPathExists(Path path) {
		return MManager.getInstance().pathExist(path.getFullPath());
	}

	@Override
	public QueryDataSet aggregate(List<Pair<Path, String>> aggres, List<FilterStructure> filterStructures)
			throws ProcessorException, IOException, PathErrorException {
		return queryEngine.aggregate(aggres, filterStructures);
	}

	@Override
	public QueryDataSet query(int formNumber, List<Path> paths, FilterExpression timeFilter,
			FilterExpression freqFilter, FilterExpression valueFilter, int fetchSize, QueryDataSet lastData)
			throws ProcessorException {

		try {
			return queryEngine.query(formNumber, paths, timeFilter, freqFilter, valueFilter, lastData, fetchSize);
		} catch (Exception e) {
			throw new ProcessorException(e.getMessage());
		}
	}

	@Override
	public boolean update(Path path, long startTime, long endTime, String value) throws ProcessorException {
		String deltaObjectId = path.getDeltaObjectToString();
		String measurementId = path.getMeasurementToString();

		try {
			String fullPath = deltaObjectId + "." + measurementId;
			if (!mManager.pathExist(fullPath)) {
				throw new ProcessorException(String.format("Timeseries %s does not exist.", fullPath));
			}
			mManager.getFileNameByPath(fullPath);
			TSDataType dataType = mManager.getSeriesType(fullPath);
			/*
			 * modify by liukun
			 */
			if (dataType == TSDataType.BOOLEAN) {
				value = value.toLowerCase();
				if (SQLConstant.BOOLEAN_FALSE_NUM.equals(value)) {
					value = "false";
				} else if (SQLConstant.BOOLEAN_TRUE_NUM.equals(value)) {
					value = "true";
				} else if (!SQLConstant.BOOLEN_TRUE.equals(value) && !SQLConstant.BOOLEN_FALSE.equals(value)) {
					throw new ProcessorException(
							String.format("The BOOLEAN data type should be true/TRUE or false/FALSE"));
				}
			} else if (dataType == TSDataType.TEXT) {
				if ((value.startsWith(SQLConstant.QUOTE) && value.endsWith(SQLConstant.QUOTE))
						|| (value.startsWith(SQLConstant.DQUOTE) && value.endsWith(SQLConstant.DQUOTE))) {
					value = value.substring(1, value.length() - 1);
				} else {
					throw new ProcessorException(String.format("The TEXT data type should be covered by \" or '"));
				}
			}

			fileNodeManager.update(deltaObjectId, measurementId, startTime, endTime, dataType, value);
			return true;
		} catch (PathErrorException e) {
			throw new ProcessorException(e.getMessage());
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			throw new ProcessorException(e.getMessage());
		}
	}

	@Override
	public boolean delete(Path path, long timestamp) throws ProcessorException {
		String deltaObjectId = path.getDeltaObjectToString();
		String measurementId = path.getMeasurementToString();
		try {
			if (!mManager.pathExist(path.getFullPath())) {
				throw new ProcessorException(String.format("Timeseries %s does not exist.", path.getFullPath()));
			}
			mManager.getFileNameByPath(path.getFullPath());
			TSDataType type = mManager.getSeriesType(path.getFullPath());
			fileNodeManager.delete(deltaObjectId, measurementId, timestamp, type);
			return true;
		} catch (PathErrorException e) {
			throw new ProcessorException(e.getMessage());
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			throw new ProcessorException(e.getMessage());
		}
	}

	@Override
	// return 0: failed, 1: Overflow, 2:Bufferwrite
	public int insert(Path path, long timestamp, String value) throws ProcessorException {
		String deltaObjectId = path.getDeltaObjectToString();
		String measurementId = path.getMeasurementToString();

		try {
			TSDataType type = mManager.getSeriesType(deltaObjectId + "," + measurementId);
			TSRecord tsRecord = new TSRecord(timestamp, deltaObjectId);
			DataPoint dataPoint = DataPoint.getDataPoint(type, measurementId, value);
			tsRecord.addTuple(dataPoint);
			return fileNodeManager.insert(tsRecord);

		} catch (PathErrorException e) {
			throw new ProcessorException("Error in insert: " + e.getMessage());
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			throw new ProcessorException(e);
		}
	}

	@Override
	public int multiInsert(String deltaObject, long insertTime, List<String> measurementList, List<String> insertValues)
			throws ProcessorException {
		try {
			TSRecord tsRecord = new TSRecord(insertTime, deltaObject);
			for (int i = 0; i < measurementList.size(); i++) {
				String p = deltaObject + "." + measurementList.get(i);
				if (!mManager.pathExist(p)) {
					throw new ProcessorException(String.format("Timeseries %s does not exist.", p));
				}
				List<Path> paths = new ArrayList<>();
				paths.add(new Path(p));
				mManager.checkFileLevel(paths);
				TSDataType dataType = mManager.getSeriesType(p);
				/*
				 * modify by liukun
				 */
				String value = insertValues.get(i);
				if (dataType == TSDataType.BOOLEAN) {
					value = value.toLowerCase();
					if (SQLConstant.BOOLEAN_FALSE_NUM.equals(value)) {
						value = "false";
					} else if (SQLConstant.BOOLEAN_TRUE_NUM.equals(value)) {
						value = "true";
					} else if (!SQLConstant.BOOLEN_TRUE.equals(value) && !SQLConstant.BOOLEN_FALSE.equals(value)) {
						throw new ProcessorException(
								String.format("The BOOLEAN data type should be true/TRUE or false/FALSE"));
					}
				} else if (dataType == TSDataType.TEXT) {
					if ((value.startsWith(SQLConstant.QUOTE) && value.endsWith(SQLConstant.QUOTE))
							|| (value.startsWith(SQLConstant.DQUOTE) && value.endsWith(SQLConstant.DQUOTE))) {
						value = value.substring(1, value.length() - 1);
					} else {
						throw new ProcessorException(String.format("The TEXT data type should be covered by \" or '"));
					}
				}
				DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementList.get(i), value);
				tsRecord.addTuple(dataPoint);
			}
			return fileNodeManager.insert(tsRecord);

		} catch (PathErrorException | FileNodeManagerException e) {
			throw new ProcessorException(e.getMessage());
		}
	}

	@Override
	public List<String> getAllPaths(String originPath) throws PathErrorException {
		return getMManager().getPaths(originPath);
	}

	private boolean operateAuthor(AuthorPlan author) throws ProcessorException {
		AuthorOperator.AuthorType authorType = author.getAuthorType();
		String userName = author.getUserName();
		String roleName = author.getRoleName();
		String password = author.getPassword();
		String newPassword = author.getNewPassword();
		Set<Integer> permissions = author.getPermissions();
		Path nodeName = author.getNodeName();
		try {
			boolean flag = true;
			switch (authorType) {
			case UPDATE_USER:
				return updateUser(userName, newPassword);
			case CREATE_USER:
				return createUser(userName, password);
			case CREATE_ROLE:
				return createRole(roleName);
			case DROP_USER:
				return deleteUser(userName);
			case DROP_ROLE:
				return deleteRole(roleName);
			case GRANT_ROLE:
				for (int i : permissions) {
					if (!addPermissionToRole(roleName, nodeName.getFullPath(), i))
						flag = false;
				}
				return flag;
			case GRANT_USER:
				for (int i : permissions) {
					if (!addPermissionToUser(userName, nodeName.getFullPath(), i))
						flag = false;
				}
				return flag;
			case GRANT_ROLE_TO_USER:
				return grantRoleToUser(roleName, userName);
			case REVOKE_USER:
				for (int i : permissions) {
					if (!removePermissionFromUser(userName, nodeName.getFullPath(), i))
						flag = false;
				}
				return flag;
			case REVOKE_ROLE:
				for (int i : permissions) {
					if (!removePermissionFromRole(roleName, nodeName.getFullPath(), i))
						flag = false;
				}
				return flag;
			case REVOKE_ROLE_FROM_USER:
				return revokeRoleFromUser(roleName, userName);
			default:
				break;

			}
		} catch (AuthException e) {
			throw new ProcessorException(e.getMessage());
		}
		return false;
	}

	private boolean operateMetadata(MetadataPlan metadataPlan) throws ProcessorException {
		MetadataOperator.NamespaceType namespaceType = metadataPlan.getNamespaceType();
		Path path = metadataPlan.getPath();
		String dataType = metadataPlan.getDataType();
		String encoding = metadataPlan.getEncoding();
		String[] encodingArgs = metadataPlan.getEncodingArgs();
		List<Path> deletePathList = metadataPlan.getDeletePathList();
		try {
			switch (namespaceType) {
			case ADD_PATH:
				if (mManager.pathExist(path.getFullPath())) {
					throw new ProcessorException(String.format("Timeseries %s already exist", path.getFullPath()));
				}
				if (!mManager.checkFileNameByPath(path.getFullPath())) {
					throw new ProcessorException("Storage group should be created first");
				}
				String fileNodePath = mManager.getFileNameByPath(path.getFullPath());
				ArrayList<ColumnSchema> columnSchemas = mManager.getSchemaForFileName(fileNodePath);
				String lastNode = path.getMeasurementToString();
				boolean isNewMeasurement = true;
				for (ColumnSchema columnSchema : columnSchemas) {
					if (columnSchema.getName().equals(lastNode)) {
						isNewMeasurement = false;
						if (!columnSchema.geTsDataType().toString().equals(dataType)
								|| !columnSchema.getEncoding().toString().equals(encoding)) {
							throw new ProcessorException(String.format(
									"The dataType or encoding of the last node %s is conflicting in the storage group %s",
									lastNode, fileNodePath));
						}
					}
				}
				mManager.addPathToMTree(path.getFullPath(), dataType, encoding, encodingArgs);
				try {
					String namespacePath = mManager.getFileNameByPath(path.getFullPath());
					if (isNewMeasurement) {
						// add time series to schema
						fileNodeManager.addTimeSeries(path, dataType, encoding, encodingArgs);
					}
					// fileNodeManager.closeOneFileNode(namespacePath);
				} catch (PathErrorException | FileNodeManagerException e) {
					throw new ProcessorException(e);
				}
				break;
			case DELETE_PATH:
				if (deletePathList != null && !deletePathList.isEmpty()) {
					Set<String> pathSet = new HashSet<>();
					for (Path p : deletePathList) {
						ArrayList<String> subPaths = mManager.getPaths(p.getFullPath());
						if (subPaths.isEmpty()) {
							throw new ProcessorException(
									String.format("There are no timeseries in the prefix of %s path", p.getFullPath()));
						}
						pathSet.addAll(subPaths);
					}
					for (String p : pathSet) {
						if (!mManager.pathExist(p)) {
							throw new ProcessorException(String.format(
									"Timeseries %s does not exist and cannot be delete its metadata and data", p));
						}
					}
					List<String> fullPath = new ArrayList<>();
					fullPath.addAll(pathSet);
					try {
						deleteDataOfTimeSeries(fullPath);
					} catch (ProcessorException e) {
						throw new ProcessorException(e);
					}
					for (String p : fullPath) {
						String nameSpacePath = null;
						try {
							nameSpacePath = mManager.getFileNameByPath(p);
						} catch (PathErrorException e) {
							throw new ProcessorException(e);
						}
						// TODO: don't delete the storage group path recursively
						String deleteNameSpacePath = mManager.deletePathFromMTree(p);
						if (deleteNameSpacePath != null) {
							// TODO: should we delete the filenode in the disk
							// delete this filenode
							try {
								// clear filenode
								fileNodeManager.clearOneFileNode(deleteNameSpacePath);
								// close processor
								fileNodeManager.deleteOneFileNode(deleteNameSpacePath);
							} catch (FileNodeManagerException e) {
								throw new ProcessorException(e);
							}
						} else if (nameSpacePath != null) {
							// TODO: should we must close the filenode
							// close this filenode
							try {
								fileNodeManager.closeOneFileNode(nameSpacePath);
							} catch (FileNodeManagerException e) {
								throw new ProcessorException(e);
							}
						}
					}
				}
				break;
			case SET_FILE_LEVEL:
				mManager.setStorageLevelToMTree(path.getFullPath());
				break;
			default:
				throw new ProcessorException("unknown namespace type:" + namespaceType);
			}
		} catch (PathErrorException | IOException | ArgsErrorException e) {
			throw new ProcessorException(e.getMessage());
		}
		return true;
	}

	/**
	 * Delete all data of timeseries in pathList.
	 *
	 * @param pathList
	 *            deleted paths
	 * @throws PathErrorException
	 * @throws ProcessorException
	 */
	private void deleteDataOfTimeSeries(List<String> pathList) throws PathErrorException, ProcessorException {
		for (String p : pathList) {
			DeletePlan deletePlan = new DeletePlan();
			deletePlan.addPath(new Path(p));
			deletePlan.setDeleteTime(Long.MAX_VALUE);
			processNonQuery(deletePlan);
		}
	}

	private boolean operateProperty(PropertyPlan propertyPlan) throws ProcessorException {
		PropertyOperator.PropertyType propertyType = propertyPlan.getPropertyType();
		Path propertyPath = propertyPlan.getPropertyPath();
		Path metadataPath = propertyPlan.getMetadataPath();
		MManager mManager = getMManager();
		try {
			switch (propertyType) {
			case ADD_TREE:
				mManager.addAPTree(propertyPath.getFullPath());
				break;
			case ADD_PROPERTY_LABEL:
				mManager.addPathToPTree(propertyPath.getFullPath());
				break;
			case DELETE_PROPERTY_LABEL:
				mManager.deletePathFromPTree(propertyPath.getFullPath());
				break;
			case ADD_PROPERTY_TO_METADATA:
				mManager.linkMNodeToPTree(propertyPath.getFullPath(), metadataPath.getFullPath());
				break;
			case DEL_PROPERTY_FROM_METADATA:
				mManager.unlinkMNodeFromPTree(propertyPath.getFullPath(), metadataPath.getFullPath());
				break;
			default:
				throw new ProcessorException("unknown namespace type:" + propertyType);
			}
		} catch (PathErrorException | IOException | ArgsErrorException e) {
			throw new ProcessorException("meet error in " + propertyType + " . " + e.getMessage());
		}
		return true;
	}

}
