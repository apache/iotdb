package cn.edu.thu.tsfiledb.qp.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import cn.edu.thu.tsfiledb.qp.logical.sys.AuthorOperator;
import cn.edu.thu.tsfiledb.qp.logical.sys.MetadataOperator;
import cn.edu.thu.tsfiledb.qp.logical.sys.PropertyOperator;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.thu.tsfiledb.qp.physical.crud.*;
import cn.edu.thu.tsfiledb.qp.physical.sys.AuthorPlan;
import cn.edu.thu.tsfiledb.qp.physical.sys.LoadDataPlan;
import cn.edu.thu.tsfiledb.qp.physical.sys.MetadataPlan;
import cn.edu.thu.tsfiledb.qp.physical.sys.PropertyPlan;
import cn.edu.thu.tsfiledb.utils.LoadDataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfiledb.auth.AuthException;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.query.engine.OverflowQueryEngine;

public class OverflowQPExecutor extends QueryProcessExecutor {

    private static final Logger logger = LoggerFactory.getLogger(OverflowQPExecutor.class);
    private OverflowQueryEngine queryEngine;
    private FileNodeManager fileNodeManager;
    private MManager mManager = MManager.getInstance();

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
                return update(update.getPath(), update.getStartTime(), update.getEndTime(), update.getValue());
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
    public QueryDataSet query(List<Path> paths, FilterExpression timeFilter, FilterExpression freqFilter,
                              FilterExpression valueFilter, int fetchSize, QueryDataSet lastData) throws ProcessorException {
        QueryDataSet ret;
        for (Path path : paths) {
            if (!mManager.pathExist(path.getFullPath())) {
                throw new ProcessorException(String.format("Timeseries %s does not exist", path.getFullPath()));
            }
            try {
                mManager.getFileNameByPath(path.getFullPath());
            } catch (PathErrorException e) {
                throw new ProcessorException(e);
            }
        }
        try {
            if (parameters.get() != null && parameters.get().containsKey(SQLConstant.IS_AGGREGATION)) {
                if (lastData != null) {
                    lastData.clear();
                    return lastData;
                }
                String aggregateFuncName = (String) parameters.get().get(SQLConstant.IS_AGGREGATION);
                ret = queryEngine.aggregate(paths.get(0), aggregateFuncName, timeFilter, freqFilter, valueFilter);
            } else {
                ret = queryEngine.query(paths, timeFilter, freqFilter, valueFilter, lastData, fetchSize);
            }

            return ret;
        } catch (Exception e) {
            logger.error("Error in query", e);
            e.printStackTrace();
            return null;
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
            if (dataType == TSDataType.TEXT) {
                if ((value.startsWith(SQLConstant.QUOTE) && value.endsWith(SQLConstant.QUOTE))
                        || (value.startsWith(SQLConstant.DQUOTE) && value.endsWith(SQLConstant.DQUOTE))) {
                    value = value.substring(1, value.length() - 1);
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
            String p = deltaObjectId + "." + measurementId;
            if (!mManager.pathExist(p)) {
                throw new ProcessorException(String.format("Timeseries %s does not exist.", p));
            }
            mManager.getFileNameByPath(p);
            TSDataType type = mManager.getSeriesType(deltaObjectId+","+measurementId);
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
            TSDataType type = mManager.getSeriesType(deltaObjectId+","+measurementId);
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
                mManager.getFileNameByPath(p);
                TSDataType dataType = mManager.getSeriesType(p);
				/*
				 * modify by liukun
				 */
                String value = insertValues.get(i);
                if (dataType == TSDataType.TEXT) {
                    if ((value.startsWith(SQLConstant.QUOTE) && value.endsWith(SQLConstant.QUOTE))
                            || (value.startsWith(SQLConstant.DQUOTE) && value.endsWith(SQLConstant.DQUOTE))) {
                        value = value.substring(1, value.length() - 1);
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

        MManager mManager = getMManager();
        try {
            switch (namespaceType) {
                case ADD_PATH:
                    mManager.addAPathToMTree(path.getFullPath(), dataType, encoding, encodingArgs);
                    break;
                case DELETE_PATH:
                    if (!mManager.pathExist(path.getFullPath())) {
                        throw new ProcessorException(
                                String.format("Timeseries %s does not exist.", path.getFullPath()));
                    }

                    List<String> pathStringList = mManager.getPaths(path.getFullPath());
                    deleteDataOfTimeSeries(mManager, pathStringList);
                    mManager.deletePathFromMTree(path.getFullPath());
                    break;
                case SET_FILE_LEVEL:
                    if (!mManager.pathExist(path.getFullPath())) {
                        throw new ProcessorException(String.format("Timeseries %s does not exist.", path.getFullPath()));
                    }
                    /**
                     * only once
                     */
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
     * @param mManager
     * @param pathList
     * @throws PathErrorException
     * @throws ProcessorException
     */
    private void deleteDataOfTimeSeries(MManager mManager, List<String> pathList) throws PathErrorException, ProcessorException {
        for (String p : pathList) {
            if (mManager.pathExist(p)) {
                DeletePlan deletePlan = new DeletePlan();
                deletePlan.addPath(new Path(p));
                deletePlan.setDeleteTime(Long.MAX_VALUE);
                processNonQuery(deletePlan);
            }
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
                    mManager.addAPathToPTree(propertyPath.getFullPath());
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
