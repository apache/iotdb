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
                int result = multiInsert(insert.getDeltaObject(), insert.getTime(), insert.getMeasurements(), insert.getValues());
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
                throw new UnsupportedOperationException(String.format("operation %s does not support", plan.getOperatorType()));
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
        String device = path.getDeltaObjectToString();
        String sensor = path.getMeasurementToString();

        try {
            TSDataType type = queryEngine.getDataTypeByDeviceAndSensor(device, sensor);
            /*
             * modify by liukun
             */
            if(type==TSDataType.BYTE_ARRAY){
            	value = value.substring(1, value.length()-1);
            }
            fileNodeManager.update(device, sensor, startTime, endTime, type, value);
            return true;
        } catch (PathErrorException e) {
            throw new ProcessorException("Error in update: " + e.getMessage());
        } catch (FileNodeManagerException e) {
            e.printStackTrace();
            throw new ProcessorException(e);
        }
    }

    @Override
    public boolean delete(Path path, long timestamp) throws ProcessorException {
        String device = path.getDeltaObjectToString();
        String sensor = path.getMeasurementToString();
        try {
            TSDataType type = queryEngine.getDataTypeByDeviceAndSensor(device, sensor);
            fileNodeManager.delete(device, sensor, timestamp, type);
            return true;
        } catch (PathErrorException e) {
            throw new ProcessorException("Error in delete: " + e.getMessage());
        } catch (FileNodeManagerException e) {
            e.printStackTrace();
            throw new ProcessorException(e);
        }
    }

    @Override
    // return 0: failed, 1: Overflow, 2:Bufferwrite
    public int insert(Path path, long timestamp, String value) throws ProcessorException {
        String device = path.getDeltaObjectToString();
        String sensor = path.getMeasurementToString();

        try {
            TSDataType type = queryEngine.getDataTypeByDeviceAndSensor(device, sensor);
            TSRecord tsRecord = new TSRecord(timestamp, device);
            DataPoint dataPoint = DataPoint.getDataPoint(type, sensor, value);
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
            MManager mManager = MManager.getInstance();
            TSRecord tsRecord = new TSRecord(insertTime, deltaObject);

            for (int i = 0; i < measurementList.size(); i++) {
                String p = deltaObject + "." + measurementList.get(i);
                if (!mManager.pathExist(p)) {
                    throw new ProcessorException("Path not exists:" + p);
                }
                TSDataType dataType = mManager.getSeriesType(p);
                /*
                 * modify by liukun
                 */
                String value = insertValues.get(i);
                if(dataType==TSDataType.BYTE_ARRAY){
                	value = value.substring(1, value.length()-1);
                }
                DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementList.get(i), value);
                tsRecord.addTuple(dataPoint);
            }
            return fileNodeManager.insert(tsRecord);

        } catch (PathErrorException e) {
            throw new ProcessorException("Path error:" + e.getMessage());
        } catch (FileNodeManagerException e) {
            e.printStackTrace();
            throw new ProcessorException(e);
        }
    }

    private boolean operateAuthor(AuthorPlan author) throws ProcessorException{
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
//                    if(!mManager.pathExist(path.getFullPath())){
//                		throw new ProcessorException(String.format("path %s does not exists", path.getFullPath()));
//                    }
                    try {
                        // First delete all data interactive
                        deleteAllData(mManager, path);
                        // Then delete the metadata
                        mManager.deletePathFromMTree(path.getFullPath());
                    } catch (Exception e) {
                        return true;
                    }
                    break;
                case SET_FILE_LEVEL:
                    mManager.setStorageLevelToMTree(path.getFullPath());
                    break;
                default:
                    throw new ProcessorException("unknown namespace type:" + namespaceType);
            }
        } catch (PathErrorException | IOException | ArgsErrorException e) {
            throw new ProcessorException("meet error in " + namespaceType + " . " + e.getMessage());
        }
        return true;
    }

    private void deleteAllData(MManager mManager, Path path) throws PathErrorException, ProcessorException {
        ArrayList<String> pathList = mManager.getPaths(path.getFullPath());
        for (String p : pathList) {
            if(mManager.pathExist(p)){
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
            throw new ProcessorException("meet error in " + propertyType +" . "+ e.getMessage());
        }
        return true;
    }


}















