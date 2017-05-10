package cn.edu.thu.tsfiledb.qp.exec;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.derby.iapi.error.PublicAPI;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.auth.dao.Authorizer;
import cn.edu.thu.tsfiledb.auth.model.AuthException;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;


public abstract class QueryProcessExecutor {
    protected final boolean isSingleFile;
    /**
     * parameters allows to set and get user-defined parameters.
     */
    protected ThreadLocal<Map<String, Object>> parameters = new ThreadLocal<Map<String, Object>>();
    protected int fetchSize = 100;

    public QueryProcessExecutor(boolean isSingleFile) {
        this.isSingleFile = isSingleFile;
    }

    protected abstract TSDataType getNonReseveredSeriesType(Path fullPath);

    protected abstract boolean judgeNonReservedPathExists(Path fullPath);

    public boolean isSingleFile() {
        return isSingleFile;
    }

    public TSDataType getSeriesType(Path fullPath) {
        if (fullPath.equals(SQLConstant.RESERVED_TIME))
            return TSDataType.INT64;
        if (fullPath.equals(SQLConstant.RESERVED_FREQ))
            return TSDataType.FLOAT;
        if (fullPath.equals(SQLConstant.RESERVED_DELTA_OBJECT))
            return TSDataType.BYTE_ARRAY;
        return getNonReseveredSeriesType(fullPath);
    }

    public boolean judgePathExists(Path pathStr) {
        if (SQLConstant.isReservedPath(pathStr))
            return true;
        else
            return judgeNonReservedPathExists(pathStr);
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    /**
     * in getIndex command, fetchSize must be specified at first.
     * 
     * @return
     */
    public int getFetchSize() {
        return fetchSize;
    }

    /**
     * 
     * @param paths
     * @param timeFilter
     * @param freqFilter
     * @param valueFilter
     * @param fetchSize
     * @param lastData
     * @return
     */
    public abstract QueryDataSet query(List<Path> paths, FilterExpression timeFilter,
            FilterExpression freqFilter, FilterExpression valueFilter, int fetchSize,
            QueryDataSet lastData) throws ProcessorException;

    /**
     * execute update command and return whether the operator is successful.
     * 
     * @param path : update series path
     * @param startTime
     * @param endTime
     * @param value - in type of string
     * @return - whether the operator is successful.
     */
    public abstract boolean update(Path path, long startTime, long endTime, String value) throws ProcessorException;
    
    /**
     * execute delete command and return whether the operator is successful.
     * 
     * @param path : delete series path
     * @param deleteTime
     * @return - whether the operator is successful.
     */
    public abstract boolean delete(Path path, long deleteTime) throws ProcessorException;
    
    /**
     * execute insert command and return whether the operator is successful.
     * 
     * @param path
     * @param insertTime - it's time point but not a range
     * @param value
     * @return - Operate Type.
     */
    public abstract int insert(Path path, long insertTime, String value) throws ProcessorException;
    
    public abstract int multiInsert(String deltaObject, long insertTime,
    		List<String> measurementList, List<String> insertValues) throws ProcessorException;

    public MManager getMManager() {
        return MManager.getInstance();
    }

    public void addParameter(String key, Object value) {
    	if(parameters.get() == null){
    		parameters.set(new HashMap<String, Object>());
    	}
        parameters.get().put(key, value);
    }

    public Object getParameter(String key) {
        return parameters.get().get(key);
    }

    public void clearParamter(){
    	if(parameters.get() != null){
    		parameters.get().clear();
    	}
    }
    /*
     * 刘昆修改，添加修改用户密码功能
     */
    public boolean updateUser(String username,String newPassword) throws AuthException{
    	return Authorizer.updateUserPassword(username, newPassword);
    }
    
    public boolean createUser(String username, String password) throws AuthException {
        return Authorizer.createUser(username, password);
    }

    /**
     * 异常可能：用户不存在，权限编号不对等
     */
    public boolean addPmsToUser(String userName, String nodeName, int permissionId)
            throws AuthException {
        return Authorizer.addPmsToUser(userName, nodeName, permissionId);
    }

    /**
     * 异常可能：用户不存在；用户不具有该权限
     */
    public boolean removePmsFromUser(String userName, String nodeName, int permissionId)
            throws AuthException {
        return Authorizer.removePmsFromUser(userName, nodeName, permissionId);
    }


    /**
     * 异常可能：用户不存在
     */
    public boolean deleteUser(String userName) throws AuthException {
        return Authorizer.deleteUser(userName);
    }


    /**
     * 异常可能：角色已经存在
     */
    public boolean createRole(String roleName) throws AuthException {
        return Authorizer.createRole(roleName);
    }

    /**
     * 异常可能：角色不存在，或者权限ID不对
     */
    public boolean addPmsToRole(String roleName, String nodeName, int permissionId)
            throws AuthException {
        return Authorizer.addPmsToRole(roleName, nodeName, permissionId);
    }

    /**
     * 异常可能：角色不存在；权限ID不对；角色本来不拥有该权限
     */
    public boolean removePmsFromRole(String roleName, String nodeName, int permissionId)
            throws AuthException {
        return Authorizer.removePmsFromRole(roleName, nodeName, permissionId);
    }

    /**
     * 异常可能：角色不存在
     */
    public boolean deleteRole(String roleName) throws AuthException {
        return Authorizer.deleteRole(roleName);
    }

    /**
     * 异常可能：用户不存在；角色不存在；该用户已经拥有该该角色
     */
    public boolean grantRoleToUser(String roleName, String username) throws AuthException {
        return Authorizer.grantRoleToUser(roleName, username);
    }

    /**
     * 异常可能：用户不存在；角色不存在；该用户没有该角色
     */
    public boolean revokeRoleFromUser(String roleName, String username) throws AuthException {
        return Authorizer.revokeRoleFromUser(roleName, username);
    }

    /**
     * 异常可能:用户不存在
     */
    public Set<Integer> getPermissions(String username, String nodeName) throws AuthException {
        return Authorizer.getPermission(username, nodeName);
    }

    public PhysicalPlan queryPhysicalOptimize(PhysicalPlan plan) {
        return plan;
    }

    public PhysicalPlan nonQueryPhysicalOptimize(PhysicalPlan plan) {
        return plan;
    }

    public List<String> getAllPaths(String fullPath) throws PathErrorException {
        return getMManager().getPaths(fullPath);
    }
}
