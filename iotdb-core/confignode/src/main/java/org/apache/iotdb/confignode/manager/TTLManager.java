package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.TTLException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.consensus.request.read.ttl.ShowTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.response.ttl.ShowTTLResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

public class TTLManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TTLManager.class);
    private final IManager configManager;

    public TTLManager(IManager configManager) {
        this.configManager = configManager;
    }

    public TSStatus setTTL(SetTTLPlan setTTLPlan) {
        PartialPath path = new PartialPath(setTTLPlan.getDatabasePathPattern());
        if(!checkIsPathValidated(path)){
            TSStatus errorStatus = new TSStatus(TSStatusCode.ILLEGAL_PARAMETER.getStatusCode());
            errorStatus.setMessage(new TTLException(path.getFullPath()).getMessage());
            return errorStatus;
        }
        // if path matches database, then extends to path.**
        if(configManager.getPartitionManager().isDatabaseExist(path.getFullPath())){
            setTTLPlan.setDatabasePathPattern(path.concatNode(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD).getNodes());
        }
        return configManager.getProcedureManager().setTTL(setTTLPlan);
    }


    public DataSet showAllTTL(ShowTTLPlan showTTLPlan) {
        try {
            ShowTTLResp resp = (ShowTTLResp) configManager.getConsensusManager().read(showTTLPlan);
            return resp;
        } catch (ConsensusException e) {
            LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
            TSStatus tsStatus = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
            tsStatus.setMessage(e.getMessage());
            ShowTTLResp resp = new ShowTTLResp();
            resp.setStatus(tsStatus);
            return resp;
        }
    }

    private boolean checkIsPathValidated(PartialPath path){
        if (!path.isPrefixPath() && path.getFullPath().contains(ONE_LEVEL_PATH_WILDCARD)) {
            return false;
        }
        return true;
    }


}
