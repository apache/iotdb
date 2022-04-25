package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupReq;
import org.apache.iotdb.confignode.procedure.DeleteStorageGroupProcedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.procedure.ProcedureExecutor;
import org.apache.iotdb.procedure.scheduler.ProcedureScheduler;
import org.apache.iotdb.procedure.scheduler.SimpleProcedureScheduler;
import org.apache.iotdb.procedure.store.ProcedureStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProcedureManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureManager.class);
    private final Manager configNodeManager;
    private ProcedureExecutor<ConfigNodeProcedureEnv> executor;
    private ProcedureScheduler scheduler;
    private ProcedureStore store;
    private ConfigNodeProcedureEnv env;

    public ProcedureManager(Manager configNodeManager) {
        this.configNodeManager = configNodeManager;
        this.scheduler = new SimpleProcedureScheduler();
        this.store = new ProcedureStore();
        this.executor = new ProcedureExecutor<>(env,store,scheduler);
        activate();
    }

    private void activate(){
        if(configNodeManager.getConsensusManager().isLeader()){
            executor.init(4);
            executor.startWorkers();
            store.setRunning(true);
        }
    }

    public TSStatus deleteStorageGroup(DeleteStorageGroupReq deleteReq) {
        List<TStorageGroupSchema> deleteSgSchemaList = deleteReq.getDeleteSgSchemaList();
        DeleteStorageGroupProcedure deleteStorageGroupProcedure = new DeleteStorageGroupProcedure(deleteSgSchemaList);


        return null;
    }


    //======================================================
    /*
        GET-SET Region
     */
    //======================================================
    public Manager getConfigNodeManager() {
        return configNodeManager;
    }

    public ProcedureExecutor<ConfigNodeProcedureEnv> getExecutor() {
        return executor;
    }

    public void setExecutor(ProcedureExecutor<ConfigNodeProcedureEnv> executor) {
        this.executor = executor;
    }

    public ProcedureScheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(ProcedureScheduler scheduler) {
        this.scheduler = scheduler;
    }

    public ProcedureStore getStore() {
        return store;
    }

    public void setStore(ProcedureStore store) {
        this.store = store;
    }

    public ConfigNodeProcedureEnv getEnv() {
        return env;
    }

    public void setEnv(ConfigNodeProcedureEnv env) {
        this.env = env;
    }
}
