package org.apache.iotdb.db.engine.measurementorderoptimizer;

import org.apache.iotdb.db.engine.divergentdesign.Replica;

import java.util.ArrayList;
import java.util.List;

public class MultiReplica {
    private Replica[] replicas;
    private int replicaNum;
    private String deviceID;
    public MultiReplica(String deviceID) {
        this.deviceID = deviceID;
        replicaNum = 3;
        replicas = new Replica[replicaNum];
    }

    public MultiReplica(String deviceID, int replicaNum) {
        this.deviceID = deviceID;
        this.replicaNum = replicaNum;
    }

    public Replica[] getReplicas() {
        return replicas;
    }

    public void randomInit() {
        List<String> measurementOrder = new ArrayList<>(MeasurementOrderOptimizer.getInstance()
                .getMeasurementsOrder(deviceID));
        for(int i = 0; i < replicaNum; ++i) {

        }
    }
}
