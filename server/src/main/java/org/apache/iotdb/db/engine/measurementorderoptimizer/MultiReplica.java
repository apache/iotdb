package org.apache.iotdb.db.engine.measurementorderoptimizer;

import org.apache.iotdb.db.engine.divergentdesign.Replica;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MultiReplica {
	private Replica[] replicas;
	private int replicaNum;
	private String deviceID;

	public MultiReplica(MultiReplica replica) {
		this.replicaNum = replica.replicaNum;
		this.deviceID = replica.deviceID;
		this.replicas = new Replica[replicaNum];
		for (int i = 0; i < replicas.length; ++i) {
        replicas[i] = new Replica(replica.getReplicas()[i]);
		}
	}

	public MultiReplica(String deviceID) {
		this.deviceID = deviceID;
		replicaNum = 3;
		replicas = new Replica[replicaNum];
	}

	public MultiReplica(String deviceID, Replica[] replicas) {
		this.deviceID = deviceID;
		replicaNum = replicas.length;
		this.replicas = new Replica[replicaNum];
		for(int i = 0; i < replicaNum; ++i) {
			this.replicas[i] = new Replica(replicas[i]);
		}
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
		for (int i = 0; i < replicaNum; ++i) {
			Collections.shuffle(measurementOrder);
			replicas[i] = new Replica(deviceID, measurementOrder);
		}
	}

	public String getDeviceID() {
		return deviceID;
	}
}
