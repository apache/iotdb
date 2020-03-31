package org.apache.iotdb.cluster.log;

public class SnapshotMeta {
	private long lastIndex;
	private long lastTerm;
	//private Configuration lastConfiguration;

	public SnapshotMeta(long lastIndex, long lastTerm) {
		this.lastIndex = lastIndex;
		this.lastTerm = lastTerm;
	}

	public long getLastIndex() {
		return lastIndex;
	}

	public long getLastTerm() {
		return lastTerm;
	}
}