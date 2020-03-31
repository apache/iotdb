package org.apache.iotdb.cluster.log;

public class RaftSnapshot {

	private SnapshotMeta meta;

	public RaftSnapshot(SnapshotMeta meta) {
		this.meta = meta;
	}

	public long getLastIndex() {
		return meta.getLastIndex();
	}

	public long getLastTerm() {
		return meta.getLastTerm();
	}
}