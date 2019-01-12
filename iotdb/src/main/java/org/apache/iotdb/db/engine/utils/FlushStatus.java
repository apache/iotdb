package org.apache.iotdb.db.engine.utils;

/**
 * This class is used to represent the state of flush. It's can be used in the
 * bufferwrite flush{@code SequenceFileManager} and overflow
 * flush{@code OverFlowProcessor}.
 *
 * @author liukun
 */
public class FlushStatus {

	private boolean isFlushing;

	public FlushStatus() {
		this.isFlushing = false;
	}

	public boolean isFlushing() {
		return isFlushing;
	}

	public void setFlushing() {
		this.isFlushing = true;
	}

	public void setUnFlushing() {
		this.isFlushing = false;
	}

}
