package cn.edu.thu.tsfiledb.engine.filenode;

import java.io.Serializable;

import cn.edu.thu.tsfile.timeseries.utils.StringContainer;

/**
 * This class is used to store one bufferwrite file status.<br>
 * 
 * @author liukun
 * @author kangrong
 *
 */
public class IntervalFileNode implements Serializable {
	private static final long serialVersionUID = -4309683416067212549L;
	public long startTime;
	public long endTime;
	public OverflowChangeType overflowChangeType;
	public String filePath;
	public String errFilePath;

	public IntervalFileNode(long startTime, long endTime, OverflowChangeType type, String filePath,
			String mergingErrFilePath) {
		this.startTime = startTime;
		this.endTime = endTime;
		this.overflowChangeType = type;
		this.filePath = filePath;
		this.errFilePath = mergingErrFilePath;
	}

	/**
	 * This is just used to construct a new bufferwritefile
	 * 
	 * @param startTime
	 * @param type
	 * @param filePath
	 * @param errFilePath
	 */
	public IntervalFileNode(long startTime, OverflowChangeType type, String filePath, String errFilePath) {
		this.startTime = startTime;
		this.overflowChangeType = type;
		this.filePath = filePath;
		this.errFilePath = errFilePath;
		this.endTime = -1;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}


	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}


	public void changeTypeToChanged(FileNodeProcessorState fileNodeProcessorState) {

		if (fileNodeProcessorState == FileNodeProcessorState.MERGING_WRITE) {
			overflowChangeType = OverflowChangeType.MERGING_CHANGE;
		} else {
			overflowChangeType = OverflowChangeType.CHANGED;
		}

		// switch (overflowChangeType) {
		// case NO_CHANGE:
		// if (fileNodeProcessorState == MERGING_WRITE)
		// overflowChangeType = OverflowChangeType.MERGING_CHANGE;
		// else
		// overflowChangeType = OverflowChangeType.CHANGED;
		// return true;
		// case CHANGED:
		// if (fileNodeProcessorState == MERGING_WRITE) {
		// overflowChangeType = OverflowChangeType.MERGING_CHANGE;
		// return true;
		// }
		// return false;
		// case MERGING_CHANGE:
		// return false;
		// default:
		// throw new
		// UnsupportedOperationException(overflowChangeType.toString());
		// }
	}

	public boolean changeTypeToUnChanged() {
		switch (overflowChangeType) {
		case NO_CHANGE:
			return false;
		case CHANGED:
			overflowChangeType = OverflowChangeType.NO_CHANGE;
			return true;
		case MERGING_CHANGE:
			overflowChangeType = OverflowChangeType.CHANGED;
			return true;
		default:
			throw new UnsupportedOperationException(overflowChangeType.toString());
		}
	}

	public boolean isClosed() {
		return endTime != -1;
	}

	@Override
	public String toString() {
		StringContainer sc = new StringContainer("_");
		sc.addTail("[", filePath, startTime, endTime, overflowChangeType, "]");
		return sc.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (endTime ^ (endTime >>> 32));
		result = prime * result + ((errFilePath == null) ? 0 : errFilePath.hashCode());
		result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
		result = prime * result + ((overflowChangeType == null) ? 0 : overflowChangeType.hashCode());
		result = prime * result + (int) (startTime ^ (startTime >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IntervalFileNode other = (IntervalFileNode) obj;
		if (endTime != other.endTime)
			return false;
		if (errFilePath == null) {
			if (other.errFilePath != null)
				return false;
		} else if (!errFilePath.equals(other.errFilePath))
			return false;
		if (filePath == null) {
			if (other.filePath != null)
				return false;
		} else if (!filePath.equals(other.filePath))
			return false;
		if (overflowChangeType != other.overflowChangeType)
			return false;
		if (startTime != other.startTime)
			return false;
		return true;
	}

	public IntervalFileNode backUp() {
		return new IntervalFileNode(startTime, endTime, overflowChangeType, filePath, errFilePath);
	}
}
