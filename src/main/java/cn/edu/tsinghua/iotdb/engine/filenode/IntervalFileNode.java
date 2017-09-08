package cn.edu.tsinghua.iotdb.engine.filenode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to store one bufferwrite file status.<br>
 * 
 * @author liukun
 * @author kangrong
 *
 */
public class IntervalFileNode implements Serializable {

	private static final long serialVersionUID = -4309683416067212549L;

	public String filePath;
	public OverflowChangeType overflowChangeType;

	private Map<String, Long> startTimeMap;
	private Map<String, Long> endTimeMap;
	private Set<String> mergeChanged = new HashSet<>();

	public IntervalFileNode(Map<String, Long> startTimeMap, Map<String, Long> endTimeMap, OverflowChangeType type,
			String filePath) {

		this.overflowChangeType = type;
		this.filePath = filePath;

		this.startTimeMap = startTimeMap;
		this.endTimeMap = endTimeMap;
		
	}

	/**
	 * This is just used to construct a new bufferwritefile
	 * 
	 * @param startTime
	 * @param type
	 * @param filePath
	 * @param errFilePath
	 */
	public IntervalFileNode(OverflowChangeType type, String filePath) {

		this.overflowChangeType = type;
		this.filePath = filePath;

		startTimeMap = new HashMap<>();
		endTimeMap = new HashMap<>();
	}

	public void setStartTime(String deltaObjectId, long startTime) {

		startTimeMap.put(deltaObjectId, startTime);
	}

	public long getStartTime(String deltaObjectId) {

		if (startTimeMap.containsKey(deltaObjectId)) {
			return startTimeMap.get(deltaObjectId);
		} else {
			return -1;
		}
	}

	public Map<String, Long> getStartTimeMap() {

		return startTimeMap;
	}

	public void setStartTimeMap(Map<String, Long> startTimeMap) {

		this.startTimeMap = startTimeMap;
	}

	public void setEndTimeMap(Map<String, Long> endTimeMap) {

		this.endTimeMap = endTimeMap;
	}

	public void setEndTime(String deltaObjectId, long timestamp) {

		this.endTimeMap.put(deltaObjectId, timestamp);
	}

	public long getEndTime(String deltaObjectId) {

		if (endTimeMap.get(deltaObjectId) == null) {
			return -1;
		}
		return endTimeMap.get(deltaObjectId);
	}

	public Map<String, Long> getEndTimeMap() {

		return endTimeMap;
	}

	public void removeTime(String deltaObjectId) {

		startTimeMap.remove(deltaObjectId);
		endTimeMap.remove(deltaObjectId);
	}

	public boolean checkEmpty() {

		return startTimeMap.isEmpty() && endTimeMap.isEmpty();
	}

	public void clear() {

		startTimeMap.clear();
		endTimeMap.clear();
		mergeChanged.clear();
		overflowChangeType = OverflowChangeType.NO_CHANGE;
		filePath = null;
	}

	public void changeTypeToChanged(FileNodeProcessorStatus fileNodeProcessorState) {

		if (fileNodeProcessorState == FileNodeProcessorStatus.MERGING_WRITE) {
			overflowChangeType = OverflowChangeType.MERGING_CHANGE;
		} else {
			overflowChangeType = OverflowChangeType.CHANGED;
		}
	}

	public void addMergeChanged(String deltaObjectId) {

		mergeChanged.add(deltaObjectId);
	}

	public Set<String> getMergeChanged() {

		return mergeChanged;
	}

	public void clearMergeChanged() {

		mergeChanged.clear();
	}

	public boolean isClosed() {

		return !endTimeMap.isEmpty();

	}

	public IntervalFileNode backUp() {

		Map<String, Long> startTimeMap = new HashMap<>(this.startTimeMap);
		Map<String, Long> endTimeMap = new HashMap<>(this.endTimeMap);
		return new IntervalFileNode(startTimeMap, endTimeMap, overflowChangeType, filePath);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((endTimeMap == null) ? 0 : endTimeMap.hashCode());
		result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
		result = prime * result + ((overflowChangeType == null) ? 0 : overflowChangeType.hashCode());
		result = prime * result + ((startTimeMap == null) ? 0 : startTimeMap.hashCode());
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
		if (endTimeMap == null) {
			if (other.endTimeMap != null)
				return false;
		} else if (!endTimeMap.equals(other.endTimeMap))
			return false;
		if (filePath == null) {
			if (other.filePath != null)
				return false;
		} else if (!filePath.equals(other.filePath))
			return false;
		if (overflowChangeType != other.overflowChangeType)
			return false;
		if (startTimeMap == null) {
			if (other.startTimeMap != null)
				return false;
		} else if (!startTimeMap.equals(other.startTimeMap))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "IntervalFileNode [filePath=" + filePath + ", overflowChangeType=" + overflowChangeType
				+ ", startTimeMap=" + startTimeMap + ", endTimeMap=" + endTimeMap + ", mergeChanged=" + mergeChanged
				+ "]";
	}
}
