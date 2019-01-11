package cn.edu.tsinghua.iotdb.engine.filenode;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cn.edu.tsinghua.iotdb.conf.directories.Directories;

/**
 * This class is used to store one bufferwrite file status.<br>
 * 
 * @author liukun
 * @author kangrong
 *
 */
public class IntervalFileNode implements Serializable {

	private static final long serialVersionUID = -4309683416067212549L;

	private int baseDirIndex;
	private String relativePath;
	public OverflowChangeType overflowChangeType;

	private Map<String, Long> startTimeMap;
	private Map<String, Long> endTimeMap;
	private Set<String> mergeChanged = new HashSet<>();

	public IntervalFileNode(Map<String, Long> startTimeMap, Map<String, Long> endTimeMap, OverflowChangeType type,
			int baseDirIndex, String relativePath) {

		this.overflowChangeType = type;
		this.baseDirIndex = baseDirIndex;
		this.relativePath = relativePath;

		this.startTimeMap = startTimeMap;
		this.endTimeMap = endTimeMap;

	}

	/**
	 * This is just used to construct a new bufferwritefile
	 * 
	 * @param type
	 * @param relativePath
	 */
	public IntervalFileNode(OverflowChangeType type, int baseDirIndex, String relativePath) {

		this.overflowChangeType = type;
		this.baseDirIndex = baseDirIndex;
		this.relativePath = relativePath;

		startTimeMap = new HashMap<>();
		endTimeMap = new HashMap<>();
	}

	public IntervalFileNode(OverflowChangeType type, String baseDir, String relativePath) {

		this.overflowChangeType = type;
		this.baseDirIndex = Directories.getInstance().getTsFileFolderIndex(baseDir);
		this.relativePath = relativePath;

		startTimeMap = new HashMap<>();
		endTimeMap = new HashMap<>();
	}

	public IntervalFileNode(OverflowChangeType type, String relativePath) {

		this(type, 0, relativePath);
	}

	public void setStartTime(String deviceId, long startTime) {

		startTimeMap.put(deviceId, startTime);
	}

	public long getStartTime(String deviceId) {

		if (startTimeMap.containsKey(deviceId)) {
			return startTimeMap.get(deviceId);
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

	public void setEndTime(String deviceId, long timestamp) {

		this.endTimeMap.put(deviceId, timestamp);
	}

	public long getEndTime(String deviceId) {

		if (endTimeMap.get(deviceId) == null) {
			return -1;
		}
		return endTimeMap.get(deviceId);
	}

	public Map<String, Long> getEndTimeMap() {

		return endTimeMap;
	}

	public void removeTime(String deviceId) {

		startTimeMap.remove(deviceId);
		endTimeMap.remove(deviceId);
	}

	public String getFilePath() {

		if (relativePath == null) {
			return relativePath;
		}
		return new File(Directories.getInstance().getTsFileFolder(baseDirIndex), relativePath).getPath();
	}

	public void setBaseDirIndex(int baseDirIndex) {
		this.baseDirIndex = baseDirIndex;
	}

	public int getBaseDirIndex() { return baseDirIndex; }

	public void setRelativePath(String relativePath) {

		this.relativePath = relativePath;
	}

	public String getRelativePath() {

		return relativePath;
	}

	public boolean checkEmpty() {

		return startTimeMap.isEmpty() && endTimeMap.isEmpty();
	}

	public void clear() {

		startTimeMap.clear();
		endTimeMap.clear();
		mergeChanged.clear();
		overflowChangeType = OverflowChangeType.NO_CHANGE;
		relativePath = null;
	}

	public void changeTypeToChanged(FileNodeProcessorStatus fileNodeProcessorState) {

		if (fileNodeProcessorState == FileNodeProcessorStatus.MERGING_WRITE) {
			overflowChangeType = OverflowChangeType.MERGING_CHANGE;
		} else {
			overflowChangeType = OverflowChangeType.CHANGED;
		}
	}

	public void addMergeChanged(String deviceId) {

		mergeChanged.add(deviceId);
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
		return new IntervalFileNode(startTimeMap, endTimeMap, overflowChangeType, baseDirIndex, relativePath);
	}

	@Override
	public int hashCode() {

		final int prime = 31;
		int result = 1;
		result = prime * result + ((endTimeMap == null) ? 0 : endTimeMap.hashCode());
		result = prime * result + ((relativePath == null) ? 0 : relativePath.hashCode());
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
		if (relativePath == null) {
			if (other.relativePath != null)
				return false;
		} else if (!relativePath.equals(other.relativePath))
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

		return String.format(
				"IntervalFileNode [relativePath=%s,overflowChangeType=%s, startTimeMap=%s, endTimeMap=%s, mergeChanged=%s]",
				relativePath, overflowChangeType, startTimeMap, endTimeMap, mergeChanged);
	}
}
