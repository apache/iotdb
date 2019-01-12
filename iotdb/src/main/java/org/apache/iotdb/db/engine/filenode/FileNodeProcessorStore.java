package org.apache.iotdb.db.engine.filenode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is used to store information about filenodeProcessor status.
 * lastUpdateTime will changed and stored by bufferwrite flush or bufferwrite
 * close. emptyIntervalFileNode and newFileNodes will changed and stored by
 * overflow flush and overflow close. fileNodeProcessorState will changed and
 * stored by work->merge merge->wait wait->work. numOfMergeFile will changed and
 * stored in work to merge.
 * 
 * @author liukun
 *
 */
public class FileNodeProcessorStore implements Serializable {

	private static final long serialVersionUID = -54525372941897565L;

	private boolean isOverflowed;
	private Map<String, Long> lastUpdateTimeMap;
	private IntervalFileNode emptyIntervalFileNode;
	private List<IntervalFileNode> newFileNodes;
	private int numOfMergeFile;
	private FileNodeProcessorStatus fileNodeProcessorStatus;

	public FileNodeProcessorStore(boolean isOverflowed,Map<String, Long> lastUpdateTimeMap, IntervalFileNode emptyIntervalFileNode,
			List<IntervalFileNode> newFileNodes, FileNodeProcessorStatus fileNodeProcessorStatus, int numOfMergeFile) {
		this.isOverflowed =  isOverflowed;
		this.lastUpdateTimeMap = lastUpdateTimeMap;
		this.emptyIntervalFileNode = emptyIntervalFileNode;
		this.newFileNodes = newFileNodes;
		this.fileNodeProcessorStatus = fileNodeProcessorStatus;
		this.numOfMergeFile = numOfMergeFile;
	}
	
	public boolean isOverflowed() {
		return isOverflowed;
	}

	public void setOverflowed(boolean isOverflowed) {
		this.isOverflowed = isOverflowed;
	}

	public FileNodeProcessorStatus getFileNodeProcessorStatus() {
		return fileNodeProcessorStatus;
	}

	public void setFileNodeProcessorStatus(FileNodeProcessorStatus fileNodeProcessorStatus) {
		this.fileNodeProcessorStatus = fileNodeProcessorStatus;
	}

	public Map<String, Long> getLastUpdateTimeMap() {
		return new HashMap<String, Long>(lastUpdateTimeMap);
	}

	public void setLastUpdateTimeMap(Map<String, Long> lastUpdateTimeMap) {
		this.lastUpdateTimeMap = lastUpdateTimeMap;
	}

	public IntervalFileNode getEmptyIntervalFileNode() {
		return emptyIntervalFileNode;
	}

	public List<IntervalFileNode> getNewFileNodes() {
		return newFileNodes;
	}

	public int getNumOfMergeFile() {
		return numOfMergeFile;
	}

	public void setEmptyIntervalFileNode(IntervalFileNode emptyIntervalFileNode) {
		this.emptyIntervalFileNode = emptyIntervalFileNode;
	}

	public void setNewFileNodes(List<IntervalFileNode> newFileNodes) {
		this.newFileNodes = newFileNodes;
	}

	public void setNumOfMergeFile(int numOfMergeFile) {
		this.numOfMergeFile = numOfMergeFile;
	}
}
