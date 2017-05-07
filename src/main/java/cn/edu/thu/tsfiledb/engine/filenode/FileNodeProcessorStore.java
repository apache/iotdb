package cn.edu.thu.tsfiledb.engine.filenode;

import java.io.Serializable;
import java.util.List;

/**
 * This is used to store information about filenodeProcessor status.<br>
 * lastUpdateTime will changed and stored by bufferwrite flush or bufferwrite close<br>
 * emptyIntervalFileNode and newFileNodes will changed and stored by overflow flush and overflow close<br>
 * fileNodeProcessorState will changed and stored by work->merge merge->wait wait->work<br>
 * numOfMergeFile will changed and stored in work to merge
 * 
 * @author liukun
 *
 */
public class FileNodeProcessorStore implements Serializable {

	private static final long serialVersionUID = -54525372941897565L;

	private  long lastUpdateTime;
	private  IntervalFileNode emptyIntervalFileNode;
	private  List<IntervalFileNode> newFileNodes;
	private  int numOfMergeFile;
	private  FileNodeProcessorState fileNodeProcessorState;

	public FileNodeProcessorStore(long lastUpdateTime, IntervalFileNode emptyIntervalFileNode,
			List<IntervalFileNode> newFileNodes, FileNodeProcessorState fileNodeProcessorState,int numOfMergeFile) {
		this.lastUpdateTime = lastUpdateTime;
		this.emptyIntervalFileNode = emptyIntervalFileNode;
		this.newFileNodes = newFileNodes;
		this.fileNodeProcessorState = fileNodeProcessorState;
		this.numOfMergeFile = numOfMergeFile;
	}
	
	public long getLastUpdateTime() {
		return lastUpdateTime;
	}
	public IntervalFileNode getEmptyIntervalFileNode() {
		return emptyIntervalFileNode;
	}
	public List<IntervalFileNode> getNewFileNodes() {
		return newFileNodes;
	}
	public FileNodeProcessorState getFileNodeProcessorState() {
		return fileNodeProcessorState;
	}

	public int getNumOfMergeFile() {
		return numOfMergeFile;
	}

	public void setLastUpdateTime(long lastUpdateTime) {
		this.lastUpdateTime = lastUpdateTime;
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

	public void setFileNodeProcessorState(FileNodeProcessorState fileNodeProcessorState) {
		this.fileNodeProcessorState = fileNodeProcessorState;
	}
}
