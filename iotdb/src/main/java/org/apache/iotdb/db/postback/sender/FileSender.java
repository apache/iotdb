package org.apache.iotdb.db.postback.sender;

import java.util.Set;
/**
 * @author lta
 */
public interface FileSender {
	/**
	 * Connect to server
	 */
	void connectToReceiver(String serverIp, int serverPort);
	
	/**
	 * Transfer UUID to receiver
	 */
	boolean transferUUID(String uuidPath);

	/**
	 * Make file snapshots before sending files
	 */
	Set<String> makeFileSnapshot(Set<String> sendingFileList);

	/**
	 * Send schema file to receiver.
	 */
	void sendSchema(String schemaPath);

	/**
	 * For each file in fileList, send it to receiver side
	 * @param fileSnapshotList:snapshot file list to send
	 */
	void startSending(Set<String> fileSnapshotList);

	/**
	 * Close socket after send files
	 */
	boolean afterSending();
	
	/**
	 * Execute a postback task.
	 */
	void postback();

}
