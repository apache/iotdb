package org.apache.iotdb.db.postback.conf;

import java.io.File;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

/**
 * @author lta
 */
public class PostBackSenderConfig {

	public static final String CONFIG_NAME = "iotdb-postbackClient.properties";

	public String[] iotdbBufferwriteDirectory = IoTDBDescriptor.getInstance().getConfig().getBufferWriteDirs();
	public String dataDirectory = new File(IoTDBDescriptor.getInstance().getConfig().dataDir).getAbsolutePath() + File.separator;
	public String uuidPath;
	public String lastFileInfo;
	public String[] snapshotPaths;
	public String schemaPath = new File(IoTDBDescriptor.getInstance().getConfig().metadataDir).getAbsolutePath()	+ File.separator + "mlog.txt";
	public String serverIp = "127.0.0.1";
	public int serverPort = 5555;
	public int clientPort = 6666;
	public int uploadCycleInSeconds = 10;
	public boolean isClearEnable = false;
}
