package cn.edu.tsinghua.iotdb.postback.conf;

import java.io.File;

import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;

/**
 * @author lta
 */
public class PostBackSenderConfig {

	public static final String CONFIG_NAME = "iotdb-postbackClient.properties";

	public String[] iotdbBufferwriteDirectory = TsfileDBDescriptor.getInstance().getConfig().getBufferWriteDirs();
	public String dataDirectory = new File(TsfileDBDescriptor.getInstance().getConfig().dataDir).getAbsolutePath() + File.separator;
	public String uuidPath;
	public String lastFileInfo;
	public String[] snapshotPaths;
	public String schemaPath = new File(TsfileDBDescriptor.getInstance().getConfig().metadataDir).getAbsolutePath()	+ File.separator + "mlog.txt";
	public String serverIp = "127.0.0.1";
	public int serverPort = 5555;
	public int clientPort = 6666;
	public int uploadCycleInSeconds = 10;
	public boolean isClearEnable = false;
}
