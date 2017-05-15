package cn.edu.thu.tsfiledb.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.constant.SystemConstant;

public class TSFileDBDescriptor {
	private static final Logger LOGGER = LoggerFactory.getLogger(TSFileDBDescriptor.class);

	private static TSFileDBDescriptor descriptor = new TSFileDBDescriptor();

	private final String CONFIG_DEFAULT_PATH = "/tsfiledb.properties";

	private TSFileDBDescriptor() {
		loadYaml();
	}

	public static TSFileDBDescriptor getInstance() {
		return descriptor;
	}

	public TSFileDBConfig getConfig() {
		return conf;
	}

	private TSFileDBConfig conf = new TSFileDBConfig();

	/**
	 * load an properties file and set TsfileDBConfig variables
	 *
	 */
	private void loadYaml() {
		String url = System.getProperty(SystemConstant.TSFILE_HOME, CONFIG_DEFAULT_PATH);
		InputStream inputStream = null;
		if (url.equals(CONFIG_DEFAULT_PATH)) {
			inputStream = this.getClass().getResourceAsStream(url);
			return;
		} else {
			url = url + "/conf/tsfiledb.properties";
			try {
				File file = new File(url);
				inputStream = new FileInputStream(file);
			} catch (FileNotFoundException e) {
				LOGGER.error("Fail to find config file {}", url, e);
				System.exit(1);
			}
		}
		LOGGER.info("Start to read config file {}", url);
		Properties properties = new Properties();
		try {
			properties.load(inputStream);

			conf.writeInstanceThreshold = Integer.parseInt(properties.getProperty("writeInstanceThreshold", conf.writeInstanceThreshold + ""));
			conf.overflowDataDir = properties.getProperty("overflowDataDir", url+"/data/overflow");
			conf.FileNodeDir = properties.getProperty("FileNodeDir", url+"/data/digest");
			conf.BufferWriteDir = properties.getProperty("BufferWriteDir", url+"/data/delta");
			conf.metadataDir = properties.getProperty("metadataDir", url+"/data/metadata");
			conf.derbyHome = properties.getProperty("derbyHome", url+"/data/derby");
			conf.mergeConcurrentThreadNum = Integer.parseInt(properties.getProperty("mergeConcurrentThreadNum", conf.mergeConcurrentThreadNum + ""));
			conf.maxFileNodeNum = Integer.parseInt(properties.getProperty("maxFileNodeNum", conf.maxFileNodeNum + ""));
			conf.maxOverflowNodeNum = Integer.parseInt(properties.getProperty("maxOverflowNodeNum", conf.maxOverflowNodeNum + ""));
			conf.maxBufferWriteNodeNum = Integer.parseInt(properties.getProperty("maxBufferWriteNodeNum", conf.maxBufferWriteNodeNum + ""));
			conf.defaultFetchSize = Integer.parseInt(properties.getProperty("defaultFetchSize", conf.defaultFetchSize + ""));
			conf.writeLogPath = properties.getProperty("writeLogPath", url+"/data/writeLog.log");

		} catch (IOException e) {
			LOGGER.warn("Cannot load config file, use default configuration", e);
		} catch (Exception e) {
			LOGGER.warn("Error format in config file, use default configuration", e);
		}
		if (inputStream != null) {
			try {
				inputStream.close();
			} catch (IOException e) {
				LOGGER.error("Fail to close config file input stream", e);
			}
		}
	}

	public static void main(String[] args) {
		TSFileDBDescriptor descriptor = TSFileDBDescriptor.getInstance();
		TSFileDBConfig config = descriptor.getConfig();

		System.out.println(config.writeInstanceThreshold);
		System.out.println(config.overflowDataDir);
		System.out.println(config.FileNodeDir);
		System.out.println(config.BufferWriteDir);
		System.out.println(config.metadataDir);
		System.out.println(config.derbyHome);
		System.out.println(config.mergeConcurrentThreadNum);
		System.out.println(config.maxFileNodeNum);
		System.out.println(config.maxOverflowNodeNum);
		System.out.println(config.maxBufferWriteNodeNum);
		System.out.println(config.defaultFetchSize);
		System.out.println(config.writeLogPath);
	}
}
