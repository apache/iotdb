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

public class TsfileDBDescriptor {
	private static final Logger LOGGER = LoggerFactory.getLogger(TsfileDBDescriptor.class);

	private static TsfileDBDescriptor descriptor = new TsfileDBDescriptor();

	private final String CONFIG_DEFAULT_PATH = "/tsfiledb.properties";

	private TsfileDBDescriptor() {
		loadYaml();
	}

	public static TsfileDBDescriptor getInstance() {
		return descriptor;
	}

	public TsfileDBConfig getConfig() {
		return conf;
	}

	private TsfileDBConfig conf = new TsfileDBConfig();

	/**
	 * load an properties file and set TsfileDBConfig variables
	 *
	 */
	private void loadYaml() {
		String tsfileHome = System.getProperty(SystemConstant.TSFILE_HOME, CONFIG_DEFAULT_PATH);
		String url;
		InputStream inputStream = null;
		if (tsfileHome.equals(CONFIG_DEFAULT_PATH)) {
			url = tsfileHome;
			inputStream = this.getClass().getResourceAsStream(url);
			return;
		} else {
			url = tsfileHome + "/conf/tsfiledb.properties";
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
			conf.overflowDataDir = properties.getProperty("overflowDataDir", tsfileHome+"/data/overflow");
			conf.FileNodeDir = properties.getProperty("FileNodeDir", tsfileHome+"/data/digest");
			conf.BufferWriteDir = properties.getProperty("BufferWriteDir", tsfileHome+"/data/delta");
			conf.metadataDir = properties.getProperty("metadataDir", tsfileHome+"/data/metadata");
			conf.derbyHome = properties.getProperty("derbyHome", tsfileHome+"/data/derby");
			conf.mergeConcurrentThreadNum = Integer.parseInt(properties.getProperty("mergeConcurrentThreadNum", conf.mergeConcurrentThreadNum + ""));
			conf.maxFileNodeNum = Integer.parseInt(properties.getProperty("maxFileNodeNum", conf.maxFileNodeNum + ""));
			conf.maxOverflowNodeNum = Integer.parseInt(properties.getProperty("maxOverflowNodeNum", conf.maxOverflowNodeNum + ""));
			conf.maxBufferWriteNodeNum = Integer.parseInt(properties.getProperty("maxBufferWriteNodeNum", conf.maxBufferWriteNodeNum + ""));
			conf.defaultFetchSize = Integer.parseInt(properties.getProperty("defaultFetchSize", conf.defaultFetchSize + ""));
			conf.walFolder = properties.getProperty("walFolder", tsfileHome+"/data/wals/");

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
		TsfileDBDescriptor descriptor = TsfileDBDescriptor.getInstance();
		TsfileDBConfig config = descriptor.getConfig();

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
		System.out.println(config.walFolder);
	}
}
