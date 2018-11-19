package cn.edu.tsinghua.tsfile.common.conf;

import cn.edu.tsinghua.tsfile.common.constant.SystemConstant;
import cn.edu.tsinghua.tsfile.timeseries.utils.Loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.Properties;
import java.util.Set;

/**
 * TSFileDescriptor is used to load TSFileConfig and provide configure
 * information
 *
 * @author kangrong
 */
public class TSFileDescriptor {
	private static final Logger LOGGER = LoggerFactory.getLogger(TSFileDescriptor.class);

	private static class TsfileDescriptorHolder {
		private static final TSFileDescriptor INSTANCE = new TSFileDescriptor();
	}

	private TSFileConfig conf = new TSFileConfig();

	private TSFileDescriptor() {
		loadProps();
	}

	public static final TSFileDescriptor getInstance() {
		return TsfileDescriptorHolder.INSTANCE;
	}

	public TSFileConfig getConfig() {
		return conf;
	}

	private void multiplicityWarning(String resource, ClassLoader classLoader) {
		try {
			Set<URL> urlSet = Loader.getResources(resource, classLoader);
			if (urlSet != null && urlSet.size() > 1) {
				LOGGER.warn("Resource [{}] occurs multiple times on the classpath", resource);
				for (URL url : urlSet) {
					LOGGER.warn("Resource [{}] occurs at [{}]", resource, url.toString());
				}
			}
		} catch (IOException e) {
			LOGGER.error("Failed to get url list for {}", resource);
		}
	}
	
	private URL getResource(String filename, ClassLoader classLoader){
		return Loader.getResource(filename, classLoader);
	}

	/**
	 * load an .properties file and set TSFileConfig variables
	 */
	private void loadProps() {
		InputStream inputStream = null;
		String url = System.getProperty(SystemConstant.TSFILE_CONF, null);
		if (url == null) {
			url = System.getProperty(SystemConstant.TSFILE_HOME, null);
			if (url != null) {
				url = url + File.separator + "conf" + File.separator + TSFileConfig.CONFIG_FILE_NAME;
			} else {
				ClassLoader  classLoader = Loader.getClassLoaderOfObject(this);
				URL u = getResource(TSFileConfig.CONFIG_FILE_NAME, classLoader);
				if(u == null){
					LOGGER.warn("Failed to find config file {} at classpath, use default configuration", TSFileConfig.CONFIG_FILE_NAME);
					return;
				} else{
					multiplicityWarning(TSFileConfig.CONFIG_FILE_NAME, classLoader);
					url = u.getFile();
				}
			}
		}
		try {
			inputStream = new FileInputStream(new File(url));
		} catch (FileNotFoundException e) {
			LOGGER.warn("Fail to find config file {}", url);
			return;
		}

		LOGGER.info("Start to read config file {}", url);
		Properties properties = new Properties();
		try {
			properties.load(inputStream);
			conf.groupSizeInByte = Integer.parseInt(properties.getProperty("group_size_in_byte", conf.groupSizeInByte + ""));
			conf.pageSizeInByte = Integer.parseInt(properties.getProperty("page_size_in_byte", conf.pageSizeInByte + ""));
			conf.maxNumberOfPointsInPage = Integer.parseInt(properties.getProperty("max_number_of_points_in_page", conf.maxNumberOfPointsInPage + ""));
			conf.timeSeriesDataType = properties.getProperty("time_series_data_type", conf.timeSeriesDataType);
			conf.maxStringLength = Integer.parseInt(properties.getProperty("max_string_length", conf.maxStringLength + ""));
			conf.floatPrecision = Integer.parseInt(properties.getProperty("float_precision", conf.floatPrecision + ""));
			conf.timeSeriesEncoder = properties.getProperty("time_series_encoder", conf.timeSeriesEncoder);
			conf.valueEncoder = properties.getProperty("value_encoder", conf.valueEncoder);
			conf.compressor = properties.getProperty("compressor", conf.compressor);
		} catch (IOException e) {
			LOGGER.warn("Cannot load config file because {}, use default configuration", e.getMessage());
		} catch (Exception e) {
			LOGGER.error("Loading settings {} failed because {}", url, e.getMessage());
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
					inputStream = null;
				} catch (IOException e) {
					LOGGER.error("Failed to close stream for loading config because {}", e.getMessage());
				}
			}
		}
	}
}
