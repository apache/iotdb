package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.RowGroupBlockMetaDataCache;
import org.apache.iotdb.db.engine.cache.TsFileMetaDataCache;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.engine.memcontrol.BasicMemController;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryTokenManager;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.engine.cache.RowGroupBlockMetaDataCache;
import org.apache.iotdb.db.engine.cache.TsFileMetaDataCache;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.engine.memcontrol.BasicMemController;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryTokenManager;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * <p>
 * This class is used for cleaning test environment in unit test and integration
 * test
 * </p>
 * 
 * @author liukun
 *
 */
public class EnvironmentUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(EnvironmentUtils.class);

	private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
	private static Directories directories = Directories.getInstance();
	private static TSFileConfig tsfileConfig = TSFileDescriptor.getInstance().getConfig();

	public static void cleanEnv() throws IOException, FileNodeManagerException {

		QueryTokenManager.getInstance().endQueryForCurrentRequestThread();

		// clear opened file streams
		FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();

		// tsFileConfig.duplicateIncompletedPage = false;
		// clean filenode manager
		try {
			if (!FileNodeManager.getInstance().deleteAll()) {
				LOGGER.error("Can't close the filenode manager in EnvironmentUtils");
				System.exit(1);
			}
		} catch (FileNodeManagerException e) {
			throw new IOException(e);
		}
		StatMonitor.getInstance().close();
		FileNodeManager.getInstance().resetFileNodeManager();
		// clean wal
		MultiFileLogNodeManager.getInstance().stop();
		// clean cache
		TsFileMetaDataCache.getInstance().clear();
		RowGroupBlockMetaDataCache.getInstance().clear();
		// close metadata
		MManager.getInstance().clear();
		MManager.getInstance().flushObjectToFile();
		// delete all directory
		cleanAllDir();
		// FileNodeManager.getInstance().reset();
		// reset MemController
		BasicMemController.getInstance().close();
		try {
			BasicMemController.getInstance().start();
		} catch (StartupException e) {
			LOGGER.error("",e);
		}
	}

	private static void cleanAllDir() throws IOException {
		// delete bufferwrite
		for(String path : directories.getAllTsFileFolders()){
			cleanDir(path);
		}
		// delete overflow
		cleanDir(config.overflowDataDir);
		// delete filenode
		cleanDir(config.fileNodeDir);
		// delete metadata
		cleanDir(config.metadataDir);
		// delete wal
		cleanDir(config.walFolder);
		// delete derby
		cleanDir(config.derbyHome);
		// delete index
		cleanDir(config.indexFileDir);
		// delte data
		cleanDir("data");
		// delte derby log
		// cleanDir("derby.log");
	}

	public static void cleanDir(String dir) throws IOException {
		File file = new File(dir);
		if (file.exists()) {
			if (file.isDirectory()) {
				for (File subFile : file.listFiles()) {
					cleanDir(subFile.getAbsolutePath());
				}
			}
			if (!file.delete()) {
				throw new IOException(String.format("The file %s can't be deleted", dir));
			}
		}
	}

	/**
	 * disable the system monitor</br>
	 * this function should be called before all code in the setup
	 */
	public static void closeStatMonitor() {
		config.enableStatMonitor = false;
	}

	/**
	 * disable memory control</br>
	 * this function should be called before all code in the setup
	 */
	public static void closeMemControl() {
		config.enableMemMonitor = false;
	}

	public static void envSetUp() throws StartupException {
		// disable the memory control
		config.enableMemMonitor = false;
		// disable the system monitor
		config.enableStatMonitor = false;
		IAuthorizer authorizer = null;
		try {
			authorizer = LocalFileAuthorizer.getInstance();
		} catch (AuthException e) {
			throw new StartupException(e.getMessage());
		}
		try {
			authorizer.reset();
		} catch (AuthException e) {
			throw new StartupException(e.getMessage());
		}
		FileNodeManager.getInstance().resetFileNodeManager();
		MultiFileLogNodeManager.getInstance().start();
	}
}
