package cn.edu.tsinghua.iotdb.engine;

import java.io.File;

import cn.edu.tsinghua.iotdb.conf.directories.Directories;
import cn.edu.tsinghua.iotdb.conf.IoTDBConfig;
import cn.edu.tsinghua.iotdb.conf.IoTDBDescriptor;

public class PathUtils {

	private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
	private static Directories directories = Directories.getInstance();

	public static File getBufferWriteDir(String nameSpacePath) {
		String bufferwriteDirPath = directories.getFolderForTest();
		if (bufferwriteDirPath.length() > 0
				&& bufferwriteDirPath.charAt(bufferwriteDirPath.length() - 1) != File.separatorChar) {
			bufferwriteDirPath = bufferwriteDirPath + File.separatorChar;
		}
		String dataDirPath = bufferwriteDirPath + nameSpacePath;
		File dataDir = new File(dataDirPath);
		return dataDir;
	}

	public static File getOverflowWriteDir(String nameSpacePath) {
		String overflowWriteDir = config.overflowDataDir;
		if (overflowWriteDir.length() > 0
				&& overflowWriteDir.charAt(overflowWriteDir.length() - 1) != File.separatorChar) {
			overflowWriteDir = overflowWriteDir + File.separatorChar;
		}
		String dataDirPath = overflowWriteDir + nameSpacePath;
		File dataDir = new File(dataDirPath);
		return dataDir;
	}

	public static File getFileNodeDir(String nameSpacePath) {
		String filenodeDir = config.fileNodeDir;
		if (filenodeDir.length() > 0 && filenodeDir.charAt(filenodeDir.length() - 1) != File.separatorChar) {
			filenodeDir = filenodeDir + File.separatorChar;
		}
		String dataDirPath = filenodeDir + nameSpacePath;
		File dataDir = new File(dataDirPath);
		return dataDir;
	}

}
