package cn.edu.tsinghua.iotdb.engine.overflow.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @author liukun
 *
 */
public class EngineTestHelper {

	private static final Logger logger = LoggerFactory.getLogger(EngineTestHelper.class);

	public static void delete(String filePath) {
		logger.info("delete file path : " + filePath);
		File file = new File(filePath);
		if (file.isDirectory()) {
			for (File subFile : file.listFiles()) {
				delete(subFile.getAbsolutePath());
			}
		}
		file.delete();
	}
}
