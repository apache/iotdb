package cn.edu.thu.tsfiledb.engine.overflow.io;

import java.io.File;

/**
 * @author liukun
 *
 */
public class EngineTestHelper {

	public static void delete(String filePath) {
		File file = new File(filePath);
		if (file.isDirectory()) {
			for (File subFile : file.listFiles()) {
				delete(subFile.getAbsolutePath());
			}
		}
		file.delete();
	}
}
