package cn.edu.tsinghua.iotdb.engine.overflow.io;

import java.io.File;

/**
 * @author liukun
 *
 */
public class EngineTestHelper {

	public static void delete(String filePath) {
		System.out.println(filePath);
		File file = new File(filePath);
		if (file.isDirectory()) {
			for (File subFile : file.listFiles()) {
				delete(subFile.getAbsolutePath());
			}
		}
		file.delete();
	}
}
