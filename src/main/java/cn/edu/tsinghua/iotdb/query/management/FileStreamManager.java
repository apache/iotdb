package cn.edu.tsinghua.iotdb.query.management;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;

/**
 * This class is useless.
 */
public class FileStreamManager {
	private static final Logger logger = LoggerFactory.getLogger(FileStreamManager.class);
	private static FileStreamManager instance = new FileStreamManager();

	private FileStreamManager(){
	}

	public ITsRandomAccessFileReader getLocalRandomAccessFileReader(String path) throws FileNotFoundException{
		return new TsRandomAccessLocalFileReader(path);
	}

	public void closeLocalRandomAccessFileReader(TsRandomAccessLocalFileReader localFileInput) throws IOException{
		localFileInput.close();
	}

	public static FileStreamManager getInstance(){
		if(instance == null){
			instance = new FileStreamManager();
		}
		return instance;
	}

	public void closeFileStreams(ITsRandomAccessFileReader raf){
		try {
			raf.close();
		} catch (IOException e) {
			logger.error("Error when close RAF: {}", e.getMessage());
			e.printStackTrace();
		}
	}
}
