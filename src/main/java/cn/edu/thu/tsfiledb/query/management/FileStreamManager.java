package cn.edu.thu.tsfiledb.query.management;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.timeseries.read.LocalFileInput;


public class FileStreamManager {
	private static final Logger logger = LoggerFactory.getLogger(FileStreamManager.class);
	private static FileStreamManager instance = new FileStreamManager();
	
	private FileStreamManager(){
	}
	
	public TSRandomAccessFileReader getLocalRandomAccessFileReader(String path) throws FileNotFoundException{
		return new LocalFileInput(path);
	}
	
	public void closeLocalRandomAccessFileReader(LocalFileInput localFileInput) throws IOException{
		localFileInput.close();
	}
	
	public static FileStreamManager getInstance(){
		if(instance == null){
			instance = new FileStreamManager();
		}
		return instance;
	}

	public void closeFileStreams(TSRandomAccessFileReader raf){
		try {
			raf.close();
		} catch (IOException e) {
			logger.error("Error when close RAF: {}", e.getMessage());
			e.printStackTrace();
		}
	}
}
