package cn.edu.thu.tsfiledb.query.management;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.corp.tsfile.read.file.LocalFileInput;
import com.corp.tsfile.read.file.TSRandomAccessFileReader;

public class FileStreamManager {
	private static final Logger logger = LoggerFactory.getLogger(FileStreamManager.class);
	private static FileStreamManager instance = new FileStreamManager();
	
	private FileStreamManager(){
		
	}
	
	public TSRandomAccessFileReader getLocalRandomAcessFileReader(String path){
		return new LocalFileInput(path);
	}
	
	public void closeLocalRandomAcessFileReader(LocalFileInput localFileInput) throws IOException{
		localFileInput.close();
	}
	
	public static FileStreamManager getInstance(){
		if(instance == null){
			instance = new FileStreamManager();
		}
		return instance;
	}
	
	public void close(TSRandomAccessFileReader raf){
		try {
			raf.close();
		} catch (IOException e) {
			logger.error("Error when close RAF: {}", e.getMessage());
			e.printStackTrace();
		}
	}
}
