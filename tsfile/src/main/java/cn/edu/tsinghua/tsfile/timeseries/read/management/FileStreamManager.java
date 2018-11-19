package cn.edu.tsinghua.tsfile.timeseries.read.management;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * This class provides some function to get one FileReader for one path.
 * Maybe in the later version, every FileReader will be managed by this class.
 *
 * @author Jinrui Zhang
 */
public class FileStreamManager {
    private static final Logger logger = LoggerFactory.getLogger(FileStreamManager.class);
    
	private static class FileStreamManagerHolder{
		private static final FileStreamManager INSTANCE = new FileStreamManager();
	}
	
    private FileStreamManager() {
    }

    public static final FileStreamManager getInstance() {
        return FileStreamManagerHolder.INSTANCE;
    }

    public ITsRandomAccessFileReader getLocalRandomAccessFileReader(String path) throws FileNotFoundException {
        return new TsRandomAccessLocalFileReader(path);
    }

    public void closeLocalRandomAccessFileReader(TsRandomAccessLocalFileReader localFileInput) throws IOException {
        localFileInput.close();
    }

    public void close(ITsRandomAccessFileReader raf) {
        try {
            raf.close();
        } catch (IOException e) {
            logger.error("Error when close RAF: {}", e.getMessage());
        }
    }
}
