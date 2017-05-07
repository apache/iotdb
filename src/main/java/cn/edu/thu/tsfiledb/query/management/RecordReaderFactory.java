package cn.edu.thu.tsfiledb.query.management;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;


/**
 * To avoid create RecordReader frequently. Add cache in later version
 * 
 * @author Jinrui Zhang
 *
 */
public class RecordReaderFactory {
	private static final Logger logger = LoggerFactory.getLogger(RecordReaderFactory.class);
	private static RecordReaderFactory instance = new RecordReaderFactory();

	private FileNodeManager fileNodeManager;
	private ReadLockManager readLockManager;
	private FileStreamManager fileStreamManager;
	
	private RecordReaderFactory() {
		fileNodeManager = FileNodeManager.getInstance();
		readLockManager = ReadLockManager.getInstance();
		fileStreamManager = FileStreamManager.getInstance();
	}

	public RecordReader getRecordReader(String deltaObjectUID, String measurementID) throws ProcessorException {
		int token = readLockManager.lock(deltaObjectUID, measurementID);
		QueryStruct fileStruct = fileNodeManager.query(deltaObjectUID, measurementID, token);

		// TODO: This can be optimized in later version
		RecordReader recordReader = createANewRecordReader(deltaObjectUID, measurementID, fileStruct, token);
		return recordReader;
	}

	public RecordReader createANewRecordReader(String deltaObjectUID, String measurementID, QueryStruct fileStruct, int token) {
		RecordReader recordReader;
		List<FileInfoSnapshot> fileInfoSnapshotList = fileStruct.filePathList;
		List<TSRandomAccessFileReader> rafList = new ArrayList<>();
		for (FileInfoSnapshot fileInfoSnapshot : fileInfoSnapshotList) {
			TSRandomAccessFileReader raf = fileStreamManager.getLocalRandomAcessFileReader(fileInfoSnapshot.filePath);
			rafList.add(raf);
		}

		if (fileStruct.hasUnEnvelopeFile()) {
			TSRandomAccessFileReader raf = fileStreamManager
					.getLocalRandomAcessFileReader(fileStruct.unEnvelopeFileSnapshot.filePath);
			recordReader = new RecordReader(rafList, raf, fileStruct.rowGroupMetaDataList, 
					deltaObjectUID, measurementID, token, fileStruct.memoryData);
		} else {
			recordReader = new RecordReader(rafList, deltaObjectUID, measurementID, token, fileStruct.memoryData);
		}
		return recordReader;
	}

	public void closeOneRecordReader(RecordReader recordReader) throws ProcessorException {
		try{
			recordReader.close();
		}catch(IOException e){
			logger.error("Error in closing RecordReader : {}", e.getMessage());
			e.printStackTrace();
		}
	}

	public static RecordReaderFactory getInstance() {
		if (instance == null) {
			instance = new RecordReaderFactory();
		}
		return instance;
	}
}
