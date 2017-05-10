package cn.edu.thu.tsfiledb.query.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.read.LocalFileInput;



/**
 * @description This class is used to construct FileReader. <br>
 * It is an adapter between {@code RecordReader} and {@code FileReader}
 * @author Jinrui Zhang
 *
 */

public class ReaderManager {

    private FileReader fileReader;
    private TSRandomAccessFileReader raf;
    
    private List<FileReader> fileReaderList;
    private List<TSRandomAccessFileReader> rafList;
    private HashMap<String, List<RowGroupReader>> rowGroupReaderMap;
	private List<RowGroupReader> rowGroupReaderList;

    public ReaderManager(TSRandomAccessFileReader raf) throws IOException {
    	this.raf = raf;
    	rowGroupReaderList = new ArrayList<>();
    	rowGroupReaderMap = new HashMap<>();
    	
        fileReader = new FileReader(raf);
        addRowGroupReadersToMap(fileReader);
		addRowGroupReadersToList(fileReader);
    }
    
    /**
     * {NEWFUNC}
     * @param rafList fileInputStreamList
     * @param rowGroupMetadataList RowGroupMetadatas for unenvelopedFile
     * @throws IOException 
     */
    public ReaderManager(List<TSRandomAccessFileReader> rafList) throws IOException{
    	this.rafList = rafList;
    	rowGroupReaderList = new ArrayList<>();
    	rowGroupReaderMap = new HashMap<>();
    	fileReaderList = new ArrayList<>();
    	
    	for(int i = 0 ; i < rafList.size() ; i ++){
    		FileReader fr = new FileReader(rafList.get(i));
    		fileReaderList.add(fr);
    		addRowGroupReadersToMap(fr);
    		addRowGroupReadersToList(fr);
    	}
    }
    
    /**
     * {NEWFUNC}
     * @param rafList
     * @param unenvelopedFileReader fileReader for unenvelopedFile
     * @param rowGroupMetadataList RowGroupMetadata List for unenvelopedFile
     * @throws IOException 
     */
    public ReaderManager(List<TSRandomAccessFileReader> rafList, 
    		TSRandomAccessFileReader unenvelopedFileReader, List<RowGroupMetaData> rowGroupMetadataList) throws IOException{
    	this(rafList);
    	this.rafList.add(unenvelopedFileReader);
    	
    	FileReader fr = new FileReader(unenvelopedFileReader, rowGroupMetadataList);
    	addRowGroupReadersToMap(fr);
		addRowGroupReadersToList(fr);
    }
    
    private void addRowGroupReadersToMap(FileReader fileReader){
    	HashMap<String, ArrayList<RowGroupReader>> rgrMap = fileReader.getRowGroupReadersMap();
    	for(String deltaObjectUID : rgrMap.keySet()){
    		if(rowGroupReaderMap.containsKey(deltaObjectUID)){
    			rowGroupReaderMap.get(deltaObjectUID).addAll(rgrMap.get(deltaObjectUID));
    		}else{
    			rowGroupReaderMap.put(deltaObjectUID, rgrMap.get(deltaObjectUID));
    		}
    	}
    }
    
    private void addRowGroupReadersToList(FileReader fileReader){
    	this.rowGroupReaderList.addAll(fileReader.getRowGroupReaderList());
    }
    
    public List<RowGroupReader> getAllRowGroupReaders(){
    	return rowGroupReaderList;
    }
    
    public List<RowGroupReader> getRowGroupReaderListByDeltaObject(String deltaObjectUID){
    	List<RowGroupReader> ret = rowGroupReaderMap.get(deltaObjectUID);
    	if(ret == null){
    		return new ArrayList<RowGroupReader>();
    	}
    	return ret;
    }
    
    public RowGroupReader getRowGroupReader(String deviceUID, int index) {
        return this.fileReader.getRowGroupReader(deviceUID, index);
    }

    public TSRandomAccessFileReader getInput() {
        return this.raf;
    }
    
    public TSDataType getDataTypeBySeriesName(String device, String sensor){
    	ArrayList<RowGroupReader> rgrlist = fileReader.getRowGroupReadersMap().get(device);
    	if(rgrlist == null || rgrlist.size() == 0){
    		return null;
    	}
    	return rgrlist.get(0).getDataTypeBySeriesName(sensor);
    }
    
    public HashMap<String, List<RowGroupReader>> getRowGroupReaderMap(){
    	return rowGroupReaderMap;
    }
    
    /**
     * @throws IOException
     */
    public void close() throws IOException{
    	for(TSRandomAccessFileReader raf : rafList){
    		if(raf instanceof LocalFileInput){
    			((LocalFileInput)raf).closeFromManager();
    		}else{
    			raf.close();
    		}
    	}
    }
}
