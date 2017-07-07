package cn.edu.thu.tsfiledb.query.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.thu.tsfile.timeseries.read.LocalFileInput;


/**
 * This class is used to construct FileReader. <br>
 * It is an adapter between {@code RecordReader} and {@code FileReader}
 *
 * @author Jinrui Zhang, CGF
 */

public class ReaderManager {

    // private FileReader fileReader;
    private List<FileReader> fileReaderList;

    private List<TSRandomAccessFileReader> rafList; // file has been serialized, sealed

    private HashMap<String, List<RowGroupReader>> rowGroupReaderMap;
    private List<RowGroupReader> rowGroupReaderList;

    //     private TSRandomAccessFileReader raf;
//    public ReaderManager(TSRandomAccessFileReader raf) throws IOException {
//        // this.raf = raf;
//        rowGroupReaderList = new ArrayList<>();
//        rowGroupReaderMap = new HashMap<>();
//
//        fileReader = new FileReader(raf);
//        addRowGroupReadersToMap(fileReader);
//        addRowGroupReadersToList(fileReader);
//    }

    /**
     * {NEWFUNC}
     *
     * @param rafList fileInputStreamList
     * @throws IOException
     */
    ReaderManager(List<TSRandomAccessFileReader> rafList) throws IOException {
        this.rafList = rafList;
        rowGroupReaderList = new ArrayList<>();
        rowGroupReaderMap = new HashMap<>();
        fileReaderList = new ArrayList<>();

        for (TSRandomAccessFileReader taf : rafList) {
            FileReader fr = new FileReader(taf);
            fileReaderList.add(fr);
            addRowGroupReadersToMap(fr);
            addRowGroupReadersToList(fr);
        }
//        for (int i = 0; i < rafList.size(); i++) {
//            FileReader fr = new FileReader(rafList.get(i));
//            fileReaderList.add(fr);
//            addRowGroupReadersToMap(fr);
//            addRowGroupReadersToList(fr);
//        }
    }

    /**
     * {NEWFUNC}
     *
     * @param rafList               file node list
     * @param unsealedFileReader fileReader for unsealedFile
     * @param rowGroupMetadataList  RowGroupMetadata List for unsealedFile
     * @throws IOException
     */
    ReaderManager(List<TSRandomAccessFileReader> rafList,
                         TSRandomAccessFileReader unsealedFileReader, List<RowGroupMetaData> rowGroupMetadataList) throws IOException {
        this(rafList);
        this.rafList.add(unsealedFileReader);

        FileReader fr = new FileReader(unsealedFileReader, rowGroupMetadataList);
        addRowGroupReadersToMap(fr);
        addRowGroupReadersToList(fr);
    }

    private void addRowGroupReadersToMap(FileReader fileReader) {
        HashMap<String, ArrayList<RowGroupReader>> rgrMap = fileReader.getRowGroupReadersMap();
        for (String deltaObjectUID : rgrMap.keySet()) {
            if (rowGroupReaderMap.containsKey(deltaObjectUID)) {
                rowGroupReaderMap.get(deltaObjectUID).addAll(rgrMap.get(deltaObjectUID));
            } else {
                rowGroupReaderMap.put(deltaObjectUID, rgrMap.get(deltaObjectUID));
            }
        }
    }

    private void addRowGroupReadersToList(FileReader fileReader) {
        this.rowGroupReaderList.addAll(fileReader.getRowGroupReaderList());
    }

    List<RowGroupReader> getAllRowGroupReaders() {
        return rowGroupReaderList;
    }

    List<RowGroupReader> getRowGroupReaderListByDeltaObject(String deltaObjectUID) {
        List<RowGroupReader> ret = rowGroupReaderMap.get(deltaObjectUID);
        if (ret == null) {
            return new ArrayList<>();
        }
        return ret;
    }

//    public RowGroupReader getRowGroupReader(String deviceUID, int index) {
//        return this.fileReader.getRowGroupReader(deviceUID, index);
//    }

//    public TSRandomAccessFileReader getInput() {
//        return this.raf;
//    }

//    TSDataType getDataTypeBySeriesName(String device, String sensor) {
//        ArrayList<RowGroupReader> rgrList = fileReader.getRowGroupReadersMap().get(device);
//        if (rgrList == null || rgrList.size() == 0) {
//            return null;
//        }
//        return rgrList.get(0).getDataTypeBySeriesName(sensor);
//    }

    HashMap<String, List<RowGroupReader>> getRowGroupReaderMap() {
        return rowGroupReaderMap;
    }

    /**
     * @throws IOException
     */
    public void close() throws IOException {
        for (TSRandomAccessFileReader raf : rafList) {
            if (raf instanceof LocalFileInput) {
                ((LocalFileInput) raf).closeFromManager();
            } else {
                raf.close();
            }
        }
    }
}
