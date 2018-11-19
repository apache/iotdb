package cn.edu.tsinghua.tsfile.file.metadata;

import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;
import cn.edu.tsinghua.tsfile.format.RowGroupBlockMetaData;

public class TsRowGroupBlockMetaData implements IConverter<RowGroupBlockMetaData>{
	/**
     * Row groups in this file
     */
    private List<RowGroupMetaData> rowGroupMetadataList;
    
    private String deltaObjectID;
    
    public TsRowGroupBlockMetaData(){
    		rowGroupMetadataList = new ArrayList<>();
    }
    
    public TsRowGroupBlockMetaData(List<RowGroupMetaData> rowGroupMetadataList){
    		this.rowGroupMetadataList = rowGroupMetadataList;
    }
    
    /**
     * add row group metadata to rowGroups. THREAD NOT SAFE
     * @param rowGroup - row group metadata to add
     */
    public void addRowGroupMetaData(RowGroupMetaData rowGroup) {
        if (rowGroupMetadataList == null) {
            rowGroupMetadataList = new ArrayList<RowGroupMetaData>();
        }
        rowGroupMetadataList.add(rowGroup);
    }
    
    public List<RowGroupMetaData> getRowGroups() {
        return rowGroupMetadataList;
    }

    public void setRowGroups(List<RowGroupMetaData> rowGroupMetadataList) {
        this.rowGroupMetadataList = rowGroupMetadataList;
    }

	@Override
	public RowGroupBlockMetaData convertToThrift() {
//        long numOfRows = 0;
        List<cn.edu.tsinghua.tsfile.format.RowGroupMetaData> rowGroupMetaDataListInThrift = null;
        if (rowGroupMetadataList != null) {
            rowGroupMetaDataListInThrift =
                    new ArrayList<cn.edu.tsinghua.tsfile.format.RowGroupMetaData>();
            for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList) {
//                numOfRows += rowGroupMetaData.getNumOfRows();
                rowGroupMetaDataListInThrift.add(rowGroupMetaData.convertToThrift());
            }
        }
        RowGroupBlockMetaData rowGroupBlockMetaData= new RowGroupBlockMetaData(rowGroupMetaDataListInThrift);
        rowGroupBlockMetaData.setDelta_object_id(deltaObjectID);
        return rowGroupBlockMetaData;
	}

	@Override
	public void convertToTSF(RowGroupBlockMetaData metadataInThrift) {
        List<cn.edu.tsinghua.tsfile.format.RowGroupMetaData> rowGroupMetaDataListInThrift =
                metadataInThrift.getRow_groups_metadata();
        if (rowGroupMetaDataListInThrift == null) {
            rowGroupMetadataList = null;
        } else {
            rowGroupMetadataList = new ArrayList<RowGroupMetaData>();
            for (cn.edu.tsinghua.tsfile.format.RowGroupMetaData rowGroupMetaDataInThrift : rowGroupMetaDataListInThrift) {
                RowGroupMetaData rowGroupMetaDataInTSFile = new RowGroupMetaData();
                rowGroupMetaDataInTSFile.convertToTSF(rowGroupMetaDataInThrift);
                rowGroupMetadataList.add(rowGroupMetaDataInTSFile);
            }
        }
		this.deltaObjectID = metadataInThrift.getDelta_object_id();
	}

	public String getDeltaObjectID() {
		return deltaObjectID;
	}

	public void setDeltaObjectID(String deltaObjectID) {
		this.deltaObjectID = deltaObjectID;
	}
}
