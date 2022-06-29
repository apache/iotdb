package org.apache.iotdb.db.mpp.plan.statement.metadata.template;

import java.util.List;
import java.util.Set;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

/**
 * @author chenhuangyun
 * @date 2022/6/28
 **/
public class CreateTemplateStatement  extends Statement {

    String name;
    Set<String> alignedDeviceId;
    String[] schemaNames;
    String[][] measurements;
    TSDataType[][] dataTypes;
    TSEncoding[][] encodings;
    CompressionType[][] compressors;

    public CreateTemplateStatement(){
        super();
        statementType = StatementType.CREATE_TEMPLATE;
    }

    @Override
    public List<? extends PartialPath> getPaths() {
        return null;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<String> getAlignedDeviceId() {
        return alignedDeviceId;
    }

    public void setAlignedDeviceId(Set<String> alignedDeviceId) {
        this.alignedDeviceId = alignedDeviceId;
    }

    public String[] getSchemaNames() {
        return schemaNames;
    }

    public void setSchemaNames(String[] schemaNames) {
        this.schemaNames = schemaNames;
    }

    public String[][] getMeasurements() {
        return measurements;
    }

    public void setMeasurements(String[][] measurements) {
        this.measurements = measurements;
    }

    public TSDataType[][] getDataTypes() {
        return dataTypes;
    }

    public void setDataTypes(TSDataType[][] dataTypes) {
        this.dataTypes = dataTypes;
    }

    public TSEncoding[][] getEncodings() {
        return encodings;
    }

    public void setEncodings(TSEncoding[][] encodings) {
        this.encodings = encodings;
    }

    public CompressionType[][] getCompressors() {
        return compressors;
    }

    public void setCompressors(CompressionType[][] compressors) {
        this.compressors = compressors;
    }
}
