package org.apache.iotdb.db.mpp.plan.statement.metadata.template;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

/**
 * @author chenhuangyun
 * @date 2022/6/28
 **/
public class CreateSchemaTemplateStatement extends Statement {


    String name;
    Set<String> alignedDeviceId;
    String[] schemaNames;
    String[][] measurements;
    TSDataType[][] dataTypes;
    TSEncoding[][] encodings;
    CompressionType[][] compressors;

    // constant to help resolve serialized sequence
    private static final int NEW_PLAN = -1;

    public CreateSchemaTemplateStatement(){
        super();
        statementType = StatementType.CREATE_TEMPLATE;
    }

    public CreateSchemaTemplateStatement(String name,
        List<List<String>> measurements,
        List<List<TSDataType>> dataTypes,
        List<List<TSEncoding>> encodings,
        List<List<CompressionType>> compressors){
        this();
        this.name = name;
        schemaNames = null;
        this.measurements = new String[measurements.size()][];
        for (int i = 0; i < measurements.size(); i++) {
            this.measurements[i] = new String[measurements.get(i).size()];
            for (int j = 0; j < measurements.get(i).size(); j++) {
                this.measurements[i][j] = measurements.get(i).get(j);
            }
        }

        this.dataTypes = new TSDataType[dataTypes.size()][];
        for (int i = 0; i < dataTypes.size(); i++) {
            this.dataTypes[i] = new TSDataType[dataTypes.get(i).size()];
            for (int j = 0; j < dataTypes.get(i).size(); j++) {
                this.dataTypes[i][j] = dataTypes.get(i).get(j);
            }
        }

        this.encodings = new TSEncoding[dataTypes.size()][];
        for (int i = 0; i < encodings.size(); i++) {
            this.encodings[i] = new TSEncoding[dataTypes.get(i).size()];
            for (int j = 0; j < encodings.get(i).size(); j++) {
                this.encodings[i][j] = encodings.get(i).get(j);
            }
        }

        this.compressors = new CompressionType[dataTypes.size()][];
        for (int i = 0; i < compressors.size(); i++) {
            this.compressors[i] = new CompressionType[compressors.get(i).size()];
            for (int j = 0; j < compressors.get(i).size(); j++) {
                this.compressors[i][j] = compressors.get(i).get(j);
            }
        }
        this.alignedDeviceId = new HashSet<>();
    }

    private CreateSchemaTemplateStatement(
        String name,
        List<List<String>> measurements,
        List<List<TSDataType>> dataTypes,
        List<List<TSEncoding>> encodings,
        List<List<CompressionType>> compressors,
        Set<String> alignedDeviceId) {
        // Only accessed by deserialization, which may cause ambiguity with align designation
        this(name, measurements, dataTypes, encodings, compressors);
        this.alignedDeviceId = alignedDeviceId;
    }

    public CreateSchemaTemplateStatement(
        String name,
        String[][] measurements,
        TSDataType[][] dataTypes,
        TSEncoding[][] encodings,
        CompressionType[][] compressors) {
        this();
        this.name = name;
        this.schemaNames = null;
        this.measurements = measurements;
        this.dataTypes = dataTypes;
        this.encodings = encodings;
        this.compressors = compressors;
    }

    @Override
    public List<? extends PartialPath> getPaths() {
        return null;
    }

    public List<String> getSchemaNames() {
        if (schemaNames != null) {
            return Arrays.asList(schemaNames);
        } else {
            return null;
        }
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

    public List<List<String>> getMeasurements() {
        List<List<String>> ret = new ArrayList<>();
        for (String[] measurement : measurements) {
            ret.add(Arrays.asList(measurement));
        }
        return ret;
    }

    public List<List<TSDataType>> getDataTypes() {
        List<List<TSDataType>> ret = new ArrayList<>();
        for (TSDataType[] alignedDataTypes : dataTypes) {
            ret.add(Arrays.asList(alignedDataTypes));
        }
        return ret;
    }

    public List<List<TSEncoding>> getEncodings() {
        List<List<TSEncoding>> ret = new ArrayList<>();
        for (TSEncoding[] alignedEncodings : encodings) {
            ret.add(Arrays.asList(alignedEncodings));
        }
        return ret;
    }

    public List<List<CompressionType>> getCompressors() {
        List<List<CompressionType>> ret = new ArrayList<>();
        for (CompressionType[] alignedCompressor : compressors) {
            ret.add(Arrays.asList(alignedCompressor));
        }
        return ret;
    }

    @Override
    public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
        return visitor.visitCreateSchemaTemplate(this, context);
    }
}
