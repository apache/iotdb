package cn.edu.tsinghua.tsfile.write.schema;

import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

import java.util.Map;

/**
 * This class is used to build FileSchema of tsfile
 */
public class SchemaBuilder {
    /** the FileSchema which is being built **/
    private FileSchema fileSchema;

    /**
     * init schema by default value
     */
    public SchemaBuilder() {
        fileSchema = new FileSchema();
    }

    /**
     * add one series to TsFile schema
     *
     * @param measurementId (not null) id of the series
     * @param dataType      (not null) series data type
     * @param tsEncoding    (not null) encoding method you specified
     * @param props         information in encoding method.
     *                      For RLE, Encoder.MAX_POINT_NUMBER
     *                      For PLAIN, Encoder.MAX_STRING_LENGTH
     * @return this
     */
    public SchemaBuilder addSeries(String measurementId, TSDataType dataType, TSEncoding tsEncoding, CompressionType type,
                                   Map<String, String> props) {
        MeasurementSchema md = new MeasurementSchema(measurementId, dataType, tsEncoding, type, props);
        fileSchema.registerMeasurement(md);
        return this;
    }

    /**
     * add one series to tsfile schema
     *
     * @param measurementId (not null) id of the series
     * @param dataType      (not null) series data type
     * @param tsEncoding    (not null) encoding method you specified
     * @return this
     */
    public SchemaBuilder addSeries(String measurementId, TSDataType dataType, TSEncoding tsEncoding ) {
        MeasurementSchema md = new MeasurementSchema(measurementId, dataType, tsEncoding);
        fileSchema.registerMeasurement(md);
        return this;
    }

    /**
     * MeasurementSchema is the schema of one series
     *
     * @param descriptor series schema
     * @return schema builder
     */
    public SchemaBuilder addSeries(MeasurementSchema descriptor) {
        fileSchema.registerMeasurement(descriptor);
        return this;
    }

    /**
     * get file schema after adding all series and properties
     *
     * @return constructed file schema
     */
    public FileSchema build() {
        return this.fileSchema;
    }
}
