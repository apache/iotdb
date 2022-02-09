/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package apache.iotdb.quality.validity;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class UDTFValidity implements UDTF {

    boolean usePreSpeed = false;
    double sMin;
    double sMax;
    ValidityComputing tsq = new ValidityComputing();

    public UDTFValidity() throws Exception {
    }


    @Override
    public void beforeStart(UDFParameters udfParameters, UDTFConfigurations udtfConfigurations) throws Exception {
        usePreSpeed = udfParameters.getBooleanOrDefault("usePreSpeed", false);
        sMax = udfParameters.getDoubleOrDefault("sMax", Double.MAX_VALUE);
        sMin = udfParameters.getDoubleOrDefault("sMin", -Double.MAX_VALUE + 1);
        udtfConfigurations.setAccessStrategy(new RowByRowAccessStrategy())
                .setOutputDataType(TSDataType.DOUBLE);
    }

    @Override
    public void transform(Row row, PointCollector collector) throws Exception {
        double value = Util.getValueAsDouble(row);
        long time = row.getTime();
        if (Double.isFinite(value)) {
            tsq.updateAll(time, value);
        }
    }

    @Override
    public void terminate(PointCollector collector) throws Exception {
        tsq.updateDPAll(usePreSpeed, sMin, sMax);
        tsq.updateReverseDPAll(usePreSpeed, sMin, sMax);
        collector.putDouble(0, tsq.getValidity());
    }

}
