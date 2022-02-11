/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package apache.iotdb.quality.alignment;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

import java.util.ArrayList;


public class UDTFAlignment implements UDTF {

    private ArrayList<ArrayList<Long>> timeArrayList = new ArrayList<>();
    private ArrayList<ArrayList<Double>> valueArrayList = new ArrayList<>();
    private int dim = -1;

    @Override
    public void beforeStart(UDFParameters udfParameters, UDTFConfigurations udtfConfigurations) throws Exception {
        udtfConfigurations.setAccessStrategy(new RowByRowAccessStrategy())
                .setOutputDataType(TSDataType.TEXT);
    }

    @Override
    public void transform(Row row, PointCollector collector) throws Exception {
        if (dim < 0) {
            dim = row.size();
            for (int i = 0; i < dim; i++) {
                timeArrayList.add(new ArrayList<>());
                valueArrayList.add(new ArrayList<>());
            }
        }
        for (int i = 0; i < dim; i++){
           if (!row.isNull(i)) {
               timeArrayList.get(i).add(row.getTime());
               valueArrayList.get(i).add(Util.getValueAsDouble(row, i));
           }
        }
    }

    @Override
    public void terminate(PointCollector collector) throws Exception {
        AlignmentExecuter alignmentExecuter = new AlignmentExecuter(this.timeArrayList, this.valueArrayList);
        alignmentExecuter.alignmentSearch();
        alignmentExecuter.trainModel();
        alignmentExecuter.alignmentSearch();
        ArrayList<Long> resultTimeList =  alignmentExecuter.getResultTimeList();
        ArrayList<String> resultValueList = alignmentExecuter.getResultValueList();

        for (int i = 0; i < resultTimeList.size(); i++) {
            collector.putString(resultTimeList.get(i), resultValueList.get(i));
        }
    }

    public static void main(String[] args) {

    }

}
