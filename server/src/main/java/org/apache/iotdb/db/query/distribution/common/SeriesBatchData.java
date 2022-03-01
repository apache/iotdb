package org.apache.iotdb.db.query.distribution.common;

import org.apache.iotdb.tsfile.read.common.BatchData;

/**
 * @author xingtanzjr
 * TODO: currently we only use it to describe the result set of SeriesScanOperator
 * The BatchData is suitable as the encapsulation of part of result set of SeriesScanOperator
 * BatchData is the class defined and generally used in single-node IoTDB
 * We leverage it as the `batch` here. We can consider a more general name or make some modifications for it.
 */
public class SeriesBatchData extends BatchData {

}
