package org.apache.iotdb.infludb;

import org.apache.iotdb.infludb.qp.constant.FilterConstant;
import org.apache.iotdb.infludb.qp.constant.SQLConstant;
import org.apache.iotdb.infludb.qp.logical.Operator;
import org.apache.iotdb.infludb.qp.logical.function.*;
import org.apache.iotdb.infludb.qp.logical.crud.*;
import org.apache.iotdb.infludb.qp.strategy.LogicalGenerator;
import org.apache.iotdb.infludb.query.expression.Expression;
import org.apache.iotdb.infludb.query.expression.ResultColumn;
import org.apache.iotdb.infludb.query.expression.unary.FunctionExpression;
import org.apache.iotdb.infludb.query.expression.unary.NodeExpression;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.influxdb.dto.*;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;

public class IotDBInfluxDB {

    private static Session session;
    //当前influxdb选择的database
    private String database;
    //当前influxdb选择的measurement
    private String measurement;
    //当前database下的所有measurement对应的tag列表及顺序
    //TODO 当前不考虑分布式的情况,假定写入都是由该实例执行的
    private Map<String, Map<String, Integer>> measurementTagOrder = new HashMap<>();
    //当前measurement下的tag列表及顺序
    private Map<String, Integer> tagOrders;

    //当前measurement下的field列表及指定规则的顺序
    private Map<String, Integer> fieldOrders;
    //反转map
    private Map<Integer, String> fieldOrdersReversed;

    //占位符
    private final String placeholder = "PH";

    /**
     * 构造函数
     *
     * @param url      包括host和port
     * @param userName 用户名
     * @param password 用户密码
     */
    public IotDBInfluxDB(String url, String userName, String password) {
        try {
            URI uri = new URI(url);
            new IotDBInfluxDB(uri.getHost(), uri.getPort(), userName, password);
        } catch (URISyntaxException | IoTDBConnectionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 构造函数
     *
     * @param host     域名
     * @param rpcPort  端口号
     * @param userName 用户名
     * @param password 用户密码
     */
    public IotDBInfluxDB(String host, int rpcPort, String userName, String password) throws IoTDBConnectionException {
        session = new Session(host, rpcPort, userName, password);
        session.open(false);

        session.setFetchSize(10000);
    }


    /**
     * 兼容influxdb的插入函数
     *
     * @param point 写入的point点
     */
    public void write(Point point) throws IoTDBConnectionException, StatementExecutionException {
        String measurement = null;
        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();
        Long time = null;
        Field[] reflectFields = point.getClass().getDeclaredFields();
        //利用反射获取influxdb中point的属性
        for (Field reflectField : reflectFields) {
            reflectField.setAccessible(true);
            try {
                if (reflectField.getType().getName().equalsIgnoreCase("java.util.Map") && reflectField.getName().equalsIgnoreCase("fields")) {
                    fields = (Map<String, Object>) reflectField.get(point);
                }
                if (reflectField.getType().getName().equalsIgnoreCase("java.util.Map") && reflectField.getName().equalsIgnoreCase("tags")) {
                    tags = (Map<String, String>) reflectField.get(point);
                }
                if (reflectField.getType().getName().equalsIgnoreCase("java.lang.String") && reflectField.getName().equalsIgnoreCase("measurement")) {
                    measurement = (String) reflectField.get(point);
                }
                if (reflectField.getType().getName().equalsIgnoreCase("java.lang.Number") && reflectField.getName().equalsIgnoreCase("time")) {
                    time = (Long) reflectField.get(point);
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        //设置为当前时间
        if (time == null) {
            time = System.currentTimeMillis();
        }
        tagOrders = measurementTagOrder.get(measurement);
        if (tagOrders == null) {
            tagOrders = new HashMap<>();
        }
        int measurementTagNum = tagOrders.size();
        //当前插入时实际tag的数量
        Map<Integer, String> realTagOrders = new HashMap<>();
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            if (tagOrders.containsKey(entry.getKey())) {
                realTagOrders.put(tagOrders.get(entry.getKey()), entry.getKey());
            } else {
                measurementTagNum++;
                updateNewTagIntoDB(measurement, entry.getKey(), measurementTagNum);
                realTagOrders.put(measurementTagNum, entry.getKey());
                tagOrders.put(entry.getKey(), measurementTagNum);
            }
        }
        //更新内存中map
        measurementTagOrder.put(measurement, tagOrders);
        StringBuilder path = new StringBuilder("root." + database + "." + measurement);
        for (int i = 1; i <= measurementTagNum; i++) {
            if (realTagOrders.containsKey(i)) {
                path.append(".").append(tags.get(realTagOrders.get(i)));
            } else {
                path.append("." + placeholder);
            }
        }

        List<String> measurements = new ArrayList<>();
        List<TSDataType> types = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            measurements.add(entry.getKey());
            Object value = entry.getValue();
            if (value instanceof String) {
                types.add(TSDataType.TEXT);
            } else if (value instanceof Integer) {
                types.add(TSDataType.INT32);
            } else if (value instanceof Double) {
                types.add(TSDataType.DOUBLE);
            } else {
                System.err.printf("can't solve type:%s", entry.getValue().getClass());
            }
            values.add(value);
        }
        session.insertRecord(String.valueOf(path), time, measurements, types, values);
    }

    /**
     * 兼容influxdb的查询函数
     *
     * @param query influxdb的查询参数，包括databaseName和sql语句
     * @return 返回Influxdb的查询结果
     */
    public QueryResult query(Query query) throws Exception {
        String sql = query.getCommand();
        String database = query.getDatabase();
        if (!this.database.equals(database)) {
            updateDatabase(database);
        }
        Operator operator = LogicalGenerator.generate(sql);
        IotDBInfluxDBUtils.checkQueryOperator(operator);
        //更新相关数据
        updateMeasurement(((QueryOperator) operator).getFromComponent().getNodeName().get(0));
        updateFiledOrders();
        //step1 生成查询的结果
        QueryResult queryResult = queryExpr(((QueryOperator) operator).getWhereComponent().getFilterOperator());
        //step2 进行select筛选
        ProcessSelectComponent(queryResult, ((QueryOperator) operator).getSelectComponent());
        return queryResult;
    }

    /**
     * 当有新的tag出现时，插入到数据库中
     *
     * @param measurement 插入的measurement
     * @param tag         对应的tag名称
     * @param order       对应的tag顺序
     */
    private void updateNewTagIntoDB(String measurement, String tag, int order) throws IoTDBConnectionException, StatementExecutionException {
        List<String> measurements = new ArrayList<>();
        List<TSDataType> types = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        measurements.add("database_name");
        measurements.add("measurement_name");
        measurements.add("tag_name");
        measurements.add("tag_order");
        types.add(TSDataType.TEXT);
        types.add(TSDataType.TEXT);
        types.add(TSDataType.TEXT);
        types.add(TSDataType.INT32);
        values.add(database);
        values.add(measurement);
        values.add(tag);
        values.add(order);
        session.insertRecord("root.TAG_INFO", System.currentTimeMillis(), measurements, types, values);
    }


    /**
     * 通过select的查询条件进一步处理获得的queryResult
     *
     * @param queryResult     需要处理的查询结果
     * @param selectComponent 需要过滤的select条件
     */
    private void ProcessSelectComponent(QueryResult queryResult, SelectComponent selectComponent) {
        //先获取当前数据结果的行顺序map
        List<String> columns = queryResult.getResults().get(0).getSeries().get(0).getColumns();
        Map<String, Integer> columnOrders = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            columnOrders.put(columns.get(i), i);
        }
        //获取当前values
        List<List<Object>> values = queryResult.getResults().get(0).getSeries().get(0).getValues();
        //新的行列表
        List<String> newColumns = new ArrayList<>();
        newColumns.add(SQLConstant.RESERVED_TIME);

        //当含有函数时
        if (selectComponent.isHasFunction()) {
            List<Function> functions = new ArrayList<>();
            for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
                Expression expression = resultColumn.getExpression();
                if (expression instanceof FunctionExpression) {
                    String functionName = ((FunctionExpression) expression).getFunctionName();
                    functions.add(FunctionFactory.generateFunction(functionName, ((FunctionExpression) expression).getExpressions()));
                    newColumns.add(functionName);
                } else if (expression instanceof NodeExpression) {
                    String columnName = ((NodeExpression) expression).getName();
                    if (!columnName.equals(SQLConstant.STAR)) {
                        newColumns.add(columnName);
                    } else {
                        newColumns.addAll(columns.subList(1, columns.size()));
                    }
                }
            }
            for (List<Object> value : values) {
                for (Function function : functions) {
                    List<Expression> expressions = function.getExpressions();
                    if (expressions == null) {
                        throw new IllegalArgumentException("not support param");
                    }
                    NodeExpression parmaExpression = (NodeExpression) expressions.get(0);
                    String parmaName = parmaExpression.getName();
                    if (columnOrders.containsKey(parmaName)) {
                        Object selectedValue = value.get(columnOrders.get(parmaName));
                        Long selectedTimestamp = (Long) value.get(0);
                        if (selectedValue != null) {
                            //选择函数
                            if (function instanceof Selector) {
                                ((Selector) function).updateValueAndRelate(new FunctionValue(selectedValue, selectedTimestamp), value)
                                ;
                            } else {
                                //聚合函数
                                ((Aggregate) function).updateValue(new FunctionValue(selectedValue, selectedTimestamp));
                            }

                        }
                    }
                }
            }
            List<Object> value = new ArrayList<>();
            values = new ArrayList<>();
            //数据构造完毕，开始生成最后的结果
            //首先判断是否含有常规查询，如果有的话，那么这种情况是允许出现一个selector函数,且不会出现聚合函数的情况
            if (selectComponent.isHasCommonQuery()) {
                Selector selector = (Selector) functions.get(0);
                List<Object> relatedValue = selector.getRelatedValues();
                for (String column : newColumns) {
                    if (SQLConstant.getNativeSelectorFunctionNames().contains(column)) {
                        value.add(selector.calculate().getValue());
                    } else {
                        if (relatedValue != null) {
                            value.add(relatedValue.get(columnOrders.get(column)));
                        }
                    }
                }
            } else {
                //如果没有常规查询，那么都是函数查询
                for (Function function : functions) {
                    if (value.size() == 0) {
                        value.add(function.calculate().getTimestamp());
                    } else {
                        value.set(0, function.calculate().getTimestamp());
                    }
                    value.add(function.calculate().getValue());
                }
                if (selectComponent.isHasAggregationFunction() || selectComponent.isHasMoreFunction()) {
                    value.set(0, 0);
                }

            }
            values.add(value);
        }
        //如果不是函数查询，那么只是常规查询时
        else if (selectComponent.isHasCommonQuery()) {
            //开始遍历select的范围
            for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
                Expression expression = resultColumn.getExpression();
                if (expression instanceof NodeExpression) {
                    //非star的情况
                    if (!((NodeExpression) expression).getName().equals(SQLConstant.STAR)) {
                        newColumns.add(((NodeExpression) expression).getName());
                    } else {
                        newColumns.addAll(columns.subList(1, columns.size()));
                    }
                }
            }
            for (List<Object> value : values) {
                for (String newColumn : newColumns) {
                    value.add(value.get(columnOrders.get(newColumn)));
                }
            }
        }
        IotDBInfluxDBUtils.updateQueryResultColumnValue(queryResult, IotDBInfluxDBUtils.removeDuplicate(newColumns), values);
    }


    /**
     * 每次查询前，先获取该measurement中所有的field列表,更新当前measure的所有的field列表及指定顺序
     */
    private void updateFiledOrders() throws IoTDBConnectionException, StatementExecutionException {
        //先初始化
        fieldOrders = new HashMap<>();
        fieldOrdersReversed = new HashMap<>();
        String showTimeseriesSql = "show timeseries root." + database + '.' + measurement;
        SessionDataSet result = session.executeQueryStatement(showTimeseriesSql);
        int fieldNums = 0;
        int tagOrderNums = tagOrders.size();
        while (result.hasNext()) {
            List<org.apache.iotdb.tsfile.read.common.Field> fields = result.next().getFields();
            String filed = IotDBInfluxDBUtils.getFiledByPath(fields.get(0).getStringValue());
            if (!fieldOrders.containsKey(filed)) {
                //field对应的顺序是1+tagNum （第一个是时间戳，接着是所有的tag，最后是所有的field）
                fieldOrders.put(filed, tagOrderNums + fieldNums + 1);
                fieldOrdersReversed.put(tagOrderNums + fieldNums + 1, filed);
                fieldNums++;
            }
        }
    }

    /**
     * 更新当前的measurement
     *
     * @param measurement 需要更改的measurement
     */
    private void updateMeasurement(String measurement) {
        if (!measurement.equals(this.measurement)) {
            this.measurement = measurement;
            tagOrders = measurementTagOrder.get(measurement);
            if (tagOrders == null) {
                tagOrders = new HashMap<>();
            }
        }
    }

    /**
     * 创建database，写入iotdb中
     *
     * @param name database的name
     */
    public void createDatabase(String name) {
        IotDBInfluxDBUtils.checkNonEmptyString(name, "database name");
        try {
            session.setStorageGroup("root." + name);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            if (e instanceof StatementExecutionException && ((StatementExecutionException) e).getStatusCode() == 300) {
                //当前database已经被创建过
                System.out.println(e.getMessage());
            } else {
                e.printStackTrace();
            }
        }
    }

    /**
     * 删除database
     *
     * @param name database的name
     */
    public void deleteDatabase(String name) {
        try {
            session.deleteStorageGroup("root." + name);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 设置database，同时获取database对应的所有tag列表及顺序
     *
     * @param database 需要设置的database的name
     */
    public void setDatabase(String database) {
        if (!database.equals(this.database)) {
            updateDatabase(database);
            this.database = database;
        }
    }


    /**
     * 当database发生改变时，更新database相关信息，即从iotdb中获取database对应的所有tag列表及顺序
     *
     * @param database 需要更新的database的name
     */
    private void updateDatabase(String database) {
        try {
            SessionDataSet result = session.executeQueryStatement("select * from root.TAG_INFO where database_name=" + String.format("\"%s\"", database));
            Map<String, Integer> tagOrder = new HashMap<>();
            String measurementName = null;
            while (result.hasNext()) {
                List<org.apache.iotdb.tsfile.read.common.Field> fields = result.next().getFields();
                String tmpMeasurementName = fields.get(1).getStringValue();
                if (measurementName == null) {
                    //首次获取到measurementName
                    measurementName = tmpMeasurementName;
                } else {
                    //不相等的话，则是遇到了新的measurement
                    if (!tmpMeasurementName.equals(measurementName)) {
                        //将当前measurement的tags加入其中
                        measurementTagOrder.put(measurementName, tagOrder);
                        tagOrder = new HashMap<>();
                    }
                }
                tagOrder.put(fields.get(2).getStringValue(), fields.get(3).getIntV());
            }
            //最后一个measurement，将当前measurement的tags加入其中
            measurementTagOrder.put(measurementName, tagOrder);
        } catch (StatementExecutionException e) {
            //首次执行时，TAG_INFO表没有创建，拦截错误，打印日志即可
            if (e.getStatusCode() == 411) {
                System.out.println(e.getMessage());
            }
        } catch (IoTDBConnectionException e) {
            e.printStackTrace();
        }
    }


    /**
     * 通过条件获取查询Influxdb格式的查询结果
     *
     * @param conditions 限制条件列表，包括tag和field条件限制
     * @return 返回Influxdb查询结果
     */
    private QueryResult queryByConditions(List<Condition> conditions) throws IoTDBConnectionException, StatementExecutionException {
        //用来存储符合tag的实际顺序
        Map<Integer, Condition> realTagOrders = new HashMap<>();
        //存储属于field的conditions列表
        List<Condition> fieldConditions = new ArrayList<>();
        //当前measurement中tag的数目
        int measurementTagNum = tagOrders.size();
        //当前查询条件中的最大tag数目
        int currentQueryMaxTagNum = 0;
        for (Condition condition : conditions) {
            //当前条件是处于tag中
            if (tagOrders.containsKey(condition.getValue())) {
                int curOrder = tagOrders.get(condition.getValue());
                //将其放入符合tag的map中
                realTagOrders.put(curOrder, condition);
                //更新当前查询条件的最大tag顺序
                currentQueryMaxTagNum = Math.max(currentQueryMaxTagNum, curOrder);
            } else {
                fieldConditions.add(condition);
            }
        }
        //构造实际的查询路径
        StringBuilder curQueryPath = new StringBuilder("root." + database + "." + measurement);
        //从1遍历到当前查询条件的最大数目
        for (int i = 1; i <= currentQueryMaxTagNum; i++) {
            if (realTagOrders.containsKey(i)) {
                //由于是path中的value，因此需要把首尾的引号去除
                curQueryPath.append(".").append(IotDBInfluxDBUtils.removeQuotation(realTagOrders.get(i).getLiteral()));
            } else {
                curQueryPath.append(".").append("*");
            }
        }
        //构造实际的查询条件
        StringBuilder realIotDBCondition = new StringBuilder();
        for (int i = 0; i < fieldConditions.size(); i++) {
            Condition condition = fieldConditions.get(i);
            if (i != 0) {
                realIotDBCondition.append(" and ");
            }
            realIotDBCondition.append(condition.getValue()).append(" ")
                    .append(FilterConstant.filterSymbol.get(condition.getFilterType())).append(" ")
                    .append(condition.getLiteral());
        }
        //实际的查询sql语句
        String realQuerySql;

        realQuerySql = "select * from " + curQueryPath;
        if (!(realIotDBCondition.length() == 0)) {
            realQuerySql += " where " + realIotDBCondition;
        }
        realQuerySql += " align by device";
        SessionDataSet sessionDataSet = session.executeQueryStatement(realQuerySql);
        return iotdbAlignByDeviceResultCvtToInfluxdbResult(sessionDataSet);
        //下面的注释内容是采用非align by device的方案
//        if (realIotDBCondition.isEmpty()) {
//            realQuerySql = ("select * from " + curQueryPath);
//            SessionDataSet sessionDataSet = session.executeQueryStatement(realQuerySql);
//            queryResult = iotdbResultCvtToInfluxdbResult(sessionDataSet);
//            System.out.println(sessionDataSet.toString());
//        } else {
//            //有了过滤条件，只能多次遍历
//            QueryResult lastQueryResult = null;
//            for (int i = currentQueryMaxTagNum; i <= measurementTagNum; i++) {
//                if (i != currentQueryMaxTagNum) {
//                    curQueryPath.append(".*");
//                }
//                realQuerySql = ("select * from " + curQueryPath + " where " + realIotDBCondition + " align by device");
//                SessionDataSet sessionDataSet = null;
//                try {
//                    sessionDataSet = session.executeQueryStatement(realQuerySql);
//                } catch (StatementExecutionException e) {
//                    if (e.getStatusCode() == 411) {
//                        //where的timeseries没有匹配的话，会抛出411的错误，将其拦截打印
//                        System.out.println(e.getMessage());
//                    } else {
//                        throw e;
//                    }
//                }
//                //暂时的转换结果
//                QueryResult tmpQueryResult = iotdbResultCvtToInfluxdbResult(sessionDataSet);
//                //如果是第一次，则直接赋值，不需要or操作
//                if (i == currentQueryMaxTagNum) {
//                    lastQueryResult = tmpQueryResult;
//                } else {
//                    //进行add操作
//                    lastQueryResult = IotDBInfluxDBUtils.addQueryResultProcess(lastQueryResult, tmpQueryResult);
//                }
//            }
//            queryResult = lastQueryResult;
//        }
    }


    /**
     * 将iotdb的align by device查询结果转换为influxdb的查询结果
     *
     * @param sessionDataSet 待转换的iotdb查询结果
     * @return influxdb格式的查询结果
     */
    private QueryResult iotdbAlignByDeviceResultCvtToInfluxdbResult(SessionDataSet sessionDataSet) throws IoTDBConnectionException, StatementExecutionException {
        if (sessionDataSet == null) {
            return IotDBInfluxDBUtils.getNullQueryResult();
        }
        //生成series
        QueryResult.Series series = new QueryResult.Series();
        series.setName(measurement);
        //获取tag的反向map
        Map<Integer, String> tagOrderReversed = tagOrders.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        int tagSize = tagOrderReversed.size();
        ArrayList<String> tagList = new ArrayList<>();
        for (int i = 1; i <= tagSize; i++) {
            tagList.add(tagOrderReversed.get(i));
        }

        ArrayList<String> fieldList = new ArrayList<>();
        for (int i = 1 + tagSize; i < 1 + tagSize + fieldOrders.size(); i++) {
            fieldList.add(fieldOrdersReversed.get(i));
        }
        ArrayList<String> columns = new ArrayList<>();
        columns.add("time");
        columns.addAll(tagList);
        columns.addAll(fieldList);
        //把columns插入series中
        series.setColumns(columns);

        List<List<Object>> values = new ArrayList<>();

        List<String> iotdbResultColumn = sessionDataSet.getColumnNames();
        while (sessionDataSet.hasNext()) {
            Object[] value = new Object[columns.size()];

            RowRecord record = sessionDataSet.next();
            List<org.apache.iotdb.tsfile.read.common.Field> fields = record.getFields();

            value[0] = record.getTimestamp();

            String deviceName = fields.get(0).getStringValue();
            String[] deviceNameList = deviceName.split("\\.");
            for (int i = 3; i < deviceNameList.length; i++) {
                if (!deviceNameList[i].equals(placeholder)) {
                    value[i - 2] = deviceNameList[i];
                }
            }
            for (int i = 1; i < fields.size(); i++) {
                Object o = IotDBInfluxDBUtils.iotdbFiledCvt(fields.get(i));
                if (o != null) {
                    //将filed的值插入其中
                    value[fieldOrders.get(iotdbResultColumn.get(i + 1))] = o;
                }
            }
            //插入实际的value
            values.add(Arrays.asList(value));
        }
        series.setValues(values);

        QueryResult queryResult = new QueryResult();
        QueryResult.Result result = new QueryResult.Result();
        result.setSeries(new ArrayList<>(Arrays.asList(series)));
        queryResult.setResults(new ArrayList<>(Arrays.asList(result)));

        return queryResult;
    }

    /**
     * 将iotdb的查询结果转换为influxdb的查询结果
     *
     * @param sessionDataSet 待转换的iotdb查询结果
     * @return influxdb格式的查询结果
     */
    private QueryResult iotdbResultCvtToInfluxdbResult(SessionDataSet sessionDataSet) throws IoTDBConnectionException, StatementExecutionException {
        if (sessionDataSet == null) {
            return IotDBInfluxDBUtils.getNullQueryResult();
        }
        //生成series
        QueryResult.Series series = new QueryResult.Series();
        series.setName(measurement);
        //获取tag的反向map
        Map<Integer, String> tagOrderReversed = tagOrders.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        int tagSize = tagOrderReversed.size();
        ArrayList<String> tagList = new ArrayList<>();
        for (int i = 1; i <= tagSize; i++) {
            tagList.add(tagOrderReversed.get(i));
        }

        ArrayList<String> fieldList = new ArrayList<>();
        for (int i = 1 + tagSize; i < 1 + tagSize + fieldOrders.size(); i++) {
            fieldList.add(fieldOrdersReversed.get(i));
        }
        ArrayList<String> columns = new ArrayList<>();
        columns.add("time");
        columns.addAll(tagList);
        columns.addAll(fieldList);
        //把columns插入series中
        series.setColumns(columns);

        List<List<Object>> values = new ArrayList<>();

        List<String> iotdbResultColumn = sessionDataSet.getColumnNames();
        ArrayList<Integer> samePath = IotDBInfluxDBUtils.getSamePathForList(iotdbResultColumn.subList(1, iotdbResultColumn.size()));
        while (sessionDataSet.hasNext()) {
            Object[] value = new Object[columns.size()];

            RowRecord record = sessionDataSet.next();
            List<org.apache.iotdb.tsfile.read.common.Field> fields = record.getFields();
            long timestamp = record.getTimestamp();
            //判断该path是否所有的值都为null
            boolean allNull = true;
            //记录sameList的当前下标
            int sameListIndex = 0;
            for (int i = 0; i < fields.size(); i++) {
                Object o = IotDBInfluxDBUtils.iotdbFiledCvt(fields.get(i));
                if (o != null) {
                    if (allNull) {
                        allNull = false;
                    }
                    //将filed的值插入其中
                    value[fieldOrders.get(IotDBInfluxDBUtils.getFiledByPath(iotdbResultColumn.get(i + 1)))] = o;
                }
                //该相同的path已经遍历完成
                if (i == samePath.get(sameListIndex)) {
                    //如果数据中有非null，则插入实际的数据中，否则直接跳过
                    if (!allNull) {
                        //先把时间插入value中
                        value[0] = timestamp;
                        //再把该path中的tag插入value中国
                        //加1，第零列是time
                        String tmpPathName = iotdbResultColumn.get(i + 1);
                        String[] tmpTags = tmpPathName.split("\\.");
                        for (int j = 3; i < tmpTags.length - 1; i++) {
                            if (!tmpTags[j].equals(placeholder)) {
                                //放入指定的序列中
                                value[j - 2] = tmpTags[j];
                            }
                        }
                    }
                    //插入实际的value
                    values.add(Arrays.asList(value));
                    //重制value
                    value = new Object[columns.size()];
                }
            }
        }
        series.setValues(values);

        QueryResult queryResult = new QueryResult();
        QueryResult.Result result = new QueryResult.Result();
        result.setSeries(new ArrayList<>(Arrays.asList(series)));
        queryResult.setResults(new ArrayList<>(Arrays.asList(result)));

        return queryResult;
    }

    /**
     * 通过Influxdb的语法树获取查询结果
     *
     * @param operator 需要处理的查询语法树
     * @return influxdb格式的查询结果
     */
    public QueryResult queryExpr(FilterOperator operator) throws Exception {
        if (operator instanceof BasicFunctionOperator) {
            List<Condition> conditions = new ArrayList<>();
            conditions.add(IotDBInfluxDBUtils.getConditionForBasicFunctionOperator((BasicFunctionOperator) operator));
            return queryByConditions(conditions);
        } else {
            FilterOperator leftOperator = operator.getChildOperators().get(0);
            FilterOperator rightOperator = operator.getChildOperators().get(1);
            if (operator.getFilterType() == FilterConstant.FilterType.KW_OR) {
                return IotDBInfluxDBUtils.orQueryResultProcess(queryExpr(leftOperator), queryExpr(rightOperator));
            } else if (operator.getFilterType() == FilterConstant.FilterType.KW_AND) {
                if (IotDBInfluxDBUtils.canMergeOperator(leftOperator) && IotDBInfluxDBUtils.canMergeOperator(rightOperator)) {
                    List<Condition> conditions1 = IotDBInfluxDBUtils.getConditionsByFilterOperatorOperator(leftOperator);
                    List<Condition> conditions2 = IotDBInfluxDBUtils.getConditionsByFilterOperatorOperator(rightOperator);
                    conditions1.addAll(conditions2);
                    return queryByConditions(conditions1);
                } else {
                    return IotDBInfluxDBUtils.andQueryResultProcess(queryExpr(leftOperator), queryExpr(rightOperator));
                }
            }
        }
        throw new IllegalArgumentException("unknown operator " + operator.toString());
    }

}
