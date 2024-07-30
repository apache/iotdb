package org.apache.iotdb.subscription.it.triple.mix;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.timechodb.test.pipe.TestConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.subscription.consumer.AckStrategy;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;
import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * PushConsumer
 * pattern: db
 * Dataset
 * result:pass
 */
public class PushConsumerPullConsumerWith1TopicShareProcessMix extends TestConfig {
    private static String topicName = "`1-group.1-consumer.db`";
    private static List<MeasurementSchema> schemaList = new ArrayList<>();
    private final String database = "root.PushConsumerPullConsumerWith1TopicShareProcessMix";
    private final String device = database+".d_0";
    private final String pattern = database+".**";
    private SubscriptionPushConsumer consumer ;
    private SubscriptionPullConsumer consumer2 ;


    @BeforeClass
    public void setUp() throws IoTDBConnectionException, StatementExecutionException {
        createTopic_s(topicName, pattern, null, null, false);
        createDB(database);
        session_src.createTimeseries(device+".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
        session_src.createTimeseries(device+".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
        session_dest.createTimeseries(device+".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
        session_dest.createTimeseries(device+".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
        session_dest2.createTimeseries(device+".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
        session_dest2.createTimeseries(device+".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
        schemaList.add(new MeasurementSchema("s_0", TSDataType.INT64));
        schemaList.add(new MeasurementSchema("s_1", TSDataType.DOUBLE));
        subs.getTopics().forEach((System.out::println));
        assertTrue(subs.getTopic(topicName).isPresent(), "创建show topics");
    }

    @AfterClass
    public void cleanUp() throws IoTDBConnectionException, StatementExecutionException {
        consumer.close();
        consumer2.close();
        subs.dropTopic(topicName);
        dropDB(database);
    }

    private void insert_data(long timestamp) throws IoTDBConnectionException, StatementExecutionException {
        Tablet tablet = new Tablet(device, schemaList,10);
        int rowIndex = 0;
        for(int row=0; row < 5; row++) {
            rowIndex = tablet.rowSize++;
            tablet.addTimestamp(rowIndex, timestamp);
            tablet.addValue("s_0", rowIndex, row*20L+row);
            tablet.addValue("s_1", rowIndex, row+2.45);
            timestamp += 2000;
        }
        session_src.insertTablet(tablet);
    }


    @Test
    public void do_test() throws InterruptedException, TException, IoTDBConnectionException, IOException, StatementExecutionException {
        // 订阅前写入数据
        Thread thread = new Thread(()->{
            long timestamp = 1706659200000L; //2024-01-31 08:00:00+08:00
            for (int i = 0; i < 100; i++) {
                try {
                    insert_data(timestamp);
                } catch (IoTDBConnectionException e) {
                    throw new RuntimeException(e);
                } catch (StatementExecutionException e) {
                    throw new RuntimeException(e);
                }
                timestamp += 20000;
            }
        });
        consumer = new SubscriptionPushConsumer.Builder().host(SRC_HOST).port(PORT)
                .consumerId("dataset_push_consumer_1").consumerGroupId("db_pull_push_mix").ackStrategy(AckStrategy.BEFORE_CONSUME)
                .consumeListener(message->{
                    for (final SubscriptionSessionDataSet dataSet :
                            message.getSessionDataSetsHandler()) {
                        try {
                            session_dest.insertTablet(dataSet.getTablet());
                        } catch (StatementExecutionException e) {
                            throw new RuntimeException(e);
                        } catch (IoTDBConnectionException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return ConsumeResult.SUCCESS;
                }).buildPushConsumer();
        consumer.open();
        consumer.subscribe(topicName);

        consumer2 = new SubscriptionPullConsumer.Builder().host(SRC_HOST).port(PORT)
                .consumerId("dataset_pull_consumer_2").consumerGroupId("db_pull_push_mix")
                .buildPullConsumer();
        consumer2.open();
        consumer2.subscribe(topicName);

        thread.start();
        thread.join();
        consume_data(consumer2, session_dest2);
        System.out.println("订阅后:");
        subs.getSubscriptions().forEach((System.out::println));
        assertEquals(subs.getSubscriptions().size(),1, "订阅后show subscriptions");

        Thread.sleep(3000);
        // 最开始的5条有可能重复数据
        String sql = "select count(s_0) from "+device;
        System.out.println("src push consumer: "+getCount(session_src, sql));
        System.out.println("dest push consumer: "+getCount(session_dest, sql));
        System.out.println("dest2 pull consumer: "+getCount(session_dest2, sql));
        AWAIT.untilAsserted(()->{
            Assert.assertEquals(getCount(session_dest, sql)+getCount(session_dest2, sql), getCount(session_src, sql), "share process");
        });

    }
}
