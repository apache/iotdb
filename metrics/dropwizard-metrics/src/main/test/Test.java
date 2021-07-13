import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.MetricService;

/**
 * @Author stormbroken
 * Create by 2021/07/13
 * @Version 1.0
 **/

public class Test {
    MetricManager metricManager = MetricService.getMetricManager();
    private static final String[] TAGS = {"tag1", "tag2", "tag3", "tag4", "tag5", "tag6", "tag7", "tag8", "tag9", "tag10"};

    private long createMeter(Integer meterNumber, String[] tags){
        long start = System.currentTimeMillis();
        for(int i = 0; i < meterNumber; i ++){
            metricManager.getOrCreateCounter("counter" + i, tags);
        }
        long stop = System.currentTimeMillis();
        System.out.println(stop - start);
        return stop - start;
    }

    public static void main(String[] args) {
        System.setProperty("METRIC_CONF", "E:\\iotdb\\metrics\\dropwizard-metrics\\src\\main\\test\\resources");
        Test test = new Test();
        String[] tags = new String[10];
        for(int i = 0; i < tags.length; i ++){
            tags[i] = TAGS[i];
        }
        Integer number = 1000000;
        long create = test.createMeter(number, tags);
        long find = test.createMeter(number, tags);

        StringBuilder stringBuilder = new StringBuilder();
        for(String tag: tags){
            stringBuilder.append(tag + "|");
        }
        System.out.println("In number=" + number + " and tags=" + stringBuilder.toString() + ", create uses " + create + " ms, find uses "+ find + " ms.");
    }
}
