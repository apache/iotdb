package cn.edu.tsinghua.web.bean;

/**
 * Created by dell on 2017/7/18.
 */
public class TimeValues {

    private long time;
    private float value;

    @Override
    public String toString() {
        return "TimeValues{" +
                "time=" + time +
                ", values=" + value +
                '}';
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }
}
