package cn.edu.tsinghua.iotdb.queryV2.engine.component.resource;

/**
 * Created by zhangjinrui on 2018/1/12.
 */
public interface QueryResource {
    /**
     * Release represents the operations for current resource such as return, close, destroy
     */
    void release();
}
