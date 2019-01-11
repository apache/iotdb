package cn.edu.tsinghua.iotdb.query.component.resource;


public interface QueryResource {
    /**
     * Release represents the operations for current resource such as return, close, destroy
     */
    void release();
}
