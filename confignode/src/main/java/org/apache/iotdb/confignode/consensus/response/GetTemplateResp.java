package org.apache.iotdb.confignode.consensus.response;

import java.util.Map;

/**
 * @author chenhuangyun
 * @date 2022/6/28
 **/
public class GetTemplateResp  implements DataSet{

    private TSStatus status;

    private Template template;
}
