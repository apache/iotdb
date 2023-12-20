package org.apache.iotdb.confignode.manager.pipe.task;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.pipe.task.PipeTask;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import java.util.HashMap;
import java.util.Map;

public class PipeConfigNodeTaskBuilder {

  private final PipeMeta pipeMeta;

  public PipeConfigNodeTaskBuilder(PipeMeta pipeMeta) {
    this.pipeMeta = pipeMeta;
  }

  public Map<TConsensusGroupId, PipeTask> build() {
    final PipeStaticMeta pipeStaticMeta = pipeMeta.getStaticMeta();

    final Map<TConsensusGroupId, PipeTask> consensusGroupIdToPipeTaskMap = new HashMap<>();

    final PipeRuntimeMeta pipeRuntimeMeta = pipeMeta.getRuntimeMeta();
    for (Map.Entry<TConsensusGroupId, PipeTaskMeta> consensusGroupIdToPipeTaskMeta :
        pipeRuntimeMeta.getConsensusGroupId2TaskMetaMap().entrySet()) {
      // TODO: getLeaderDataNodeId -> getLeaderNodeId
      if (consensusGroupIdToPipeTaskMeta.getValue().getLeaderDataNodeId()
          == ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId()) {
        consensusGroupIdToPipeTaskMap.put(
            consensusGroupIdToPipeTaskMeta.getKey(),
            new PipeConfigNodeTask(
                new PipeConfigNodeTaskStage(
                    pipeStaticMeta.getPipeName(),
                    pipeStaticMeta.getCreationTime(),
                    pipeStaticMeta.getExtractorParameters().getAttribute(),
                    pipeStaticMeta.getProcessorParameters().getAttribute(),
                    pipeStaticMeta.getConnectorParameters().getAttribute())));
      }
    }

    return consensusGroupIdToPipeTaskMap;
  }
}
