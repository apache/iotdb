from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.rpc.handler import AINodeRPCServiceHandler
from iotdb.ainode.core.rpc.status import TSStatusCode, get_status
from iotdb.thrift.ainode.ttypes import (
    TClassifyReq,
    TClassifyResp,
    TDeleteModelReq,
    TForecastReq,
    TForecastResp,
    TInferenceReq,
    TInferenceResp,
    TLoadModelReq,
    TRegisterModelReq,
    TRegisterModelResp,
    TTuningReq,
    TUnloadModelReq,
)
from iotdb.thrift.common.ttypes import TSStatus
from timecho.ainode.core.manager.finetune_manager import FinetuneManager

logger = Logger()
AIN_CONFIG = AINodeDescriptor().get_config()


class TimechoAINodeRPCServiceHandler(AINodeRPCServiceHandler):
    def __init__(self, ainode):
        super().__init__(ainode)
        self._finetune_manager = FinetuneManager()
        self._finetune_manager.start()

    def stop(self):
        """Ensure FinetuneManager consumer thread is cleanly shut down."""
        self._finetune_manager.stop()
        super().stop()

    # ==================== Model Management ====================

    def registerModel(self, req: TRegisterModelReq) -> TRegisterModelResp:
        if not AIN_CONFIG.is_activated():
            return TRegisterModelResp(
                status=get_status(
                    TSStatusCode.AINODE_INTERNAL_ERROR,
                    "Reject model registration because TimechoDB-AINode is unactivated.",
                )
            )
        return super().registerModel(req)

    def deleteModel(self, req: TDeleteModelReq) -> TSStatus:
        if not AIN_CONFIG.is_activated():
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR,
                "Reject model deletion because TimechoDB-AINode is unactivated.",
            )
        return super().deleteModel(req)

    def loadModel(self, req: TLoadModelReq) -> TSStatus:
        if not AIN_CONFIG.is_activated():
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR,
                "Reject load model because TimechoDB-AINode is unactivated.",
            )
        return super().loadModel(req)

    def unloadModel(self, req: TUnloadModelReq) -> TSStatus:
        if not AIN_CONFIG.is_activated():
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR,
                "Reject unload model because TimechoDB-AINode is unactivated.",
            )
        return super().unloadModel(req)

    # ==================== Inference ====================

    def inference(self, req: TInferenceReq) -> TInferenceResp:
        if not AIN_CONFIG.is_activated():
            return TInferenceResp(
                get_status(
                    TSStatusCode.AINODE_INTERNAL_ERROR,
                    "Reject inference because TimechoDB-AINode is unactivated.",
                ),
                [],
            )
        return super().inference(req)

    def forecast(self, req: TForecastReq) -> TForecastResp:
        if not AIN_CONFIG.is_activated():
            return TForecastResp(
                get_status(
                    TSStatusCode.AINODE_INTERNAL_ERROR,
                    "Reject forecast because TimechoDB-AINode is unactivated.",
                ),
                [],
            )
        return super().forecast(req)

    def classify(self, req: TClassifyReq) -> TClassifyResp:
        if not AIN_CONFIG.is_activated():
            return TClassifyResp(
                get_status(
                    TSStatusCode.AINODE_INTERNAL_ERROR,
                    "Reject classify because TimechoDB-AINode is unactivated.",
                ),
                [],
            )
        status = self._ensure_model_is_registered(req.modelId)
        if status.code != TSStatusCode.SUCCESS_STATUS.value:
            return TClassifyResp(status, [])
        return self._inference_manager.classify(req)

    # ==================== FineTune ====================

    def createTuningTask(self, req: TTuningReq) -> TSStatus:
        if not AIN_CONFIG.is_activated():
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR,
                "Reject tuning task creation because TimechoDB-AINode is unactivated.",
            )
        try:
            params = getattr(req, "parameters", {}) or {}
            params["model_id"] = req.modelId
            params["base_model_id"] = req.existingModelId
            if req.targetDataSchema:
                params["data_schema_list"] = req.targetDataSchema
            if req.dbType:
                params["dataset_type"] = req.dbType.lower()

            task = self._finetune_manager.create_task(params)

            return get_status(
                TSStatusCode.SUCCESS_STATUS,
                f"Task {task.task_id} created successfully.",
            )
        except Exception as e:
            logger.error(f"Failed to create tuning task: {e}")
            return get_status(TSStatusCode.INVALID_TRAINING_CONFIG, str(e))

    # ------------------------------------------------------------------
    # The following interfaces are NOT yet defined in the Thrift IDL.
    # Corresponding Thrift Request/Response types and Processor
    # registration will be added in a future iteration.
    # Currently accessible via internal calls or CLI.
    # SQL semantic mapping:
    #   SHOW TRAINING TASK <id>  -> getTuningTaskDetail
    #   SHOW TRAINING TASKS      -> listTuningTasks
    #   CANCEL TASK <id>         -> cancelTuningTask
    #   DROP TASK <id>           -> dropTuningTask
    # ------------------------------------------------------------------

    def getTuningTaskDetail(self, task_id: str) -> TSStatus:
        """Get full details of a single fine-tuning task."""
        if not AIN_CONFIG.is_activated():
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR, "AINode is unactivated."
            )
        task = self._finetune_manager.get_task(task_id)
        if task is None:
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR, f"Task not found: {task_id}"
            )
        return get_status(TSStatusCode.SUCCESS_STATUS, task.to_detail())

    def listTuningTasks(
        self,
        include_completed: bool = True,
        limit: int = 100,
    ) -> TSStatus:
        """List summaries of all fine-tuning tasks."""
        if not AIN_CONFIG.is_activated():
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR, "AINode is unactivated."
            )
        tasks = self._finetune_manager.list_tasks(include_completed)[:limit]
        body = "\n".join(t.to_summary() for t in tasks) if tasks else "No tasks"
        return get_status(TSStatusCode.SUCCESS_STATUS, body)

    def cancelTuningTask(self, task_id: str) -> TSStatus:
        """Cancel a fine-tuning task in PENDING or RUNNING state."""
        if not AIN_CONFIG.is_activated():
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR, "AINode is unactivated."
            )
        if self._finetune_manager.cancel_task(task_id):
            return get_status(
                TSStatusCode.SUCCESS_STATUS, f"Task {task_id} cancel requested."
            )
        return get_status(
            TSStatusCode.AINODE_INTERNAL_ERROR, f"Cannot cancel task: {task_id}"
        )

    def dropTuningTask(self, task_id: str, cleanup: bool = True) -> TSStatus:
        """Drop a task record. When cleanup=True, also remove training artifacts."""
        if not AIN_CONFIG.is_activated():
            return get_status(
                TSStatusCode.AINODE_INTERNAL_ERROR, "AINode is unactivated."
            )
        if self._finetune_manager.drop_task(task_id, cleanup):
            return get_status(TSStatusCode.SUCCESS_STATUS, f"Task {task_id} dropped.")
        return get_status(
            TSStatusCode.AINODE_INTERNAL_ERROR, f"Cannot drop task: {task_id}"
        )
