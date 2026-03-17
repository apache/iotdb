"""ModelInfo and builtin map; no iotdb."""
from typing import Dict, Optional

from standalone_finetune.model.model_constants import ModelCategory, ModelStates


class ModelInfo:
    def __init__(
        self,
        model_id: str,
        category: ModelCategory,
        state: ModelStates,
        model_type: str = "",
        pipeline_cls: str = "",
        repo_id: str = "",
        auto_map: Optional[Dict] = None,
        transformers_registered: bool = False,
        base_model_id: Optional[str] = None,
    ):
        self.model_id = model_id
        self.model_type = model_type
        self.category = category
        self.state = state
        self.pipeline_cls = pipeline_cls
        self.repo_id = repo_id
        self.auto_map = auto_map or {}
        self.transformers_registered = transformers_registered
        self.base_model_id = base_model_id

    def copy(
        self,
        model_id: Optional[str] = None,
        category: Optional[ModelCategory] = None,
        state: Optional[ModelStates] = None,
        base_model_id: Optional[str] = None,
    ) -> "ModelInfo":
        return ModelInfo(
            model_id=model_id or self.model_id,
            category=category or self.category,
            state=state or self.state,
            model_type=self.model_type,
            pipeline_cls=self.pipeline_cls,
            repo_id=self.repo_id,
            auto_map=dict(self.auto_map) if self.auto_map else None,
            transformers_registered=self.transformers_registered,
            base_model_id=base_model_id if base_model_id is not None else self.base_model_id,
        )


BUILTIN_HF_TRANSFORMERS_MODEL_MAP = {
    "sundial": ModelInfo(
        model_id="sundial",
        category=ModelCategory.BUILTIN,
        state=ModelStates.ACTIVE,
        model_type="sundial",
        pipeline_cls="pipeline_sundial.SundialPipeline",
        repo_id="thuml/sundial-base-128m",
        auto_map={
            "AutoConfig": "configuration_sundial.SundialConfig",
            "AutoModelForCausalLM": "modeling_sundial.SundialForPrediction",
        },
        transformers_registered=True,
    ),
    "timer_xl": ModelInfo(
        model_id="timer_xl",
        category=ModelCategory.BUILTIN,
        state=ModelStates.ACTIVE,
        model_type="timer",
        pipeline_cls="pipeline_timer.TimerPipeline",
        repo_id="thuml/timer-base-84m",
        auto_map={
            "AutoConfig": "configuration_timer.TimerConfig",
            "AutoModelForCausalLM": "modeling_timer.TimerForPrediction",
        },
        transformers_registered=True,
    ),
}
