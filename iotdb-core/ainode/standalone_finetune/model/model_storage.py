"""Minimal model registry for standalone: register_finetuned_model, complete_finetune, fail_finetune."""
import logging
import os
import shutil
from typing import Dict, Optional

from standalone_finetune.config import get_models_dir
from standalone_finetune.model.model_constants import ModelCategory, ModelStates
from standalone_finetune.model.model_info import BUILTIN_HF_TRANSFORMERS_MODEL_MAP, ModelInfo

logger = logging.getLogger(__name__)


def _ensure_init(path: str) -> None:
    os.makedirs(path, exist_ok=True)
    init_file = os.path.join(path, "__init__.py")
    if not os.path.exists(init_file):
        with open(init_file, "w"):
            pass


class ModelStorage:
    def __init__(self):
        self._models_dir = get_models_dir()
        self._finetuned: Dict[str, ModelInfo] = {}
        self._init_dirs()

    def _init_dirs(self):
        for cat in ModelCategory:
            p = os.path.join(self._models_dir, cat.value)
            _ensure_init(p)

    def get_model_info(self, model_id: str, category: Optional[ModelCategory] = None) -> Optional[ModelInfo]:
        if category == ModelCategory.FINE_TUNED:
            return self._finetuned.get(model_id)
        if model_id in BUILTIN_HF_TRANSFORMERS_MODEL_MAP:
            return BUILTIN_HF_TRANSFORMERS_MODEL_MAP[model_id]
        for cat in (ModelCategory.USER_DEFINED, ModelCategory.FINE_TUNED):
            dir_path = os.path.join(self._models_dir, cat.value, model_id)
            if os.path.isdir(dir_path):
                info = ModelInfo(
                    model_id=model_id,
                    category=cat,
                    state=ModelStates.ACTIVE,
                    model_type="",
                    base_model_id=model_id if cat == ModelCategory.FINE_TUNED else None,
                )
                if model_id in BUILTIN_HF_TRANSFORMERS_MODEL_MAP:
                    base = BUILTIN_HF_TRANSFORMERS_MODEL_MAP.get(model_id)
                    if base:
                        info.model_type = base.model_type
                        info.auto_map = base.auto_map
                        info.pipeline_cls = base.pipeline_cls
                return info
        return None

    def register_finetuned_model(self, model_id: str, base_model_id: str) -> ModelInfo:
        if model_id in self._finetuned:
            raise ValueError(f"Model already registered: {model_id}")
        base = self.get_model_info(base_model_id)
        if base is None:
            raise ValueError(f"Base model not found: {base_model_id}")
        model_dir = os.path.join(self._models_dir, ModelCategory.FINE_TUNED.value, model_id)
        _ensure_init(model_dir)
        sft_info = base.copy(
            model_id=model_id,
            category=ModelCategory.FINE_TUNED,
            state=ModelStates.TRAINING,
            base_model_id=base_model_id,
        )
        self._finetuned[model_id] = sft_info
        logger.info(f"Registered fine-tuned model {model_id} based on {base_model_id}")
        return base

    def complete_finetune(self, model_id: str) -> None:
        if model_id in self._finetuned:
            self._finetuned[model_id].state = ModelStates.ACTIVE
        logger.info(f"Fine-tuned model {model_id} is now ACTIVE")

    def fail_finetune(self, model_id: str, cleanup: bool = False) -> None:
        if model_id not in self._finetuned:
            return
        if cleanup:
            path = os.path.join(self._models_dir, ModelCategory.FINE_TUNED.value, model_id)
            if os.path.exists(path):
                shutil.rmtree(path)
            del self._finetuned[model_id]
            logger.info(f"Cleaned up failed fine-tuned model {model_id}")
        else:
            self._finetuned[model_id].state = ModelStates.FAILED
            logger.warning(f"Fine-tuned model {model_id} marked as FAILED")
