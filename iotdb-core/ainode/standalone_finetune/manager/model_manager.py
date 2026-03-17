"""Thin wrapper over ModelStorage for CLI."""
from standalone_finetune.model.model_info import ModelInfo
from standalone_finetune.model.model_storage import ModelStorage


class ModelManager:
    def __init__(self):
        self._storage = ModelStorage()

    def register_fine_tuned_model(self, model_id: str, base_model_id: str) -> ModelInfo:
        return self._storage.register_finetuned_model(model_id, base_model_id)

    def complete_finetune(self, model_id: str) -> None:
        self._storage.complete_finetune(model_id)

    def fail_finetune(self, model_id: str, cleanup: bool = False) -> None:
        self._storage.fail_finetune(model_id, cleanup=cleanup)
