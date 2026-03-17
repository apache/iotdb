"""Model file names and enums; no external deps."""
from enum import Enum

CONFIG_JSON = "config.json"
MODEL_SAFETENSORS = "model.safetensors"
MODEL_PT = "model.pt"
ADAPTER_CONFIG = "adapter_config.json"
ADAPTER_PT = "adapter_model.pt"
ADAPTER_SAFETENSORS = "adapter_model.safetensors"
ADAPTER_BIN = "adapter_model.bin"
TRAINING_STATE = "training_state.pt"
MODEL_WEIGHT_FILES = (MODEL_SAFETENSORS, MODEL_PT,)


class ModelCategory(Enum):
    BUILTIN = "builtin"
    USER_DEFINED = "user_defined"
    FINE_TUNED = "fine_tuned"


class ModelStates(Enum):
    INACTIVE = "inactive"
    ACTIVE = "active"
    TRAINING = "training"
    FAILED = "failed"
