"""
Standalone fine-tune config: paths from environment, no AINode dependency.
"""
import os

FINETUNE_MODELS_DIR = os.getenv("FINETUNE_MODELS_DIR", os.path.join(os.getcwd(), "data", "models"))
FINETUNE_EXP_DIR = os.getenv("FINETUNE_EXP_DIR", os.path.join(os.getcwd(), "data", "experiments"))
# Module prefix for builtin model classes (e.g. sundial, timer). Set PYTHONPATH so this module exists.
FINETUNE_BUILTIN_MODULE = os.getenv("FINETUNE_BUILTIN_MODULE", "iotdb.ainode.core.model")


def get_models_dir() -> str:
    return FINETUNE_MODELS_DIR


def get_exp_dir() -> str:
    return FINETUNE_EXP_DIR


def get_builtin_module() -> str:
    return FINETUNE_BUILTIN_MODULE
