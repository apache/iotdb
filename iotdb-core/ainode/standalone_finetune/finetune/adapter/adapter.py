import torch

import logging

from standalone_finetune.finetune.adapter.weaver.base_weaver import WeaverConfig
from standalone_finetune.finetune.adapter.weaver.dual_weaver import DualWeaver
from standalone_finetune.hparams.finetune_args import FinetuneArguments
from standalone_finetune.hparams.model_args import ModelArguments

logger = logging.getLogger(__name__)

logger = Logger()


def freeze_model_parameters(
    model: torch.nn.Module,
    trainable_patterns: list = None,
) -> None:
    """
    Freeze model parameters except those matching trainable_patterns.
    Used for linear probing, adapter-based fine-tuning, etc.
    """
    if trainable_patterns is None:
        trainable_patterns = ["lm_head", "flow_loss", "output_patch_embedding"]

    for name, param in model.named_parameters():
        param.requires_grad = any(pattern in name for pattern in trainable_patterns)

    trainable = sum(p.numel() for p in model.parameters() if p.requires_grad)
    total = sum(p.numel() for p in model.parameters())
    logger.info(
        f"Frozen model: {trainable:,} / {total:,} parameters trainable "
        f"({100 * trainable / total:.2f}%)"
    )


def apply_adaptation(
    model: torch.nn.Module,
    model_args: ModelArguments,
    finetune_args: FinetuneArguments,
) -> torch.nn.Module:
    """
    Apply the specified adaptation method to the model.

    Args:
        model: Base model to adapt
        model_args: Model arguments
        finetune_args: Fine-tuning arguments

    Returns:
        Adapted model
    """
    finetune_type = finetune_args.finetune_type

    if finetune_type == "full":
        logger.info("Full fine-tuning: all parameters trainable")
        return model

    elif finetune_type == "linear":
        patterns = finetune_args.get_trainable_module_patterns()
        freeze_model_parameters(model, trainable_patterns=patterns)
        return model

    elif finetune_type == "lora":
        return _apply_lora(model, finetune_args)

    elif finetune_type in ("weaver_cnn", "weaver_mlp"):
        model_type = model_args.model_type if model_args.model_type else "sundial"
        return _apply_weaver(model, model_args, finetune_args, model_type=model_type)

    else:
        logger.warning(
            f"Unknown finetune_type: {finetune_type}, using full fine-tuning"
        )
        return model


def _apply_lora(
    model: torch.nn.Module,
    finetune_args: FinetuneArguments,
) -> torch.nn.Module:
    try:
        from peft import LoraConfig, get_peft_model
    except ImportError:
        logger.error("peft not installed. Install with: pip install peft")
        raise ImportError("peft library required for LoRA")

    lora_config = LoraConfig(
        r=finetune_args.lora_rank,
        lora_alpha=finetune_args.lora_alpha,
        lora_dropout=finetune_args.lora_dropout,
        target_modules=finetune_args.lora_target_modules,
        init_lora_weights=finetune_args.init_lora_weights,
        use_rslora=finetune_args.use_rslora,
        use_dora=finetune_args.use_dora,
    )

    model = get_peft_model(model, lora_config)

    trainable = sum(p.numel() for p in model.parameters() if p.requires_grad)
    total = sum(p.numel() for p in model.parameters())
    logger.info(
        f"LoRA applied: {trainable:,} / {total:,} parameters trainable "
        f"({100*trainable/total:.2f}%)"
    )

    return model


def _apply_weaver(
    model: torch.nn.Module,
    model_args: ModelArguments,
    finetune_args: FinetuneArguments,
    model_type: str = "sundial",
) -> torch.nn.Module:
    weaver_type = "cnn" if finetune_args.finetune_type == "weaver_cnn" else "mlp"

    config_dict = finetune_args.to_weaver_config_dict()
    config_dict["weaver_type"] = weaver_type
    # Inject model-specific structural parameters from model_args
    # so that WeaverCNN/MLP internal dimensions match the base model.
    config_dict["seq_len"] = model_args.seq_len
    config_dict["input_token_len"] = model_args.input_token_len
    config_dict["max_output_token_len"] = (
        model_args.output_token_len
    )  # max(output_token_lens)
    weaver_config = WeaverConfig.from_dict(config_dict)

    freeze_model_parameters(model, trainable_patterns=[])

    # Create DualWeaver wrapper
    wrapped_model = DualWeaver(
        config=weaver_config,
        ltm=model,
        model_type=model_type,
        test_pred_len=finetune_args.test_pred_len,
        test_n_sample=finetune_args.test_n_sample,
    )

    trainable = sum(p.numel() for p in wrapped_model.parameters() if p.requires_grad)
    total = sum(p.numel() for p in wrapped_model.parameters())
    logger.info(
        f"DualWeaver ({weaver_type}) applied: {trainable:,} / {total:,} parameters trainable "
        f"({100*trainable/total:.2f}%)"
    )

    return wrapped_model
