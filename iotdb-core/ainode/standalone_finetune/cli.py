import argparse
import os
import sys
from typing import List

from standalone_finetune.manager.model_manager import ModelManager
from standalone_finetune.finetune.task.task_executor import TaskExecutor
from standalone_finetune.finetune.task.task_info import FinetuneTask
from standalone_finetune.hparams.parser import parse_params_into_args


def cmd_train(args: argparse.Namespace) -> int:
    model_manager = ModelManager()
    executor = TaskExecutor()

    exclude_keys = {"command", "model_id", "base_model_id"}
    params = {k: v for k, v in vars(args).items() if k not in exclude_keys}
    params["test_ratio"] = 1.0 - args.train_ratio - args.val_ratio
    params["trust_remote_code"] = True
    params["model_id"] = args.model_id
    params["base_model_id"] = args.base_model_id

    try:
        base_model_info = model_manager.register_fine_tuned_model(
            model_id=params["model_id"],
            base_model_id=params["base_model_id"],
        )
    except Exception as e:
        print(f"Error registering model: {e}")
        return 1

    try:
        model_args, data_args, training_args, finetune_args = parse_params_into_args(
            params, base_model_info
        )
    except Exception as e:
        print(f"Error parsing parameters: {e}")
        try:
            model_manager.fail_finetune(params["model_id"], cleanup=True)
        except Exception:
            pass
        return 1

    task = FinetuneTask.create(
        base_model_info=base_model_info,
        model_args=model_args,
        data_args=data_args,
        training_args=training_args,
        finetune_args=finetune_args,
    )
    task.start(pid=os.getpid())

    print(f"Task {task.task_id}")
    print("Executing training ...")

    try:
        result = executor.execute(task)
    except Exception as e:
        print(f"Error executing task {task.task_id}: {e}")
        try:
            model_manager.fail_finetune(task.model_args.model_id)
        except Exception:
            pass
        return 1

    if result.get("success", False):
        model_manager.complete_finetune(task.model_args.model_id)
        print(f"Task {task.task_id} completed successfully")
        return 0
    else:
        error = result.get("error", "Unknown error")
        model_manager.fail_finetune(task.model_args.model_id)
        print(f"Task {task.task_id} failed: {error}")
        return 1


def main(argv: List[str] = None) -> int:
    parser = argparse.ArgumentParser(description="Standalone fine-tuning CLI")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    train_parser = subparsers.add_parser("train", help="Run a fine-tuning task")
    train_group = train_parser.add_argument_group("Training")
    train_group.add_argument("--base-model-id", type=str, required=True)
    train_group.add_argument("--model-id", type=str, default="finetune_model")
    train_group.add_argument("--model-type", type=str, default="sundial")
    train_group.add_argument("--model-name-or-path", type=str, default="")
    train_group.add_argument(
        "--finetune-type",
        type=str,
        default="full",
        choices=["full", "linear", "lora", "weaver_cnn", "weaver_mlp"],
    )
    train_group.add_argument("--priority", type=str, default="normal", choices=["urgent", "normal", "low"])

    data_group = train_parser.add_argument_group("Data")
    data_group.add_argument(
        "--dataset-type",
        type=str,
        default="tsfile",
        choices=["csv", "parquet", "tsfile", "iotdb.tree"],
    )
    data_group.add_argument("--data-path", type=str, default="")
    data_group.add_argument("--sql", type=str, default="", help="SQL for iotdb.tree")
    data_group.add_argument("--iotdb-ip", type=str, default="127.0.0.1")
    data_group.add_argument("--iotdb-port", type=int, default=6667)
    data_group.add_argument("--iotdb-username", type=str, default="root")
    data_group.add_argument("--iotdb-password", type=str, default="root")
    data_group.add_argument("--train-ratio", type=float, default=0.7)
    data_group.add_argument("--val-ratio", type=float, default=0.15)
    data_group.add_argument("--scale", action="store_true", default=False)
    data_group.add_argument("--num-workers", type=int, default=0)

    model_group = train_parser.add_argument_group("Model")
    model_group.add_argument("--input-token-len", type=int, default=16)
    model_group.add_argument("--output-token-len", type=int, default=720)
    model_group.add_argument("--seq-len", type=int, default=2880)
    model_group.add_argument("--torch-dtype", type=str, default="float32", choices=["auto", "float32", "float16", "bfloat16"])

    optim_group = train_parser.add_argument_group("Optimization")
    optim_group.add_argument("--num-train-epochs", type=int, default=1)
    optim_group.add_argument("--max-steps", type=int, default=100)
    optim_group.add_argument("--iter-per-epoch", type=int, default=100)
    optim_group.add_argument("--train-batch-size", type=int, default=64)
    optim_group.add_argument("--val-batch-size", type=int, default=64)
    optim_group.add_argument("--learning-rate", type=float, default=1e-4)
    optim_group.add_argument("--weight-decay", type=float, default=0.01)
    optim_group.add_argument("--warmup-ratio", type=float, default=0.1)
    optim_group.add_argument("--use-amp", action="store_true", default=True)
    optim_group.add_argument("--gradient-accumulation-steps", type=int, default=1)
    optim_group.add_argument("--max-grad-norm", type=float, default=1.0)
    optim_group.add_argument("--early-stopping-patience", type=int, default=5)
    optim_group.add_argument("--save-steps", type=int, default=50)
    optim_group.add_argument("--logging-steps", type=int, default=10)
    optim_group.add_argument("--seed", type=int, default=42)
    optim_group.add_argument("--ddp", action="store_true", default=False)
    optim_group.add_argument("--world-size", type=int, default=1)

    lora_group = train_parser.add_argument_group("LoRA")
    lora_group.add_argument("--lora-rank", type=int, default=8)
    lora_group.add_argument("--lora-alpha", type=int, default=16)
    lora_group.add_argument("--lora-dropout", type=float, default=0.1)

    weaver_group = train_parser.add_argument_group("Weaver")
    weaver_group.add_argument("--input-channel", type=int, default=1)
    weaver_group.add_argument("--output-channel", type=int, default=64)

    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0
    if args.command != "train":
        print("Only 'train' is supported in standalone mode.")
        return 1
    return cmd_train(args)


if __name__ == "__main__":
    sys.exit(main())
