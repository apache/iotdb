import argparse
import os
import sys
from typing import List

# ── Bootstrap: 合并 pip 的 iotdb 包与本地 iotdb.ainode 子包 ──────────
# cli.py 位于 <AINODE_DIR>/timecho/ainode/core/finetune/tools/cli.py
# 向上 6 层即 AINODE_DIR，其下的 iotdb/ainode/ 是本地独有的业务代码
_AINODE_ROOT = os.path.abspath(__file__)
for _ in range(6):
    _AINODE_ROOT = os.path.dirname(_AINODE_ROOT)

if _AINODE_ROOT not in sys.path:
    sys.path.insert(0, _AINODE_ROOT)

try:
    import iotdb as _iotdb_pkg
    _local_iotdb = os.path.join(_AINODE_ROOT, "iotdb")
    if os.path.isdir(_local_iotdb) and _local_iotdb not in _iotdb_pkg.__path__:
        _iotdb_pkg.__path__.insert(0, _local_iotdb)
except ImportError:
    pass
# ── End Bootstrap ────────────────────────────────────────────────────

from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.manager.model_manager import ModelManager
from timecho.ainode.core.finetune.task.task_constants import TaskStatus
from timecho.ainode.core.finetune.task.task_executor import TaskExecutor
from timecho.ainode.core.finetune.task.task_info import FinetuneTask
from timecho.ainode.core.hparams.parser import parse_params_into_args
from timecho.ainode.core.manager.finetune_manager import FinetuneManager

logger = Logger()


def cmd_train(args: argparse.Namespace) -> int:
    model_manager = ModelManager()
    executor = TaskExecutor()

    # Build flat params from CLI args; exclude control-only keys
    exclude_keys = {"command", "model_id", "base_model_id"}
    params = {k: v for k, v in vars(args).items() if k not in exclude_keys}
    params["test_ratio"] = 1.0 - args.train_ratio - args.val_ratio
    params["trust_remote_code"] = True
    params["model_id"] = args.model_id
    params["base_model_id"] = args.base_model_id

    # 1. Register fine-tuned model
    try:
        base_model_info = model_manager.register_fine_tuned_model(
            model_id=params["model_id"],
            base_model_id=params["base_model_id"],
        )
    except Exception as e:
        print(f"Error registering model: {e}")
        return 1

    # 2. Parse params into argument objects
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

    # 3. Create task
    task = FinetuneTask.create(
        base_model_info=base_model_info,
        model_args=model_args,
        data_args=data_args,
        training_args=training_args,
        finetune_args=finetune_args,
    )
    task.start(pid=os.getpid())

    print(f"Created task: {task.task_id}")
    print(f"Executing task directly (synchronous mode) ...")

    # 4. Execute training directly via TaskExecutor (no async thread)
    try:
        result = executor.execute(task)
    except Exception as e:
        print(f"Error executing task {task.task_id}: {e}")
        try:
            model_manager.fail_finetune(task.model_args.model_id)
        except Exception:
            pass
        return 1

    # 5. Handle result
    if result.get("success", False):
        model_manager.complete_finetune(task.model_args.model_id)
        print(f"Task {task.task_id} completed successfully")
        return 0
    else:
        error = result.get("error", "Unknown error")
        model_manager.fail_finetune(task.model_args.model_id)
        print(f"Task {task.task_id} failed: {error}")
        return 1


def cmd_list(args: argparse.Namespace) -> int:
    manager = FinetuneManager()
    tasks = manager.list_tasks(include_completed=args.all)

    if not tasks:
        print("No tasks found")
        return 0

    for task in tasks:
        print(task.to_summary())

    return 0


def cmd_show(args: argparse.Namespace) -> int:
    manager = FinetuneManager()
    task = manager.get_task(args.task_id)

    if task is None:
        print(f"Task not found: {args.task_id}")
        return 1

    print(task.to_detail())
    return 0


def cmd_cancel(args: argparse.Namespace) -> int:
    manager = FinetuneManager()
    success = manager.cancel_task(args.task_id)

    if success:
        print(f"Canceled task: {args.task_id}")
        return 0
    else:
        print(f"Failed to cancel task: {args.task_id}")
        return 1


def cmd_retry(args: argparse.Namespace) -> int:
    manager = FinetuneManager()
    new_task = manager.retry_task(args.task_id)

    if new_task:
        print(f"Retried task as: {new_task.task_id}")
        return 0
    else:
        print(f"Failed to retry task: {args.task_id}")
        return 1


def cmd_status(args: argparse.Namespace) -> int:
    manager = FinetuneManager()

    active = manager.get_active_task()
    pending = manager.pending_count()

    print(f"Pending tasks: {pending}")
    if active:
        print(f"Active task: {active.task_id} ({active.status.value})")
    else:
        print("No active task")

    return 0


def main(argv: List[str] = None) -> int:
    parser = argparse.ArgumentParser(description="AINode Fine-tuning CLI")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    train_parser = subparsers.add_parser("train", help="Start a fine-tuning task")
    train_group = train_parser.add_argument_group("Training")
    train_group.add_argument(
        "--base-model-id", type=str, required=True, help="Base model ID"
    )
    train_group.add_argument(
        "--model-id", type=str, default="finetune_model", help="Output model ID"
    )
    train_group.add_argument(
        "--model-type", type=str, default="sundial", help="Model type"
    )
    train_group.add_argument(
        "--model-name-or-path", type=str, default="", help="Model path"
    )
    train_group.add_argument(
        "--finetune-type",
        type=str,
        default="full",
        choices=["full", "linear", "lora", "weaver_cnn", "weaver_mlp"],
    )
    train_group.add_argument(
        "--priority", type=str, default="normal", choices=["urgent", "normal", "low"]
    )

    data_group = train_parser.add_argument_group("Data")
    data_group.add_argument(
        "--dataset-type",
        type=str,
        default="tsfile",
        choices=["csv", "parquet", "tsfile", "iotdb.table", "iotdb.tree"],
    )
    data_group.add_argument("--data-path", type=str, default="", help="Path to data")
    data_group.add_argument(
        "--sql",
        type=str,
        default="",
        help="SQL query for iotdb.tree dataset, e.g. "
        "'SELECT time, field0 FROM root.sg.* WHERE time < 1000000'",
    )
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
    model_group.add_argument(
        "--torch-dtype",
        type=str,
        default="float32",
        choices=["auto", "float32", "float16", "bfloat16"],
    )

    optim_group = train_parser.add_argument_group("Optimization")
    optim_group.add_argument("--num-train-epochs", type=int, default=1)
    optim_group.add_argument("--max-steps", type=int, default=50)
    optim_group.add_argument("--iter-per-epoch", type=int, default=50)
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

    list_parser = subparsers.add_parser("list", help="List tasks")
    list_parser.add_argument(
        "--all", action="store_true", help="Include completed tasks"
    )

    show_parser = subparsers.add_parser("show", help="Show task details")
    show_parser.add_argument("task_id", type=str, help="Task ID")

    cancel_parser = subparsers.add_parser("cancel", help="Cancel a task")
    cancel_parser.add_argument("task_id", type=str, help="Task ID")

    retry_parser = subparsers.add_parser("retry", help="Retry a failed task")
    retry_parser.add_argument("task_id", type=str, help="Task ID")

    subparsers.add_parser("status", help="Show queue status")

    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0

    command_map = {
        "train": cmd_train,
        "list": cmd_list,
        "show": cmd_show,
        "cancel": cmd_cancel,
        "retry": cmd_retry,
        "status": cmd_status,
    }

    return command_map[args.command](args)


if __name__ == "__main__":
    import shutil

    path = "/root/iotv2_paper/iotdb-2.0/timechodb/iotdb-core/ainode/data/ainode/models/fine_tuned/sundial_air"
    if os.path.exists(path):
        shutil.rmtree(path)
    sys.exit(main())
