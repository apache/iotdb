import argparse
import concurrent
import heapq
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep

import matplotlib.pyplot as plt
import torch
import torch.nn.functional as F

from iotdb.table_session_pool import TableSessionPool, TableSessionPoolConfig
from iotdb.utils.IoTDBConstants import TSDataType

NODE_URLS = ["127.0.0.1:6667"]
USERNAME = "root"
PASSWORD = "TimechoDB@2021"

TRAINING_POINTS_PER_MODEL = 20000

LOOK_BACK_LEN = 1440
PRED_LEN = 96
START_OFFSET = int(TRAINING_POINTS_PER_MODEL - LOOK_BACK_LEN)
# QUERY_COUNT = int(TRAINING_POINTS_PER_MODEL * 0.2 - 10)
QUERY_COUNT = 2000


class MaxHeap:
    def __init__(self):
        self.heap = []

    def push(self, index, weight):
        heapq.heappush(self.heap, (-weight, index))

    def pop(self):
        if not self.is_empty():
            neg_weight, index = heapq.heappop(self.heap)
            return index, -neg_weight
        return None

    def peek(self):
        if not self.is_empty():
            neg_weight, index = self.heap[0]
            return index, -neg_weight
        return None

    def is_empty(self):
        return len(self.heap) == 0

    def size(self):
        return len(self.heap)

    def __str__(self):
        return str([(idx, -w) for w, idx in self.heap])


import atexit
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Iterable, List, Optional


class BatchExecutor:
    """
    Abstract batch concurrent executor.
    - Default fire-and-forget: automatically logs task exceptions (ignores return values)
    - Allows customization of completion logic by overriding callback hooks via inheritance
    - Supports single/batch submission, explicit closure, context management, and exit-time cleanup
    """

    def __init__(
        self,
        max_workers: int = 8,
        thread_name_prefix: str = "batch-exec",
        cancel_on_close: bool = False,
    ):
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix=thread_name_prefix
        )
        self._closing = threading.Event()
        self._lock = threading.Lock()
        self._cancel_on_close = cancel_on_close
        atexit.register(self._atexit_close)

    def submit(self, fn: Callable[..., Any], *args, **kwargs) -> Optional[Future]:
        """
        Submit a single task.
        Default fire-and-forget: internally attaches a done callback to automatically log exceptions.
        To take over result handling, inherit and override on_task_done().
        """
        if self._closing.is_set():
            raise RuntimeError("Executor is closing; reject new submissions.")

        fut = self._executor.submit(fn, *args, **kwargs)
        self._attach_done_callback(fut, item=None)
        self.on_task_submitted(item=None, future=fut)
        return fut

    def submit_batch(
        self, items: Iterable[Any], fn: Callable[[Any], Any]
    ) -> List[Future]:
        """
        Submit a batch of tasks. Executes fn(item) for each item.
        """
        if self._closing.is_set():
            raise RuntimeError("Executor is closing; reject new submissions.")

        futures: List[Future] = []
        for item in items:
            fut = self._executor.submit(fn, item)
            self._attach_done_callback(fut, item=item)
            self.on_task_submitted(item=item, future=fut)
            futures.append(fut)
        return futures

    def close(self, wait: bool = True) -> None:
        """
        Close the executor:
          - wait=True: wait for submitted tasks to complete (default; suitable for graceful shutdown)
          - wait=False: return quickly; if cancel_on_close=True, attempts to cancel pending tasks
        """
        with self._lock:
            if self._closing.is_set():
                return
            self._closing.set()
            self._executor.shutdown(
                wait=wait, cancel_futures=(self._cancel_on_close and not wait)
            )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # Context manager exit: close and wait for tasks to finish
        self.close(wait=True)

    def on_task_submitted(self, item: Any, future: Future) -> None:
        # Triggered when a task is submitted. Default does nothing.
        pass

    def on_task_done(self, item: Any, future: Future) -> None:
        """
        Triggered when a task is done. Default logs exceptions (key for fire-and-forget).
        To customize behavior: inherit and override this method.
        - If you need the result (which may raise), call future.result() here.
        """
        try:
            _ = future.result()
        except Exception as e:
            print(f"Batch task failed (item={item}), because {e}")

    def _attach_done_callback(self, fut: Future, item: Any) -> None:
        def _cb(f: Future, _item=item, self_ref=self):
            try:
                self_ref.on_task_done(_item, f)
            except Exception as e:
                print(f"Error in on_task_done callback (item={_item}), because {e}")

        fut.add_done_callback(_cb)

    def _atexit_close(self) -> None:
        try:
            self.close(wait=False)
        except Exception:
            pass


def parse_args():
    parser = argparse.ArgumentParser(
        description="Process sundial model finetune for specified database, table and column."
    )
    parser.add_argument("--database", "-d", required=True, help="database name")
    parser.add_argument("--table", "-t", required=True, help="table name")
    parser.add_argument("--column", "-c", required=True, help="column name")
    parser.add_argument(
        "--model_num", "-m", required=True, help="the number of fine-tuned models"
    )
    parser.add_argument(
        "--save_num", "-s", required=True, help="the number of shortcuts to save"
    )
    args = parser.parse_args()
    return args


def submit_and_wait_finetune(session_pool, database, table, column, model_num):
    fine_tune_session = session_pool.get_session()
    for index in range(model_num):
        fine_tune_sql = f"""
        CREATE MODEL sundial_{table}_v{index} WITH HYPERPARAMETERS (
            train_epochs=5, num_warmup_steps=800, num_training_steps=30000, learning_rate=0.00001) 
            FROM MODEL sundial ON DATASET ('SELECT time, {column} from {database}.{table}
            OFFSET {index * TRAINING_POINTS_PER_MODEL} LIMIT {TRAINING_POINTS_PER_MODEL}')
        """
        fine_tune_session.execute_non_query_statement(fine_tune_sql)
        print(f"Submit finetune task for model sundial_{table}_v{index}")
    fine_tune_session.close()
    print(f"All finetune tasks submitted, waiting for finish.")

    cur_model_index = 0
    show_model_session = session_pool.get_session()
    while cur_model_index < model_num:
        model_status_dataset = show_model_session.execute_query_statement(
            f"SHOW MODELS sundial_{table}_v{cur_model_index}"
        )
        if model_status_dataset.has_next():
            model_status_data = model_status_dataset.next()
            model_status = model_status_data.get_fields()[3].get_string_value()
            if model_status == "ACTIVE":
                print(f"Finetune model sundial_{table}_v{cur_model_index} finished.")
                cur_model_index += 1
        sleep(5)
    show_model_session.close()
    print(f"All finetune tasks finished.")


def load_model_instances(session_pool, table, model_num):
    print("Start loading finetuned model instances.")
    load_model_session = session_pool.get_session()
    load_model_session.execute_non_query_statement(
        "LOAD MODEL sundial TO DEVICES '0,1'"
    )
    for index in range(model_num):
        load_model_session.execute_non_query_statement(
            f"LOAD MODEL sundial_{table}_v{index} TO DEVICES '0,1'"
        )
    while True:
        instance_status_dict = {}
        for device in range(2):
            instance_status_dict[f"sundial_{device}"] = False
        for index in range(model_num):
            for device in range(2):
                instance_status_dict[f"sundial_{table}_v{index}_{device}"] = False
        instance_status_dataset = load_model_session.execute_query_statement(
            "SHOW LOADED MODELS"
        )
        while instance_status_dataset.has_next():
            instance_status_data = instance_status_dataset.next()
            device_id = instance_status_data.get_fields()[0].get_string_value()
            model_id = instance_status_data.get_fields()[1].get_string_value()
            instance_count = instance_status_data.get_fields()[2].get_int_value()
            if instance_count > 0:
                instance_status_dict[f"{model_id}_{device_id}"] = True
        all_loaded = True
        for key, value in instance_status_dict.items():
            if not value:
                all_loaded = False
                break
        if all_loaded:
            break
        sleep(5)
    load_model_session.close()
    print(f"Finetune models are all loaded, start testing.")


def execute_forecast_compare(
    session_pool, database, table, column, model_num, save_num
):
    save_root_dir = f"/root/internal_fine_tune/{database}-{table}"
    if not os.path.exists(save_root_dir):
        os.makedirs(save_root_dir)
    forecast_executor = BatchExecutor(model_num)

    def _forecast_finetune_model(model_index: int):
        cur_model_dir = os.path.join(save_root_dir, f"sundial_{table}_v{model_index}")
        if not os.path.exists(cur_model_dir):
            os.makedirs(cur_model_dir)
        heap = MaxHeap()
        forecast_session = session_pool.get_session()
        base_offset = TRAINING_POINTS_PER_MODEL * model_index + START_OFFSET
        raw_session = session_pool.get_session()
        zero_shot_session = session_pool.get_session()
        fine_tune_session = session_pool.get_session()
        for i in range(QUERY_COUNT):
            raw_data_sql = f"""
            SELECT time, {column} FROM {table} OFFSET {base_offset + i + LOOK_BACK_LEN} LIMIT {PRED_LEN}
            """
            zero_shot_sql = f"""
            SELECT * FROM FORECAST (
                model_id => 'sundial',
                input => (SELECT time, {column} FROM {table} OFFSET {base_offset + i} LIMIT {LOOK_BACK_LEN}) ORDER BY time,
                output_length => {PRED_LEN}
            )
            """
            fine_tune_sql = f"""
            SELECT * FROM FORECAST (
                model_id => 'sundial_{table}_v{model_index}',
                input => (SELECT time, {column} FROM {table} OFFSET {base_offset + i} LIMIT {LOOK_BACK_LEN}) ORDER BY time,
                output_length => {PRED_LEN}
            )
            """

            def _run_forecast_queries(
                raw_session,
                raw_data_sql,
                zero_shot_session,
                zero_shot_sql,
                fine_tune_session,
                forecast_sql,
            ):
                def _collect_datalist(session, sql):
                    dataset = session.execute_query_statement(sql)
                    data_list = []
                    for _ in range(PRED_LEN):
                        data = dataset.next()
                        data_list.append(
                            float(
                                data.get_fields()[1].get_object_value(TSDataType.FLOAT)
                            )
                        )
                    return data_list

                with ThreadPoolExecutor(max_workers=3) as executor:
                    future_raw = executor.submit(
                        _collect_datalist, raw_session, raw_data_sql
                    )
                    future_zero = executor.submit(
                        _collect_datalist, zero_shot_session, zero_shot_sql
                    )
                    future_forecast = executor.submit(
                        _collect_datalist, fine_tune_session, forecast_sql
                    )
                results = {}
                for future in as_completed([future_raw, future_zero, future_forecast]):
                    if future == future_raw:
                        results["raw_dataset"] = future.result()
                    elif future == future_zero:
                        results["zero_shot_dataset"] = future.result()
                    elif future == future_forecast:
                        results["forecast_dataset"] = future.result()
                return (
                    results["raw_dataset"],
                    results["zero_shot_dataset"],
                    results["forecast_dataset"],
                )

            raw_datalist, zero_shot_datalist, forecast_datalist = _run_forecast_queries(
                raw_session,
                raw_data_sql,
                zero_shot_session,
                zero_shot_sql,
                fine_tune_session,
                fine_tune_sql,
            )

            raw_data_tensor = torch.tensor(raw_datalist, dtype=torch.float32)
            zero_shot_data_tensor = torch.tensor(
                zero_shot_datalist, dtype=torch.float32
            )
            forecast_data_tensor = torch.tensor(forecast_datalist, dtype=torch.float32)
            zero_shot_mse_loss = (
                F.mse_loss(zero_shot_data_tensor, raw_data_tensor, reduction="none")
                .mean(dim=-1)
                .sum()
            )
            fine_tune_mse_loss = (
                F.mse_loss(forecast_data_tensor, raw_data_tensor, reduction="none")
                .mean(dim=-1)
                .sum()
            )
            mse_improve = (
                (zero_shot_mse_loss - fine_tune_mse_loss) / zero_shot_mse_loss * 100
            )
            mse_improve = mse_improve.item()
            heap.push(i, mse_improve)
        raw_session.close()
        zero_shot_session.close()
        fine_tune_session.close()
        shortcut_index_list = []
        for save_index in range(save_num):
            index, mse_improve = heap.pop()
            shortcut_index_list.append(mse_improve)
            raw_data_sql = f"""
            SELECT time, {column} FROM {table} OFFSET {base_offset + index} LIMIT {LOOK_BACK_LEN + PRED_LEN}
            """
            zero_shot_sql = f"""
            SELECT * FROM FORECAST (
                model_id => 'sundial',
                input => (SELECT time, {column} FROM {table} OFFSET {base_offset + index} LIMIT {LOOK_BACK_LEN}) ORDER BY time,
                output_length => {PRED_LEN}
            )
            """
            fine_tune_sql = f"""
            SELECT * FROM FORECAST (
                model_id => 'sundial_{table}_v{model_index}',
                input => (SELECT time, {column} FROM {table} OFFSET {base_offset + index} LIMIT {LOOK_BACK_LEN}) ORDER BY time,
                output_length => {PRED_LEN}
            )
            """
            raw_datalist = []
            zero_shot_datalist = []
            forecast_datalist = []
            raw_dataset = forecast_session.execute_query_statement(raw_data_sql)
            zero_shot_dataset = forecast_session.execute_query_statement(zero_shot_sql)
            forecast_dataset = forecast_session.execute_query_statement(fine_tune_sql)
            for _ in range(LOOK_BACK_LEN + PRED_LEN):
                raw_data = raw_dataset.next()
                raw_datalist.append(
                    float(raw_data.get_fields()[1].get_object_value(TSDataType.FLOAT))
                )
            for _ in range(PRED_LEN):
                zero_shot_data = zero_shot_dataset.next()
                forecast_data = forecast_dataset.next()
                zero_shot_datalist.append(
                    float(
                        zero_shot_data.get_fields()[1].get_object_value(
                            TSDataType.FLOAT
                        )
                    )
                )
                forecast_datalist.append(
                    float(
                        forecast_data.get_fields()[1].get_object_value(TSDataType.FLOAT)
                    )
                )

            plt.figure(figsize=(12, 4))
            plt.xticks([])
            plt.yticks([])
            plt.plot(
                raw_datalist,
                color="limegreen",
                label="Groundtruth",
            )
            plt.plot(
                range(LOOK_BACK_LEN, LOOK_BACK_LEN + PRED_LEN),
                zero_shot_datalist,
                color="tomato",
                label="Zero-shot Prediction",
            )
            plt.plot(
                range(LOOK_BACK_LEN, LOOK_BACK_LEN + PRED_LEN),
                forecast_datalist,
                color="royalblue",
                label="Fine-tuned Prediction",
            )
            plt.legend()
            plt.savefig(os.path.join(cur_model_dir, f"{save_index}.png"))
            plt.close()
        print(
            f"Finetune model sundial_{table}_v{model_index} save results with improvements {shortcut_index_list}, you can find them under {cur_model_dir}."
        )
        forecast_session.close()

    forecast_futures = forecast_executor.submit_batch(
        range(model_num), _forecast_finetune_model
    )
    concurrent.futures.wait(
        forecast_futures, return_when=concurrent.futures.ALL_COMPLETED
    )
    print(
        "Finetune model forecast testing all finished, plz don't shutdown to wait for cleaning environment."
    )


def unload_model_instances(session_pool, table, model_num):
    unload_model_session = session_pool.get_session()
    unload_model_session.execute_non_query_statement(
        "UNLOAD MODEL sundial FROM DEVICES '0,1'"
    )
    for index in range(model_num):
        unload_model_session.execute_non_query_statement(
            f"UNLOAD MODEL sundial_{table}_v{index} FROM DEVICES '0,1'"
        )
    while True:
        instance_status_dict = {}
        instance_status_dataset = unload_model_session.execute_query_statement(
            "SHOW LOADED MODELS"
        )
        while instance_status_dataset.has_next():
            instance_status_data = instance_status_dataset.next()
            device_id = instance_status_data.get_fields()[0].get_string_value()
            model_id = instance_status_data.get_fields()[1].get_string_value()
            instance_status_dict[f"{model_id}_{device_id}"] = True
        if len(instance_status_dict) == 0:
            break
        sleep(5)
    unload_model_session.close()
    print(f"Finetune models are all unloaded.")


def main():
    args = parse_args()
    database = args.database
    table = args.table
    column = args.column
    model_num = int(args.model_num)
    save_num = int(args.save_num)

    config = TableSessionPoolConfig(
        node_urls=NODE_URLS,
        username=USERNAME,
        password=PASSWORD,
        database=database,
        max_pool_size=20,
        fetch_size=PRED_LEN,
        wait_timeout_in_ms=3000,
    )
    session_pool = TableSessionPool(config)

    submit_and_wait_finetune(session_pool, database, table, column, model_num)
    load_model_instances(session_pool, table, model_num)
    execute_forecast_compare(session_pool, database, table, column, model_num, save_num)
    unload_model_instances(session_pool, table, model_num)
    session_pool.close()


if __name__ == "__main__":
    main()
