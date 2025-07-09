# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import multiprocessing
import queue
import random
import threading
import time
from multiprocessing import Process

import numpy as np
import torch
from transformers import PretrainedConfig, PreTrainedModel

from ainode.core.inference.inference_request import InferenceRequest
from ainode.core.log import Logger

logger = Logger()


class InferenceRequestPool(Process):
    """
    The request pool to handle inference for a specific model.
    """

    FIX_SEED = 2021
    WAITING_INTERVAL_IN_MS = (
        15  # How often to check for requests in the waiting/running queue
    )

    def __init__(
        self,
        model: PreTrainedModel,
        config: PretrainedConfig,
        request_queue: multiprocessing.Queue,
        **pool_kwargs,
    ):
        super().__init__()
        self.model = model
        self.device = self.model.device
        self.config = config
        self.pool_kwargs = pool_kwargs

        # TODO: A scheduler is necessary for better handling following queues
        self.waiting_queue = request_queue  # Requests that are waiting to be processed
        self.running_queue = (
            queue.Queue()
        )  # Requests that are currently being processed, TODO: we might need coroutine to accelerate different stages
        self.finished_queue = queue.Queue()  # Requests that are finished
        self._stop_event = multiprocessing.Event()

        # Fix inference seed
        random.seed(self.FIX_SEED)
        torch.manual_seed(self.FIX_SEED)
        np.random.seed(self.FIX_SEED)

    def memory_is_available(self, request):
        # need test with several rounds of dummy data
        pass

    def _activate_requests(self):
        while not self.waiting_queue.empty():
            request: InferenceRequest = self.waiting_queue.get()
            # TODO: Check memory size before activating requests
            request.inputs = request.strategy.preprocess_inputs(request.inputs)
            request.mark_running()
            self.running_queue.put(request)

    def _requests_activate_loop(self):
        while not self._stop_event.is_set():
            time.sleep(self.WAITING_INTERVAL_IN_MS / 1000)
            self._activate_requests()

    def _step(self):
        if self.running_queue.empty():
            return
        # TODO: We need a batcher to accelerate the concurrent inference
        # TODO: Check memory size before executing requests
        request: InferenceRequest = self.running_queue.get()
        output = self.model.generate(
            request.inputs,
            max_new_tokens=request.max_new_tokens,
            num_samples=10,
            revin=True,
        )
        request.write_step_output(output[0].mean(dim=0))
        request.strategy.post_decode()
        if request.is_finished():
            self.finished_queue.put(request)
        else:
            self.waiting_queue.put(request)

    def _requests_execute_loop(self):
        while not self._stop_event.is_set():
            time.sleep(self.WAITING_INTERVAL_IN_MS / 1000)
            self._step()

    def _finish(self):
        while not self.finished_queue.empty():
            request: InferenceRequest = self.finished_queue.get()
            request.strategy.post_inference()
            request.notify_completion()

    def _requests_finish_loop(self):
        while not self._stop_event.is_set():
            time.sleep(self.WAITING_INTERVAL_IN_MS / 1000)
            self._finish()

    def run(self):
        activate_daemon = threading.Thread(target=self._activate_requests)
        activate_daemon.daemon = True
        activate_daemon.start()
        execute_daemon = threading.Thread(target=self._requests_execute_loop)
        execute_daemon.daemon = True
        execute_daemon.start()
        finish_daemon = threading.Thread(target=self._requests_finish_loop)
        finish_daemon.daemon = True
        finish_daemon.start()
        activate_daemon.join()
        execute_daemon.join()
        finish_daemon.join()

    def stop(self):
        self._stop_event.set()


def pool_worker(p, done_event):
    while not done_event.is_set():
        p._step()
        time.sleep(0.001)


"""
The following code is used to test the difference in inference speed and the difference in result values when using and not using requestPool
"""
if __name__ == "__main__":
    config = TimerConfig()
    config.ckpt_path = "/data/mahaoke/AINode/ainode/TimerXL/model.safetensors"
    model = TimerForPrediction(config).eval()
    if config.ckpt_path is not None and config.ckpt_path != "":
        state_dict = load_file(config.ckpt_path)
        model.load_state_dict(state_dict, strict=True)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)

    BATCH = 1
    INPUT_LEN = config.input_token_len * 7  # 例如 4 × 96
    x1 = torch.randn(BATCH, INPUT_LEN, device=device)
    x2 = torch.randn(BATCH, INPUT_LEN, device=device)
    x3 = torch.randn(BATCH, INPUT_LEN, device=device)

    pool = InferenceRequestPool(model, config, total_memory_availble=24 * 1024)

    def _always_true(self, req):
        return True

    InferenceRequestPool.memory_is_availble = _always_true

    def prepare_inputs(model, x, max_new_steps: int = 96, **model_kwargs):
        model_inputs = model.prepare_inputs_for_generation(x, **model_kwargs)
        return model_inputs

    def baseline_generate(model, inp: torch.Tensor, max_steps: int, **model_kwargs):
        cur_ids = inp
        preds = []
        remain = max_steps

        model_kwargs["attention_mask"] = pool.prepare_attention_mask_for_generation(inp)

        batch_size, cur_len = inp.shape

        model_kwargs["unfinished_sequences"] = torch.ones(
            batch_size, dtype=torch.long, device=inp.device
        )
        model_kwargs["cache_position"] = torch.arange(cur_len, device=inp.device)
        true_seq_len = cur_len // config.input_token_len
        model_kwargs["attention_mask"] = model_kwargs["attention_mask"][
            :, -true_seq_len:
        ]
        model_kwargs["past_key_values"] = None
        model_kwargs["position_ids"] = None
        model_kwargs["is_encoder_decoder"] = getattr(
            config, "is_encoder_decoder", False
        )
        model_kwargs["max_output_length"] = max_steps

        while remain > 0:
            chunk = 96
            model_inputs = prepare_inputs(model, cur_ids, max_steps, **model_kwargs)
            out = model(**model_inputs)
            # [B, chunk]
            tok = out.logits.detach()
            preds.append(tok.cpu())
            cur_ids = torch.cat([cur_ids, tok.to(device)], dim=-1)

            horizon_len = 96 // config.input_token_len
            model_kwargs = pool._update_model_kwargs_for_generation(
                out, model_kwargs, horizon_len, False
            )

            remain -= chunk
        return torch.cat(preds, dim=-1)  # [B, max_steps]

    # warm up
    for i in range(3):
        base_res1 = baseline_generate(model, x1, 192)

    torch.cuda.synchronize()
    t_base_start = time.perf_counter()
    base_res1 = baseline_generate(model, x1, 192)
    base_res2 = baseline_generate(model, x2, 192)
    base_res3 = baseline_generate(model, x3, 192)
    base_reses = [base_res1, base_res2, base_res3]
    # print(f'base_reses:{base_reses}')
    torch.cuda.synchronize()
    t_base_end = time.perf_counter()
    base_time = t_base_end - t_base_start
    print(f"[Baseline]    total time: {base_time*1000:.1f} ms")

    done_event = threading.Event()
    threading.Thread(target=pool_worker, args=(pool, done_event), daemon=True).start()

    torch.cuda.synchronize()
    t_pool_start = time.perf_counter()
    pool.add_request(1, x1, max_new_steps=192)
    # time.sleep(0.010)
    pool.add_request(2, x2, max_new_steps=192)
    # time.sleep(0.010)
    pool.add_request(3, x3, max_new_steps=192)
    pool_results = []
    while len(pool_results) < 3:
        pool_results.append(pool.results_queue.get())
    torch.cuda.synchronize()
    t_pool_end = time.perf_counter()
    pool_time = t_pool_end - t_pool_start
    print(f"[RequestPool] total time: {pool_time*1000:.1f} ms")

    done_event.set()  # stop pool

    def mae(a, b):
        return (a - b).abs().mean().item()

    diff1 = mae(
        pool_results[0][1].to("cpu"), base_reses[pool_results[0][0] - 1].to("cpu")
    )
    diff2 = mae(
        pool_results[1][1].to("cpu"), base_reses[pool_results[1][0] - 1].to("cpu")
    )
    diff3 = mae(
        pool_results[2][1].to("cpu"), base_reses[pool_results[2][0] - 1].to("cpu")
    )

    print(f"MAE diff (req1/2/3): {diff1:.6f}, {diff2:.6f}, {diff3:.6f}")
    print(f"Speed-up: {base_time/pool_time:.2f}× faster with RequestPool")
