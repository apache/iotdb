import torch
from torch import nn
import torch.nn.functional as F
from typing import Optional, List, Dict, Any, Tuple
import queue 
import time
import threading
import gc
from transformers.modeling_outputs import MoeModelOutputWithPast, MoeCausalLMOutputWithPast
from safetensors.torch import load_file

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

from ainode.core.inference.request import Request
from ainode.core.inference.utils import split_moe_output

class RequestPool:
    def __init__(self, model, config, total_memory_availble):
        super().__init__()
        self.model = model
        self.config = config
        self.total_memory_availble = total_memory_availble      # MB
            
        self.waiting_list = queue.Queue()
        self.running_list = queue.Queue()

        self.results_queue = queue.Queue()
        
    # TODO introduce pipeline
    def add_request(self, req_id, x, max_new_steps: int = 96, post_inference_fn = None, **model_kwargs):
        if len(x.shape) == 2:
            batch_size, cur_len = x.shape
            if cur_len < self.config.input_token_len:
                raise ValueError(
                    f"Input length must be at least {self.config.input_token_len}")
            elif cur_len % self.config.input_token_len != 0:
                new_len = (cur_len // self.config.input_token_len) * \
                    self.config.input_token_len
                x = x[:, -new_len:]
        else:
            raise ValueError('Input shape must be: [batch_size, seq_len]')
        
        x = x.to(self.model.device)
        model_kwargs["attention_mask"] = self.prepare_attention_mask_for_generation(x)
        
        batch_size, cur_len = x.shape
        
        model_kwargs["unfinished_sequences"] = torch.ones(batch_size, dtype=torch.long, device=x.device)
        model_kwargs["cache_position"] = torch.arange(cur_len, device=x.device)
        true_seq_len = cur_len // self.config.input_token_len
        model_kwargs["attention_mask"] = model_kwargs["attention_mask"][:, -true_seq_len:]
        model_kwargs["past_key_values"] = None
        model_kwargs["position_ids"] = None
        model_kwargs["is_encoder_decoder"] = getattr(self.config, "is_encoder_decoder", False)
        
        req = Request(req_id, x, max_new_steps, post_inference_fn, 96, **model_kwargs)
        self.waiting_list.put(req)

        logger.info(f"Enqueued req={req_id}  batch={batch_size}  "
                f"seq={cur_len}  target={max_new_steps} chunk_size={req.chunk_size}")
        
    def memory_is_availble(self, request):
        # TODO test with several rounds of dummy data
        pass
        
    def step(self):
        while not self.waiting_list.empty():
            request = self.waiting_list.queue[0]
            all_not_running = all(item.state != 'running' for item in list(self.running_list.queue))
            if self.memory_is_availble(request) and all_not_running:
                request = self.waiting_list.get()
                request.state = 'ready'
                self.running_list.put(request)
                logger.debug(f"Request {request.id} moved to running_list")
            else:
                break

        if self.running_list.empty():
            return []

        # Merge step_outputs from all requests in running_list into a batch
        running_reqs: List[Request] = []
        while not self.running_list.empty():
            request = self.running_list.get()
            running_reqs.append(request)
            
        model_inputs = self.prepare_model_inputs(running_reqs)

        logger.info(f"Running batch: {len(running_reqs)} reqs | "
            f"total_samples={sum(r.batch_size for r in running_reqs)}")
        with torch.no_grad():
            # Uniformly generate 96 steps; crop inside request.write_step_output if it exceeds max length
            batch_output = self.model(
                **model_inputs, max_output_length=96
            )
            
        results = []
        # print(f'type:{type(batch_output)}')
        if type(batch_output) == torch.Tensor:
            offset = 0  
            for request in running_reqs:
                b = request.batch_size
                output_i = batch_output[offset : offset + b]   # [B_i, chunk]
                offset += b

                next_tokens = output_i                        # [B_i, chunk]
                request.all_input_ids = torch.cat(
                    [request.all_input_ids, next_tokens], dim=-1
                )

                horizon_length = next_tokens.shape[1] // self.config.input_token_len

                request.model_kwargs = self._update_model_kwargs_for_generation(
                    output_i,
                    request.model_kwargs,
                    horizon_length=horizon_length,
                    is_encoder_decoder=self.config.is_encoder_decoder,
                )
                
                request.write_step_output(output_i)

                logger.debug(
                    "Tensor"
                    f"req={request.id}  wrote {pred_tokens.shape[1]} steps  "
                    f"({request.cur_token_idx}/{request.max_new_tokens})"
                )

                if not request.is_finished():
                    request.state = 'running'
                    self.running_list.put(request)
                else:
                    results.append((request.id, request.run_post_inference_fn()))
        elif type(batch_output) == MoeModelOutputWithPast or type(batch_output) == MoeCausalLMOutputWithPast or type(batch_output) == tuple:
            split_sizes = [req.batch_size for req in running_reqs]
            outs_per_req = split_moe_output(batch_output, split_sizes)

            for req, out_i in zip(running_reqs, outs_per_req):
                pred_tokens = out_i.logits               # shape [B_i, chunk]
                req.all_input_ids = torch.cat(
                    [req.all_input_ids, pred_tokens], dim=-1
                )

                # print(f"chunksize:{req.chunk_size}, token_len:{self.config.input_token_len}")
                horizon_len = req.chunk_size // self.config.input_token_len

                req.model_kwargs = self._update_model_kwargs_for_generation(
                    out_i,
                    req.model_kwargs,
                    horizon_length=horizon_len,
                    is_encoder_decoder=self.config.is_encoder_decoder,
                )

                req.write_step_output(pred_tokens)

                logger.debug(
                    "Tuple"
                    f"req={req.id}  wrote {pred_tokens.shape[1]} steps  "
                    f"({req.cur_step_idx}/{req.max_new_steps})"
                )

                if not req.is_finished():
                    request.state = 'running'
                    self.running_list.put(req)
                else:
                    results.append((req.id, req.run_post_inference_fn()))
        else:
            raise NotImplementedError

        for r in results:
            self.results_queue.put(r)
        # return results
        logger.debug(f"Pushed {len(results)} finished results to results_queue")
    
    def run_inference(self):
        while True:
            time.sleep(15)  # Check every 15ms
            results = self.step()

    def prepare_model_inputs(self, running_reqs: List[Request]):
        model_inputs = []

        for req in running_reqs:
            # TODO call preprocessing pipeline
            
            single_req_model_inputs = self.prepare_model_inputs_for_single_req(req)
            model_inputs.append(single_req_model_inputs)
        
        # Left pad with zeros
        def pad_and_merge_model_inputs(list_model_inputs: List[Dict]):
            batched_model_inputs = {}
            keys_to_batch = ['input_ids', 'attention_mask', 'position_ids', 'past_key_values']
            other_keys = [k for k in list_model_inputs[0].keys()
                    if k not in keys_to_batch]
    
            # start merge
            for k in other_keys:
                batched_model_inputs[k] = list_model_inputs[0][k]
                
            for k in ["input_ids", "attention_mask", "position_ids"]:
                max_len = max(inp[k].size(-1) for inp in list_model_inputs)
                padded = [self.left_pad(item[k], max_len, pad_value=(0 if k != "position_ids" else 0)) if item[k] is not None else None
                        for item in list_model_inputs]
                batched_model_inputs[k] = torch.cat(padded, dim=0) if padded[0] is not None else None  # [B_total, max_len]

            # ---- past_key_values ----
            # List[Tuple[k, v]]，len = num_layers
            if list_model_inputs[0]["past_key_values"] is not None:
                num_layers = len(list_model_inputs[0]["past_key_values"])
                pkv_merged: List[Tuple[torch.Tensor, torch.Tensor]] = []

                for layer in range(num_layers):
                    k_list, v_list = [], []

                    # 只计算有值的 max_len
                    max_k_len = max(
                        (item["past_key_values"][layer][0].size(-2)
                        for item in list_model_inputs if item["past_key_values"] is not None),
                        default=0
                    )
                    max_v_len = max(
                        (item["past_key_values"][layer][1].size(-2)
                        for item in list_model_inputs if item["past_key_values"] is not None),
                        default=0
                    )

                    for item in list_model_inputs:
                        if item["past_key_values"] is None:
                            # first iteration：empty k/v
                            B = item["input_ids"].shape[0]
                            H, D = self.config.num_heads, self.config.hidden_size // self.config.num_heads
                            empty_k = torch.zeros(B, H, max_k_len, D, device=item["input_ids"].device)
                            empty_v = torch.zeros(B, H, max_v_len, D, device=item["input_ids"].device)
                            k_list.append(empty_k)
                            v_list.append(empty_v)
                        else:
                            k, v = item["past_key_values"][layer]
                            k_padded = self.left_pad(k, max_k_len, pad_value=0, dim=-2)
                            v_padded = self.left_pad(v, max_v_len, pad_value=0, dim=-2)
                            k_list.append(k_padded)
                            v_list.append(v_padded)

                    k_cat = torch.cat(k_list, dim=0)
                    v_cat = torch.cat(v_list, dim=0)
                    pkv_merged.append((k_cat, v_cat))

                batched_model_inputs["past_key_values"] = pkv_merged
            else:
                batched_model_inputs["past_key_values"] = None
            
            return batched_model_inputs
        
        model_inputs = pad_and_merge_model_inputs(model_inputs)
        return model_inputs
    
    @staticmethod
    def left_pad(t: torch.Tensor, target_len: int, pad_value: int = 0, dim: int = -1) -> torch.Tensor:
        """Pad the last dimension with zeros or a constant to target_len on the left side"""
        if t is None:
            return None
        dim = dim if dim >= 0 else t.dim() + dim  # Convert negative dim to positive index
        pad_len = target_len - t.size(dim)
        if pad_len <= 0:
            return t
        
        # pad_sizes = [pad_len, 0]  # (last_dim_left, last_dim_right)
        pad_sizes = [0] * (2 * t.dim())  # PyTorch expects pad in reverse order
        pad_sizes[-(2 * dim + 2)] = pad_len  # pad left side of dimension `dim`

        return F.pad(t, pad_sizes, value=pad_value)

    def prepare_model_inputs_for_single_req(self, running_req: Request):
        return self.model.prepare_inputs_for_generation(running_req.all_input_ids, **running_req.model_kwargs)
    
    def prepare_attention_mask_for_generation(
        self,
        inputs: torch.Tensor,
    ) -> torch.LongTensor:
        return torch.ones(inputs.shape[:2], dtype=torch.long, device=inputs.device)
    
    def _update_model_kwargs_for_generation(
        self,
        outputs,
        model_kwargs: Dict[str, Any],
        horizon_length: int = 1,
        is_encoder_decoder: bool = False,
        standardize_cache_format: bool = False,
    ) -> Dict[str, Any]:
        # update past_key_values
        model_kwargs["past_key_values"] = outputs.past_key_values
        if getattr(outputs, "state", None) is not None:
            model_kwargs["state"] = outputs.state
            
        # update token_type_ids with last value
        if "token_type_ids" in model_kwargs:
            token_type_ids = model_kwargs["token_type_ids"]
            model_kwargs["token_type_ids"] = torch.cat(
                [token_type_ids, token_type_ids[:, -1].unsqueeze(-1)], dim=-1)

        # update attention mask
        if not is_encoder_decoder:
            # update attention mask
            if "attention_mask" in model_kwargs:
                attention_mask = model_kwargs["attention_mask"]
                model_kwargs["attention_mask"] = torch.cat(
                    [attention_mask, attention_mask.new_ones((attention_mask.shape[0], horizon_length))], dim=-1
                )
        else:
            # update decoder attention mask
            if "decoder_attention_mask" in model_kwargs:
                decoder_attention_mask = model_kwargs["decoder_attention_mask"]
                model_kwargs["decoder_attention_mask"] = torch.cat(
                    [decoder_attention_mask, decoder_attention_mask.new_ones(
                        (decoder_attention_mask.shape[0], horizon_length))],
                    dim=-1,
                )

        if "cache_position" in model_kwargs and model_kwargs["cache_position"] is not None:
            model_kwargs["cache_position"] = model_kwargs["cache_position"][-1:] + horizon_length

        return model_kwargs
    
def pool_worker(p, done_event):
    while not done_event.is_set():
        p.step()
        time.sleep(0.001)

'''
The following code is used to test the difference in inference speed and the difference in result values when using and not using requestpool
'''
# if __name__ == '__main__':
#     config = TimerConfig()
#     config.ckpt_path = '/data/mahaoke/AINode/ainode/TimerXL/model.safetensors'
#     model = TimerForPrediction(config).eval()
#     if config.ckpt_path is not None and config.ckpt_path != '':
#         state_dict = load_file(config.ckpt_path)
#         model.load_state_dict(state_dict, strict=True)

#     device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
#     model = model.to(device)

#     BATCH = 1
#     INPUT_LEN = config.input_token_len * 7         # 例如 4 × 96
#     x1 = torch.randn(BATCH, INPUT_LEN, device=device)
#     x2 = torch.randn(BATCH, INPUT_LEN, device=device)
#     x3 = torch.randn(BATCH, INPUT_LEN, device=device)

#     pool = RequestPool(model, config, total_memory_availble=24 * 1024)
#     def _always_true(self, req):
#         return True
#     RequestPool.memory_is_availble = _always_true

#     def prepare_inputs(model, x, max_new_steps: int = 96, **model_kwargs):  
#         model_inputs = model.prepare_inputs_for_generation(x, **model_kwargs)
#         return model_inputs

#     def baseline_generate(model, inp: torch.Tensor, max_steps: int, **model_kwargs):
#         cur_ids = inp
#         preds = []
#         remain = max_steps

#         model_kwargs["attention_mask"] = pool.prepare_attention_mask_for_generation(inp)
        
#         batch_size, cur_len = inp.shape
        
#         model_kwargs["unfinished_sequences"] = torch.ones(batch_size, dtype=torch.long, device=inp.device)
#         model_kwargs["cache_position"] = torch.arange(cur_len, device=inp.device)
#         true_seq_len = cur_len // config.input_token_len
#         model_kwargs["attention_mask"] = model_kwargs["attention_mask"][:, -true_seq_len:]
#         model_kwargs["past_key_values"] = None
#         model_kwargs["position_ids"] = None
#         model_kwargs["is_encoder_decoder"] = getattr(config, "is_encoder_decoder", False)
#         model_kwargs["max_output_length"] = max_steps

#         while remain > 0:
#             chunk = 96
#             model_inputs = prepare_inputs(model, cur_ids, max_steps, **model_kwargs)
#             out = model(**model_inputs)
#             # [B, chunk]
#             tok = out.logits.detach()
#             preds.append(tok.cpu())
#             cur_ids = torch.cat([cur_ids, tok.to(device)], dim=-1)

#             horizon_len = 96 // config.input_token_len
#             model_kwargs = pool._update_model_kwargs_for_generation(
#                 out,
#                 model_kwargs,
#                 horizon_len,
#                 False
#             )

#             remain -= chunk
#         return torch.cat(preds, dim=-1)            # [B, max_steps]
    
#     # warm up
#     for i in range(3):
#         base_res1 = baseline_generate(model, x1, 192)

#     torch.cuda.synchronize()
#     t_base_start = time.perf_counter()
#     base_res1 = baseline_generate(model, x1, 192)
#     base_res2 = baseline_generate(model, x2, 192)
#     base_res3 = baseline_generate(model, x3, 192)
#     base_reses = [base_res1, base_res2, base_res3]
#     # print(f'base_reses:{base_reses}')
#     torch.cuda.synchronize()
#     t_base_end = time.perf_counter()
#     base_time = t_base_end - t_base_start
#     print(f"[Baseline]    total time: {base_time*1000:.1f} ms")

#     done_event = threading.Event()
#     threading.Thread(target=pool_worker, args=(pool, done_event), daemon=True).start()

#     torch.cuda.synchronize()
#     t_pool_start = time.perf_counter()
#     pool.add_request(1, x1, max_new_steps=192)
#     # time.sleep(0.010)
#     pool.add_request(2, x2, max_new_steps=192)
#     # time.sleep(0.010)
#     pool.add_request(3, x3, max_new_steps=192)
#     pool_results = []
#     while len(pool_results) < 3:
#         pool_results.append(pool.results_queue.get())
#     torch.cuda.synchronize()
#     t_pool_end = time.perf_counter()
#     pool_time = t_pool_end - t_pool_start
#     print(f"[RequestPool] total time: {pool_time*1000:.1f} ms")

#     done_event.set()          # stop pool

#     def mae(a, b):
#         return (a - b).abs().mean().item()

#     diff1 = mae(pool_results[0][1].to('cpu'), base_reses[pool_results[0][0]-1].to('cpu'))
#     diff2 = mae(pool_results[1][1].to('cpu'), base_reses[pool_results[1][0]-1].to('cpu'))
#     diff3 = mae(pool_results[2][1].to('cpu'), base_reses[pool_results[2][0]-1].to('cpu'))

#     print(f"MAE diff (req1/2/3): {diff1:.6f}, {diff2:.6f}, {diff3:.6f}")
#     print(f"Speed-up: {base_time/pool_time:.2f}× faster with RequestPool")
