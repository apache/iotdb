from typing import Callable, Optional, List, Dict, Any
import torch

class Request:
    def __init__(
        self,
        id: int,
        all_input_ids: torch.Tensor,
        max_new_steps: int = 96,
        post_inference_fn: Optional[Callable] = None,
        chunk_size: int = 96,                   # token size, how many time steps a token has
        **model_kwargs,
    ):      
        if all_input_ids.ndim == 1:
            all_input_ids = all_input_ids.unsqueeze(0)

        self.id = id
        self.all_input_ids = all_input_ids
        self.model_kwargs = model_kwargs
        self.max_new_steps = max_new_steps      # Number of time steps to generate
        self.chunk_size = chunk_size
        self.post_inference_fn = post_inference_fn

        self.batch_size = all_input_ids.size(0)
        self.state = 'waiting'
        self.cur_step_idx = 0                  # Current write position in the output step index

        # Preallocate output buffer [batch_size, max_new_tokens]
        device = all_input_ids.device
        self.output_tensor = torch.zeros(self.batch_size, max_new_steps, device=device) # shape: [self.batch_size, max_new_steps]

    def mark_running(self):
        self.state = 'running'

    def mark_finished(self):
        self.state = 'finished'

    def is_finished(self) -> bool:
        return self.cur_step_idx >= self.max_new_steps

    def write_step_output(self, step_output: torch.Tensor):
        if step_output.ndim == 1:
            step_output = step_output.unsqueeze(0)

        B, S = step_output.shape
        assert B == self.batch_size, f"batch mismatch {B} vs {self.batch_size}"
        assert S == self.chunk_size, f"chunk mismatch {S} vs {self.chunk_size}"
        end_idx = self.cur_step_idx + S

        if end_idx > self.max_new_steps:
            # raise ValueError(f"write_step_output exceeds allocated output space: {end_idx} > {self.max_new_steps}")
            self.output_tensor[:, self.cur_step_idx:] = step_output[:, :self.max_new_steps - self.cur_step_idx]
            self.cur_step_idx = self.max_new_steps
        else:
            self.output_tensor[:, self.cur_step_idx:end_idx] = step_output
            self.cur_step_idx = end_idx

        if self.is_finished():
            self.mark_finished()

    def get_final_output(self) -> torch.Tensor:
        return self.output_tensor[:, :self.cur_step_idx]

    def run_post_inference_fn(self) -> Optional[torch.Tensor]:
        if self.post_inference_fn is not None:
            return self.post_inference_fn(self.get_final_output())
        return self.get_final_output()

    def reset(self):
        self.state = 'waiting'
        self.cur_step_idx = 0
        self.output_tensor.zero_()
        
