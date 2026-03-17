from typing import Dict, List

import torch


class TimeSeriesCollator:
    def __init__(self, pad_value: float = 0.0):
        self.pad_value = pad_value

    def __call__(self, batch: List[Dict[str, torch.Tensor]]) -> Dict[str, torch.Tensor]:
        result = {}

        keys = batch[0].keys()

        for key in keys:
            tensors = [sample[key] for sample in batch]
            result[key] = torch.stack(tensors, dim=0)

        return result


class PaddedTimeSeriesCollator:
    def __init__(self, pad_value: float = 0.0, max_len: int = None):
        self.pad_value = pad_value
        self.max_len = max_len

    def __call__(self, batch: List[Dict[str, torch.Tensor]]) -> Dict[str, torch.Tensor]:
        result = {}

        keys = batch[0].keys()

        for key in keys:
            tensors = [sample[key] for sample in batch]

            if self.max_len:
                max_seq_len = self.max_len
            else:
                max_seq_len = max(t.shape[0] for t in tensors)

            padded = []
            masks = []

            for t in tensors:
                seq_len = t.shape[0]
                if seq_len < max_seq_len:
                    pad_size = [max_seq_len - seq_len] + list(t.shape[1:])
                    padding = torch.full(pad_size, self.pad_value, dtype=t.dtype)
                    padded_t = torch.cat([t, padding], dim=0)
                else:
                    padded_t = t[:max_seq_len]
                padded.append(padded_t)

                mask = torch.zeros(max_seq_len, dtype=torch.bool)
                mask[: min(seq_len, max_seq_len)] = True
                masks.append(mask)

            result[key] = torch.stack(padded, dim=0)
            result[f"{key}_mask"] = torch.stack(masks, dim=0)

        return result
