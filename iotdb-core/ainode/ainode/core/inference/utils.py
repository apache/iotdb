from transformers.modeling_outputs import MoeCausalLMOutputWithPast
import torch

def _slice_tensor(t, s, e):
    return None if t is None else t[s:e]

def _slice_tuple_of_tensors(tup, s, e):
    """
    hidden_states / attentions: Tuple[layer0, layer1, ...]
    every layer maybe Tensor or None。
    """
    if tup is None:
        return None
    sliced = []
    for x in tup:
        sliced.append(_slice_tensor(x, s, e) if torch.is_tensor(x) else x)
    return tuple(sliced)

def _slice_pkv(pkv, s, e):
    if pkv is None:
        return None
    out = []
    for layer in pkv:                       # layer: Tuple[key, value, ...]
        out.append(tuple(x[s:e] for x in layer))
    return out

def split_moe_output(batch_out: MoeCausalLMOutputWithPast, split_sizes):
    """
    split batch_out with type: MoeCausalLMOutputWithPast into len(split_sizes)
    split_sizes[i] = ith request's batch_size。
    """
    outs = []
    start = 0
    for bsz in split_sizes:
        end = start + bsz
        outs.append(
            MoeCausalLMOutputWithPast(
                loss           = _slice_tensor(batch_out.loss, start, end),
                logits         = batch_out.logits[start:end],
                past_key_values= _slice_pkv(batch_out.past_key_values, start, end),
                hidden_states  = _slice_tuple_of_tensors(batch_out.hidden_states, start, end),
                attentions     = _slice_tuple_of_tensors(batch_out.attentions,  start, end),
            )
        )
        start = end
    return outs
