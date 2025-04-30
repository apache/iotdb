import torch

class TriangularCausalMask():
    def __init__(self, B, L, device="cpu"):
        mask_shape = [B, 1, L, L]
        with torch.no_grad():
            self._mask = torch.triu(torch.ones(mask_shape, dtype=torch.bool), diagonal=1).to(device)

    @property
    def mask(self):
        return self._mask

class TimerMultivariateMask():
    def __init__(self, B, n_vars, n_tokens, device="cpu"):
        mask_shape = [B, 1, n_tokens, n_tokens]
        with torch.no_grad():
            self._mask1 = torch.ones((n_vars, n_vars), dtype=torch.bool).to(device)
            self._mask2 = torch.triu(torch.ones(mask_shape, dtype=torch.bool), diagonal=1).to(device)
            self._mask = torch.kron(self._mask1, self._mask2)
    @property
    def mask(self):
        return self._mask

class TimerCovariateMask():
    def __init__(self, B, n_vars, n_tokens, device="cpu"):
        mask_shape = [B, 1, n_tokens, n_tokens]
        with torch.no_grad():
            self._mask1 = torch.eye(n_vars, dtype=torch.bool).to(device)
            self._mask2 = torch.tril(torch.ones(mask_shape, dtype=torch.bool)).to(device)
            self._mask = ~torch.kron(self._mask1, self._mask2)
            self._mask[:, :, -n_tokens:, :-n_tokens] = False
            
    @property
    def mask(self):
        return self._mask
    
def prepare_4d_causal_attention_mask(
    attention_mask,
    input_shape,                  # (B, T_query)
    inputs_embeds: torch.Tensor,
    past_key_values_length: int = 0,
):
    """
    返回形状 [B, 1, T_query, T_total] 的加性掩码:
        ─ 允许位置: 0
        ─ 屏蔽位置: -inf
    """
    B, T = input_shape
    S = T + past_key_values_length
    dtype, device = inputs_embeds.dtype, inputs_embeds.device

    # 1) causal mask (含历史)
    q_pos = torch.arange(past_key_values_length,
                         past_key_values_length + T, device=device)  # [T]
    k_pos = torch.arange(S, device=device)                           # [S]
    causal = (k_pos.unsqueeze(0) <= q_pos.unsqueeze(1))              # [T,S] bool

    mask = torch.zeros((T, S), dtype=dtype, device=device)           # 先全 0
    mask.masked_fill_(~causal, torch.finfo(dtype).min)               # 不可见 → -inf
    mask = mask.unsqueeze(0).unsqueeze(0)                            # [1,1,T,S]

    # 2) padding mask
    if attention_mask is not None:
        pad = (1.0 - attention_mask.to(dtype)) * torch.finfo(dtype).min  # [B,S]
        pad = pad[:, None, None, :]                                      # [B,1,1,S]
    else:
        pad = 0.

    return mask + pad                     # [B,1,T,S]
