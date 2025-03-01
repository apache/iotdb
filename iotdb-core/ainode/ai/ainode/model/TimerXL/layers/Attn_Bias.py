import abc
import math
import torch
from einops import rearrange
from torch import nn


class AttentionBias(nn.Module, abc.ABC):
    def __init__(self, dim: int, num_heads: int):
        super().__init__()
        assert num_heads > 0 and dim % num_heads == 0

        self.num_heads = num_heads
        self.head_dim = dim // num_heads

    @abc.abstractmethod
    def forward(self, query_id, kv_id): ...


class BinaryAttentionBias(AttentionBias):
    def __init__(self, dim: int, num_heads: int):
        super().__init__(dim, num_heads)
        self.emb = nn.Embedding(num_embeddings=2, embedding_dim=self.num_heads)

    def forward(self, query_id, kv_id):
        ind = torch.eq(query_id.unsqueeze(-1), kv_id.unsqueeze(-2))
        weight = rearrange(
            self.emb.weight, "two num_heads -> two num_heads 1 1")
        bias = ~ind * weight[:1] + ind * weight[1:]
        return bias


def _relative_position_bucket(relative_position, bidirectional=True, num_buckets=32, max_distance=128):
    relative_buckets = 0
    if bidirectional:
        num_buckets //= 2
        relative_buckets += (relative_position >
                             0).to(torch.long) * num_buckets
        relative_position = torch.abs(relative_position)
    else:
        relative_position = - \
            torch.min(relative_position, torch.zeros_like(relative_position))

    max_exact = num_buckets // 2
    is_small = relative_position < max_exact
    relative_position_if_large = max_exact + (
        torch.log(relative_position.float() / max_exact)
        / math.log(max_distance / max_exact)
        * (num_buckets - max_exact)
    ).to(torch.long)
    relative_position_if_large = torch.min(
        relative_position_if_large, torch.full_like(
            relative_position_if_large, num_buckets - 1)
    )

    relative_buckets += torch.where(is_small,
                                    relative_position, relative_position_if_large)
    return relative_buckets


class T5AttentionBias(AttentionBias):
    def __init__(self, dim: int, num_heads: int):
        super().__init__(dim, num_heads)
        self.num_buckets = 32
        self.max_distance = 32
        self.relative_attention_bias = nn.Embedding(self.num_buckets, 1)

    def forward(self, n_vars, n_tokens):
        context_position = torch.arange(n_tokens, dtype=torch.long,)[:, None]
        memory_position = torch.arange(n_tokens, dtype=torch.long, )[None, :]
        relative_position = memory_position - context_position
        bucket = _relative_position_bucket(relative_position=relative_position, bidirectional=False,
                                           num_buckets=self.num_buckets, max_distance=self.max_distance).to(self.relative_attention_bias.weight.device)
        bias = self.relative_attention_bias(bucket).squeeze(-1)
        bias = bias.reshape(1, 1, bias.shape[0], bias.shape[1])
        mask1 = torch.ones((n_vars, n_vars), dtype=torch.bool).to(bias.device)
        final_bias = torch.kron(mask1, bias)
        return final_bias
