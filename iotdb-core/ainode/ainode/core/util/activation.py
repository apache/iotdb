import torch
import torch.nn as nn
import torch.nn.functional as F
from collections import OrderedDict

class ClampedGELU(nn.Module):
    def __init__(self, min_val=-10, max_val=10):
        super().__init__()
        self.act = nn.GELU()
        self.min_val = min_val
        self.max_val = max_val

    def forward(self, x):
        return torch.clamp(self.act(x), self.min_val, self.max_val)

class ClassInstantier(OrderedDict):
    def __getitem__(self, key):
        content = super().__getitem__(key)
        cls, kwargs = content if isinstance(content, tuple) else (content, {})
        return cls(**kwargs)

ACT2CLS = {
    "gelu": nn.GELU,
    "gelu_10": (ClampedGELU, {"min": -10, "max": 10}),
    "leaky_relu": nn.LeakyReLU,
    "relu": nn.ReLU,
    "relu6": nn.ReLU6,
    "sigmoid": nn.Sigmoid,
    "silu": nn.SiLU,
    "swish": nn.SiLU,
    "tanh": nn.Tanh,
    "prelu": nn.PReLU,
}
ACT2FN = ClassInstantier(ACT2CLS)