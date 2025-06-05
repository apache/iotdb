"""
TimerXL 模型配置，兼容 HuggingFace 格式
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List

from ..model_config import ThuTLModelConfig


@dataclass
class TimerXLConfig(ThuTLModelConfig):
    """TimerXL 模型配置"""

    # 基础配置覆写
    model_type: str = "timer"
    hidden_size: int = 1024
    num_hidden_layers: int = 8
    num_attention_heads: int = 8
    input_token_len: int = 96
    output_token_lens: List[int] = field(default_factory=lambda: [96])

    # TimerXL 专用参数
    attention_dropout: float = 0.0
    rope_theta: int = 10000

    # 前馈网络参数
    intermediate_size: int = 2048
    hidden_act: str = "silu"

    def __post_init__(self):
        """初始化后处理"""
        # 确保 intermediate_size 的合理默认值
        if not hasattr(self, "intermediate_size") or self.intermediate_size is None:
            self.intermediate_size = self.hidden_size * 4
