from typing import List

class TimerxlConfig:
    model_type = "timerxl"
    # keys_to_ignore_at_inference = ["past_key_values"]

    def __init__(
        self,
        input_token_len: int = 96,
        hidden_size: int = 1024,
        intermediate_size: int = 2048,
        output_token_lens: List[int] = [96],
        num_hidden_layers: int = 8,
        num_attention_heads: int = 8,
        hidden_act: str = "silu",
        use_cache: bool = True,
        rope_theta: int = 10000,
        attention_dropout: float = 0.0,
        initializer_range: float = 0.02,
        max_position_embeddings: int = 10000,
        ckpt_path: str = None,
        **kwargs,
    ):
        self.input_token_len = input_token_len
        self.hidden_size = hidden_size
        self.intermediate_size = intermediate_size
        self.num_hidden_layers = num_hidden_layers
        self.num_attention_heads = num_attention_heads
        self.hidden_act = hidden_act
        self.output_token_lens = output_token_lens
        self.use_cache = use_cache
        self.rope_theta = rope_theta
        self.attention_dropout = attention_dropout
        self.initializer_range = initializer_range
        self.max_position_embeddings = max_position_embeddings
        self.ckpt_path = ckpt_path

        super().__init__(
            **kwargs,
        )
        
    @classmethod
    def from_dict(cls, config_dict: dict) -> "TimerxlConfig":
        return cls(**config_dict)
