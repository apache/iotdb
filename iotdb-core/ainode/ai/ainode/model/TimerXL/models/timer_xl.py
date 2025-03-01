import torch
from torch import nn
from ai.ainode.model.TimerXL.layers.Transformer_EncDec import TimerBlock, TimerLayer
from ai.ainode.model.TimerXL.layers.SelfAttention_Family import AttentionLayer, TimeAttention


class Model(nn.Module):
    """
    Timer-XL: Long-Context Transformers for Unified Time Series Forecasting 

    Paper: https://arxiv.org/abs/2410.04803
    
    GitHub: https://github.com/thuml/Timer-XL
    
    Citation: @article{liu2024timer,
        title={Timer-XL: Long-Context Transformers for Unified Time Series Forecasting},
        author={Liu, Yong and Qin, Guo and Huang, Xiangdong and Wang, Jianmin and Long, Mingsheng},
        journal={arXiv preprint arXiv:2410.04803},
        year={2024}
    }
    """
    def __init__(self, configs):
        super().__init__()
        self.input_token_len = configs.input_token_len
        self.embedding = nn.Linear(self.input_token_len, configs.d_model)
        self.output_attention = configs.output_attention
        self.blocks = TimerBlock(
            [
                TimerLayer(
                    AttentionLayer(
                        TimeAttention(True, attention_dropout=configs.dropout,
                                    output_attention=self.output_attention, 
                                    d_model=configs.d_model, num_heads=configs.n_heads,
                                    covariate=configs.covariate, flash_attention=configs.flash_attention),
                                    configs.d_model, configs.n_heads),
                    configs.d_model,
                    configs.d_ff,
                    dropout=configs.dropout,
                    activation=configs.activation
                ) for l in range(configs.e_layers)
            ],
            norm_layer=torch.nn.LayerNorm(configs.d_model)
        )
        self.head = nn.Linear(configs.d_model, configs.output_token_len)
        self.use_norm = configs.use_norm

    def forecast(self, x, x_mark, y_mark):
        if self.use_norm:
            means = x.mean(1, keepdim=True).detach()
            x = x - means
            stdev = torch.sqrt(
                torch.var(x, dim=1, keepdim=True, unbiased=False) + 1e-5)
            x /= stdev
        B, _, C = x.shape
        # [B, C, L]
        x = x.permute(0, 2, 1)
        # [B, C, N, P]
        x = x.unfold(
            dimension=-1, size=self.input_token_len, step=self.input_token_len)
        N = x.shape[2]
        # [B, C, N, D]
        embed_out = self.embedding(x)
        # [B, C * N, D]
        embed_out = embed_out.reshape(B, C * N, -1)
        embed_out, attns = self.blocks(embed_out, n_vars=C, n_tokens=N)
        # [B, C * N, P]
        dec_out = self.head(embed_out)
        # [B, C, N * P]
        dec_out = dec_out.reshape(B, C, N, -1).reshape(B, C, -1)
        # [B, L, C]
        dec_out = dec_out.permute(0, 2, 1)

        if self.use_norm:
            dec_out = dec_out * stdev + means
        if self.output_attention:
            return dec_out, attns
        return dec_out

    def forward(self, x, x_mark, y_mark):
        return self.forecast(x, x_mark, y_mark)
