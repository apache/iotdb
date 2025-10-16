1# Licensed to the Apache Software Foundation (ASF) under one
1# or more contributor license agreements.  See the NOTICE file
1# distributed with this work for additional information
1# regarding copyright ownership.  The ASF licenses this file
1# to you under the Apache License, Version 2.0 (the
1# "License"); you may not use this file except in compliance
1# with the License.  You may obtain a copy of the License at
1#
1#     http://www.apache.org/licenses/LICENSE-2.0
1#
1# Unless required by applicable law or agreed to in writing,
1# software distributed under the License is distributed on an
1# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1# KIND, either express or implied.  See the License for the
1# specific language governing permissions and limitations
1# under the License.
1#
1
1import math
1
1import torch
1import torch.nn as nn
1
1
1class FlowLoss(nn.Module):
1    """Flow Loss"""
1
1    def __init__(self, target_channels, z_channels, depth, width, num_sampling_steps):
1        super(FlowLoss, self).__init__()
1        self.in_channels = target_channels
1        self.net = SimpleMLPAdaLN(
1            in_channels=target_channels,
1            model_channels=width,
1            out_channels=target_channels,
1            z_channels=z_channels,
1            num_res_blocks=depth,
1        )
1        self.num_sampling_steps = num_sampling_steps
1
1    def forward(self, target, z, mask=None, mask_y=None):
1        noise = torch.randn_like(target)
1        t = torch.rand(target.shape[0], device=target.device)
1
1        noised_target = t[:, None] * target + (1 - t[:, None]) * noise
1
1        predict_v = self.net(noised_target, t * 1000, z)
1
1        weights = 1.0 / torch.arange(
1            1, self.in_channels + 1, dtype=torch.float32, device=target.device
1        )
1        if mask_y is not None:
1            loss = (mask_y * weights * (predict_v - target) ** 2).sum(dim=-1)
1        else:
1            loss = (weights * (predict_v - target) ** 2).sum(dim=-1)
1
1        if mask is not None:
1            loss = (loss * mask).sum() / mask.sum()
1        return loss.mean()
1
1    def sample(self, z, num_samples=1):
1        z = z.repeat(num_samples, 1)
1        noise = torch.randn(z.shape[0], self.in_channels).to(z.device)
1        x = noise
1        dt = 1.0 / self.num_sampling_steps
1        for i in range(self.num_sampling_steps):
1            t = (torch.ones((x.shape[0])) * i / self.num_sampling_steps).to(x.device)
1            pred = self.net(x, t * 1000, z)
1            x = x + (pred - noise) * dt
1        x = x.reshape(num_samples, -1, self.in_channels).transpose(0, 1)
1        return x
1
1
1def modulate(x, shift, scale):
1    return x * (1 + scale) + shift
1
1
1class TimestepEmbedder(nn.Module):
1    """
1    Embeds scalar timesteps into vector representations.
1    """
1
1    def __init__(self, hidden_size, frequency_embedding_size=256):
1        super().__init__()
1        self.mlp = nn.Sequential(
1            nn.Linear(frequency_embedding_size, hidden_size, bias=True),
1            nn.SiLU(),
1            nn.Linear(hidden_size, hidden_size, bias=True),
1        )
1        self.frequency_embedding_size = frequency_embedding_size
1
1    @staticmethod
1    def timestep_embedding(t, dim, max_period=10000):
1        """
1        Create sinusoidal timestep embeddings.
1        :param t: a 1-D Tensor of N indices, one per batch element.
1                          These may be fractional.
1        :param dim: the dimension of the output.
1        :param max_period: controls the minimum frequency of the embeddings.
1        :return: an (N, D) Tensor of positional embeddings.
1        """
1        # https://github.com/openai/glide-text2im/blob/main/glide_text2im/nn.py
1        half = dim // 2
1        freqs = torch.exp(
1            -math.log(max_period)
1            * torch.arange(start=0, end=half, dtype=torch.float32)
1            / half
1        ).to(device=t.device)
1        args = t[:, None].float() * freqs[None]
1        embedding = torch.cat([torch.cos(args), torch.sin(args)], dim=-1)
1        if dim % 2:
1            embedding = torch.cat(
1                [embedding, torch.zeros_like(embedding[:, :1])], dim=-1
1            )
1        return embedding
1
1    def forward(self, t):
1        t_freq = self.timestep_embedding(t, self.frequency_embedding_size)
1        t_emb = self.mlp(t_freq)
1        return t_emb
1
1
1class ResBlock(nn.Module):
1    """
1    A residual block that can optionally change the number of channels.
1    :param channels: the number of input channels.
1    """
1
1    def __init__(self, channels):
1        super().__init__()
1        self.channels = channels
1
1        self.in_ln = nn.LayerNorm(channels, eps=1e-6)
1        self.mlp = nn.Sequential(
1            nn.Linear(channels, channels, bias=True),
1            nn.SiLU(),
1            nn.Linear(channels, channels, bias=True),
1        )
1
1        self.adaLN_modulation = nn.Sequential(
1            nn.SiLU(), nn.Linear(channels, 3 * channels, bias=True)
1        )
1
1    def forward(self, x, y):
1        shift_mlp, scale_mlp, gate_mlp = self.adaLN_modulation(y).chunk(3, dim=-1)
1        h = modulate(self.in_ln(x), shift_mlp, scale_mlp)
1        h = self.mlp(h)
1        return x + gate_mlp * h
1
1
1class FinalLayer(nn.Module):
1    """
1    The final layer adopted from DiT.
1    """
1
1    def __init__(self, model_channels, out_channels):
1        super().__init__()
1        self.norm_final = nn.LayerNorm(
1            model_channels, elementwise_affine=False, eps=1e-6
1        )
1        self.linear = nn.Linear(model_channels, out_channels, bias=True)
1        self.adaLN_modulation = nn.Sequential(
1            nn.SiLU(), nn.Linear(model_channels, 2 * model_channels, bias=True)
1        )
1
1    def forward(self, x, c):
1        shift, scale = self.adaLN_modulation(c).chunk(2, dim=-1)
1        x = modulate(self.norm_final(x), shift, scale)
1        x = self.linear(x)
1        return x
1
1
1class SimpleMLPAdaLN(nn.Module):
1    """
1    The MLP for Diffusion Loss.
1    :param in_channels: channels in the input Tensor.
1    :param model_channels: base channel count for the model.
1    :param out_channels: channels in the output Tensor.
1    :param z_channels: channels in the condition.
1    :param num_res_blocks: number of residual blocks per downsample.
1    """
1
1    def __init__(
1        self,
1        in_channels,
1        model_channels,
1        out_channels,
1        z_channels,
1        num_res_blocks,
1    ):
1        super().__init__()
1
1        self.in_channels = in_channels
1        self.model_channels = model_channels
1        self.out_channels = out_channels
1        self.num_res_blocks = num_res_blocks
1
1        self.time_embed = TimestepEmbedder(model_channels)
1        self.cond_embed = nn.Linear(z_channels, model_channels)
1
1        self.input_proj = nn.Linear(in_channels, model_channels)
1
1        res_blocks = []
1        for i in range(num_res_blocks):
1            res_blocks.append(
1                ResBlock(
1                    model_channels,
1                )
1            )
1
1        self.res_blocks = nn.ModuleList(res_blocks)
1        self.final_layer = FinalLayer(model_channels, out_channels)
1
1        self.initialize_weights()
1
1    def initialize_weights(self):
1        def _basic_init(module):
1            if isinstance(module, nn.Linear):
1                torch.nn.init.xavier_uniform_(module.weight)
1                if module.bias is not None:
1                    nn.init.constant_(module.bias, 0)
1
1        self.apply(_basic_init)
1
1        # Initialize timestep embedding MLP
1        nn.init.normal_(self.time_embed.mlp[0].weight, std=0.02)
1        nn.init.normal_(self.time_embed.mlp[2].weight, std=0.02)
1
1        # Zero-out adaLN modulation layers
1        for block in self.res_blocks:
1            nn.init.constant_(block.adaLN_modulation[-1].weight, 0)
1            nn.init.constant_(block.adaLN_modulation[-1].bias, 0)
1
1        # Zero-out output layers
1        nn.init.constant_(self.final_layer.adaLN_modulation[-1].weight, 0)
1        nn.init.constant_(self.final_layer.adaLN_modulation[-1].bias, 0)
1        nn.init.constant_(self.final_layer.linear.weight, 0)
1        nn.init.constant_(self.final_layer.linear.bias, 0)
1
1    def forward(self, x, t, c):
1        """
1        Apply the model to an input batch.
1        :param x: an [N x C] Tensor of inputs.
1        :param t: a 1-D batch of timesteps.
1        :param c: conditioning from AR transformer.
1        :return: an [N x C] Tensor of outputs.
1        """
1        x = self.input_proj(x)
1        t = self.time_embed(t)
1        c = self.cond_embed(c)
1        y = t + c
1
1        for block in self.res_blocks:
1            x = block(x, y)
1
1        return self.final_layer(x, y)
1