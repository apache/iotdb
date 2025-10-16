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
1"""
1copied from transformers.cache_utils.py(transformers==4.40.1)
1"""
1
1from dataclasses import dataclass
1from typing import Any, Dict, List, Optional, Tuple
1
1import torch
1import torch.nn as nn
1
1
1class Cache:
1    """
1    Base, abstract class for all caches. The actual data structure is specific to each subclass.
1    """
1
1    # def __init__(self):
1    #     # to avoid torch.jit.script error
1    #     super().__init__()
1    #     self._seen_tokens = 0
1
1    def update(
1        self,
1        key_states: torch.Tensor,
1        value_states: torch.Tensor,
1        layer_idx: int,
1        cache_kwargs: Optional[Dict[str, Any]] = None,
1    ) -> Tuple[torch.Tensor, torch.Tensor]:
1        """
1        Updates the cache with the new `key_states` and `value_states` for the layer `layer_idx`.
1
1        Parameters:
1            key_states (`torch.Tensor`):
1                The new key states to cache.
1            value_states (`torch.Tensor`):
1                The new value states to cache.
1            layer_idx (`int`):
1                The index of the layer to cache the states for.
1            cache_kwargs (`Dict[str, Any]`, `optional`):
1                Additional arguments for the cache subclass. These are specific to each subclass and allow new types of
1                cache to be created.
1
1        Return:
1            A tuple containing the updated key and value states.
1        """
1        raise NotImplementedError("Make sure to implement `update` in a subclass.")
1
1    def get_seq_length(self, layer_idx: Optional[int] = 0) -> int:
1        """Returns the sequence length of the cached states. A layer index can be optionally passed."""
1        raise NotImplementedError(
1            "Make sure to implement `get_seq_length` in a subclass."
1        )
1
1    def get_max_length(self) -> Optional[int]:
1        """Returns the maximum sequence length of the cached states, if there is any."""
1        raise NotImplementedError(
1            "Make sure to implement `get_max_length` in a subclass."
1        )
1
1    def get_usable_length(
1        self, new_seq_length: int, layer_idx: Optional[int] = 0
1    ) -> int:
1        """Given the sequence length of the new inputs, returns the usable length of the cache."""
1        # Cache without size limit -> all cache is usable
1        # Cache with size limit -> if the length cache plus the length of the new inputs is larger the maximum cache
1        #   length, we will need to evict part of the cache (and thus not all cache is usable)
1        max_length = self.get_max_length()
1        previous_seq_length = self.get_seq_length(layer_idx)
1        if max_length is not None and previous_seq_length + new_seq_length > max_length:
1            return max_length - new_seq_length
1        return previous_seq_length
1
1    @property
1    def seen_tokens(self):
1        if hasattr(self, "_seen_tokens"):
1            return self._seen_tokens
1        else:
1            return None
1
1
1class DynamicCache(Cache):
1    """
1    A cache that grows dynamically as more tokens are generated. This is the default for generative models.
1
1    It stores the Key and Value states as a list of tensors, one for each layer. The expected shape for each tensor is
1    `[batch_size, num_heads, seq_len, head_dim]`.
1    """
1
1    def __init__(self) -> None:
1        self.key_cache: List[torch.Tensor] = []
1        self.value_cache: List[torch.Tensor] = []
1        self._seen_tokens = (
1            0  # Used in `generate` to keep tally of how many tokens the cache has seen
1        )
1
1    def __getitem__(self, layer_idx: int) -> List[Tuple[torch.Tensor]]:
1        """
1        Support for backwards-compatible `past_key_value` indexing, e.g. `past_key_value[0][0].shape[2]` to get the
1        sequence length.
1        """
1        if layer_idx < len(self):
1            return (self.key_cache[layer_idx], self.value_cache[layer_idx])
1        else:
1            raise KeyError(
1                f"Cache only has {len(self)} layers, attempted to access layer with index {layer_idx}"
1            )
1
1    # def __iter__(self):
1    #     """
1    #     Support for backwards-compatible `past_key_value` iteration, e.g. `for x in past_key_value:` to iterate over
1    #     keys and values
1    #     """
1    #     for layer_idx in range(len(self)):
1    #         yield (self.key_cache[layer_idx], self.value_cache[layer_idx])
1
1    def __len__(self):
1        """
1        Support for backwards-compatible `past_key_value` length, e.g. `len(past_key_value)`. This value corresponds
1        to the number of layers in the model.
1        """
1        return len(self.key_cache)
1
1    def update(
1        self,
1        key_states: torch.Tensor,
1        value_states: torch.Tensor,
1        layer_idx: int,
1    ) -> Tuple[torch.Tensor, torch.Tensor]:
1        """
1        Updates the cache with the new `key_states` and `value_states` for the layer `layer_idx`.
1
1        Parameters:
1            key_states (`torch.Tensor`):
1                The new key states to cache.
1            value_states (`torch.Tensor`):
1                The new value states to cache.
1            layer_idx (`int`):
1                The index of the layer to cache the states for.
1            cache_kwargs (`Dict[str, Any]`, `optional`):
1                Additional arguments for the cache subclass. No additional arguments are used in `DynamicCache`.
1
1        Return:
1            A tuple containing the updated key and value states.
1        """
1        # Update the number of seen tokens
1        if layer_idx == 0:
1            self._seen_tokens += key_states.shape[-2]
1
1        # Update the cache
1        if len(self.key_cache) <= layer_idx:
1            self.key_cache.append(key_states)
1            self.value_cache.append(value_states)
1        else:
1            self.key_cache[layer_idx] = torch.cat(
1                [self.key_cache[layer_idx], key_states], dim=-2
1            )
1            self.value_cache[layer_idx] = torch.cat(
1                [self.value_cache[layer_idx], value_states], dim=-2
1            )
1
1        return self.key_cache[layer_idx], self.value_cache[layer_idx]
1
1    def get_seq_length(self, layer_idx: int = 0) -> int:
1        """Returns the sequence length of the cached states. A layer index can be optionally passed."""
1        if len(self.key_cache) <= layer_idx:
1            return 0
1        return self.key_cache[layer_idx].shape[-2]
1
1    def get_max_length(self) -> Optional[int]:
1        """Returns the maximum sequence length of the cached states. DynamicCache does not have a maximum length."""
1        return None
1
1    def reorder_cache(self, beam_idx: torch.LongTensor):
1        """Reorders the cache for beam search, given the selected beam indices."""
1        for layer_idx in range(len(self.key_cache)):
1            device = self.key_cache[layer_idx].device
1            self.key_cache[layer_idx] = self.key_cache[layer_idx].index_select(
1                0, beam_idx.to(device)
1            )
1            device = self.value_cache[layer_idx].device
1            self.value_cache[layer_idx] = self.value_cache[layer_idx].index_select(
1                0, beam_idx.to(device)
1            )
1
1    def to_legacy_cache(self) -> Tuple[Tuple[torch.Tensor], Tuple[torch.Tensor]]:
1        """Converts the `DynamicCache` instance into the its equivalent in the legacy cache format."""
1        legacy_cache = ()
1        for layer_idx in range(len(self)):
1            legacy_cache += ((self.key_cache[layer_idx], self.value_cache[layer_idx]),)
1        return legacy_cache
1
1    def init_data(
1        self, past_key_values: Optional[List[Tuple[torch.Tensor, torch.Tensor]]] = None
1    ):
1        if past_key_values is not None:
1            for layer_idx in range(len(past_key_values)):
1                key_states, value_states = past_key_values[layer_idx]
1                self.update(key_states, value_states, layer_idx)
1
1    @classmethod
1    def from_legacy_cache(
1        cls, past_key_values: Optional[Tuple[Tuple[torch.FloatTensor]]] = None
1    ) -> "DynamicCache":
1        """Converts a cache in the legacy cache format into an equivalent `DynamicCache`."""
1        cache = cls()
1        if past_key_values is not None:
1            for layer_idx in range(len(past_key_values)):
1                key_states, value_states = past_key_values[layer_idx]
1                cache.update(key_states, value_states, layer_idx)
1        return cache
1