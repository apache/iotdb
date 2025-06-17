# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pathlib import Path
from typing import Dict, Iterator, Tuple, Union, Optional

import torch

from ainode.core.log import Logger
from ainode.core.constant import WEIGHT_FORMAT_PRIORITY  # 使用constant中定义的优先级

logger = Logger()


def load_weights_as_state_dict(
    weights_path: Union[str, Path]
) -> Dict[str, torch.Tensor]:
    """
    加载权重文件并返回state_dict格式
    优先级：.safetensors > .pt > .pth > .bin

    Args:
        weights_path: 权重文件路径

    Returns:
        权重字典 {key: tensor}
    """
    weights_path = Path(weights_path)

    if weights_path.is_file():
        return _load_single_weight_file(weights_path)
    elif weights_path.is_dir():
        return _load_weights_from_directory(weights_path)
    else:
        raise FileNotFoundError(f"权重路径不存在: {weights_path}")


def load_weights_for_from_pretrained(
    model_dir: Union[str, Path], 
    weight_filename: Optional[str] = None
) -> Dict[str, torch.Tensor]:
    """
    专门为from_pretrained方法加载权重
    
    Args:
        model_dir: 模型目录
        weight_filename: 指定权重文件名，如果为None则自动检测
        
    Returns:
        权重字典
    """
    model_dir = Path(model_dir)
    
    if weight_filename:
        # 使用指定的权重文件
        weight_path = model_dir / weight_filename
        if weight_path.exists():
            return _load_single_weight_file(weight_path)
        else:
            raise FileNotFoundError(f"指定的权重文件不存在: {weight_path}")
    else:
        # 自动检测权重文件
        return _load_weights_from_directory(model_dir)


def iter_weights(weights_path: Union[str, Path]) -> Iterator[Tuple[str, torch.Tensor]]:
    """
    迭代器方式加载权重，节省内存

    Args:
        weights_path: 权重路径

    Yields:
        (key, tensor) 元组
    """
    weights = load_weights_as_state_dict(weights_path)
    for key, tensor in weights.items():
        yield key, tensor


def _load_single_weight_file(file_path: Path) -> Dict[str, torch.Tensor]:
    """加载单个权重文件"""
    suffix = file_path.suffix.lower()

    logger.debug(f"加载权重文件: {file_path}")

    if suffix == ".safetensors":
        return _load_safetensors_file(file_path)
    elif suffix in [".pt", ".pth", ".bin"]:
        return _load_pytorch_file(file_path)
    else:
        raise ValueError(f"不支持的权重文件格式: {suffix}")


def _load_weights_from_directory(dir_path: Path) -> Dict[str, torch.Tensor]:
    """从目录加载权重，按优先级查找"""
    # 使用constant中定义的优先级
    priority_patterns = WEIGHT_FORMAT_PRIORITY

    # 查找单一文件
    for pattern in priority_patterns:
        file_path = dir_path / pattern
        if file_path.exists():
            logger.debug(f"找到权重文件: {file_path}")
            return _load_single_weight_file(file_path)

    # 查找分片文件
    safetensor_files = list(dir_path.glob("*.safetensors"))
    pytorch_files = list(dir_path.glob("*.pt")) + list(dir_path.glob("*.pth"))

    if safetensor_files:
        logger.info(f"找到 {len(safetensor_files)} 个SafeTensors分片文件")
        return _load_sharded_safetensors(safetensor_files)
    elif pytorch_files:
        logger.info(f"找到 {len(pytorch_files)} 个PyTorch分片文件")
        return _load_sharded_pytorch(pytorch_files)

    raise FileNotFoundError(f"在目录 {dir_path} 中找不到权重文件")


def _load_safetensors_file(file_path: Path) -> Dict[str, torch.Tensor]:
    """加载SafeTensors格式文件"""
    try:
        from safetensors import safe_open
    except ImportError:
        logger.warning("safetensors未安装，尝试使用PyTorch格式")
        # 尝试查找对应的.pt文件
        pt_path = file_path.with_suffix(".pt")
        if pt_path.exists():
            logger.info(f"Fallback到PyTorch格式: {pt_path}")
            return _load_pytorch_file(pt_path)
        
        # 尝试查找其他PyTorch格式文件
        for suffix in [".pth", ".bin"]:
            fallback_path = file_path.with_suffix(suffix)
            if fallback_path.exists():
                logger.info(f"Fallback到PyTorch格式: {fallback_path}")
                return _load_pytorch_file(fallback_path)
        
        raise ImportError("需要安装safetensors: pip install safetensors")

    weights = {}
    try:
        with safe_open(file_path, framework="pt", device="cpu") as f:
            for key in f.keys():
                weights[key] = f.get_tensor(key)
    except Exception as e:
        logger.error(f"SafeTensors文件损坏或格式错误: {e}")
        raise

    logger.debug(f"SafeTensors加载完成: {len(weights)} 个参数")
    return weights


def _load_pytorch_file(file_path: Path) -> Dict[str, torch.Tensor]:
    """加载PyTorch格式文件"""
    try:
        weights = torch.load(file_path, map_location="cpu")
    except Exception as e:
        logger.error(f"PyTorch文件加载失败: {e}")
        raise

    # 处理不同的权重结构
    if isinstance(weights, dict):
        if "state_dict" in weights:
            weights = weights["state_dict"]
        elif "model" in weights:
            weights = weights["model"]
        elif "model_state_dict" in weights:
            weights = weights["model_state_dict"]
    
    # 验证权重格式
    if not isinstance(weights, dict):
        raise ValueError(f"权重文件格式不正确，期望dict，得到: {type(weights)}")

    logger.debug(f"PyTorch权重加载完成: {len(weights)} 个参数")
    return weights


def _load_sharded_safetensors(file_list: list) -> Dict[str, torch.Tensor]:
    """加载分片的SafeTensors文件"""
    try:
        from safetensors import safe_open
    except ImportError:
        logger.error("分片SafeTensors文件需要安装safetensors库")
        raise ImportError("需要安装safetensors: pip install safetensors")

    weights = {}
    for file_path in sorted(file_list):
        try:
            with safe_open(file_path, framework="pt", device="cpu") as f:
                for key in f.keys():
                    if key in weights:
                        logger.warning(f"权重键冲突，覆盖: {key}")
                    weights[key] = f.get_tensor(key)
        except Exception as e:
            logger.error(f"加载分片文件失败: {file_path}, 错误: {e}")
            raise

    logger.debug(
        f"分片SafeTensors加载完成: {len(file_list)} 个文件, {len(weights)} 个参数"
    )
    return weights


def _load_sharded_pytorch(file_list: list) -> Dict[str, torch.Tensor]:
    """加载分片的PyTorch文件"""
    weights = {}
    for file_path in sorted(file_list):
        try:
            shard_weights = torch.load(file_path, map_location="cpu")
            if isinstance(shard_weights, dict):
                # 处理键冲突
                for key, value in shard_weights.items():
                    if key in weights:
                        logger.warning(f"权重键冲突，覆盖: {key}")
                    weights[key] = value
        except Exception as e:
            logger.error(f"加载分片文件失败: {file_path}, 错误: {e}")
            raise

    logger.debug(
        f"分片PyTorch权重加载完成: {len(file_list)} 个文件, {len(weights)} 个参数"
    )
    return weights


def get_available_weight_files(model_dir: Union[str, Path]) -> Dict[str, list]:
    """
    获取目录中可用的权重文件
    
    Args:
        model_dir: 模型目录
        
    Returns:
        {"single": [...], "sharded_safetensors": [...], "sharded_pytorch": [...]}
    """
    model_dir = Path(model_dir)
    result = {
        "single": [],
        "sharded_safetensors": [],
        "sharded_pytorch": []
    }
    
    # 查找单一文件
    for pattern in WEIGHT_FORMAT_PRIORITY:
        file_path = model_dir / pattern
        if file_path.exists():
            result["single"].append(str(file_path))
    
    # 查找分片文件
    result["sharded_safetensors"] = [str(p) for p in model_dir.glob("*.safetensors") 
                                   if p.name not in WEIGHT_FORMAT_PRIORITY]
    result["sharded_pytorch"] = [str(p) for p in model_dir.glob("*.pt") 
                               if p.name not in WEIGHT_FORMAT_PRIORITY]
    result["sharded_pytorch"].extend([str(p) for p in model_dir.glob("*.pth")])
    
    return result