#!/usr/bin/env python3
# test_timesfm_debug.py

import os
import sys
import inspect
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 模拟环境
class MockLogger:
    def __init__(self, name=None): self.name = name
    def debug(self, msg, *args, **kwargs): print(f"DEBUG: {msg}")
    def info(self, msg, *args, **kwargs): print(f"INFO: {msg}")
    def warning(self, msg, *args, **kwargs): print(f"WARNING: {msg}")
    def error(self, msg, *args, **kwargs): print(f"ERROR: {msg}")

class MockAINodeConfig:
    def get_ain_models_dir(self): return "data/ainode/models"
    def get_ain_builtin_models_dir(self): return "data/ainode/models/weights"

class MockAINodeDescriptor:
    def get_config(self): return MockAINodeConfig()

os.environ['AINODE_TEST'] = 'true'
sys.modules['ainode.core.log'] = type('MockLogModule', (), {'Logger': MockLogger})()
sys.modules['ainode.core.config'] = type('MockConfigModule', (), {'AINodeDescriptor': MockAINodeDescriptor})()

# 导入模块
try:
    from ainode.core.model.timesfm.configuration_timesfm import TimesFmConfig
    from ainode.core.model.timesfm.modeling_timesfm import TimesFmForPrediction
    print("✓ Successfully imported TimesFM modules")
except ImportError as e:
    print(f"✗ Import error: {e}")
    sys.exit(1)

def debug_model_interface():
    """调试模型接口"""
    print("🔍 Debugging TimesFM Model Interface")
    print("=" * 50)
    
    # 创建配置和模型
    config = TimesFmConfig(
        patch_length=16,
        context_length=128,
        horizon_length=32,
        hidden_size=64,
        num_hidden_layers=1,
        num_attention_heads=2,
        head_dim=32,
        freq_size=3
    )
    
    model = TimesFmForPrediction(config)
    
    # 检查模型类层次结构
    print(f"📋 Model Class Hierarchy:")
    mro = TimesFmForPrediction.__mro__
    for i, cls in enumerate(mro):
        print(f"  {i}: {cls.__name__} ({cls.__module__})")
    
    # 检查forward方法
    print(f"\n🔍 Forward Method Analysis:")
    if hasattr(model, 'forward'):
        forward_method = getattr(model, 'forward')
        print(f"  - Has forward method: ✓")
        print(f"  - Forward method type: {type(forward_method)}")
        print(f"  - Forward method: {forward_method}")
        
        # 获取方法签名
        try:
            sig = inspect.signature(forward_method)
            print(f"  - Method signature: {sig}")
            print(f"  - Parameters:")
            for name, param in sig.parameters.items():
                print(f"    - {name}: {param}")
        except Exception as e:
            print(f"  - Could not get signature: {e}")
    else:
        print(f"  - Has forward method: ✗")
    
    # 检查所有可用方法
    print(f"\n📝 Available Methods:")
    methods = [method for method in dir(model) if callable(getattr(model, method)) and not method.startswith('_')]
    for method in sorted(methods):
        print(f"  - {method}")
    
    # 检查特殊方法
    print(f"\n🔧 Special Methods:")
    special_methods = [method for method in dir(model) if method.startswith('_') and callable(getattr(model, method))]
    important_methods = ['__call__', '_forward_unimplemented', 'forward']
    for method in important_methods:
        if method in special_methods:
            try:
                method_obj = getattr(model, method)
                sig = inspect.signature(method_obj)
                print(f"  - {method}: {sig}")
            except:
                print(f"  - {method}: present but can't get signature")
        else:
            print(f"  - {method}: not found")
    
    # 尝试不同的调用方式
    print(f"\n🧪 Testing Different Call Methods:")
    import torch
    
    # 准备测试数据
    test_data = [torch.randn(128) for _ in range(2)]
    
    # 测试1: 直接调用模型 (使用__call__)
    try:
        print("  1. Testing model() call...")
        with torch.no_grad():
            output = model(test_data)
        print(f"     ✓ model() works, output type: {type(output)}")
    except Exception as e:
        print(f"     ✗ model() failed: {e}")
    
    # 测试2: 使用past_values参数
    try:
        print("  2. Testing model(past_values=...)...")
        with torch.no_grad():
            output = model(past_values=test_data)
        print(f"     ✓ model(past_values=...) works, output type: {type(output)}")
    except Exception as e:
        print(f"     ✗ model(past_values=...) failed: {e}")
    
    # 测试3: 检查是否有predict方法
    if hasattr(model, 'predict'):
        try:
            print("  3. Testing model.predict()...")
            with torch.no_grad():
                output = model.predict(test_data)
            print(f"     ✓ model.predict() works, output type: {type(output)}")
        except Exception as e:
            print(f"     ✗ model.predict() failed: {e}")
    else:
        print("  3. model.predict() not available")
    
    # 测试4: 检查是否有其他预测方法
    prediction_methods = ['predict_step', 'generate', 'forecast']
    for method_name in prediction_methods:
        if hasattr(model, method_name):
            try:
                print(f"  4. Testing model.{method_name}()...")
                method = getattr(model, method_name)
                with torch.no_grad():
                    output = method(test_data)
                print(f"     ✓ model.{method_name}() works, output type: {type(output)}")
            except Exception as e:
                print(f"     ✗ model.{method_name}() failed: {e}")
    
    print(f"\n✅ Debug analysis complete!")

if __name__ == "__main__":
    debug_model_interface()