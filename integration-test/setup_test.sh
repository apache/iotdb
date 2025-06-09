#!/bin/bash
# setup_test_resources.sh

# 创建目录结构
mkdir -p src/test/resources/timerxl-example
mkdir -p src/test/resources/sundial-example  
mkdir -p src/test/resources/legacy-example

# 生成Python脚本并执行
cat > generate_test_models.py << 'EOF'
import torch
import torch.nn as nn
from safetensors.torch import save_file

def create_timerxl_weights():
    weights = {
        "embeddings.weight": torch.randn(1000, 512),
        "layers.0.attention.query.weight": torch.randn(512, 512),
        "layers.0.attention.key.weight": torch.randn(512, 512),
        "layers.0.attention.value.weight": torch.randn(512, 512),
        "layers.0.attention.output.weight": torch.randn(512, 512),
        "layers.0.mlp.gate_proj.weight": torch.randn(2048, 512),
        "layers.0.mlp.up_proj.weight": torch.randn(2048, 512),
        "layers.0.mlp.down_proj.weight": torch.randn(512, 2048),
        "layers.0.input_layernorm.weight": torch.randn(512),
        "prediction_head.weight": torch.randn(96, 512),
    }
    return weights
 
def create_sundial_weights():
    weights = {
        "patch_embed.proj.weight": torch.randn(512, 1, 16),
        "blocks.0.norm1.weight": torch.randn(512),
        "blocks.0.attn.qkv.weight": torch.randn(1536, 512),
        "blocks.0.attn.proj.weight": torch.randn(512, 512),
        "blocks.0.mlp.fc1.weight": torch.randn(2048, 512),
        "blocks.0.mlp.fc2.weight": torch.randn(512, 2048),
        "head.weight": torch.randn(96, 512),
    }
    return weights

class SimpleLegacyModel(nn.Module):
    def __init__(self):
        super().__init__()
        self.linear = nn.Linear(96, 96)
    
    def forward(self, x):
        return self.linear(x)

try:
    # TimerXL
    timerxl_weights = create_timerxl_weights()
    save_file(timerxl_weights, "src/test/resources/timerxl-example/model.safetensors")
    
    # Sundial  
    sundial_weights = create_sundial_weights()
    save_file(sundial_weights, "src/test/resources/sundial-example/model.safetensors")
    
    # Legacy
    model = SimpleLegacyModel()
    traced_model = torch.jit.trace(model, torch.randn(1, 96))
    torch.jit.save(traced_model, "src/test/resources/legacy-example/model.pt")
    
    print("All test model files generated successfully!")
    
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Please install: pip install torch safetensors")
EOF

python generate_test_models.py
rm generate_test_models.py

echo "Test resources setup completed!"