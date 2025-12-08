# -*- mode: python ; coding: utf-8 -*-
#
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
# software distributed under this License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from pathlib import Path

# Get project root directory
project_root = Path(SPECPATH).parent

block_cipher = None

# Auto-collect all submodules of large dependency libraries
# Using collect_all automatically includes all dependencies and avoids manual maintenance of hiddenimports
from PyInstaller.utils.hooks import collect_all, collect_submodules, collect_data_files

# Collect only essential data files and binaries for large libraries
# Using collect_all for all submodules slows down startup significantly.
# However, for certain libraries with many dynamic imports (e.g., torch, transformers, safetensors),
# collect_all is necessary to ensure all required modules are included.
# For other libraries, we use lighter-weight collection methods to improve startup time.
all_datas = []
all_binaries = []
all_hiddenimports = []

# Only collect essential data files and binaries for critical libraries
# This reduces startup time by avoiding unnecessary module imports
essential_libraries = {
    'torch': True,
    'transformers': True,
    'tokenizers': True,
    'huggingface_hub': True,
    'safetensors': True,
    'hf_xet': True,
    'numpy': True,
    'scipy': True,
    'pandas': True,
    'sklearn': True,
    'statsmodels': True,
    'sktime': True,
    'pmdarima': True,
    'hmmlearn': True,
    'accelerate': True
}

for lib in essential_libraries:
    try:
        lib_datas, lib_binaries, lib_hiddenimports = collect_all(lib)
        all_datas.extend(lib_datas)
        all_binaries.extend(lib_binaries)
        all_hiddenimports.extend(lib_hiddenimports)
    except Exception:
        pass


# Helper function to collect submodules with fallback
def collect_submodules_with_fallback(package, fallback_modules=None, package_name=None):
    """
    Collect all submodules for a package, with fallback to manual module list if collection fails.
    
    Args:
        package: Package name to collect submodules from
        fallback_modules: List of module names to add if collection fails (optional)
        package_name: Display name for logging (defaults to package)
    """
    if package_name is None:
        package_name = package
    try:
        submodules = collect_submodules(package)
        all_hiddenimports.extend(submodules)
        print(f"Collected {len(submodules)} submodules from {package_name}")
    except Exception as e:
        print(f"Warning: Failed to collect {package_name} submodules: {e}")
        if fallback_modules:
            all_hiddenimports.extend(fallback_modules)
            print(f"Using fallback modules for {package_name}")


# Packages that need submodule collection due to dynamic imports
# Format: (package_name, fallback_modules_list, display_name)
submodule_collection_configs = [
    # torch._dynamo.polyfills - critical for torch dynamo functionality
    (
        'torch._dynamo.polyfills',
        [
            'torch._dynamo.polyfills',
            'torch._dynamo.polyfills.functools',
            'torch._dynamo.polyfills.operator',
            'torch._dynamo.polyfills.collections',
        ],
        'torch._dynamo.polyfills'
    ),
    # transformers packages with dynamic imports
    ('transformers.generation', None, 'transformers.generation'),
    ('transformers.models.auto', None, 'transformers.models.auto'),
    # scipy.stats - contains many private modules (starting with _)
    (
        'scipy.stats',
        [
            'scipy.stats._variation',
            'scipy.stats._morestats',
            'scipy.stats._stats',
            'scipy.stats._distn_infrastructure',
        ],
        'scipy.stats'
    ),
    # sklearn - has many submodules that may be dynamically imported
    ('sklearn', None, 'sklearn'),
]

# Collect submodules for all configured packages
for package, fallback_modules, display_name in submodule_collection_configs:
    collect_submodules_with_fallback(package, fallback_modules, display_name)

# Project-specific packages that need their submodules collected
# Only list top-level packages - collect_submodules will recursively collect all submodules
project_packages = [
    'iotdb.ainode.core',  # This will include all sub-packages: manager, model, inference, etc.
    'iotdb.thrift',        # This will include all thrift sub-packages
]

# Collect all submodules for project packages automatically
# Using top-level packages avoids duplicate collection
# If collection fails, add the package itself as fallback
for package in project_packages:
    collect_submodules_with_fallback(package, fallback_modules=[package], package_name=package)

# Add parent packages to ensure they are included
all_hiddenimports.extend(['iotdb', 'iotdb.ainode'])

# Multiprocessing support for PyInstaller
# When using multiprocessing with PyInstaller, we need to ensure proper handling
multiprocessing_modules = [
    'multiprocessing',
    'multiprocessing.spawn',
    'multiprocessing.popen_spawn_posix',
    'multiprocessing.popen_spawn_win32',
    'multiprocessing.popen_fork',
    'multiprocessing.popen_forkserver',
    'multiprocessing.context',
    'multiprocessing.reduction',
    'multiprocessing.util',
    'torch.multiprocessing',
    'torch.multiprocessing.spawn',
]

# Additional dependencies that may need explicit import
# These are external libraries that might use dynamic imports
external_dependencies = [
    'einops',
    'dynaconf',
    'tzlocal',
    'thrift',
    'psutil',
    'requests',
]

all_hiddenimports.extend(multiprocessing_modules)
all_hiddenimports.extend(external_dependencies)

# Analyze main entry file
# Note: Do NOT add virtual environment site-packages to pathex manually.
# When PyInstaller is run from the virtual environment's Python, it automatically
# detects and uses the virtual environment's site-packages.
a = Analysis(
    ['iotdb/ainode/core/script.py'],
    pathex=[str(project_root)],
    binaries=all_binaries,
    datas=all_datas,
    hiddenimports=all_hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Exclude unnecessary modules to reduce size and improve startup time
        # Note: Do not exclude unittest, as torch and other libraries require it
        # Only exclude modules that are definitely not used and not required by dependencies
        'matplotlib',
        'IPython',
        'jupyter',
        'notebook',
        'pytest',
        'test',
        'tests'
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=True,  # Set to True to speed up startup - files are not archived into PYZ
)

# Package all PYZ files
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

# Create executable (onedir mode for faster startup)
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='ainode',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

# Collect all files into a directory (onedir mode)
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='ainode',
)