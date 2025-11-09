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
    'torch': True,  # Keep collect_all for torch as it has many dynamic imports
    'transformers': True,  # Keep collect_all for transformers
    'safetensors': True,  # Keep collect_all for safetensors
}

# For other libraries, use selective collection to speed up startup
other_libraries = ['sktime', 'scipy', 'pandas', 'sklearn', 'statsmodels', 'optuna']

for lib in essential_libraries:
    try:
        lib_datas, lib_binaries, lib_hiddenimports = collect_all(lib)
        all_datas.extend(lib_datas)
        all_binaries.extend(lib_binaries)
        all_hiddenimports.extend(lib_hiddenimports)
    except Exception:
        pass

# For other libraries, only collect submodules (lighter weight)
# This relies on PyInstaller's dependency analysis to include what's actually used
for lib in other_libraries:
    try:
        submodules = collect_submodules(lib)
        all_hiddenimports.extend(submodules)
        # Only collect essential data files and binaries, not all submodules
        # This significantly reduces startup time
        try:
            lib_datas, lib_binaries, _ = collect_all(lib)
            all_datas.extend(lib_datas)
            all_binaries.extend(lib_binaries)
        except Exception:
            # If collect_all fails, try collect_data_files for essential data only
            try:
                lib_datas = collect_data_files(lib)
                all_datas.extend(lib_datas)
            except Exception:
                pass
    except Exception:
        pass

# Project-specific packages that need their submodules collected
# Only list top-level packages - collect_submodules will recursively collect all submodules
TOP_LEVEL_PACKAGES = [
    'iotdb.ainode.core',  # This will include all sub-packages: manager, model, inference, etc.
    'iotdb.thrift',        # This will include all thrift sub-packages
]

# Collect all submodules for project packages automatically
# Using top-level packages avoids duplicate collection
for package in TOP_LEVEL_PACKAGES:
    try:
        submodules = collect_submodules(package)
        all_hiddenimports.extend(submodules)
    except Exception:
        # If package doesn't exist or collection fails, add the package itself
        all_hiddenimports.append(package)

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
    'huggingface_hub',
    'tokenizers',
    'hf_xet',
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