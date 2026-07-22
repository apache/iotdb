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
#


"""
PyInstaller build script (Python version)
"""

import hashlib
import os
import shutil
import subprocess
import sys
from pathlib import Path


def get_venv_base_dir():
    """
    Get the base directory for virtual environments outside the project.

    Returns:
        Path: Base directory path
        - Linux/macOS: ~/.cache/iotdb-ainode-build/
        - Windows: %LOCALAPPDATA%\\iotdb-ainode-build\\
    """
    if sys.platform == "win32":
        localappdata = os.environ.get("LOCALAPPDATA") or os.environ.get(
            "APPDATA", os.path.expanduser("~")
        )
        base_dir = Path(localappdata) / "iotdb-ainode-build"
    else:
        base_dir = Path.home() / ".cache" / "iotdb-ainode-build"

    return base_dir


def setup_venv():
    """
    Create virtual environment outside the project directory.

    The virtual environment is created in a platform-specific location:
    - Linux/macOS: ~/.cache/iotdb-ainode-build/<project-name>/
    - Windows: %LOCALAPPDATA%\\iotdb-ainode-build\\<project-name>\\

    The same venv is reused across multiple builds of the same project.

    Returns:
        Path: Path to the virtual environment directory
    """
    script_dir = Path(__file__).parent
    venv_base_dir = get_venv_base_dir()
    venv_dir = venv_base_dir / script_dir.name

    if venv_dir.exists():
        print(f"Virtual environment already exists at: {venv_dir}")
        return venv_dir

    venv_base_dir.mkdir(parents=True, exist_ok=True)
    print(f"Creating virtual environment at: {venv_dir}")
    subprocess.run([sys.executable, "-m", "venv", str(venv_dir)], check=True)
    print("Virtual environment created successfully")
    return venv_dir


def get_venv_python(venv_dir):
    """Get Python executable path in virtual environment"""
    if sys.platform == "win32":
        return venv_dir / "Scripts" / "python.exe"
    else:
        return venv_dir / "bin" / "python"


def update_pip(venv_python):
    """Update pip in the virtual environment to the latest version."""
    print("Updating pip...")
    subprocess.run(
        [str(venv_python), "-m", "pip", "install", "--upgrade", "pip"], check=True
    )
    print("pip updated successfully")


def install_poetry(venv_python):
    """Install poetry 2.2.1 in the virtual environment."""
    print("Installing poetry 2.2.1...")
    subprocess.run(
        [
            str(venv_python),
            "-m",
            "pip",
            "install",
            "poetry==2.2.1",
        ],
        check=True,
    )
    # Get installed version
    version_result = subprocess.run(
        [str(venv_python), "-m", "poetry", "--version"],
        capture_output=True,
        text=True,
        check=True,
    )
    print(f"Poetry installed: {version_result.stdout.strip()}")


def get_venv_env(venv_dir):
    """
    Get environment variables configured for the virtual environment.

    Sets VIRTUAL_ENV and prepends the venv's bin/Scripts directory to PATH
    so that tools installed in the venv take precedence.

    Returns:
        dict: Environment variables dictionary
    """
    env = os.environ.copy()
    env["VIRTUAL_ENV"] = str(venv_dir.absolute())

    venv_bin = str(venv_dir / ("Scripts" if sys.platform == "win32" else "bin"))
    env["PATH"] = f"{venv_bin}{os.pathsep}{env.get('PATH', '')}"

    return env


def get_poetry_executable(venv_dir):
    """Get poetry executable path in the virtual environment."""
    if sys.platform == "win32":
        return venv_dir / "Scripts" / "poetry.exe"
    else:
        return venv_dir / "bin" / "poetry"


def install_dependencies(venv_python, venv_dir, script_dir):
    """
    Install project dependencies using poetry.

    Configures poetry to use the external virtual environment and installs
    all dependencies from pyproject.toml.
    """
    print("Installing dependencies with poetry...")
    venv_env = get_venv_env(venv_dir)
    poetry_exe = get_poetry_executable(venv_dir)

    # Configure poetry to NOT create its own virtual environments.
    # Poetry will use the already-activated venv via the VIRTUAL_ENV
    # environment variable set in get_venv_env().
    print("Configuring poetry settings...")
    try:
        subprocess.run(
            [str(poetry_exe), "config", "virtualenvs.in-project", "false"],
            cwd=str(script_dir),
            env=venv_env,
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            [str(poetry_exe), "config", "virtualenvs.create", "false"],
            cwd=str(script_dir),
            env=venv_env,
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception as e:
        print(f"Warning: Failed to configure poetry settings: {e}")

    # Verify the virtual environment Python is valid
    print(f"Verifying virtual environment Python at: {venv_python}")
    if not venv_python.exists():
        print(f"ERROR: Virtual environment Python not found at: {venv_python}")
        sys.exit(1)

    python_version_result = subprocess.run(
        [str(venv_python), "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    if python_version_result.returncode != 0:
        print(f"ERROR: Virtual environment Python is not executable: {venv_python}")
        sys.exit(1)
    print(f"  Python version: {python_version_result.stdout.strip()}")

    # Update lock file and install dependencies
    print("Running poetry lock...")
    result = subprocess.run(
        [str(poetry_exe), "lock"],
        cwd=str(script_dir),
        env=venv_env,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        print(f"ERROR: poetry lock failed with exit code {result.returncode}")
        sys.exit(1)

    accelerator = detect_accelerator()
    print(f"Selected accelerator: {accelerator}")

    print("Running poetry install...")
    subprocess.run(
        [str(poetry_exe), "install", "--no-root"],
        cwd=str(script_dir),
        env=venv_env,
        check=True,
        text=True,
    )
    poetry_install_with_accel(poetry_exe, script_dir, venv_env, accelerator)

    # Verify installation by checking if key packages are installed
    print("Verifying package installation...")
    test_packages = ["torch", "transformers", "tokenizers"]
    missing_packages = []
    for package in test_packages:
        test_result = subprocess.run(
            [str(venv_python), "-c", f"import {package}; print({package}.__version__)"],
            capture_output=True,
            text=True,
            check=False,
        )
        if test_result.returncode == 0:
            version = test_result.stdout.strip()
            print(f"{package} {version} installed")
        else:
            error_msg = (
                test_result.stderr.strip() if test_result.stderr else "Unknown error"
            )
            print(f"{package} NOT found in virtual environment: {error_msg}")
            missing_packages.append(package)

    if missing_packages:
        print(
            f"\nERROR: Required packages are missing from virtual environment: {', '.join(missing_packages)}"
        )
        print("This indicates that poetry did not install dependencies correctly.")
        print("Please check the poetry install output above for errors.")
        sys.exit(1)

    print("Dependencies installed successfully")


def check_pyinstaller(venv_python):
    """
    Check if PyInstaller is installed.

    PyInstaller should be installed via poetry install from pyproject.toml.
    If it's missing, it means poetry install failed or didn't complete.
    """
    try:
        result = subprocess.run(
            [
                str(venv_python),
                "-c",
                "import PyInstaller; print(PyInstaller.__version__)",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        version = result.stdout.strip()
        print(f"PyInstaller version: {version}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("ERROR: PyInstaller is not installed in the virtual environment")
        print("PyInstaller should be installed via poetry install from pyproject.toml")
        print(
            "This indicates that poetry install may have failed or didn't complete correctly."
        )
        return False


def detect_accelerator():
    """Auto-detect accelerator: prefer NPU if available, else CUDA GPU, otherwise CPU."""

    # Try NVIDIA CUDA detection
    try:
        cuda_result = subprocess.run(
            ["nvidia-smi", "-L"], capture_output=True, text=True, check=False
        )
        if cuda_result.returncode == 0 and "GPU" in cuda_result.stdout:
            return "cuda"
    except FileNotFoundError:
        pass

    return "cpu"


def poetry_install_with_accel(poetry_exe, script_dir, venv_env, accelerator):
    """Run poetry install selecting dependency groups: cuda(default), npu."""
    cmd = [str(poetry_exe), "install"]
    print(f"Running poetry install for accelerator={accelerator} -> {' '.join(cmd)}")
    subprocess.run(
        cmd,
        cwd=str(script_dir),
        env=venv_env,
        check=True,
        capture_output=True,
        text=True,
    )


def compute_source_hash(script_dir):
    """
    Compute a SHA256 hash over all files that affect the PyInstaller output.

    Includes Python source files, the spec file, pyproject.toml, and poetry.lock.
    """
    hasher = hashlib.sha256()

    # Include Python version so cache invalidates on interpreter upgrades
    hasher.update(sys.version.encode())

    hash_targets = []

    excluded_dirs = {"build", "dist", "__pycache__"}

    for pattern in ("**/*.py", "**/*.spec"):
        for f in script_dir.glob(pattern):
            if not any(
                part in excluded_dirs for part in f.relative_to(script_dir).parts
            ):
                hash_targets.append(f)

    for name in ("pyproject.toml", "poetry.lock"):
        f = script_dir / name
        if f.exists():
            hash_targets.append(f)

    # Also include the thrift/client-py sources that get copied in
    client_py_dir = script_dir.parent.parent / "iotdb-client" / "client-py" / "iotdb"
    if client_py_dir.is_dir():
        hash_targets.extend(client_py_dir.rglob("*.py"))

    hash_targets.sort(key=lambda p: str(p))

    for f in hash_targets:
        try:
            rel = f.relative_to(script_dir)
        except ValueError:
            rel = f
        hasher.update(str(rel).encode())
        hasher.update(f.read_bytes())

    return hasher.hexdigest()


def get_dist_cache_dir():
    """Get the directory used to cache PyInstaller dist output."""
    return get_venv_base_dir() / "dist-cache"


def try_restore_dist_cache(script_dir):
    """
    Try to restore the dist/ directory from cache.

    Returns True if cache hit, False otherwise.
    """
    source_hash = compute_source_hash(script_dir)
    cache_dir = get_dist_cache_dir()
    hash_file = cache_dir / "source_hash"
    cached_dist = cache_dir / "ainode"
    dist_dir = script_dir / "dist" / "ainode"

    print(f"Source hash: {source_hash}")

    if hash_file.exists() and cached_dist.is_dir():
        cached_hash = hash_file.read_text().strip()
        if cached_hash == source_hash:
            print("Cache hit — restoring dist/ from cache, skipping PyInstaller build")
            dist_dir.parent.mkdir(parents=True, exist_ok=True)
            if dist_dir.exists():
                shutil.rmtree(dist_dir)
            shutil.copytree(cached_dist, dist_dir, symlinks=True)
            return True
        else:
            print("Cache miss — source hash changed, will rebuild")
    else:
        print("No dist cache found, will build from scratch")

    return False


def save_dist_cache(script_dir):
    """Save the dist/ directory to cache after a successful build."""
    source_hash = compute_source_hash(script_dir)
    cache_dir = get_dist_cache_dir()
    cached_dist = cache_dir / "ainode"
    dist_dir = script_dir / "dist" / "ainode"

    if not dist_dir.is_dir():
        print("Warning: dist/ainode not found, skipping cache save")
        return

    cache_dir.mkdir(parents=True, exist_ok=True)
    if cached_dist.exists():
        shutil.rmtree(cached_dist)
    shutil.copytree(dist_dir, cached_dist, symlinks=True)
    (cache_dir / "source_hash").write_text(source_hash)
    print(f"Saved dist cache (hash: {source_hash})")


def build():
    """
    Execute the complete build process.

    Steps:
    1. Setup virtual environment (outside project directory)
    2. Update pip and install 2.2.1 poetry
    3. Install project dependencies (including PyInstaller from pyproject.toml)
    4. Check dist cache — skip PyInstaller if source hasn't changed
    5. Build executable using PyInstaller (if cache miss)
    6. Save dist to cache
    """
    script_dir = Path(__file__).parent

    venv_dir = setup_venv()
    venv_python = get_venv_python(venv_dir)

    update_pip(venv_python)
    install_poetry(venv_python)
    install_dependencies(venv_python, venv_dir, script_dir)

    if not check_pyinstaller(venv_python):
        sys.exit(1)

    print("=" * 50)
    print("IoTDB AINode PyInstaller Build Script")
    print("=" * 50)
    print()

    if try_restore_dist_cache(script_dir):
        print()
        print("=" * 50)
        print("Build completed (from cache)!")
        print("=" * 50)
        return

    print("Starting build...")
    print()

    spec_file = script_dir / "ainode.spec"
    if not spec_file.exists():
        print(f"Error: Spec file not found: {spec_file}")
        sys.exit(1)

    # Set up environment for PyInstaller
    # When using venv_python, PyInstaller should automatically detect the virtual environment
    # and use its site-packages. We should NOT manually add site-packages to pathex.
    pyinstaller_env = get_venv_env(venv_dir)

    # Verify we're using the correct Python
    python_prefix_result = subprocess.run(
        [str(venv_python), "-c", "import sys; print(sys.prefix)"],
        capture_output=True,
        text=True,
        check=True,
    )
    python_prefix = python_prefix_result.stdout.strip()
    print(f"Using Python from: {python_prefix}")

    # Ensure PyInstaller runs from the virtual environment
    # The venv_python should automatically set up the correct environment
    cmd = [
        str(venv_python),
        "-m",
        "PyInstaller",
        "--noconfirm",
        str(spec_file),
    ]

    try:
        subprocess.run(cmd, check=True, env=pyinstaller_env)
    except subprocess.CalledProcessError as e:
        print(f"\nError: Build failed: {e}")
        sys.exit(1)

    save_dist_cache(script_dir)

    print()
    print("=" * 50)
    print("Build completed!")
    print("=" * 50)
    print()
    print("Executable location: dist/ainode/ainode")
    print()
    print("Usage:")
    print("  ./dist/ainode/ainode start   # Start AINode")
    print()


if __name__ == "__main__":
    build()
