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
    Also sets POETRY_VIRTUALENVS_PATH to force poetry to use our venv.

    Returns:
        dict: Environment variables dictionary
    """
    env = os.environ.copy()
    env["VIRTUAL_ENV"] = str(venv_dir.absolute())

    venv_bin = str(venv_dir / ("Scripts" if sys.platform == "win32" else "bin"))
    env["PATH"] = f"{venv_bin}{os.pathsep}{env.get('PATH', '')}"

    # Force poetry to use our virtual environment by setting POETRY_VIRTUALENVS_PATH
    # This tells poetry where to look for/create virtual environments
    env["POETRY_VIRTUALENVS_PATH"] = str(venv_dir.parent.absolute())

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

    # Configure poetry settings
    print("Configuring poetry settings...")
    try:
        # Set poetry to not create venvs in project directory
        subprocess.run(
            [str(poetry_exe), "config", "virtualenvs.in-project", "false"],
            cwd=str(script_dir),
            env=venv_env,
            check=True,
            capture_output=True,
            text=True,
        )
        # Set poetry virtualenvs path to our venv directory's parent
        # This forces poetry to look for/create venvs in the same location as our venv
        subprocess.run(
            [
                str(poetry_exe),
                "config",
                "virtualenvs.path",
                str(venv_dir.parent.absolute()),
            ],
            cwd=str(script_dir),
            env=venv_env,
            check=True,
            capture_output=True,
            text=True,
        )
        # Ensure poetry can use virtual environments
        subprocess.run(
            [str(poetry_exe), "config", "virtualenvs.create", "true"],
            cwd=str(script_dir),
            env=venv_env,
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception as e:
        print(f"Warning: Failed to configure poetry settings: {e}")
        # Continue anyway, as these may not be critical

    # Remove any existing poetry virtual environments for this project
    # This ensures poetry will use our specified virtual environment
    print("Removing any existing poetry virtual environments...")
    remove_result = subprocess.run(
        [str(poetry_exe), "env", "remove", "--all"],
        cwd=str(script_dir),
        env=venv_env,
        check=False,  # Don't fail if no venv exists
        capture_output=True,
        text=True,
    )
    if remove_result.stdout:
        print(remove_result.stdout.strip())
    if remove_result.stderr:
        stderr = remove_result.stderr.strip()
        # Ignore "No virtualenv has been activated" error
        if "no virtualenv" not in stderr.lower():
            print(remove_result.stderr.strip())

    # Verify the virtual environment Python is valid before configuring poetry
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

    # Instead of using poetry env use (which creates new venvs), we'll use a different approach:
    # 1. Create a symlink from poetry's expected venv location to our venv
    # 2. Or, directly use poetry install with VIRTUAL_ENV set (poetry should detect it)
    #
    # The issue is that poetry env use creates venvs with hash-based names in its cache.
    # We need to work around this by either:
    # - Creating a symlink from poetry's expected location to our venv
    # - Or bypassing poetry env use entirely and using poetry install directly

    # Strategy: Create a symlink from poetry's expected venv location to our venv
    # Poetry creates venvs with names like: <project-name>-<hash>-py<python-version>
    # We need to find out what poetry would name our venv, then create a symlink

    print(f"Configuring poetry to use virtual environment at: {venv_dir}")

    # Get poetry's expected venv name by checking what it would create
    # First, let's try poetry env use, but catch if it tries to create a new venv
    result = subprocess.run(
        [str(poetry_exe), "env", "use", str(venv_python)],
        cwd=str(script_dir),
        env=venv_env,
        check=False,
        capture_output=True,
        text=True,
    )

    output_text = (result.stdout or "") + (result.stderr or "")

    # If poetry is creating a new venv, we need to stop it and use a different approach
    if (
        "Creating virtualenv" in output_text
        or "Creating virtual environment" in output_text
        or "Using virtualenv:" in output_text
    ):
        print("Poetry is attempting to create/use a new virtual environment.")
        print(
            "Stopping this and using alternative approach: creating symlink to our venv..."
        )

        # Extract the venv path poetry is trying to create/use
        # Look for patterns like "Using virtualenv: /path/to/venv" or "Creating virtualenv name in /path"
        import re

        poetry_venv_path = None

        # Try to extract from "Using virtualenv: /path/to/venv"
        using_match = re.search(r"Using virtualenv:\s*([^\s\n]+)", output_text)
        if using_match:
            poetry_venv_path = Path(using_match.group(1))

        # If not found, try to extract from "Creating virtualenv name in /path"
        if not poetry_venv_path:
            creating_match = re.search(
                r"Creating virtualenv[^\n]*in\s+([^\s\n]+)", output_text
            )
            if creating_match:
                venv_dir_path = Path(creating_match.group(1))
                # Extract venv name from the output
                name_match = re.search(r"Creating virtualenv\s+([^\s]+)", output_text)
                if name_match:
                    venv_name = name_match.group(1)
                    poetry_venv_path = venv_dir_path / venv_name

        # If still not found, try to find any path in pypoetry/virtualenvs
        if not poetry_venv_path:
            pypoetry_match = re.search(
                r"([^\s]+pypoetry[^\s]*virtualenvs[^\s]+)", output_text
            )
            if pypoetry_match:
                poetry_venv_path = Path(pypoetry_match.group(1))

        if poetry_venv_path:
            print(f"Poetry wants to create/use venv at: {poetry_venv_path}")

            # Remove the venv poetry just created (if it exists)
            if poetry_venv_path.exists() and poetry_venv_path.is_dir():
                print(f"Removing poetry's newly created venv: {poetry_venv_path}")
                shutil.rmtree(poetry_venv_path, ignore_errors=True)

            # Create a symlink from poetry's expected location to our venv
            print(f"Creating symlink from {poetry_venv_path} to {venv_dir}")
            try:
                if poetry_venv_path.exists() or poetry_venv_path.is_symlink():
                    if poetry_venv_path.is_symlink():
                        poetry_venv_path.unlink()
                    elif poetry_venv_path.is_dir():
                        shutil.rmtree(poetry_venv_path, ignore_errors=True)
                poetry_venv_path.parent.mkdir(parents=True, exist_ok=True)
                poetry_venv_path.symlink_to(venv_dir)
                print(f"Symlink created successfully")
            except Exception as e:
                print(f"WARNING: Failed to create symlink: {e}")
                print("Will try to use poetry install directly with VIRTUAL_ENV set")
        else:
            print("Could not determine poetry's venv path from output")
            print(f"Output was: {output_text}")
    else:
        if result.stdout:
            print(result.stdout.strip())
        if result.stderr:
            stderr = result.stderr.strip()
            if stderr:
                print(f"Poetry output: {stderr}")

    # Verify poetry is using the correct virtual environment BEFORE running lock/install
    # This is critical - if poetry uses the wrong venv, dependencies won't be installed correctly
    print("Verifying poetry virtual environment...")

    # Wait a moment for symlink to be recognized (if we created one)
    import time

    time.sleep(0.5)

    verify_result = subprocess.run(
        [str(poetry_exe), "env", "info", "--path"],
        cwd=str(script_dir),
        env=venv_env,
        check=False,  # Don't fail if poetry hasn't activated a venv yet
        capture_output=True,
        text=True,
    )

    expected_venv_path_resolved = str(Path(venv_dir.absolute()).resolve())

    # If poetry env info fails, it might mean poetry hasn't activated the venv yet
    if verify_result.returncode != 0:
        print(
            "Warning: poetry env info failed, poetry may not have activated the virtual environment yet"
        )
        print(
            "This may be okay if we created a symlink - poetry should use it when running commands"
        )
        poetry_venv_path_resolved = None
    else:
        poetry_venv_path = verify_result.stdout.strip()

        # Normalize paths for comparison (resolve symlinks, etc.)
        poetry_venv_path_resolved = str(Path(poetry_venv_path).resolve())

    # Only verify path if we successfully got poetry's venv path
    if poetry_venv_path_resolved is not None:
        if poetry_venv_path_resolved != expected_venv_path_resolved:
            print(
                f"ERROR: Poetry is using {poetry_venv_path}, but expected {expected_venv_path_resolved}"
            )
            print(
                "Poetry must use the virtual environment we created for the build to work correctly."
            )
            print("The symlink approach may not have worked. Please check the symlink.")
            sys.exit(1)
        else:
            print(f"Poetry is correctly using virtual environment: {poetry_venv_path}")
    else:
        print("Warning: Could not verify poetry virtual environment path")
        print(
            "Continuing anyway - poetry should use the venv via symlink or VIRTUAL_ENV"
        )

    # Update lock file and install dependencies
    # Re-verify environment before each command to ensure poetry doesn't switch venvs
    def verify_poetry_env():
        verify_result = subprocess.run(
            [str(poetry_exe), "env", "info", "--path"],
            cwd=str(script_dir),
            env=venv_env,
            check=False,  # Don't fail if poetry env info is not available
            capture_output=True,
            text=True,
        )
        if verify_result.returncode == 0:
            current_path = str(Path(verify_result.stdout.strip()).resolve())
            expected_path = str(Path(venv_dir.absolute()).resolve())
            if current_path != expected_path:
                print(
                    f"ERROR: Poetry switched to different virtual environment: {current_path}"
                )
                print(f"Expected: {expected_path}")
                sys.exit(1)
        # If poetry env info fails, we can't verify, but continue anyway
        # Poetry should still use the Python we specified via env use
        return True

    print("Running poetry lock...")
    verify_poetry_env()  # Verify before lock
    result = subprocess.run(
        [str(poetry_exe), "lock"],
        cwd=str(script_dir),
        env=venv_env,
        check=True,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    verify_poetry_env()  # Verify after lock

    accelerator = detect_accelerator()
    print(f"Selected accelerator: {accelerator}")

    print("Running poetry install...")
    subprocess.run(
        [str(poetry_exe), "lock"],
        cwd=str(script_dir),
        env=venv_env,
        check=True,
        capture_output=True,
        text=True,
    )
    verify_poetry_env()  # Verify before install
    poetry_install_with_accel(poetry_exe, script_dir, venv_env, accelerator)
    verify_poetry_env()  # Verify after install

    # Verify installation by checking if key packages are installed
    # This is critical - if packages aren't installed, PyInstaller won't find them
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


def build():
    """
    Execute the complete build process.

    Steps:
    1. Setup virtual environment (outside project directory)
    2. Update pip and install 2.2.1 poetry
    3. Install project dependencies (including PyInstaller from pyproject.toml)
    4. Build executable using PyInstaller
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
