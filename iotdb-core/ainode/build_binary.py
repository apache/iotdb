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
        localappdata = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA", os.path.expanduser("~"))
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
    """Install poetry 2.1.2 in the virtual environment."""
    print("Installing poetry 2.1.2...")
    subprocess.run(
        [
            str(venv_python),
            "-m",
            "pip",
            "install",
            "poetry==2.1.2",
        ],
        check=True,
    )
    print("poetry 2.1.2 installed successfully")


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

    # Configure poetry to use external virtual environments
    print("Configuring poetry to use external virtual environments...")
    try:
        subprocess.run(
            [str(poetry_exe), "config", "virtualenvs.in-project", "false"],
            cwd=str(script_dir),
            env=venv_env,
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        pass  # Configuration may already be set

    # Link poetry to the existing virtual environment
    print(f"Configuring poetry to use virtual environment at: {venv_dir}")
    result = subprocess.run(
        [str(poetry_exe), "env", "use", str(venv_python)],
        cwd=str(script_dir),
        env=venv_env,
        check=False,
        capture_output=True,
        text=True,
    )

    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        stderr = result.stderr.strip()
        # Only print non-benign errors
        if "already been activated" not in stderr.lower() and "already in use" not in stderr.lower():
            print(stderr)

    # Update lock file and install dependencies
    print("Running poetry lock...")
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

    print("Running poetry install...")
    result = subprocess.run(
        [str(poetry_exe), "install"],
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

    print("Dependencies installed successfully")


def install_pyinstaller(venv_python):
    """Install PyInstaller in the virtual environment."""
    print("Installing PyInstaller...")
    subprocess.run(
        [str(venv_python), "-m", "pip", "install", "pyinstaller"],
        check=True,
    )
    print("PyInstaller installed successfully")


def check_pyinstaller(venv_python):
    """Check if PyInstaller is installed, install if missing."""
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
        print("PyInstaller not found, installing...")
        install_pyinstaller(venv_python)
        return True


def build():
    """
    Execute the complete build process.
    
    Steps:
    1. Setup virtual environment (outside project directory)
    2. Update pip and install poetry 2.1.2
    3. Install project dependencies
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

    cmd = [
        str(venv_python),
        "-m",
        "PyInstaller",
        "--noconfirm",
        str(spec_file),
    ]

    try:
        subprocess.run(cmd, check=True)
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
    print("  ./dist/ainode/ainode remove  # Remove AINode")
    print()


if __name__ == "__main__":
    build()
