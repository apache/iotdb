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


def setup_venv():
    """Create virtual environment if it doesn't exist"""
    script_dir = Path(__file__).parent
    venv_dir = script_dir / ".venv"

    if venv_dir.exists():
        print(f"Virtual environment already exists at: {venv_dir}")
        return venv_dir

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
    """
    Update pip in virtual environment.

    Note: subprocess.run() is synchronous and blocks until the subprocess completes.
    This ensures pip upgrade finishes before the script continues.
    """
    print("Updating pip...")
    subprocess.run(
        [str(venv_python), "-m", "pip", "install", "--upgrade", "pip"], check=True
    )
    print("pip updated successfully")


def install_poetry(venv_python):
    """
    Install poetry 2.1.2 in virtual environment.

    Note: subprocess.run() is synchronous and blocks until the subprocess completes.
    This ensures poetry installation finishes before the script continues.
    """
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
    """Get environment variables for virtual environment activation"""
    env = os.environ.copy()
    venv_path = str(venv_dir.absolute())
    env["VIRTUAL_ENV"] = venv_path

    # Add venv bin directory to PATH
    if sys.platform == "win32":
        venv_bin = str(venv_dir / "Scripts")
    else:
        venv_bin = str(venv_dir / "bin")

    # Prepend venv bin to PATH to ensure venv tools take precedence
    env["PATH"] = f"{venv_bin}{os.pathsep}{env.get('PATH', '')}"

    return env


def install_dependencies(venv_python, venv_dir, script_dir):
    """
    Install dependencies using poetry.

    Note: subprocess.run() is synchronous and blocks until each command completes.
    This ensures each step (poetry lock, install) finishes before proceeding.

    We configure poetry to use our .venv directory by:
    1. Configuring poetry to use in-project virtualenvs
    2. Setting poetry to use our .venv via poetry env use
    3. Running poetry lock and install which will use our .venv
    """
    print("Installing dependencies with poetry...")

    # Get environment with VIRTUAL_ENV set
    venv_env = get_venv_env(venv_dir)

    # Configure poetry to use in-project virtualenvs
    # This makes poetry create/use .venv in the project directory
    print("Configuring poetry to use in-project virtualenvs...")
    try:
        subprocess.run(
            ["poetry", "config", "virtualenvs.in-project", "true"],
            cwd=str(script_dir),
            env=venv_env,
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        pass  # Configuration may already be set

    # Configure poetry to use our existing virtual environment
    # This links poetry's management to our .venv directory
    print(f"Configuring poetry to use virtual environment at: {venv_dir}")
    result = subprocess.run(
        ["poetry", "env", "use", str(venv_python)],
        cwd=str(script_dir),
        env=venv_env,
        check=False,  # Don't fail if venv is already configured
        capture_output=True,
        text=True,
    )

    # Check output - if poetry tries to recreate venv, that's okay as it will use our path
    if result.stdout:
        output = result.stdout.strip()
        print(output)
    if result.stderr:
        stderr = result.stderr.strip()
        # Ignore warnings about venv already existing or being created
        if (
            "already been activated" not in stderr.lower()
            and "already in use" not in stderr.lower()
        ):
            print(stderr)

    # Run poetry lock
    print("Running poetry lock...")
    result = subprocess.run(
        ["poetry", "lock"],
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

    # Run poetry install
    # With VIRTUAL_ENV set and poetry env use configured, this should install into our .venv
    print("Running poetry install...")
    result = subprocess.run(
        ["poetry", "install"],
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


def check_pyinstaller(venv_python):
    """Check if PyInstaller is installed"""
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
        print("Error: PyInstaller is not installed")
        print("Please run: pip install pyinstaller")
        return False


def build():
    """Execute build process"""
    script_dir = Path(__file__).parent

    # Setup virtual environment
    venv_dir = setup_venv()
    venv_python = get_venv_python(venv_dir)

    # Update pip
    update_pip(venv_python)

    # Install poetry 2.1.2
    install_poetry(venv_python)

    # Install dependencies
    install_dependencies(venv_python, venv_dir, script_dir)

    # Check PyInstaller
    if not check_pyinstaller(venv_python):
        sys.exit(1)

    print("=" * 50)
    print("IoTDB AINode PyInstaller Build Script")
    print("=" * 50)
    print()

    # Execute build (incremental build - no cleanup for faster rebuilds)
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
