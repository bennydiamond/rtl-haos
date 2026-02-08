import pytest
import os
import subprocess

def test_run_script_health():
    """
    Checks if 'run.sh' exists, is executable, and has valid bash syntax.
    """
    script_path = "run.sh"
    
    # 1. Skip if file doesn't exist (maybe you use a different entrypoint)
    if not os.path.exists(script_path):
        pytest.skip(f"{script_path} not found. Skipping shell check.")
    
    # 2. Check Execution Permissions
    assert os.access(script_path, os.X_OK), f"CRITICAL: {script_path} is NOT executable! Run 'chmod +x {script_path}'"
    
    # 3. Check Bash Syntax (bash -n)
    # This catches missing quotes, unclosed loops, etc.
    result = subprocess.run(["bash", "-n", script_path], capture_output=True)
    assert result.returncode == 0, f"Shell Syntax Error in {script_path}:\n{result.stderr.decode()}"

def test_dockerfile_standards():
    """
    Ensures Dockerfile exists and uses a valid base image instruction.
    """
    if not os.path.exists("Dockerfile"):
        pytest.skip("Dockerfile not found.")
        
    with open("Dockerfile", "r") as f:
        content = f.read()
        
    # Check for valid FROM instructions
    # 1. Standard Python
    has_python = "FROM python" in content
    # 2. UV (Fast Python)
    has_uv = "ghcr.io/astral-sh/uv" in content
    # 3. Home Assistant Add-on (Uses dynamic build arg)
    has_ha_build = ("FROM $BUILD_FROM" in content) or ("FROM ${BUILD_FROM}" in content)
    
    assert has_python or has_uv or has_ha_build, \
        "Dockerfile is missing a known base image (FROM python, FROM $BUILD_FROM, etc.)"
    
    # Check that we copy the source code
    assert "COPY" in content, "Dockerfile doesn't seem to COPY any code!"

def test_dependency_files_exist():
    """
    Ensures we haven't lost our dependency definitions.
    """
    # We expect pyproject.toml because you are using 'uv' / modern python
    has_pyproject = os.path.exists("pyproject.toml")
    has_requirements = os.path.exists("requirements.txt")
    
    assert has_pyproject or has_requirements, "CRITICAL: No dependency file found! (Missing pyproject.toml OR requirements.txt)"

def test_git_security():
    """
    Scans .gitignore to ensure we aren't leaking secrets.
    """
    if not os.path.exists(".gitignore"):
        pytest.skip(".gitignore not found.")
        
    with open(".gitignore", "r") as f:
        ignored = f.read()
        
    # Critical files that must NOT be committed
    assert ".env" in ignored, "SECURITY: .env is not in .gitignore! You might leak passwords."
    assert "__pycache__" in ignored, "CLEANLINESS: __pycache__ should be ignored."
    assert "venv" in ignored or ".venv" in ignored, "CLEANLINESS: Virtual env folders should be ignored."