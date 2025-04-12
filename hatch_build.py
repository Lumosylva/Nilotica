#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica_dev
@FileName   : hatch_build
@Date       : 2025/4/12 10:02
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 编译vnctpmd、vnctptd模块生成pyd文件，并生成对应的pyi文件
"""

import os
import platform
import shutil
import subprocess
import logging
import sys
from pathlib import Path
from typing import Any, Dict

try:
    import pybind11
except ImportError:
    print("Error: pybind11 is required for building extensions. Please install it.", file=sys.stderr)
    sys.exit(1)

try:
    from setuptools import Distribution, Extension
    from setuptools.command.build_ext import build_ext as _build_ext
except ImportError:
    print("Error: setuptools is required for building extensions. Please ensure it's installed.", file=sys.stderr)
    sys.exit(1)


from hatchling.builders.hooks.plugin.interface import BuildHookInterface

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - HatchBuild - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CustomBuildHook(BuildHookInterface):
    """
    Hatch build hook for compiling C++/pybind11 extensions and generating stubs.
    """
    def initialize(self, version: str, build_data: Dict[str, Any]) -> None:
        """
        Called before the build process begins.
        Compiles extensions, generates stubs, and copies artifacts.
        """
        logger.info("--- CustomBuildHook: initialize started ---")
        root_path = Path(self.root).resolve()
        logger.info(f"Project root: {root_path}")

        system = platform.system()
        if system not in ["Windows", "Linux"]:
            logger.warning(f"Unsupported system {system}. Skipping C++ extension build.")
            return

        # --- Target directory for artifacts ---
        # Relative path within the source tree where artifacts should be placed
        # Hatch will then pick these up when building the wheel.
        artifact_dest_dir_rel = Path("vnpy_ctp/api")
        artifact_dest_dir_abs = root_path / artifact_dest_dir_rel
        artifact_dest_dir_abs.mkdir(parents=True, exist_ok=True)
        logger.info(f"Artifact destination directory (absolute): {artifact_dest_dir_abs}")
        logger.info(f"Artifact destination directory (relative to root): {artifact_dest_dir_rel}")


        # --- Get pybind11 include path ---
        try:
            pybind11_include_path = pybind11.get_include()
            logger.info(f"Found pybind11 include path: {pybind11_include_path}")
        except Exception as e:
            logger.error(f"Failed to get pybind11 include path: {e}", exc_info=True)
            raise RuntimeError("Could not find pybind11 include path.") from e

        # --- Define Extension Modules ---
        libraries = ["thostmduserapi_se", "thosttraderapi_se"]

        # Use paths relative to the project root
        md_source_rel = "vnpy_ctp/api/vnctp/vnctpmd/vnctpmd.cpp"
        td_source_rel = "vnpy_ctp/api/vnctp/vnctptd/vnctptd.cpp"
        md_source_abs = root_path / md_source_rel
        td_source_abs = root_path / td_source_rel

        include_dirs_rel = [
            "vnpy_ctp/api/include",
            # pybind11_include_path is absolute, no need to make relative
            "vnpy_ctp/api/vnctp",
            "vnpy_ctp/api/vnctp/vnctpmd",
            "vnpy_ctp/api/vnctp/vnctptd"
        ]
        include_dirs_abs = [str(root_path / d) for d in include_dirs_rel]
        include_dirs_abs.append(pybind11_include_path) # Add absolute pybind11 path

        library_dirs_rel_base = ["vnpy_ctp/api"]
        library_dirs_rel_win = ["vnpy_ctp/api/libs"]

        extensions = []

        common_settings = {
            "include_dirs": include_dirs_abs, # Use absolute paths for build
            "define_macros": [("NDEBUG", None)],
            "language": "cpp",
        }

        if system == "Linux":
            specific_settings = {
                "library_dirs": [str(root_path / d) for d in library_dirs_rel_base],
                "extra_compile_args": ["-std=c++17", "-O3", "-Wno-delete-incomplete", "-Wno-sign-compare", "-fPIC", "-DNDEBUG"],
                "extra_link_args": ["-lstdc++"],
                "runtime_library_dirs": ["$ORIGIN"],
            }
        elif system == "Windows":
            specific_settings = {
                "library_dirs": [str(root_path / d) for d in library_dirs_rel_base + library_dirs_rel_win],
                "extra_compile_args": ["/O2", "/MT", "/EHsc", "/W4", "/std:c++17", "/wd4100"],
                "extra_link_args": [],
                "runtime_library_dirs": [],
            }
        else: # Should not happen due to check above, but defensively include
             raise RuntimeError(f"Unsupported system: {system}")


        # --- Check source file existence ---
        if md_source_abs.is_file():
            extensions.append(
                Extension(
                    # Use package name for the extension
                    name="vnpy_ctp.api.vnctpmd",
                    sources=[str(md_source_rel)], # Relative path for Extension source
                    libraries=libraries,
                    **common_settings,
                    **specific_settings,
                )
            )
            logger.info(f"Added vnctpmd extension. Source: {md_source_rel}")
        else:
            logger.warning(f"Source file not found, skipping vnctpmd extension: {md_source_abs}")

        if td_source_abs.is_file():
            extensions.append(
                Extension(
                    name="vnpy_ctp.api.vnctptd",
                    sources=[str(td_source_rel)], # Relative path for Extension source
                    libraries=libraries,
                    **common_settings,
                    **specific_settings,
                )
            )
            logger.info(f"Added vnctptd extension. Source: {td_source_rel}")
        else:
            logger.warning(f"Source file not found, skipping vnctptd extension: {td_source_abs}")

        if not extensions:
            logger.warning("No C++ extensions defined or source files found. Skipping build steps.")
            return

        # --- Execute Build ---
        logger.info("Configuring setuptools distribution for building extensions...")
        dist = Distribution({"name": "nilotica_cpp_exts", "ext_modules": extensions})
        cmd = _build_ext(dist)
        cmd.ensure_finalized()
        cmd.inplace = False # Build in a temporary location first

        logger.info("Running build_ext command...")
        try:
            cmd.run()
            logger.info("build_ext command finished successfully.")
        except Exception as e:
            logger.error(f"build_ext command failed: {e}", exc_info=True)
            if hasattr(e, 'cause') and e.cause:
                 logger.error(f"Compilation error details: {e.cause}")
            raise RuntimeError("Failed to build C++ extensions.") from e

        # --- Process Each Extension: Copy Artifacts, Copy DLLs, Generate Stubs ---
        logger.info("Processing compiled artifacts and generating stubs individually...")
        build_lib_dir = Path(cmd.build_lib).resolve()
        logger.info(f"Searching for artifacts in: {build_lib_dir}")

        # Prepare lists to collect file paths for final inclusion
        copied_artifacts = []
        copied_ctp_dlls_map = {} # Track unique DLLs copied
        generated_stubs = []

        # Prepare stubgen command base if available
        stubgen_cmd_base = None
        try:
            import pybind11_stubgen
            stubgen_cmd_base = [sys.executable, "-m", "pybind11_stubgen"]
            logger.info("pybind11-stubgen found.")
        except ImportError:
             logger.error("pybind11-stubgen not found. Cannot generate stubs.")

        # Setup env for stubgen (only needs PYTHONPATH)
        stubgen_env = os.environ.copy()
        stubgen_env["PYTHONPATH"] = str(artifact_dest_dir_abs) + os.pathsep + stubgen_env.get("PYTHONPATH", "")
        logger.debug(f"Stubgen PYTHONPATH: {stubgen_env['PYTHONPATH']}")

        # Define DLLs to copy (assuming they are in the source tree)
        ctp_dlls_to_copy = ["thostmduserapi_se.dll", "thosttraderapi_se.dll"]
        ctp_dll_source_dir = root_path / "vnpy_ctp" / "api"

        for ext in extensions:
            module_name = ext.name.split('.')[-1]
            full_module_name = ext.name
            logger.info(f"--- Processing extension: {full_module_name} ---")

            # 1. Find and Copy Artifact (.pyd/.so)
            ext_suffix = ".pyd" if system == "Windows" else ".so"
            pattern = f"{module_name}*{ext_suffix}"
            found_artifacts = list(build_lib_dir.rglob(pattern))

            if not found_artifacts:
                logger.warning(f"Could not find compiled artifact for {module_name} in {build_lib_dir}. Skipping.")
                continue

            src_artifact = found_artifacts[0]
            dest_artifact = artifact_dest_dir_abs / src_artifact.name
            logger.info(f"Found artifact: {src_artifact.name}. Copying to {dest_artifact}")
            try:
                shutil.copy2(src_artifact, dest_artifact)
                copied_artifacts.append(dest_artifact)
                logger.info(f"Successfully copied {dest_artifact.name}")
                artifact_copied = True
            except Exception as e:
                logger.error(f"Failed to copy artifact {src_artifact}: {e}", exc_info=True)
                artifact_copied = False
                continue # If artifact copy fails, skip DLL copy and stubgen

            # 3. Generate Stubs (if artifact copied successfully)
            # Note: Stubgen might still fail silently if DLLs are not found in its environment
            if stubgen_cmd_base and artifact_copied:
                 cmd_stub = stubgen_cmd_base + [
                    # Use short module name and --output-dir as tested manually
                    module_name, # Use short name like 'vnctpmd'
                    "--output-dir", str(artifact_dest_dir_abs), # Output to the API dir
                    "--ignore-all-errors", # Re-add flag for robustness
                 ]
                 logger.info(f"Running stubgen for {module_name}: {' '.join(cmd_stub)}")
                 try:
                    result = subprocess.run(
                        cmd_stub, check=True, env=stubgen_env, cwd=root_path,
                        capture_output=True, text=True, encoding='gbk', errors='ignore'
                    )
                    # Use the actual module name for the stub file
                    stub_file_name = f"{module_name}.pyi"
                    stub_file = artifact_dest_dir_abs / stub_file_name
                    # Check if the stub file was created in the output directory
                    if stub_file.exists():
                        generated_stubs.append(stub_file)
                        logger.info(f"Successfully generated stub: {stub_file_name}")
                        if result.stdout: logger.debug(f"Stubgen STDOUT ({full_module_name}):\\n{result.stdout}")
                        if result.stderr: logger.debug(f"Stubgen STDERR ({full_module_name}):\\n{result.stderr}")
                    else:
                        logger.warning(f"Stubgen command for {full_module_name} completed but stub file {stub_file_name} not found. Module might be unimportable or stubgen failed silently.")
                        if result.stderr: logger.warning(f"Stubgen STDERR ({full_module_name}):\\n{result.stderr}")

                 except subprocess.CalledProcessError as e:
                    logger.error(f"Stub generation failed for module {full_module_name}.")
                    logger.error(f"Command: {' '.join(e.cmd)}")
                    logger.error(f"Return code: {e.returncode}")
                    if e.stdout: logger.error(f"STDOUT:\\n{e.stdout}")
                    if e.stderr: logger.error(f"STDERR:\\n{e.stderr}")
            elif not stubgen_cmd_base:
                 logger.info(f"Skipping stub generation for {full_module_name} as pybind11-stubgen is not available.")
            # Add other conditions if needed

        # --- Update build_data for Hatch ---
        logger.info("Updating build_data['force_include'] for Hatch...")
        if 'force_include' not in build_data:
            build_data['force_include'] = {}

        # Collect all unique files to include
        # NOTE: We are NOT including core CTP DLLs via force_include anymore.
        # They need to be present in the runtime environment independently,
        # or included via other packaging mechanisms if distributing.
        all_files_to_include_in_wheel = copied_artifacts + generated_stubs

        for f_abs in all_files_to_include_in_wheel:
             try:
                 f_rel = f_abs.relative_to(root_path)
                 build_data['force_include'][str(f_rel)] = str(f_rel)
                 logger.info(f"Added to force_include: {f_rel}")
             except ValueError as ve: # Should not happen if paths are correct
                 logger.error(f"Failed to make path relative: {f_abs}. Error: {ve}")


        logger.info(f"Final force_include data: {build_data['force_include']}")
        logger.info("--- CustomBuildHook: initialize finished ---")


# Make the hook class discoverable by Hatch
# This doesn't seem necessary if specified directly in pyproject.toml `tool.hatch.build.hooks.custom.path`
# hatch_register_build_hook = CustomBuildHook
