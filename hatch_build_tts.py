#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica_dev
@FileName   : hatch_build
@Date       : 2025/4/26 21:02
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 编译vnttsmd、vnttstd模块生成pyd文件，并生成对应的pyi文件
"""
import os
import platform
import shutil
import subprocess
import logging
import sys
from pathlib import Path
from typing import Any, Dict

import pybind11

from setuptools import Distribution, Extension
from setuptools.command.build_ext import build_ext as _build_ext

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - HatchBuildTTS - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CustomBuildHookTTS(BuildHookInterface):
    """
    Hatch build hook for compiling vnpy_tts C++/pybind11 extensions and generating stubs.
    """
    def initialize(self, version: str, build_data: Dict[str, Any]) -> None:
        """
        Called before the build process begins.
        Compiles extensions, generates stubs, and copies artifacts for vnpy_tts.
        """
        logger.info("--- CustomBuildHookTTS: initialize started ---")
        root_path = Path(self.root).resolve()
        logger.info(f"Project root: {root_path}")

        system = platform.system()
        if system not in ["Windows", "Linux"]:
            logger.warning(f"Unsupported system {system}. Skipping C++ extension build for vnpy_tts.")
            return

        # --- Target directory for artifacts within the source tree ---
        artifact_dest_dir_rel = Path("vnpy_tts/api")
        artifact_dest_dir_abs = root_path / artifact_dest_dir_rel
        artifact_dest_dir_abs.mkdir(parents=True, exist_ok=True)
        logger.info(f"TTS Artifact destination directory (absolute): {artifact_dest_dir_abs}")
        logger.info(f"TTS Artifact destination directory (relative to root): {artifact_dest_dir_rel}")

        # --- Get pybind11 include path ---
        try:
            pybind11_include_path = pybind11.get_include()
            logger.info(f"Found pybind11 include path: {pybind11_include_path}")
        except Exception as e:
            logger.error(f"Failed to get pybind11 include path: {e}", exc_info=True)
            raise RuntimeError("Could not find pybind11 include path.") from e

        # --- Define Extension Modules for vnpy_tts ---
        # Linker libraries (e.g., .lib on Windows)
        link_libraries = ["thostmduserapi_se", "thosttraderapi_se"]

        # Use paths relative to the project root for sources in Extension definition
        md_source_rel = "vnpy_tts/api/vntts/vnttsmd/vnttsmd.cpp"
        td_source_rel = "vnpy_tts/api/vntts/vnttstd/vnttstd.cpp"
        md_source_abs = root_path / md_source_rel
        td_source_abs = root_path / td_source_rel

        # Use paths relative to the project root for easier management
        include_dirs_rel = [
            "vnpy_tts/api/include",
            "vnpy_tts/api/vntts",
            "vnpy_tts/api/vntts/vnttsmd", # Added specific include dir
            "vnpy_tts/api/vntts/vnttstd" # Added specific include dir
        ]
        # Convert to absolute paths for the build command
        include_dirs_abs = [str(root_path / d) for d in include_dirs_rel]
        include_dirs_abs.append(pybind11_include_path) # Add absolute pybind11 path

        library_dirs_rel_base = ["vnpy_tts/api"]
        library_dirs_rel_win = ["vnpy_tts/api/libs"]

        extensions = []

        common_settings = {
            "include_dirs": include_dirs_abs, # Use absolute paths for build
            "define_macros": [("NDEBUG", None)], # Ensure NDEBUG is defined for release builds
            "language": "cpp",
        }

        if system == "Linux":
            specific_settings = {
                "library_dirs": [str(root_path / d) for d in library_dirs_rel_base], # Tell linker where to find .so for linking (if needed, usually not for .so)
                "extra_compile_args": ["-std=c++17", "-O3", "-Wno-delete-incomplete", "-Wno-sign-compare", "-fPIC", "-DNDEBUG"],
                "extra_link_args": ["-lstdc++", f"-Wl,-rpath,$ORIGIN"], # Embed rpath
                "runtime_library_dirs": ["$ORIGIN"], # Linker searches in the same directory as the extension at runtime
            }
        elif system == "Windows":
            specific_settings = {
                "library_dirs": [str(root_path / d) for d in library_dirs_rel_base + library_dirs_rel_win], # Tell linker where to find .lib files
                "extra_compile_args": ["/O2", "/MT", "/EHsc", "/W4", "/std:c++17", "/wd4100"], # Specify minimum Windows version (e.g., 0x0A00 for Win 10)
                "extra_link_args": [],
                "runtime_library_dirs": [],
            }
        else:
             raise RuntimeError(f"Unsupported system: {system}")

        # --- Check source file existence ---
        if md_source_abs.is_file():
            extensions.append(
                Extension(
                    name="vnpy_tts.api.vnttsmd",
                    sources=[str(md_source_rel)],
                    libraries=link_libraries, # Link against these .lib names on Windows
                    **common_settings,
                    **specific_settings,
                )
            )
            logger.info(f"Added vnttsmd extension. Source: {md_source_rel}")
        else:
            logger.warning(f"Source file not found, skipping vnttsmd extension: {md_source_abs}")

        if td_source_abs.is_file():
            extensions.append(
                Extension(
                    name="vnpy_tts.api.vnttstd",
                    sources=[str(td_source_rel)],
                    libraries=link_libraries, # Link against these .lib names on Windows
                    **common_settings,
                    **specific_settings,
                )
            )
            logger.info(f"Added vnttstd extension. Source: {td_source_rel}")
        else:
            logger.warning(f"Source file not found, skipping vnttstd extension: {td_source_abs}")

        if not extensions:
            logger.warning("No vnpy_tts C++ extensions defined or source files found. Skipping build steps.")
            return

        # --- Execute Build ---
        logger.info("Configuring setuptools distribution for building vnpy_tts extensions...")
        dist = Distribution({"name": "vnpy_tts_exts", "ext_modules": extensions})
        cmd = _build_ext(dist)
        cmd.ensure_finalized()
        cmd.inplace = False # Build in a temporary location

        logger.info("Running build_ext command for vnpy_tts...")
        try:
            cmd.run()
            logger.info("build_ext command for vnpy_tts finished successfully.")
        except Exception as e:
            logger.error(f"build_ext command for vnpy_tts failed: {e}", exc_info=True)
            if hasattr(e, 'cause') and e.cause:
                logger.error(f"Compilation error details: {e.cause}")
            raise RuntimeError("Failed to build vnpy_tts C++ extensions.") from e

        # --- Process Each Extension: Copy Artifacts, Generate Stubs ---
        logger.info("Processing compiled vnpy_tts artifacts and generating stubs individually...")
        build_lib_dir = Path(cmd.build_lib).resolve()
        logger.info(f"Searching for vnpy_tts artifacts in: {build_lib_dir}")

        copied_artifacts = []
        generated_stubs = []

        # Prepare stubgen command base
        stubgen_cmd_base = None
        try:
            import pybind11_stubgen
            stubgen_cmd_base = [sys.executable, "-m", "pybind11_stubgen"]
            logger.info("pybind11-stubgen found.")
        except ImportError:
             logger.warning("pybind11-stubgen not found. Cannot generate stubs for vnpy_tts.")

        # Setup env for stubgen
        stubgen_env = os.environ.copy()
        # Add artifact destination to PYTHONPATH for stubgen import
        stubgen_env["PYTHONPATH"] = str(artifact_dest_dir_abs) + os.pathsep + stubgen_env.get("PYTHONPATH", "")
        logger.debug(f"TTS Stubgen PYTHONPATH: {stubgen_env['PYTHONPATH']}")

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
                continue  # If artifact copy fails, skip DLL copy and stubgen

            # 3. Generate Stubs (if artifact copied successfully)
            if stubgen_cmd_base and artifact_copied:
                 # Run stubgen using the *short* module name relative to the output dir
                 cmd_stub = stubgen_cmd_base + [
                    module_name, # Use short name relative to output dir
                    "--output-dir", str(artifact_dest_dir_abs),
                    "--ignore-all-errors",
                 ]
                 logger.info(f"Running stubgen for {module_name}: {' '.join(cmd_stub)}")
                 try:
                    # Run stubgen with the artifact destination as CWD to help find DLLs
                    result = subprocess.run(
                        cmd_stub, check=True, env=stubgen_env, cwd=artifact_dest_dir_abs, # CWD is key for DLL finding on Windows
                        capture_output=True, text=True, encoding='gbk', errors='ignore'
                    )
                    stub_file_name = f"{module_name}.pyi"
                    stub_file = artifact_dest_dir_abs / stub_file_name
                    if stub_file.exists():
                        generated_stubs.append(stub_file)
                        logger.info(f"Successfully generated stub: {stub_file_name}")
                        if result.stdout: logger.debug(f"Stubgen STDOUT ({full_module_name}):\\n{result.stdout}")
                        if result.stderr: logger.debug(f"Stubgen STDERR ({full_module_name}):\\n{result.stderr}")
                    else:
                        logger.warning(
                            f"Stubgen command for {full_module_name} completed but stub file {stub_file_name} not found. Module might be unimportable or stubgen failed silently.")
                        if result.stderr: logger.warning(f"Stubgen STDERR ({full_module_name}):\\n{result.stderr}")

                 except subprocess.CalledProcessError as e:
                     logger.error(f"Stub generation failed for module {full_module_name}.")
                     logger.error(f"Command: {' '.join(e.cmd)}")
                     logger.error(f"Return code: {e.returncode}")
                     if e.stdout: logger.error(f"STDOUT:\\n{e.stdout}")
                     if e.stderr: logger.error(f"STDERR:\\n{e.stderr}")
                 except Exception as stub_exc:
                    logger.error(f"An unexpected error occurred during stub generation for {module_name}: {stub_exc}", exc_info=True)
            elif not stubgen_cmd_base:
                logger.info(f"Skipping stub generation for {full_module_name} as pybind11-stubgen is not available.")

        # --- Update build_data for Hatch ---
        logger.info("Updating build_data['force_include'] for vnpy_tts artifacts...")
        if 'force_include' not in build_data:
            build_data['force_include'] = {}

        # Add only the compiled extensions (.pyd/.so) and generated stubs (.pyi)
        # DO NOT add the base SDK DLLs/SOs here.
        all_files_to_include_in_wheel = copied_artifacts + generated_stubs

        for f_abs in all_files_to_include_in_wheel:
             try:
                 f_rel = f_abs.relative_to(root_path)
                 build_data['force_include'][str(f_rel)] = str(f_rel)
                 logger.info(f"Added to force_include: {f_rel}")
             except ValueError as ve:
                 logger.error(f"Failed to make path relative: {f_abs}. Error: {ve}")

        logger.info(f"Final force_include data after TTS: {build_data.get('force_include', {})}")
        logger.info("--- CustomBuildHookTTS: initialize finished ---")
