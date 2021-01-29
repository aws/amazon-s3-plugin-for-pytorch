# Copyright 2017-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.


import os
import re
import sys
import platform
import subprocess

from pathlib import Path
from setuptools import setup, Extension, find_packages 
from setuptools.command.build_ext import build_ext
from distutils.version import LooseVersion

class CMakeExtension(Extension):
    def __init__(self, name, sourcedir=''):
        Extension.__init__(self, name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)


class CMakeBuild(build_ext):
    def run(self):
        try:
            out = subprocess.check_output(['cmake', '--version'])
        except OSError:
            raise RuntimeError("CMake must be installed to build the following extensions: " +
                               ", ".join(e.name for e in self.extensions))

        if platform.system() == "Windows":
            cmake_version = LooseVersion(re.search(r'version\s*([\d.]+)', out.decode()).group(1))
            if cmake_version < '3.1.0':
                raise RuntimeError("CMake >= 3.1.0 is required on Windows")

        for ext in self.extensions:
            self.build_extension(ext)

    def build_extension(self, ext):
        extdir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))
        # required for auto-detection of auxiliary "native" libs
        if not extdir.endswith(os.path.sep):
            extdir += os.path.sep

        cmake_args = ['-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=' + extdir,
                      '-DPYTHON_EXECUTABLE=' + sys.executable,
                      '-DCMAKE_PREFIX_PATH=' + os.environ['CMAKE_PREFIX_PATH'],
                      '-DCMAKE_CXX_FLAGS=' + "-fPIC"]

        cfg = 'Debug' if self.debug else 'Release'
        build_args = ['--config', cfg]

        if platform.system() == "Windows":
            cmake_args += ['-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_{}={}'.format(cfg.upper(), extdir)]
            if sys.maxsize > 2**32:
                cmake_args += ['-A', 'x64']
            build_args += ['--', '/m']
        else:
            cmake_args += ['-DCMAKE_BUILD_TYPE=' + cfg]
            build_args += ['--', '-j2']

        env = os.environ.copy()
        env['CXXFLAGS'] = '{} -DVERSION_INFO=\\"{}\\"'.format(env.get('CXXFLAGS', ''),
                                                              self.distribution.get_version())
        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)
        subprocess.check_call(['cmake', ext.sourcedir] + cmake_args, cwd=self.build_temp, env=env)
        subprocess.check_call(['cmake', '--build', '.'] + build_args, cwd=self.build_temp)


def get_sha():
    try:
        return subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode('ascii').strip()
    except Exception:
        return 'Unknown'

def get_version(sha):
    version = open('version.txt', 'r').read().strip()
    if sha != 'Unknown':
        version += '+' + sha[:7]
    return version

def write_version_file():
    sha = get_sha()
    version = get_version(sha)
    version_path = os.path.join(Path.cwd(), 'awsio', '_version.py') 
    with open(version_path, 'w') as f:
        f.write(f"__version__ = \"{version}\"\n")

if __name__ == "__main__":
    # metadata
    package_name = 'awsio'
    required_packages = ["torch>=1.5.1"]

    # define __version__
    write_version_file()
    exec(open("awsio/_version.py").read())
    print(f"Building wheel for {package_name}-{__version__}")

    with open('README.md') as f:
        readme = f.read()

    setup(
        name=package_name,
        version=__version__,
        author='Amazon Web Services',
        author_email='aws-pytorch@amazon.com',
        description='A package for creating PyTorch Datasets using objects in AWS S3 buckets',
        long_description=readme,
        license='Apache License 2.0',
        keywords='ML Amazon AWS AI PyTorch',

        # Package info
        packages=find_packages(exclude=('test',)),
        zip_safe=False,
        install_requires=required_packages,
        extras_require={
            "scipy": ["scipy"],
        },
        ext_modules=[CMakeExtension('aws_io')],
        cmdclass=dict(build_ext=CMakeBuild),
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: Apache Software License",
            "Operating System :: OS Independent",
        ],
    )
