#!/usr/bin/env python
import os
import sys
import re

# require python 3.6 or newer
if sys.version_info < (3, 7):
    print("Error: dbt does not support this version of Python.")
    print("Please upgrade to Python 3.7 or higher.")
    sys.exit(1)


# require version of setuptools that supports find_namespace_packages
from setuptools import setup

try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print("Error: dbt requires setuptools v40.1.0 or higher.")
    print('Please upgrade setuptools with "pip install --upgrade setuptools" ' "and try again")
    sys.exit(1)


# pull long description from README
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md"), "r", encoding="utf8") as f:
    long_description = f.read()


# get this package's version from dbt/adapters/<name>/__version__.py
def _get_plugin_version_dict():
    _version_path = os.path.join(this_directory, "dbt", "adapters", "iomete", "__version__.py")
    _semver = r"""(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)"""
    _pre = r"""((?P<prekind>a|b|rc)(?P<pre>\d+))?"""
    _version_pattern = rf"""version\s*=\s*["']{_semver}{_pre}["']"""
    with open(_version_path) as f:
        match = re.search(_version_pattern, f.read().strip())
        if match is None:
            raise ValueError(f"invalid version at {_version_path}")
        return match.groupdict()


def _get_plugin_version():
    parts = _get_plugin_version_dict()
    return "{major}.{minor}.{patch}{prekind}{pre}".format(**parts)


# require a compatible minor version (~=), prerelease if this is a prerelease
def _get_dbt_core_version():
    parts = _get_plugin_version_dict()
    minor = "{major}.{minor}.0".format(**parts)
    pre = parts["prekind"] + "1" if parts["prekind"] else ""
    return f"{minor}{pre}"


package_name = "dbt-iomete"
package_version = "1.7.9"
dbt_core_version = _get_dbt_core_version()
description = """The Apache Spark (IOMETE) adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="iomete",
    author_email="support@iomete.com",
    url="https://github.com/iomete/dbt-iomete",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "protobuf==4.25.5",
        "dbt-core~={}".format(dbt_core_version),
        "dbt-common==1.9.0",
        "sqlparams>=3.0.0",
        "py-hive-iomete>=2.1.3",
        "sentry-sdk==2.20.0",
        "iomete-sdk==3.0.0"
    ],
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.7",
)
