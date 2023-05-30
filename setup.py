from setuptools import setup

__version="1.0.8"

spec = {
    "name": "oc-pyfs",
    "version": __version,
    "license": "Apache License 2.0",
    "description": "PyFilesystem interfaces",
    "long_description": "",
    "long_description_content_type": "text/plain",
    "packages": ["oc_pyfs"],
    "install_requires": [
        "chardet >= 2.3.0",
        "fs",
        "oc-cdtapi"
    ],
    "package_data": {},
    "python_requires": ">=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*,!=3.5.*"
}

setup(**spec)
