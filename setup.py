from setuptools import setup

__version="1.0.6"

spec = {
    "name": "oc_pyfs",
    "version": __version,
    "license": "Apache License 2.0",
    "description": "PyFilesystem interfaces",
    "long_description": "",
    "long_description_content_type": "text/plain",
    "packages": ["oc_pyfs"],
    "install_requires": [
        "chardet >= 2.3.0",
        "fs",
        "oc_cdtapi"
    ],
    "package_data": {},
    "python_requires": ">=2.6",
}

setup(**spec)
