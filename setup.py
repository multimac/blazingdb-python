""" A setuptools based setup module """

from setuptools import setup, find_packages

setup(
    name="blazingdb",
    version="1.3.0.beta3.dev13",

    description=" ".join([
        "Contains the relevant classes for connecting to, and",
        "importing data into BlazingDB"
    ]),

    keywords="blazingdb",
    url="https://github.com/multimac/blazingdb-python",

    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3"
    ],

    packages=find_packages(),
    install_requires=["aiofiles", "aiohttp", "py-flags"]
)
