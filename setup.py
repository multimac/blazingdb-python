""" A setuptools based setup module """

from setuptools import setup, find_packages

setup(
    name="blazingdb",
    version="1.1.0",

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
    install_requires=["requests"]
)
