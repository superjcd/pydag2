#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open("README.md", encoding="utf-8") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

requirements = ["pydot", "rich", "pygocron>=0.2.0", "pandas", "redis"]

test_requirements = []

setup(
    author="Jiang Chaodi",
    author_email="929760274@qq.com",
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="lightweigt python workflow(pipeline) builder",
    entry_points={"console_scripts": ["pydag=pydag.cli:main",],},
    install_requires=requirements,
    license="MIT license",
    # long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="pydag2",
    name="pydag2",
    packages=find_packages(include=["pydag", "pydag.*"]),
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/superjcd/pydag2",
    version="0.1.7",
    zip_safe=False,
)
