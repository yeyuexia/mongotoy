#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name="mongotoy",
    version="0.2",
    description="Simple ORM for Mongo",
    long_description="https://github.com/yeyuexia/mongotoy/blob/master/README.md",
    url="https://github.com/yeyuexia/mongotoy",
    author="yeyuexia",
    author_email="yyxworld@gmail.com",

    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=["pymongo==2.7"]
)
