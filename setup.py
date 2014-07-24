#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name="mongotoy",
    version="0.01",
    description="Simple ORM for Mongo",
    long_description="",
    url="",
    author="yeyuexia",
    author_email="yyxworld@gmail.com",

    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=["pymongo==2.7"]
)
