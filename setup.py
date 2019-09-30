# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


def readme():
    with open('README.md') as f:
        return f.read()


setup(
    name='MBStats',
    version='0.0.2',
    long_description=readme(),
    url='https://github.com/metabrainz/mbstats',
    author='Laurent Monin',
    author_email='github@norz.org',
    license='GPLv2+',
    description='Fast web logs parser and influxdb feeder',
    packages=find_packages(),
    install_requires=['influxdb', 'pygtail'],
    test_suite='mbstats.tests.get_suite',
)
