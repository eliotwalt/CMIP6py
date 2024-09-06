import os
from setuptools import setup, find_packages, Command

class CustomInstallCommand(Command):
    """Custom installation command to run additional tasks."""
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        # Run Playwright installation
        os.system('playwright install')

setup(
    name="cmip6py",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "requests",
        "beautifulsoup4",
        "playwright",
        "seaborn",
        "keyring",
        "esgf-pyclient",
        "pandas",
        "humanfriendly",
        "netCDF4",
        "cftime",
        "xarray[io]"
    ],
    # extras_require={
    # },
    cmdclass={
        'install': CustomInstallCommand,
    },
    author="Eliot Walt",
    author_email="walt.eliot@hotmail.com",
    description="A Python package for CMIP6 data processing.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/eliotwalt/CMIP6py/",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.10',
)
