# setup.py

from setuptools import setup, find_packages

setup(
    name="quantum-data-manager",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        'mysql-connector-python',
        'pandas',
        'numpy',
        'python-dotenv',
        'polygon-api-client',
        'fastapi',
        'uvicorn',
        'pytest',
        'pytest-asyncio',
        'aiohttp'
    ],
    python_requires='>=3.8',
    author="Your Name",
    author_email="your.email@example.com",
    description="A modular data management system for trading applications",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
)