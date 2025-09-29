from setuptools import setup, find_packages

setup(
    name="portfolio_opt",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas",
        "numpy",
        "scipy",
    ],
)