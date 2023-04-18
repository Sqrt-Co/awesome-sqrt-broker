import os
from setuptools import setup, find_packages

base_path = os.path.dirname(__file__)


def get_version(package_name: str) -> str:
    with open(os.path.join(base_path, package_name, "__init__.py")) as f:
        for line in f:
            if line.startswith("__version__"):
                return line.split("=")[1].strip().strip('"').strip("'")


def get_requirements() -> list:
    with open(os.path.join(base_path, "requirements.txt")) as f:
        return [
            l.strip()
            for l in f.readlines()
            if (not l.startswith("#")) and (l.strip() != "")
        ]


setup(
    name="sqrt_broker",
    version=get_version("sqrt_broker"),
    description="broker for SqRT",
    author="red, jin",
    author_email="red@sqrt.team",
    url="https://github.com/Sqrt-Co/awesome-sqrt-broker.git",
    packages=find_packages(),
    install_requires=get_requirements(),
    include_package_data=True,
)
