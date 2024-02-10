from setuptools import find_packages, setup

setup(
    name="wibbley",
    version="0.1.0",
    packages=find_packages(),
    install_requires=["uvicorn", "uvloop", "orjson", "click"],
    entry_points={
        "console_scripts": [
            "wibbley = wibbley.api.main:main",
        ],
    },
    python_requires=">=3.8",
    author="Ryan Wible",
    author_email="ryanwible343@gmail.com",
    description="An asynchronous web framework for event-driven applications",
    url="https://github.com/your_username/your_package",
)
