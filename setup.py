from setuptools import find_packages, setup

setup(
    name="wibbley",
    version="0.1.0",
    packages=find_packages(),
    install_requires=["uvicorn", "uvloop", "orjson"],
    entry_points={
        "console_scripts": [
            "wibbley = wibbley.api.main:main",
        ],
    },
    python_requires=">=3.8",
    author="Your Name",
    author_email="your_email@example.com",
    description="Description of your package",
    url="https://github.com/your_username/your_package",
)
