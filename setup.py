from pathlib import Path

from setuptools import setup

with open(Path(__file__).parent / "kafa" / "VERSION") as f:
    version = f.read().strip()

if __name__ == "__main__":
    setup(version=version)
