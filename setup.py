import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent

README = (HERE / "README.md").read_text()

setup(name="adh-deployment-manager",
      version="0.0.1",
      description="Library for interacting with ADH REST API.",
      long_description=README,
      long_description_content_type="text/markdown",
      url="https://github.com/google/adh-deployment-manager",
      author="Google Inc. (gTech gPS CSE team)",
      author_email="no-reply@google.com",
      license="Apache 2.0",
      classifiers=[
          "Programming Language :: Python :: 3",
          "Intended Audience :: Developers",
          "Topic :: Software Development :: Libraries :: Python Modules",
          "Operating System :: OS Independent",
          "License :: OSI Approved :: Apache Software License"
      ],
      packages=find_packages(),
      install_requires=[
          "pyyaml",
          "google_auth_oauthlib",
          "google-api-python-client",
          "pandas",
          "oauth2client",
          "google-cloud-bigquery",
      ],
      setup_requires=["pytest-runner"],
      tests_requires=["pytest"],
      entry_points={
          "console_scripts": [
              "adm=adh_deployment_manager.cli.adm:main",
          ]
      })
