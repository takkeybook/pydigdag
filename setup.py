from setuptools import setup, find_packages
import sys, os
try: # for pip >= 10
    from pip._internal.req import parse_requirements
    from pip._internal import download
except ImportError: # for pip < 10
    from pip.req import parse_requirements
    from pip import download

version = '0.0.3'

requirements = [
      str(requirement.req)
      for requirement in parse_requirements('requirements.txt', session = download.PipSession())
]

setup(name='pydigdag',
      version=version,
      description="module for a task script executed on digdag server",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='digdag TreasureData MySQL',
      author='Takashi Kagimoto',
      author_email='takashi.kagimoto@drecom.co.jp',
      url='',
      license='',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=requirements,
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
