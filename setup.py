import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
  name = 'pysparkifier',         # How you named your package folder (MyLib)
  packages = ['pysparkifier'],   # Chose the same as "name"
  include_package_data=True,
  version = '0.8',      # Start with a small number and increase it with every change you make
  # license='apache-2.0',        # Chose a license from here: https://help.github.com/articles/licensing-a-repository
  description = 'Streamlined pyspark usage',   # Give a short description about your library
  author = 'Stephen Boesch',                   # Type in your name
  author_email = 'javadba@gmail.com',      # Type in your E-Mail
  url = 'https://github.com/javadba/pysparkifier',   # Provide either the link to your github or to your website
  license='Apache License 2.0',
  download_url = 'https://github.com/javadba/pysparkifier/archive/0.8.tar.gz',
  keywords = ['pyspark','utilities','utils'],
  install_requires=[ 'pyspark', 'pandas'],
  classifiers=[
    'Development Status :: 5 - Production/Stable',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
    'Intended Audience :: Developers',      # Define that your audience are developers
    'Topic :: Software Development :: Build Tools',
    # 'License :: OSI Approved :: Apache License, Version 2.0 (Apache-2.0)',   # Again, pick a license
    'Programming Language :: Python :: 3',      #Specify which pyhton versions that you want to support
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8'
  ],
)
