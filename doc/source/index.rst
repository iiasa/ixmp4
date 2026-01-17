The ixmp4 package for scenario data management
==============================================

Copyright © 2023-2024 IIASA - Energy, Climate, and Environment Program (ECE)

|license| |ruff| |python|

.. |license| image:: https://img.shields.io/badge/license-MIT-brightgreen
   :target: https://github.com/iiasa/ixmp4/blob/main/LICENSE

.. |ruff| image:: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json
   :target: https://github.com/astral-sh/ruff

.. |python| image:: https://img.shields.io/badge/python-3.10_|_3.11_|_3.12-blue?logo=python&logoColor=white
   :target: https://github.com/iiasa/ixmp4

Overview
--------

The **ixmp4** package is a data warehouse for high-powered scenario analysis
in the domain of integrated assessment of climate change and energy systems modeling.


.. toctree::
   :maxdepth: 1

   installation
   setup
   data-model

.. toctree::
   :caption: Usage
   :maxdepth: 1

   configuration
   auth
   cli

   core/platform
   core/run
   core/region
   core/unit
   core/model
   core/scenario
   
   core/meta
   core/docs

   core/iamc
   core/optimization

   locks-and-versioning
   exceptions

.. toctree::
   :caption: Development
   :maxdepth: 1
   
   architecture
   data/modules
   server/modules
   database
   tests
   docker

.. toctree::
   :caption: Reference
   :maxdepth: 1

   references
   license
   funding
   contributors

.. include:: funding.rst
