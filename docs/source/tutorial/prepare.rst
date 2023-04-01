.. _preparation:

Preparation
-----------

1. Install the latest version of heizer

.. note::

    The stable version of heizer is not yet released. To install the latest
    version, run the following command.

Install tagged version

.. code-block:: bash

    pip install heizer


Install dev version

.. code-block:: bash

    pip install --pre heizer


2. Spin up a kafka cluster

.. literalinclude :: ./../../../docker-compose.yaml
   :language: yaml

.. code-block:: bash

    docker-compose up -d

3. you can now access the kafka-ui at http://localhost:8080
