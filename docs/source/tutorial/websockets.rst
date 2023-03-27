Websocket
###########

Introduction
============

In this post, I will show you how to integrate kafka consumer with websocket server. The websocket server will send message to client when it receives message from kafka consumer. `source code <https://github.com/claudezss/heizer/tree/main/samples/websockets>`_


#. :ref:`Start kafka server and install heizer<preparation>`

#. Create a kafka topic `topic.test`. (you can create new topic in `kafka-ui <http://localhost:8080/ui/clusters/local/topics>`_)

    .. image:: ./../_static/websocket/create-topic.png
      :width: 400

#. Create websocket server `server.py` on `ws://localhost:8001`. `code <https://github.com/claudezss/heizer/blob/main/samples/websockets/server.py>`_

    .. literalinclude :: ./../../../samples/websockets/server.py
       :language: python

    .. code-block:: bash

        python server.py


#. Create websocket client `client.html` and listen to `ws://localhost:8001`. `html file <https://github.com/claudezss/heizer/blob/main/samples/websockets/client.html>`_

    .. literalinclude :: ./../../../samples/websockets/client.html
       :language: html

    and open `client.html` in browser

#. Publish message to kafka topic `topic.test` and you will see the message in browser

    .. image:: ./../_static/websocket/publish-kafka-msg.png
      :width: 400

#. You will see the message in browser

    .. image:: ./../_static/websocket/message-in-browser.png
      :width: 400
