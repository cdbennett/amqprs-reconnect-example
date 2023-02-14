# amqp-rs connection loss and reconnecting example

Real applications will want to be robust and somehow handle the case where a
RabbitMQ broker is restarted or the connection is lost and then restored.

Without handling this situation, applications are fragile and could be broken
at any time and unless there is monitoring infrastructure in place, it would
not even be noticed immediately. The application would simply lose its
connection to the RabbitMQ server indefinitely, until the client program was
manually restarted.

This is an attempt to demonstrate how to implement production-grade
applications with amqp-rs by implementing robust connection management.
