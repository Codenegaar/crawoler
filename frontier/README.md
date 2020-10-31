# URL Frontier for Crawoler

Crawoler is a basic web crawler I'm creating as a university project.

The URL frontier is responsible of receiving new URLs, deciding on whether they should be fetched
or not, and putting them in another queue for fetching.

## Architecture

Reads from a RabbitMQ queue named to\_enqueue,
checks if the URL belongs to the required host,
checks if the URL is not duplicate,
creates a pair in the redis store if the URL is new,
and finally publishes it to the to\_crawl queue.

## Requirements

Redis server should be installed and running.

RabbitMQ server should be installed and running.

## Configuration

Basinc configuration can be done by editing the .end file.

## Resources

* [RabbitMQ tutorial](https://www.rabbitmq.com/tutorials/tutorial-one-javascript.html)
* [Using Redis with Node.js](https://www.sitepoint.com/using-redis-node-js/)

