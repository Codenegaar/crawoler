const path = require("path");
const fs = require("fs");
require("dotenv").config({
    path: path.join(__dirname, "../../.env")
});
const amqp = require("amqplib/callback_api");
const winston = require("winston");

const Bar = require("./Bar");
const Stats = require("./Stats");

/**
 * Configure the logger
 */
let logger;
async function configureLogger() {
    const format = winston.format.combine(
        winston.format.colorize(),
        winston.format.align(),
        winston.format.cli({ colors: {
            info: 'blue',
            error: 'red',
            warning: 'yellow',
        }})
    );

    logger = new winston.createLogger({
        transports: [
            new winston.transports.Console({
                format: format
            }),
            new winston.transports.File({
                filename: path.join(__dirname, "../../logs/analyzer.log"),
                level: 'silly',
                maxsize: 5000
            }),
        ],
        level: 'silly'
    });

    logger.info(`Logger started`);
}

/**
 * Connect to rabbitMQ
 */
let rmqConnection;
async function connectToRmq() {
    //Create connection
    rmqConnection = await new Promise((resolve, reject) => {
        amqp.connect(
            "amqp://" + process.env.AMQP_HOST,
            (error, connection) => {
                if (error) {
                    reject(error);
                } else {
                    logger.info(`Connected to rabbitmq`);
                    resolve(connection);
                }
            }
        );
    });
}

/**
 * Create consumer channel
 * This channel consumes from to_crawl queue
 */
async function createConsumerChannel(stats) {
    let channel = await new Promise((resolve, reject) => {
        rmqConnection.createChannel((error, channel) => {
            if (error) reject(error);
            else resolve(channel);
        });
    });

    let queueName = "to_analyze";
    channel.assertQueue(queueName, {
        durable: false
    });
    
    logger.info("Consumer queue has been registered and is ready to consume");

    channel.consume(queueName, consume(stats), {
        noAck: true
    });
}

/**
 * consume callback to process message
 * and update analytic data
 */
function consume(stats) {
    return function (message) {
        let parsed = JSON.parse(message.content.toString());
        switch(parsed.event) {
            case 'FETCH':
                let size = parsed.payload.size;
                stats.pageFetched(size);
                break;
            case 'DISCOVER':
                let url = parsed.payload.url;
                stats.linkDiscovered(url);
                break;
        }
    }
}

/**
 * Configure and connect
 */
async function start() {
    //Configure the logger
    await configureLogger();

    //Connect to RabbitMQ
    await connectToRmq();

    //Create progress bars
    let bar = new Bar();

    //Create stats analyzer
    let stats = new Stats(bar);

    //Create consumer and start consuming
    await createConsumerChannel(stats);
}

//Fire!
start();
