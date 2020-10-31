const path = require("path");
const fs = require("fs");
require("dotenv").config({
    path: path.join(__dirname, "../../.env")
});
const amqp = require("amqplib/callback_api");
const winston = require("winston");
const cheerio = require("cheerio");
const redis = require("redis");

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
                filename: path.join(__dirname, "../../logs/parser.log"),
                level: 'silly',
                maxsize: 5000
            }),
        ],
        level: 'silly'
    });

    logger.info(`Logger started`);
}

/**
 * Connect to redis
 */
let redisClient;
async function connectToRedis() {
    await new Promise((resolve, reject) => {
        redisClient = redis.createClient(process.env.REDIS_PORT, process.env.REDIS_HOST);
        redisClient.on('connect', () => {
            logger.info(`Connected to the redis store`);
            resolve();
        });
    });
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
 * Create enqueueChannel.
 * Published messages will be added to "to_enqueue" queue.
 * Frontier takes URLs from that queue
 */
let enqueueChannel;
const enqueueQueue = "to_enqueue";
async function createEnqueueChannel() {
    enqueueChannel = await new Promise((resolve, reject) => {
        rmqConnection.createChannel((err, channel) => {
            if (err) {
                logger.error(`Error creating a channel for enqueuing: ${err}`);
                reject(err);
                return;
            }
            resolve(channel);
        });
    });
    enqueueChannel.assertQueue(enqueueQueue, { durable: false });
    logger.info(`Enqueue channel has been registered and is ready to publish`);
}

/**
 * Create analyzerChannel.
 * This channel publishes messages which reach the
 * analyzer service through "to_analyze" queue
 */
let analyzerChannel;
const analyzeQueue = "to_analyze";
async function createAnalyzerChannel() {
    analyzerChannel = await new Promise((resolve, reject) => {
        rmqConnection.createChannel((err, channel) => {
            if (err) {
                logger.error(`Error creating a channel for analyzing: ${err}`);
                reject(err);
                return;
            }
            resolve(channel);
        });
    });
    analyzerChannel.assertQueue(analyzeQueue, { durable: false });
    logger.info(`Analyzer channel has been registered and is ready to publish`);
}

/**
 * Create consumer channel.
 * Consume messages from "to_parse" queue
 */
async function createConsumerChannel() {
    let channel = await new Promise((resolve, reject) => {
        rmqConnection.createChannel((error, channel) => {
            if (error) reject(error);
            else resolve(channel);
        });
    });

    let queueName = "to_parse";
    channel.assertQueue(queueName, {
        durable: false
    });
    
    logger.info("Consumer queue has been registered and is ready to consume");

    channel.consume(queueName, consume, {
        noAck: true
    });
}

/**
 * consume callback
 */
async function consume(message) {
    let id = message.content.readUInt32BE();
    logger.info(`Consumed URL ID ${id} to parse`);

    await parseLinks(id);
}

/**
 * Parse all links in a webpage
 */
async function parseLinks(urlId) {
    logger.verbose(`Parsing links of ${urlId}`);

    logger.verbose(`Getting URL for ID ${urlId}`);
    let url = await new Promise( (resolve, reject) => {
        redisClient.get(urlId, (err, reply) => {
            if (err) {
                logger.error(`Error finding URL ID: ${urlId}: ${err}`);
                reject();
            } else {
                resolve(reply);
            }
        });
    } );
    logger.debug(`URL for ID ${urlId} is ${url}`);

    $ = cheerio.load(fs.readFileSync(
        path.join(__dirname, "../../websites/" + urlId + "/index.html")
    ));

    links = $('a');
    $(links).each(function(i, link) {
        let finalUrl = $(link).attr('href');
        if (finalUrl.startsWith("/")) 
            finalUrl = (url + finalUrl);

        //Publish to "to_enqueue"
        publishToEnqueue(finalUrl);
        //Publish to "to_analyze"
    });
}

/**
 * Publish new link to analyzer 
 */
function publishToAnalyzer(){}

/**
 * PUblish to to_enqueue
 */
function publishToEnqueue(url) {
    logger.debug(`Publishing URL to enqueue: ${url}`);
    let message = { url: url };
    enqueueChannel.sendToQueue(enqueueQueue, Buffer.from(
        JSON.stringify(message)
    ));
}

/**
 * Start the service
 */
async function start() {
    await configureLogger();
    await connectToRmq();
    await connectToRedis();

    await createEnqueueChannel();
    await createAnalyzerChannel();

    await createConsumerChannel();
}

//Fire!
start();