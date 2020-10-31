const path = require("path");
const fs = require("fs");
require("dotenv").config({
    path: path.join(__dirname, "../../.env")
});
const amqp = require("amqplib/callback_api");
const winston = require("winston");
const redis = require("redis");
const scrape = require("website-scraper");

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
                filename: path.join(__dirname, "../../logs/fetcher.log"),
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
            
            logger.verbose(`Making sure ID counter is set`);
            redisClient.exists('urlIdSeq', (err, reply) => {
                if (err) {
                    logger.error(`Error checking key existence: urlIdSeq`);
                    reject();
                    return;
                }

                if (reply === 1) {
                    logger.verbose(`urlIdSeq already exists`);
                    resolve();
                } else {
                    logger.verbose(`urlIdSeq doesn't exist, creating with value = 1 `);
                    redisClient.set('urlIdSeq', 1, () => {
                        resolve();
                    });
                }
            });
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
 * Create consumer channel
 * This channel consumes from to_crawl queue
 */
async function createConsumerChannel() {
    let channel = await new Promise((resolve, reject) => {
        rmqConnection.createChannel((error, channel) => {
            if (error) reject(error);
            else resolve(channel);
        });
    });

    let queueName = "to_crawl";
    channel.assertQueue(queueName, {
        durable: false
    });
    
    logger.info("Consumer queue has been registered and is ready to consume");

    channel.consume(queueName, consume, {
        noAck: true
    });
}

/**
 * Create publisher channel.
 * This channel publishes messages to "to_parse" queue
 */
let publisherChannel;
const publishQueue = "to_parse";
async function createPublisherChannel() {
    publisherChannel = await new Promise((resolve, reject) => {
        rmqConnection.createChannel((err, channel) => {
            if (err) {
                logger.error(`Error creating a channel for publishing: ${err}`);
                reject(err);
                return;
            }
            resolve(channel);
        });
    });
    publisherChannel.assertQueue(publishQueue, { durable: false });
    logger.info(`Publisher channel has been registered and is ready to publish`);
}

/**
 * Create analyzer channel.
 * Messages published on this channel will be forwarded to the analyzer 
 * to make analysis and calculations
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
 * consume callback, called whenever a new message
 * is consumed from to_crawl
 */
async function consume(message) {
    let id = message.content.readUInt32BE();
    logger.info(`Consumed message to fetch URL ID: ${id}`);

    //Get URL associated with this ID
    let url = await new Promise( (resolve, reject) => {
        redisClient.get(id, (err, reply) => {
            if (err) {
                logger.error(`Error finding URL ID: ${id}: ${err}`);
                reject();
            } else {
                resolve(reply);
            }
        });
    } );
    logger.verbose(`URL matching ID: ${id} is: ${url}`);

    //Fetch and save
    let scrapingOptions = {
        urls: [ url ],
        directory: path.join(__dirname, "../../websites/" + id),
        sources: [] //only download HTML
    };
    try {
        const result = await scrape(scrapingOptions);
        logger.verbose(`Saved URL ID ${id}`);

        //Publish message to inform parser
        publishUrl(id);

        //Publish message to analyzer
        publishPageSize(id);

        logger.info(`Done fetching URL ID ${id}`);
    } catch (err) {
        logger.error(`Error scraping URL ID ${id}: ${err}`);
    }
}

/**
 * Publish a message to the "to_parse" queue
 */
function publishUrl(urlId) {
    logger.verbose(`Publising URL ID ${urlId} to be parsed`);
    let buf = Buffer.allocUnsafe(4);
    buf.writeUInt32BE(urlId);
    publisherChannel.sendToQueue(publishQueue, buf);
}

/**
 * Find webpage size and send it to be analyzed (to_analyze queue)
 */
function publishPageSize(urlId) {
    logger.verbose(`Publishing page size of ${urlId} to be analyzed`);

    //Get file size
    const stat = fs.statSync(path.join(
        __dirname, "../../websites/" + urlId + "/index.html"
    ));
    const size = stat.size;

    //Create message
    let message = {
        dataType: "FILE_SIZE",
        payload: {
            size: size,
            urlId: urlId
        }
    };
    //Publish
    analyzerChannel.sendToQueue(analyzeQueue, Buffer.from(
        JSON.stringify(message)
    ));
}

async function start() {
    //Configure the logger
    await configureLogger();

    //Connect to Redis
    await connectToRedis();

    //Connect to RabbitMQ
    await connectToRmq();

    //Create publisher channel, should be done before consuming
    //  so consumer won't fail on sending to undefined publisher
    await createPublisherChannel();

    //Create analyzer channel
    await createAnalyzerChannel();

    //Create consumer and start consuming
    await createConsumerChannel();
}

start();
