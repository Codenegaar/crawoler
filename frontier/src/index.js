const path = require("path");
require("dotenv").config({
    path: path.join(__dirname, "../../.env")
});
const amqp = require("amqplib/callback_api");
const winston = require("winston");
const redis = require("redis");

/**
 * Configures the logger
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
                filename: path.join(__dirname, "../../logs/frontier.log"),
                level: 'silly',
                maxsize: 5000
            }),
        ],
        level: 'silly'
    });

    logger.info(`Logger started`);
}

/**
 * Connect to redis store
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
 * Connect to rabbitmq
 */
let rmqConnection;
async function connectToRabbitmq() {
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
 * Create a channel for consuming from to_enqueue
 */
let consumerChannel;
async function createConsumerChannel() {
    //Create channel
    consumerChannel = await new Promise((resolve, reject) => {
        rmqConnection.createChannel((error, channel) => {
            if (error) reject(error);
            else resolve(channel);
        });
    });

    let queueName = "to_enqueue";
    consumerChannel.assertQueue(queueName, {
        durable: false
    });
    consumerChannel.prefetch(1);
    
    logger.info("Consumer queue has been registered and is ready to consume");

    consumerChannel.consume(queueName, consume, {
        noAck: false
    });
}

/**
 * Create a channel to publish to "to_crawl"
 */
let publisherChannel;
const publishQueue = 'to_crawl';
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
    logger.info(`Publisher queue has been registered and is ready to publish`);
}

/**
 * Publish a URL to be crawled
 */
function publishUrl(urlId) {
    let buf = Buffer.allocUnsafe(4);
    buf.writeUInt32BE(urlId);
    publisherChannel.sendToQueue(publishQueue, buf);

    logger.info(`Published URL with ID ${urlId} to be crawled`);
}

/**
 * Starts consuming the to_enqueue queue.
 * Processes the consumed URL 
 * if matches the host
 */
async function consume(message) {
    let payload = message.content.toString();
    let url;
    try {
        url = JSON.parse(payload).url;
    } catch (err) {
        logger.verbose(`Provided payload is not a parsable JSON object`);
        return;
    }

    logger.info(`Consumed from to_enqueue: ${url}`);
    let match = await matchesHost(url);
    
    if (match) {
        //URL matches the host, process and enqueue it if valid
        logger.verbose(`Consumed URL ${url} matches host ${process.env.FRONTIER_HOST}`);
        await processUrl(url, message);
    } else {
        logger.verbose(`Consumed URL ${url} did not match host ${process.env.FRONTIER_HOST}`);
    }
}

/**
 * Check if the consumed URL matches the host
 */
async function matchesHost(url) {
    let hostReg = new RegExp("^(http://)?" + process.env.FRONTIER_HOST + ".*$");
    return url.match(hostReg);
}

/**
 * Processes a received URL and adds it to the to_crawl queue.
 * Checks if the URL has not been processed before 
 */
async function processUrl(url, message) {
    logger.verbose(`Processing URL: ${url}`);

    //Check if URL exists in the store
    redisClient.exists(url, (err, reply) => {
        if (err) {
            logger.error(`Error checking existence of URL ${url}: ${err}`);
            //Acknowledge
            consumerChannel.nack(message);
            return;
        }

        if (reply === 1) {
            logger.info(`URL ${url} already exists`);
            //Acknowledge
            consumerChannel.ack(message);
            return;
        }

        //URL is new
        //Add it to the store
        redisClient.get('urlIdSeq', (err, reply) => {
            if (err) {
                logger.error(`Error getting urlIdSeq: ${err}`);
                //Acknowledge
                consumerChannel.nack(message);
                return;
            }

            //Add URL:ID pair
            redisClient.set(url, reply);
            //Add ID:URL pair
            redisClient.set(reply, url);
            //Increment sequence counter
            redisClient.incr('urlIdSeq', (err, nextId) => {
                if (err) {
                    logger.error(`Error updating URL ID Sequence: ${err}`);
                    return;
                }

                logger.verbose(`URL is new and has been assigned with ID: ${reply}`);
                //Put URL in queue
                publishUrl(reply);
                //Wait and Acknowledge
                setTimeout(() => {
                    consumerChannel.ack(message);
                }, 5000);
            });
        });
    });
}

/**
 * Start the frontier
 */
async function start() {
    //Configure logger
    await configureLogger();

    //Connect to the redis store
    await connectToRedis();

    //Connect to rabbitmq
    await connectToRabbitmq();

    //Create the publisher channel
    await createPublisherChannel();

    //Create the consumer channel
    await createConsumerChannel();
}

//Fire!
start();