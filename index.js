var config = require('config');
var amqpConnectionPromise = require('amqplib').connect(config.get('AMQP_URL'));
var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var amqpExchange = config.get('AMQP_EXCHANGE');
var amqpQueue = config.get('AMQP_QUEUE');

var mongoPromise = MongoClient.connect(config.get('MONGO_URL'));
amqpConnectionPromise.then(function (conn) {
    var channel = conn.createChannel();
    channel = channel.then(function (ch) {
        ch.assertQueue(amqpQueue, {autoDelete: true, deadLetterExchange: amqpExchange + "_dlx"});
        ch.bindQueue(amqpQueue, amqpExchange, 'event.#');
        ch.bindQueue(amqpQueue, amqpExchange, 'events');

        mongoPromise.then(function(db) {
            ch.consume(amqpQueue, function (msg) {
                if (msg !== null) {
                    insertToMongo(db, msg);
                    console.log(msg.content.toString());
                    ch.ack(msg);
                }
            });
        })
    });
    return channel;
}).then(null, console.warn);

function insertToMongo(db, msg)
{
    db.collection(config.get('MONGO_COLLECTION')).insertOne( JSON.parse(msg.content.toString()), function(err, result) {
        assert.equal(err, null);
        console.log("Inserted a document into the collection.");
    });
}
