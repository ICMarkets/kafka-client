var {signal} = require('signal');
var Kafka = require('kafka-node');
var {encode, decode} = require('msgpack5')();
var logger = require('js-logger');
var on_error = error => {logger.error(error);process.exit(1)};
var skip = () => null;
var producer;

function create_producer (cb) {
    var client = new Kafka.KafkaClient({kafkaHost})
    client.on('error', () => producer = null)
    client.on('ready', () => {
        producer = new Kafka.Producer(client);
        cb()
    })
}

function send (topic, message, done) {
    if (producer) producer.send([{topic, messages: encode(message)}], done || skip)
    else create_producer(() => send(topic, message, done))
}

function stream (topic) {
    return signal(emit => {
        var client = new Kafka.KafkaClient({kafkaHost})
        client.on('error', on_error)
        client.on('ready', () =>
            new Kafka.Offset(client).fetch([{ topic, time: -1}], (error, offsets_per_partitions) => {
                if (error) on_error(error)
                var offset = offsets_per_partitions[topic]['0'][0] - 1
                var consumer = new Kafka.Consumer(
                    client,
                    [{topic, offset}],
                    {
                        fromOffset: true,
                        groupId: String(Date.now()) + Math.random(),
                        encoding: 'buffer'
                    }
                )
                consumer.on('error',  on_error)
                consumer.on('message', message => emit(decode(message.value)))
            })
        )
    })
}

function filter (topic, key) {
    return signal(emit => {
        var client = new Kafka.KafkaClient({kafkaHost})
        client.on('error', on_error)
        client.on('ready', () =>
            new Kafka.Offset(client).fetch([{ topic, time: -1}], (error, offsets_per_partitions) => {
                if (error) on_error(error)
                var offset = offsets_per_partitions[topic]['0'][0] - 1
                var consumer = new Kafka.Consumer(
                    client,
                    [{topic, offset}],
                    {
                        fromOffset: true,
                        groupId: String(Date.now()) + Math.random(),
                        encoding: 'buffer'
                    }
                )
                consumer.on('error',  on_error)
                consumer.on('message', message => key(message.key) && emit(decode(message.value)))
            })
        )
    })
}

module.exports = _ => {
    kafkaHost = _;

    return {send, stream, filter};
}
