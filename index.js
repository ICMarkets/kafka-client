var {signal} = require('signal');
var Kafka = require('kafka-node');
var KeyedMessage = Kafka.KeyedMessage;
var {encode, decode} = require('msgpack5')();
var logger = require('js-logger');
var on_error = error => {logger.error(error);process.exit(1)};
var skip = () => null;
var producer;

function create_producer (cb) {
    var client = new Kafka.KafkaClient({kafkaHost})
    client.on('error', () => producer = null)
    client.on('ready', () => {
        var p = new Kafka.Producer(client);
        var timeout = setTimeout(() => create_producer(cb), 1000);
        p.on('ready', () => {
            producer = p;
            clearTimeout(timeout);
            cb()
        });
        p.on('error', err => {
            clearTimeout(timeout);
            logger.error(err);
            producer = null;
            create_producer(cb);
        });
    })
}

function send (topic, message, done) {
    if (producer) producer.send([{topic, messages: encode(message)}], done || skip)
    else create_producer(() => send(topic, message, done))
}

function send_key (topic, key, message, done) {
    if (producer) console.log('send_key', topic, key) || producer.send([{topic, key, messages: [encode(message)]}], done || skip)
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

function stream_group (topic, groupId) {
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
                        groupId,
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
                consumer.on('message', message => key(String(message.key)) && emit(decode(message.value)))
            })
        )
    })
}

module.exports = _ => {
    kafkaHost = _;

    return {
        send,
        send_key,
        stream,
        stream_group,
        filter
    };
}
