var {signal_changes} = require('signal');
var Kafka = require('kafka-node');
var KeyedMessage = Kafka.KeyedMessage;
var {encode, decode} = require('msgpack5')();
var logger = require('js-logger');
var on_error = error => {logger.error(error);process.exit(1)};
var skip = () => null;
var producer;

var retry_count = 5;
var count = retry_count;
var min_timeout = 0;
var max_timeout = 30 * 60 * 1000;
var step = 1000;
var time = min_timeout;
var reset_timeout = () => {
    count = retry_count;
    time = min_timeout;
};
var timeout = () => {
    if (count > 0) {
        count -= 1;
        return min_timeout;
    }
    if (time > max_timeout) {
        return max_timeout;
    }
    time += step;
    return time;
};

function create_producer (cb) {
    var client = new Kafka.KafkaClient({kafkaHost})
    client.on('error', () => producer = null)
    client.on('ready', () => {
        var p = new Kafka.Producer(client);
        var timeout = setTimeout(() => create_producer(cb), 1000);
        p.on('ready', () => {
            producer = p;
            clearTimeout(timeout);
            reset_timeout();
            cb()
        });
        p.on('error', err => {
            clearTimeout(timeout);
            logger.error(err);
            producer = null;
            setTimeout(() => create_producer(cb), timeout());
        });
    })
}

function send (topic, message, done) {
    if (producer) producer.send([{topic, messages: encode(message)}], done || skip)
    else create_producer(() => send(topic, message, done))
}

function send_key (topic, key, message, done) {
    if (producer) producer.send([{topic, key, messages: [encode(message)]}], done || skip)
    else create_producer(() => send(topic, message, done))
}

function stream (topic) {
    return signal_changes(emit => {
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
    return signal_changes(emit => {
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
    return signal_changes(emit => {
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
