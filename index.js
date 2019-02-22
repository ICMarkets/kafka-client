var {signal} = require('signal');
var Kafka = require('kafka-node');
var KeyedMessage = Kafka.KeyedMessage;
var {encode, decode} = require('msgpack5')();
var logger = require('js-logger');
var on_error = error => {logger.error(error);process.exit(1)};
var skip = (err) => null;
var global_producer;

var retry_count = 5;
var count = retry_count;
var min_timeout = 0;
var max_timeout = 30 * 60 * 1000;
var step = 1000;
var time = min_timeout;
var reset_timeout = () => {
    count = retry_count
    time = min_timeout
}
var timeout = () => {
    if (count > 0) {
        count -= 1
        return min_timeout
    }
    if (time > max_timeout) {
        return max_timeout
    }
    time += step
    return time
}

function create_producer (cb) {
    var client = new Kafka.KafkaClient({kafkaHost})
    var fresh_client = true;
    var timeout_id = setTimeout(() => {
        logger.error('Client not ready during timeout.')
        fresh_client = false;
        client.close()
        create_producer(cb)
    }, timeout())
    client.on('error', () => client.close())
    client.on('close', err => {
        if (global_producer == null) {
            setTimeout(() => create_producer(cb), timeout())
        }
    })
    client.on('ready', () => {
        if (!fresh_client) return;
        clearTimeout(timeout_id)
        var producer = new Kafka.Producer(client)
        var fresh_producer = true;
        timeout_id = setTimeout(() => {
            logger.error('Producer not ready during timeout.')
            fresh_producer = false
            producer.close()
            create_producer(cb)
        }, timeout())
        producer.on('ready', () => {
            if (!fresh_producer) return;
            global_producer = producer
            clearTimeout(timeout_id)
            reset_timeout()
            cb()
        })
        producer.on('error', err => {
            clearTimeout(timeout_id)
            logger.error(err)
            if (producer === global_producer) {
                global_producer = null
                setTimeout(() => create_producer(cb), timeout())
            }
        })
    })
}

function send (topic, message, done) {
    if (global_producer) global_producer.send([{topic, messages: encode(message)}], () => console.log('producer.send done', JSON.stringify({topic,message}) ) || (done || skip).call())
    else create_producer(() => send(topic, message, done))
}

function send_key (topic, key, message, done) {
    if (global_producer) global_producer.send([{topic, key, messages: [encode(message)]}], () => console.log('producer.send done', JSON.stringify({topic,key,message}) ) || (done || skip).call())
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
                consumer.on('message', message => console.log(String(message.key), decode(message.value)) || key(String(message.key)) && emit(decode(message.value)))
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
