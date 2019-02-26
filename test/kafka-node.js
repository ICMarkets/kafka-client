var assert = require('assert')
var Kafka = require('kafka-node')
var client1//for consumer1
var client2//for consumer2
var client3//for producer
var consumer1
var consumer2
var producer
var topic = 'kafka-test';

describe('kafka stream example', () => {
    before(() => {
        var kafkaHost = 'kafka.dev.icm:9092';
        client1    = new Kafka.KafkaClient({kafkaHost})
        client2    = new Kafka.KafkaClient({kafkaHost})
        client3    = new Kafka.KafkaClient({kafkaHost})

        return Promise.all([
            new Promise((resolve, reject) => {
                client1.on('error', reject)
                client1.on('ready', resolve)
            }),
            new Promise((resolve, reject) => {
                client2.on('error', reject)
                client2.on('ready', resolve)
            }),
            new Promise((resolve, reject) => {
                client3.on('error', reject)
                client3.on('ready', ()  => client3.createTopics([topic], (error, result) => error ? reject(error) : resolve()))
            }),
            new Promise((resolve, reject) => {
                producer  = new Kafka.Producer(client3)
                producer.on('error', reject)
                producer.on('ready', resolve)
            })
        ])
    })

    it('two consumers of one topic in different groups should get same last message', done => {
        new Kafka.Offset(client3).fetch([{ topic, time: -1}], function (err, data) {
            offset = data[topic]['0'][0] - 1;

            consumer1 = new Kafka.Consumer(client1, [{topic, offset}], {fromOffset: true, groupId: 'consumer1'})
            consumer2 = new Kafka.Consumer(client2, [{topic, offset}], {fromOffset: true, groupId: 'consumer2'})

            consumer1.on('error', done)
            consumer2.on('error', done)
            producer.on( 'error', done)

            Promise.all([
                new Promise(resolve => consumer1.on('message', m => resolve(m.value))),
                new Promise(resolve => consumer2.on('message', m => resolve(m.value)))
            ]).then(([m1, m2]) => {
                assert.equal(m1, m2)
                done()
            })
        });
    });

    it('new message should came to both consumers', done => {
        var message = 'payload' + Math.random();
        Promise.all([
            new Promise(resolve => consumer1.on('message', m => resolve(m.value))),
            new Promise(resolve => consumer2.on('message', m => resolve(m.value))),
            new Promise((resolve, reject) => producer.send([{topic, messages: message}], error => error ? reject() : resolve()))
        ]).then(([m1, m2]) => {
            assert.equal(m1, message)
            assert.equal(m2, message)
            done()
        })
    })
})
