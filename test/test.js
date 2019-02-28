var kafkaHost = 'kafka.devicm:9092';
//var kafkaHost = 'ark-03.srvs.cloudkafka.com:9094,ark-02.srvs.cloudkafka.com:9094,ark-01.srvs.cloudkafka.com:9094';
var kafka = require('../index')(kafkaHost);
var topic   = 'test-topic-' + Math.random();
var message = Math.random();
var logger = require('js-logger')

logger.setAppName('Test kafka-client')

describe('pub/sub', () =>
    it('send 1 to topic & get 1 from topic', done =>
        kafka.send(topic, message, () =>
            kafka.stream(topic).onValue(value =>
                done(
                    message === value
                        ? undefined
                        : value
                    )))))

describe('req/res', () =>
    it('send 3 same message should stream with 3 messages', done => {
        var
            same_message = Math.random(),
            messages = [
                same_message,
                same_message,
                same_message
            ],
            receive = 0

        kafka.stream(topic).onValue(value => {
            if (value === same_message)
                receive += 1
            if (receive === messages.length)
                done()
        })

        messages.forEach(message => kafka.send(topic, message, e => console.log('sent', e)))
    }))
