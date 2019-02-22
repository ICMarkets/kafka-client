var kafkaHost = 'kafka.dev.icm:9092';
var kafka = require('./index')(kafkaHost);
var topic   = 'test-topic-encoded-with-messagepack-print-pow2' + Math.random();
var message = Math.random();

describe('pub/sub', () =>
    it('send 1 to topic & get 1 from topic', () =>
        kafka.send(topic, message, () =>
            kafka.stream(topic).onValue(value =>
                done(
                    message === value
                        ? undefined
                        : value
                    )))))

describe('req/res', () =>
    it('send 3 same message should stream with 3 messages', () => {
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

        messages.forEach(message => kafka.send(topic, message))
    }))
