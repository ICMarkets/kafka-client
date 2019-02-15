var kafkaHost = 'kafka.devicm:9092';
var kafka = require('./index')(kafkaHost);
var topic   = 'test-topic-encoded-with-messagepack-print-pow2' + Math.random();
var pow2 = 1;

describe('pub/sub', () =>
    it('send 1 to topic & get 1 from topic', () =>
        kafka.send(topic, pow2, () =>
            kafka.stream(topic).onValue(value =>
                done(
                    pow === value
                        ? undefined
                        : value
                    )))))
