var kafkaHost = 'kafka.devicm:9092';
var kafka = require('./index')(kafkaHost);
var topic   = 'test-topic-encoded-with-messagepack-print-pow2' + Math.random();
var pow2 = 1;

kafka.send(topic, pow2, () =>
    kafka.stream(topic).onValue(value => {
        console.log(value);
        pow2 += value;
        setTimeout(() => kafka.send(topic, pow2), 1000);
    }))
