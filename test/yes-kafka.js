var fs = require('fs')
var path = require('path')
var Kafka = require('yes-kafka')
var kafkaHost = 'ark-03.srvs.cloudkafka.com:9094,ark-02.srvs.cloudkafka.com:9094,ark-01.srvs.cloudkafka.com:9094';

producer = new Kafka.Producer({
  connectionString: kafkaHost, // should match `listeners` SSL option in Kafka config
  ssl: {
    ca: fs.readFileSync(path.join(__dirname, '..', 'ca'), 'utf8')
  }
});

var CLOUDKARAFKA_TOPIC_PREFIX="drhgn7ak-"
var topic = CLOUDKARAFKA_TOPIC_PREFIX + 'test'

producer.init().then(
    () =>
        producer
            .send({
                topic,
                partition: 0,
                message: {
                    value: 'Hello!'
                }
            }).then(
                result => console.log(r),
                e => console.error(e)),
    e => console.error(e))
