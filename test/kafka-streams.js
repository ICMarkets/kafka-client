const {KafkaStreams} = require('kafka-streams');
const kafkaStreams = new KafkaStreams({
    kafkaHost: 'kafka.dev.icm:9092',
    logger: {
        debug: msg => console.log('logger', msg),
        info : msg => console.log('logger', msg),
        warn : msg => console.log('logger', msg),
        error: msg => console.log('logger', msg)
    },
    groupId: 'kafka-streams-test' + Math.random(),
    clientName: 'kafka-streams-test-name' + Math.random(),
    workerPerPartition: 1,
    options: {
        sessionTimeout: 8000,
        protocol: ['roundrobin'],
        fromOffset: 'latest',//'earliest', //latest
        fetchMaxBytes: 1024 * 100,
        fetchMinBytes: 1,
        fetchMaxWaitMs: 10,
        heartbeatInterval: 250,
        retryMinTimeout: 250,
        autoCommit: false,
        autoCommitIntervalMs: 1000,
        requireAcks: 1,
        ackTimeoutMs: 100,
        partitionerType: 3
    }
})


const topic = 'kafka-test'
const c = kafkaStreams.getKStream(topic)

c.forEach(m => console.log('consumer message', m))

c.start().then(
    () => console.log('consumer started'),
    e  => console.log('consumer error', e)
)

setTimeout(() => {
    const p = kafkaStreams.getKStream(null)
    p.to(topic)
    p.start().then(
        () => {
            console.log('producer started')
            p.writeToStream(Math.random() + 'ppppp')
        },
        e => console.log('producer error', e)
    )
}, 3000)
