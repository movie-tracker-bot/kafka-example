const kafka = require('kafka-node')
const { promisify } = require('bluebird')

const config = {
    'kafkaHost': process.env.KAFKA_HOST,
    'batch': undefined,
    'ssl': false,
    'groupId': process.env.KAFKA_GROUP,
    'sessionTimeout': 15000,
    'protocol': ['roundrobin'],
    'encoding': 'utf8',
    'fromOffset': 'earliest',
    'commitOffsetsOnFirstJoin': true,
    'outOfRangeOffset': 'earliest',
    'onRebalance': (isAlreadyMember, callback) => { callback() }
}

class KafkaConsumer {
    static getClient() {
        return new kafka.KafkaClient(config)
    }

    static createTopic(client) {
        client.createTopics([{
            'topic': process.env.KAFKA_TOPIC,
            'partitions': 2,
            'replicationFactor': 1
        }], (err) => {
            if (err) {
                console.log(err)
            }
        })
    }


    static getConsumer(client) {
        this.createTopic(client)
        return new kafka.Consumer(client, [], {
            'groupId': process.env.KAFKA_GROUP,
            'encoding': 'utf8',
            'autoCommit': true
        })
    }

    static getConsumerGroup(topic, group) {
        config.groupId = group
        const consumerGroup = new kafka.ConsumerGroup(config, topic)
        consumerGroup.pause = promisify(consumerGroup.pause)
        consumerGroup.resume = promisify(consumerGroup.resume)
        return consumerGroup
    }

    static onError(consumer) {
        consumer.on('error', (err) => {
            Log.err('Error in get message')
            console.log(err)
            Log.err('Pausing consumer...')
            consumer.pause()
        })
    }
}

module.exports = KafkaConsumer
