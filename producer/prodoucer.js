const { KafkaClient, HighLevelProducer } = require('kafka-node')
const uuid = require('uuid/v1')

const client = new KafkaClient({ 'kafkaHost': process.env.KAFKA_CLIENT })
const producer = new HighLevelProducer(client)

const Promise = require('bluebird')
Promise.promisify(producer.send)

class Producer {
    static sendMessage(topic, message) {
        const code = uuid()
        let payload = [{
            'key': code,
            'topic': process.env.KAFKA_TOPIC,
            'messages': message,
            'timestamp': Date.now()
        }]

        console.log(`Starting sending a message to topic: ${topic} | uuid: ${code}`)
        producer.send(payload, (err) => {
            if (err) {
                console.log(`Error to send message to topic: ${err}`)
                return null
            }
        })

        console.log(`Message sent to topic: ${topic} | uuid: ${code}`)
        return code
    }
}

module.exports = Producer
