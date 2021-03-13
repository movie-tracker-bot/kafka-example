const path = require('path')
require('dotenv').config({ path: path.resolve(__dirname, '.env') })

const KafkaConsumer = require('./consumer-config')
console.log(`Starting mailing consumer | topic ${process.env.KAFKA_TOPIC} | group ${process.env.KAFKA_GROUP}`)
const consumerInstance = KafkaConsumer.getConsumerGroup(process.env.KAFKA_TOPIC, process.env.KAFKA_GROUP)
consumerInstance.on('message', (msg) => { console.log(`Message Recievied: ${JSON.stringify(msg)}`) })