const path = require('path')
require('dotenv').config({ path: path.resolve(__dirname, '.env') })

const Producer = require('./prodoucer')
for (let i = 0; i < 10; i++) {
    Producer.sendMessage(process.env.KAFKA_TOPIC, `message of number ${i * 2}`)
}