'use strict';

module.exports = {
    db: process.env.MONGO_STRING || 'mongodb://10.240.162.13,10.240.253.155/one-platform?replicaSet=rs0',
    dbOptions: { useNewUrlParser: true },
    mqttoptions: {
        clientId: 'worker_gateway',
        username: 'worker',
        password: process.env.MQTT_PASSWORD || ''
    }
};
