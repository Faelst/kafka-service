import express from 'express';
import { Kafka, logLevel } from 'kafkajs';
import routers from './routers';

const app = express();

const kafka = new Kafka({
  clientId: 'sirius-producer',
  brokers: ['localhost:9092'],
  retry: {
    initialRetryTime: 400,
    retries: 5,
  },
  logLevel: logLevel.ERROR,
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'sirius-service' });

app.use(express.json());
app.use((req, res, next) => {
  req.producer = producer;
  return next();
});
app.use(routers);

async function main() {
  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({ topic: 'certifications-processed' });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const data = message.value.toString();
      console.log(`Received message ${data}`);
    },
  });

  app.listen(3000, () => {
    console.log('Server running on port 3000');
  });
}

main().catch(console.error);
