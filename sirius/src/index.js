import { Kafka, logLevel } from 'kafkajs';

const kafka = new Kafka({
  brokers: ['localhost:9092'],
  clientId: 'sirius-consumer',
  logLevel: logLevel.ERROR,
});

const topic = 'certifications';
const consumer = kafka.consumer({ groupId: 'sirius-service' });

const producer = kafka.producer();

const counter = 0;

async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      counter++;
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      console.log(`- ${prefix} ${message.value}`);

      setTimeout(async () => {
        await producer.send({
          topic: 'certifications-processed',
          messages: [{ value: `Certificado ${counter}` }],
        });
      }, 1000);
    },
  });
}

run().catch(console.error);
