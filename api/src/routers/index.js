import express from 'express';
const router = express.Router();
import { CompressionTypes } from 'kafkajs';

router.get('/', (req, res) => {
  res.send('Hello World!');
});

router.post('/certifications', async (req, res) => {
  const bodyData = req.body;
  const producer = req.producer;

  await producer.send({
    topic: 'certifications',
    compression: CompressionTypes.GZIP,
    messages: [{ key: 'key 1', value: JSON.stringify(bodyData) }],
  });

  res.status(200).send(bodyData);
});

export default router;
