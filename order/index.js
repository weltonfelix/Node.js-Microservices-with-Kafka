const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();

app.use(express.json());

const kafka = new Kafka({
  clientId: 'micro_store',
  brokers: ['localhost:9092'],
});

app.get('/', (request, response) => {
  return response.send('Hello, Order API');
});

app.post('/order', async (request, response) => {
  const order = request.body;

  order.id = Math.floor(Date.now() / 1000);

  order.status = 'PAYMENT_PENDING';

  const producer = kafka.producer();
  await producer.connect();

  await producer.send({
    topic: 'ORDER_PLACED',
    messages: [
      { value: JSON.stringify(order) },
    ]
  })

  await producer.disconnect();

  return response.json({
    order,
    message: 'Order successfully created. We are analyzing your payment. You will receive more information soon.',
  });
});

app.listen(3001, () => {
  console.log('Express API: https://localhost:3001')
});