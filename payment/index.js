const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'micro_store',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'payment' });
const producer = kafka.producer();

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'ORDER_PLACED', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());

      processPayment(order);
    }
  });
};

async function processPayment(order) {
  order = {
    ...order,
    status: 'PAID',
    paymentId: Math.floor(Date.now() / 1000),
  }

  await producer.connect();

  await producer.send({
    topic: 'ORDER_PAID',
    messages: [
      { value: JSON.stringify(order) },
    ]
  });

  await producer.disconnect();
}

run().catch(console.error);
