const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'micro_store',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'invoice' });
const producer = kafka.producer();

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'ORDER_PAID', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());

      billOrder(order);
    }
  });
};

async function billOrder(order) {
  order.nfe = `nfe_${order.id}_${new Date().getFullYear()}.xml`;
  order.status = 'BILLED';

  await producer.connect();
  
  await producer.send({
    topic: 'ORDER_BILLED',
    messages: [
      { value: JSON.stringify(order) },
    ]
  });

  await producer.disconnect();
}

run().catch(console.error);
