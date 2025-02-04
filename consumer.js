import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "my-consumer",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "test-group" });

const run = async () => {
  while (true) {

    try{
      
      await consumer.connect();
      console.log("✅ Consumer connected");
      
      await consumer.subscribe({ topic: "test-topic", fromBeginning: true });
      
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          console.log(`📩 Received: ${message.value.toString()}`);
        },
      });
      break;
    } catch(error) {
      console.error("❌Error reading message", error);
      console.log("🔁 Retrying in 5s ...");
      await new Promise(res => setTimeout(res, 5000));
    }
  }
  };

run();
