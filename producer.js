import { Kafka, logLevel } from "kafkajs";

const kafka = new Kafka({
  clientId: "my-producer",
  brokers: ["localhost:9092"],
  logLevel: logLevel.ERROR
});

const producer = kafka.producer();

const sendMessage = async () => {
  try{

    await producer.connect();
    console.log("✅ Producer connected");
    
    await producer.send({
      topic: "test-topic",
      messages: [{ value: "Hello Kafka from Node.js!" }],
    });
    
    console.log("📨 Message sent!");
  } catch (error){
    console.error("❌Error sending message", error)
  }finally{
    await producer.disconnect();
  }
};

sendMessage().catch(console.error);
