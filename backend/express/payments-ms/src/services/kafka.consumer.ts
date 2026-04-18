// src/services/kafka.consumer.ts
import { Kafka, EachMessagePayload } from "kafkajs";
import { Payment } from "../models/payment.model";

const kafka = new Kafka({
  clientId: "payments-service",
  brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS ?? "localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "payments-group" });
const producer = kafka.producer(); // 🔥 agregamos productor DLT

const retryMap: Map<string, number> = new Map();
const MAX_RETRIES = 3;
const DLT_TOPIC = "order_created_dlq";

async function sendToDeadLetterTopic(key: string, value: string, reason: string) {
  try {
    await producer.send({
      topic: DLT_TOPIC,
      messages: [
        {
          key,
          value,
          headers: {
            "x-error-reason": reason,
            "x-origin-topic": "order_created",
            "x-timestamp": new Date().toISOString(),
          },
        },
      ],
    });
    console.warn(`💀 Message sent to DLQ (${DLT_TOPIC}): ${reason}`);
  } catch (err) {
    console.error("❌ Failed to send message to DLQ:", err);
  }
}

async function processMessage(payload: EachMessagePayload) {
  const { topic, partition, message } = payload;

  if (!message.value) return;

  const key = message.key?.toString() || `${topic}-${partition}-${message.offset}`;
  const retries = retryMap.get(key) ?? 0;

  try {
    const data = JSON.parse(message.value.toString());
    console.log("📩 Received order_created event:", data);

    const { orderId, amount } = data;
    if (!orderId || !amount) {
      throw new Error("Invalid message payload");
    }

    const payment = new Payment({ orderId, amount, status: "PENDING" });
    await payment.save();

    console.log(`💾 Payment created for order ${orderId}`);
    retryMap.delete(key); // limpiar reintentos
  } catch (err: any) {
    console.error(`❌ Error processing message (attempt ${retries + 1}):`, err.message);

    if (retries < MAX_RETRIES - 1) {
      retryMap.set(key, retries + 1);
      throw err; // no commit → Kafka lo reintentará
    } else {
      console.error(`🚨 Max retries reached for message ${key}. Sending to DLQ...`);
      await sendToDeadLetterTopic(key, message.value.toString(), err.message);
      retryMap.delete(key);
    }
  }
}

export async function startKafkaConsumer() {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: "order_created", fromBeginning: true });

  console.log("✅ Kafka consumer connected. Listening to 'order_created'...");

  await consumer.run({
    autoCommit: true,
    eachMessage: async (payload) => {
      await processMessage(payload);
    },
  });
}
