// src/services/kafka.producer.ts
import { Kafka, Producer } from "kafkajs";

/* Singleton Kafka producer for Payments.MS */
let producer: Producer | null = null;

const kafka = new Kafka({
  clientId: "payments-ms",
  brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS ?? "localhost:9092"],
});

export type PaymentStatus = "APPROVED" | "REJECTED" | "PENDING";

/* Payload contract for the event to Orders.MS */
export interface PaymentCompletedEvent {
  // Business identifiers
  orderId: string;           // UUID from Orders.MS
  paymentId: string;         // Mongo _id as string
  amount: number;
  status: PaymentStatus;     // APPROVED | REJECTED | PENDING (we’ll emit only APPROVED/REJECTED)

  // Metadata
  occurredAt: string;        // ISO date
  source: "payments-ms";     // who emits
  version: number;           // event versioning
}

/* Lazily create and return a connected producer */
export async function getKafkaProducer(): Promise<Producer> {
  if (producer) return producer;
  producer = kafka.producer();
  await producer.connect();
  // eslint-disable-next-line no-console
  console.log("✅ Kafka producer connected (payments-ms)");
  return producer;
}

/* Publish a payment_completed domain event */
export async function publishPaymentCompleted(evt: PaymentCompletedEvent): Promise<void> {
  const p = await getKafkaProducer();
  const topic = "payment_completed"; // topic listened by Orders.MS

  // Key is the orderId to keep ordering per aggregate (good for compaction/partitioning)
  const key = evt.orderId;

  await p.send({
    topic,
    messages: [
      {
        key,
        value: JSON.stringify(evt),
        headers: {
          "content-type": "application/json",
          "x-event-name": "payment_completed",
          "x-event-version": String(evt.version ?? 1),
          "x-source": evt.source,
        },
      },
    ],
  });

  // eslint-disable-next-line no-console
  console.log(`📤 payment_completed published for order ${evt.orderId} (payment ${evt.paymentId})`);
}

/* Optional: graceful shutdown helper (to call on process exit) */
export async function disconnectKafkaProducer(): Promise<void> {
  if (producer) {
    await producer.disconnect();
    producer = null;
    // eslint-disable-next-line no-console
    console.log("👋 Kafka producer disconnected (payments-ms)");
  }
}
