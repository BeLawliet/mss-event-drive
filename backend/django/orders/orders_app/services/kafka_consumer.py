# orders/orders_app/services/kafka_consumer.py
from kafka import KafkaConsumer
import json
import threading
import logging
import os
from django.db import transaction
from orders_app.models import Order

logger = logging.getLogger(__name__)

def start_payment_completed_consumer():
    """
    Inicia un hilo que escucha el tópico 'payment_completed' y actualiza las órdenes.
    """
    print("⚙️  Entrando a start_payment_completed_consumer()")

    def consume():
        print("🧵  Hilo del consumer Kafka iniciado")
        try:
            bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
            print("📡  Intentando conectar con Kafka...")
            consumer = KafkaConsumer(
                "payment_completed",
                bootstrap_servers=bootstrap_servers,
                group_id="orders-ms-group",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                enable_auto_commit=True,
                auto_offset_reset="earliest",
            )
            logger.info(f"Kafka consumer configured with bootstrap servers: {bootstrap_servers}")
            print("✅  Orders.MS Kafka consumer listening to 'payment_completed'...")

            for message in consumer:
                event = message.value
                print(f"📩  Recibido evento: {event}")

                try:
                    order_id = event.get("orderId")
                    status = event.get("status")

                    if not order_id or not status:
                        print("⚠️  Evento con payload inválido")
                        continue

                    # 🔍 Diagnóstico con actualización controlada
                    with transaction.atomic():
                        updated = Order.objects.filter(id=order_id).update(
                            status="PAID" if status == "APPROVED" else "FAILED"
                        )

                        if updated == 0:
                            print(f"⚠️  No se encontró la orden {order_id}")
                        else:
                            # Confirmamos leyendo desde DB
                            order = Order.objects.get(id=order_id)
                            print(f"✅  Orden {order_id} actualizada en BD a estado '{order.status}'")

                except Exception as e:
                    print(f"❌  Error procesando evento Kafka: {e}")

        except Exception as e:
            print(f"❌  Error general en el consumer: {e}")

    print("🚀  Lanzando hilo daemon para consumer Kafka")
    thread = threading.Thread(target=consume, daemon=True)
    thread.start()
