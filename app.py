import asyncio
import uuid
import logging

from aiokafka import AIOKafkaConsumer, ConsumerRecord


logging.basicConfig(level=logging.INFO)

async def main():
    consumer = AIOKafkaConsumer(
        # "debezium.chii.bangumi.chii_members",
        "debezium.chii",
        bootstrap_servers="192.168.1.3:29092",
        group_id=f"tg-notify-bot-{uuid.uuid4()}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        msg: ConsumerRecord
        async for msg in consumer:
            print(msg.topic)
            await consumer.commit()

    finally:
        await consumer.stop()

asyncio.run(main())
