from pymongo import MongoClient
import asyncio
import aiormq
import json
import os
import logging

RABBITMQ_URL = "amqp://user:password@rabbitmq/"
RABBIT_TASK_QUEUE_NAME = "queue"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
client = MongoClient(MONGO_URI)
db = client["mydatabase"]
collection = db["data"]

class JSONFormatter(logging.Formatter):
    def format(self, record):
        record_dict = {
            "level": record.levelname,
            "message": record.getMessage(),
            "time": self.formatTime(record),
            "module": record.module,
            "name": record.name,
        }
        return json.dumps(record_dict)

logger = logging.getLogger("rule_engine_logger")
handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

class RuleMatcher():
    def __init__(self):
        # define how rules are matched
        rule_acceptor_1 = lambda x: x["A"] > 5

        # device_id: rule_accept_func
        self.instant_rules = {10: rule_acceptor_1,}
        # device_id: (rule_accept_func, number of packets to trigger)
        self.ongoing_rules = {10: (rule_acceptor_1, 10)}
        self.__ongoing_rules_tracking_table = {}

    def match_rule(self, decoded_dict) -> list[bool, str, bool, str]:
        answ = [False, "", False, ""]

        # match instant rule
        rule_matcher = self.instant_rules.get(decoded_dict["device"])

        if rule_matcher is not None and rule_matcher(decoded_dict):
            answ[0] = True
            answ[1] = "instant rule"

        # match ongoing rule
        device_id = decoded_dict["device"]
        rule_data = self.ongoing_rules.get(device_id)

        if rule_data is not None:
            already_matched_num = self.__ongoing_rules_tracking_table.get(device_id, 0)

            if already_matched_num >= rule_data[1]:
                answ[2] = True
                answ[3] = "ongoing rule"
                self.__ongoing_rules_tracking_table[device_id] = 0

            else:
                self.__ongoing_rules_tracking_table[device_id] = already_matched_num + 1

        return answ

THE_GREAT_MATCHER = RuleMatcher()

async def on_message(message):
    message_dict = json.loads(message.body.decode())
    logger.info(f"Received message: {message_dict}")

    device_id = message_dict["device"]
    match = THE_GREAT_MATCHER.match_rule(message_dict)

    if (match[0]):
        collection.insert_one({"device": device_id, "rule": match[1]})
        logger.info(f"Inserted instant rule for device {device_id}")

    if (match[2]):
        collection.insert_one({"device": device_id, "rule": match[3]})
        logger.info(f"Inserted ongoing rule for device {device_id}")


async def consume_messages():

    # imagine creating such a shitty broker that i need to brute force the
    # connetion in a while loop
    while True:
        try:
            connection = await aiormq.connect(RABBITMQ_URL)
            channel = await connection.channel()
            declare_ok = await channel.queue_declare(queue=RABBIT_TASK_QUEUE_NAME, durable=True)
            consume_ok = await channel.basic_consume(declare_ok.queue, on_message, no_ack=True)
            break

        except aiormq.AMQPConnectionError as e:
            logger.error("Error while connecting to RabbitMQ", exc_info=e)
            await asyncio.sleep(2)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(consume_messages())
    loop.run_forever()
