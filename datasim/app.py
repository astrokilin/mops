import json
import time
import random
import os
import asyncio
import aiohttp

# service settings
SERVER_URL  = "http://controller:5000/api/data"
DEVICES_NUM = int(os.environ['DEVICES_NUM'])
QUERIES_SEC = int(os.environ['QUERIES_SEC'])
MAX_TIME_DIFF = 1000000000 # 1 second in nanoseconds


async def gen_message(start_time, this_task_ind):
    global MAX_TIME_DIFF
    global DEVICES_NUM
    global SERVER_URL

    if time.time_ns() - start_time >= MAX_TIME_DIFF:
        print(f"Task {this_task_ind}: not enought time")
        return

    async with aiohttp.ClientSession() as session:
        data = {
                "device": random.randint(1, DEVICES_NUM),
                "A": random.randint(1, 10),
                "timestamp": time.time()
                }
        print(f"Task {this_task_ind}: Sending data: {data}")

        try:
            async with session.post(SERVER_URL, json=data) as response:
                print(f"Task {this_task_ind }: Responce {response.status}")

        except Exception as e:
            print(f"Task {this_task_ind}: Error {e}")

async def main():
    global MAX_TIME_DIFF
    global QUERIES_SEC

    tasks_inds = list(range(QUERIES_SEC))
    tasks = []

    while True:
        start_time = time.time_ns()

        for ind in tasks_inds:
            tasks.append(asyncio.create_task(gen_message(start_time, ind)))

        await asyncio.gather(*tasks)
        tasks.clear()
        time_to_wait = MAX_TIME_DIFF - (time.time_ns() - start_time)

        if time_to_wait > 0:
            print(f"sleeping {time_to_wait} nanoseconds")
            await asyncio.sleep(time_to_wait / 1_000_000_000)


if __name__ == "__main__":
    asyncio.run(main())
