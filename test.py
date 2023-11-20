import aiohttp
import asyncio
# from datetime import datetime

async def make_request(url, i):
    # current_time = datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    # print(f"[{i}]Current Time = ", current_time)

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()

async def main():
    url = 'http://localhost:5000/ping'

    tasks = []
    x = 15
    requests_per_second = 5  # Adjust as needed
    delay_between_requests = 1 / (requests_per_second)

    # 0 delay 1 delay 2 ... 15

    for i in range(x):
        task = asyncio.create_task(make_request(url, i))
        tasks.append(task)

        # Introduce a delay between requests
        print("\n----\n")
        await asyncio.sleep(delay_between_requests)

    responses = await asyncio.gather(*tasks)

    for response in responses:
        print(response)

if __name__ == '__main__':
    asyncio.run(main())
