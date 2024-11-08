import asyncio
import random
import aiohttp
import itertools
import uvloop # linux

from typing import List, Optional
from datetime import datetime
from string import digits, ascii_letters
from concurrent.futures import ProcessPoolExecutor, wait

WORKER_COUNT = 6
TASK_COUNT = 230

PROXY = ''
ROTATE_EVERY = 10_000 # rotate proxies every N  http requests
MAX_RETRY = 5 # max retries per item

INPUT_FILENAME  = ''
OUTPUT_FILENAME = 'OUTPUT_FILENAME.txt'

HEADERS = {
    'User-Agent': '7331',
}

URL = 'https://checkip.amazonaws.com/'

async def trigger(item: str):
    if item == '1337':
        print(f'{asyncio.current_task().get_name()}: Found: {item}')

def get_proxy(proxy: Optional[str] = PROXY, k: int = 6) -> str:
    return f'{proxy}?{"".join(random.choices(ascii_letters + digits, k=k))}' if proxy else ''

async def aiorequest(client: aiohttp.ClientSession, method: str = 'GET', **kwargs):
    # json=orjson.loads,
    async with client.request(method, **kwargs) as response:
        return response.status, await response.text()

async def task_aiohttp(client: aiohttp.ClientSession, input_queue: asyncio.Queue, output_queue: asyncio.Queue) -> None:

    await asyncio.sleep(random.uniform(0.1, 1))
    proxy = get_proxy()
    for idx in itertools.count():
        if idx % ROTATE_EVERY == 0:
            proxy = get_proxy()

        try:
            attempt, item = await asyncio.wait_for(input_queue.get(), timeout=0.5)

            try:
                status_code, response_json = await aiorequest(client, method='GET', url=URL, proxy=proxy, raise_for_status=True)
                await trigger(item)
                #print(proxy, status_code, str(response_json).strip())
                output_queue.put_nowait(item)

            except asyncio.CancelledError:
                raise

            except Exception as err:
                attempt += 1
                if attempt <= MAX_RETRY:
                    input_queue.put_nowait((attempt, item))

                if not isinstance(err, aiohttp.ClientError): # this is too verbose... it increments on proxy errors rather than retry errors like rate limits.
                    raise

                proxy = get_proxy()

            finally:
                input_queue.task_done()

        except asyncio.TimeoutError:
            if input_queue.empty():
                break

        except asyncio.CancelledError:
            #print(f'Shutting down {asyncio.current_task().get_name()}...')
            raise

async def worker_aiohttp(name: str, chunk: List, task_count: int = TASK_COUNT):
    asyncio.current_task().set_name(name)

    input_queue = asyncio.Queue()
    for item in chunk:
        input_queue.put_nowait((0, item))

    output_queue = asyncio.Queue()

    resolver = aiohttp.AsyncResolver(nameservers=['8.8.8.8', '8.8.4.4'])
    connector = aiohttp.TCPConnector(
        limit=0,
        force_close=False,
        ssl=False,
        ttl_dns_cache=10_000,
        resolver=resolver,
    )


    timeout = aiohttp.ClientTimeout(
        total=5,  # Total timeout for the request
        sock_connect=2,  # Timeout for establishing a connection
        sock_read=2  # Timeout for reading data
    )

    headers = HEADERS.copy()

    async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers) as session:
        task_count = max(1, min(task_count, input_queue.qsize()))
        tasks = {
            asyncio.create_task(task_aiohttp(session, input_queue, output_queue))
            for _ in range(task_count)
        }

        try:
            await input_queue.join()

        except asyncio.CancelledError:
            pass
            #print(f'Shutting down {asyncio.current_task().get_name()}...')

        finally:
            for task in tasks:
                task.cancel()

            await asyncio.gather(*tasks, return_exceptions=True)

    results = []
    while not output_queue.empty():
        item = output_queue.get_nowait()
        results.append(item)

    return results

def run_worker(name: str, chunk: List, task_count: int = TASK_COUNT):
    uvloop.install()
    return asyncio.run(worker_aiohttp(name, chunk, task_count))

def split_input_list(items: List, items_count: int, worker_count: int = WORKER_COUNT):
    chunk_size, remainder = divmod(items_count, worker_count)
    return [items[i * chunk_size + min(i, remainder):(i + 1) * chunk_size + min(i + 1, remainder)] for i in range(worker_count)]


if __name__ == '__main__':

    items = [str(x) for x in range(250_000)] # 250,000 items for the HTTP requests
    items_count = len(items)

    chunks = split_input_list(items, items_count)

    start_time = datetime.now()
    print(start_time.strftime('START: %B %d, %Y at %I:%M:%S %p'))

    # run 1 core
    #results = run_worker('Worker_1', items)

    # run N cores
    # multiprocessing
    results = []
    with ProcessPoolExecutor(max_workers=WORKER_COUNT) as executor:
        futures = {
            executor.submit(run_worker, f'Worker_{i}', chunk) for i, chunk in enumerate(chunks)
        }

        print('RUNNING...')

        try:
            done, not_done = wait(futures)

        except KeyboardInterrupt:
            print('NOTE: Shutting down Workers.')

        finally:
            for future in futures:
                future.cancel()

                results.extend(future.result())         

    end_time = datetime.now()
    elapsed = round((end_time - start_time).total_seconds(), 2)
    results_count = len(results)

    print(f'TIME: {elapsed} seconds')
    print(f'RPS: ~{round(results_count / elapsed):,}')
    print(f'TOTAL: #{results_count:,}/{items_count:,}')

    '''
    with open(OUTPUT_FILENAME, mode='a', encoding='latin-1') as output_file:
        output_file.writelines(f'{item}\n' for item in items)
        output_file.flush()
    print(f'NOTE: #{results_count:,} to {OUTPUT_FILENAME}...')
    '''
    print(end_time.strftime('END: %B %d, %Y at %I:%M:%S %p'))
