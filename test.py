# test_redis.py
import asyncio

async def send_command(command: str):
    """Send a raw command to the Redis server"""
    reader, writer = await asyncio.open_connection('localhost', 6378)
    
    writer.write(command.encode())
    await writer.drain()
    
    response = await reader.read(1024)
    writer.close()
    await writer.wait_closed()
    
    return response.decode()

async def test_commands():
    print("Testing PING...")
    print(await send_command("PING\r\n"))
    
    print("\nTesting LPUSH...")
    print(await send_command("LPUSH mylist item1\r\n"))
    
    print("Testing LPUSH again...")
    print(await send_command("LPUSH mylist item2\r\n"))
    
    print("\nTesting LRANGE...")
    print(await send_command("LRANGE mylist 0 -1\r\n"))
    
    print("\nTesting LLEN...")
    print(await send_command("LLEN mylist\r\n"))
    
    print("\nTesting LPOP with count 1...")
    print(await send_command("LPOP mylist 1\r\n"))
    
    print("\nTesting LRANGE after LPOP...")
    print(await send_command("LRANGE mylist 0 -1\r\n"))
    
    print("\nTesting BLPOP with timeout 2...")
    print(await send_command("BLPOP mylist 2\r\n"))
    
    print("\nTesting BLPOP on empty list with timeout 1...")
    print(await send_command("BLPOP emptylist 1\r\n"))

    print("\nTesting LPUSH again...")
    print(await send_command("LPUSH emptylist itemx\r\n"))

    print("\nTesting LLEN...")
    print(await send_command("LLEN emptylist\r\n"))
    
    # NEW TEST: Item added before timeout
    print("\n--- Testing item added before timeout ---")
    print("Starting BLPOP with 5s timeout (should return when item is added)...")
    
    # Schedule adding item after 1 second
    async def add_item_after_delay():
        await asyncio.sleep(1)
        print("Adding item to newlist after 1 second...")
        result = await send_command("LPUSH newlist delayeditem\r\n")
        print(f"LPUSH result: {result}")
    
    # Start both tasks concurrently
    blpop_task = asyncio.create_task(send_command("BLPOP newlist 5\r\n"))
    add_task = asyncio.create_task(add_item_after_delay())
    
    blpop_result = await blpop_task
    await add_task
    
    print(f"BLPOP returned: {blpop_result}")

if __name__ == "__main__":
    asyncio.run(test_commands())