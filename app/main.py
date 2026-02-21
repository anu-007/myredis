import asyncio
import argparse
from datetime import datetime, timedelta

EMPTY_RES = "$-1\r\n"

class RedisServer:
    def __init__(self, host="localhost", port=6378, replica_of=None):
        self.host = host
        self.port = port
        self.map = {}
        self.role = "master" if replica_of is None else "replica"
        self.replication_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.replication_offset = 0
        self.master_reader = None
        self.master_writer = None
        self.replicas = []
        self.replica_of = replica_of
        self.replica_acks = {}  # Track ACKs from replicas: {writer: offset}

    async def send(self, writer, cmd):
        writer.write((cmd + "\r\n").encode())
        await writer.drain()

    async def read(self, reader):
        data = await reader.read(1024)
        return data.decode()

    async def propagate_to_replicas(self, cmd):
        if self.role == "master":
            # Update replication offset
            self.replication_offset += len(cmd) + 2  # +2 for \r\n

            for replica in self.replicas:
                replica.write((cmd + "\r\n").encode())
                await replica.drain()

    async def receive_replication(self):
        while True:
            data = await self.master_reader.read(1024)
            if not data:
                break

            # Process commands from master
            commands = data.decode().strip().split('\r\n')
            for cmd in commands:
                if cmd:
                    print("Replication command:", cmd)
                    # Process the command (this will update replication_offset)
                    await self.process_command(cmd, transaction=False, queue=[], writer=self.master_writer)

    async def connect_to_master(self):
        host, port = self.replica_of.split(":")
        port = int(port)

        print(f"Connecting to master {host}:{port}")

        reader, writer = await asyncio.open_connection(host, port)

        self.master_reader = reader
        self.master_writer = writer

        await self.send(writer, "PING")
        print(await self.read(reader))

        await self.send(writer, f"REPLCONF listening-port {self.port}")
        print(await self.read(reader))

        await self.send(writer, "REPLCONF capa psync2")
        print(await self.read(reader))

        await self.send(writer, "PSYNC ? -1")

        # Read FULLRESYNC response
        fullresync_response = await reader.readline()
        print("PSYNC response:", fullresync_response.decode())

        # Read RDB file
        # First read the bulk string length: $<length>\r\n
        rdb_length_line = await reader.readline()
        print("RDB length line:", rdb_length_line.decode().strip())

        # Parse the length
        if rdb_length_line.startswith(b'$'):
            rdb_length = int(rdb_length_line[1:].strip())
            # Read the exact number of bytes for the RDB file
            rdb_data = await reader.readexactly(rdb_length)
            print(f"Received RDB file: {len(rdb_data)} bytes")

        asyncio.create_task(self.receive_replication())

    async def handleTask(self, reader:asyncio.StreamReader, writer:asyncio.StreamWriter):
        queue = []
        transaction = False
        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    break

                response = await self.process_command(data.decode(), transaction, queue, writer)
                if response:
                    writer.write(response.encode())
                    await writer.drain()  # Flush the buffer but keep connection open
            print(self.map)
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
        except Exception as e:
            if not isinstance(e, (ConnectionResetError, BrokenPipeError)):
                print(e)
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def process_command(self, command: str, transaction: bool, queue: list, writer: asyncio.StreamWriter = None):
        issue = self.validate_command(command)

        if issue:
            return issue

        if transaction:
            queue.append(command)
            return "QUEUED\r\n"

        split_cmd = command.split()
        if "PING" in command:
            return "+PONG\r\n"
        elif "ECHO" in command:
            return command.split()[1]
        elif "SET" in command:
            key = split_cmd[1]
            value_obj = {"val": split_cmd[2], "exp": -1}
            if "EX" in split_cmd:
                value_obj["exp"] = datetime.now() + timedelta(seconds=int(split_cmd[4]))
            elif "PX" in split_cmd:
                value_obj["exp"] = datetime.now() + timedelta(milliseconds=int(split_cmd[4]))
            else:
                value_obj["exp"] = -1
            
            self.map[key] = value_obj
            await self.propagate_to_replicas(command)
            return "OK\r\n"
        elif "GET" in command:
            if split_cmd[1] in self.map:
                value_obj = self.map[split_cmd[1]]

                if isinstance(value_obj, list):
                    return f"{value_obj}\r\n"
                if value_obj.get("exp", -1) != -1 and datetime.now() > value_obj.get("exp"):
                    return EMPTY_RES
                else:
                    return f"{value_obj.get("val")}\r\n"
            else:
                return EMPTY_RES
        elif "RPUSH" in command or "LPUSH" in command:
            for idx in range(2, len(split_cmd)):
                if split_cmd[1] not in self.map:
                    self.map[split_cmd[1]] = [split_cmd[idx]]
                else:
                    if "RPUSH" in command:
                        self.map[split_cmd[1]].append(split_cmd[idx])
                    else:
                        self.map[split_cmd[1]] = [split_cmd[idx]] + self.map.get(split_cmd[1], [])

            return f"{len(self.map[split_cmd[1]])}\r\n"
        elif "LRANGE" in command:
            value_list = self.map.get(split_cmd[1], [])
            value_list_len = len(value_list)
            lb = int(split_cmd[2]) if int(split_cmd[2]) >= 0 else value_list_len + int(split_cmd[2]) + 1
            ub = int(split_cmd[3]) if int(split_cmd[3]) >= 0 else value_list_len + int(split_cmd[3]) + 1
            if lb >= value_list_len or lb > ub:
                return EMPTY_RES
            elif ub >= value_list_len and lb <= value_list_len:
                result = value_list[lb:value_list_len]
            else:
                result = value_list[lb:ub]
            
            resp = f"*{len(result)}\r\n"
            for item in result:
                resp += f"${len(item)}\r\n{item}\r\n"
            return resp
        elif "LLEN" in command:
            value_list = self.map.get(split_cmd[1], [])
            return f"{len(value_list)}\r\n"
        elif "LPOP" in command:
            value_list = self.map.get(split_cmd[1], [])
            elem_removed = []
            for _ in range(int(split_cmd[2])):
                if len(value_list):
                    el = value_list.pop()
                    elem_removed.append(el)
            self.map[split_cmd[1]] = value_list

            resp = f"*{len(elem_removed)}\r\n"
            for item in elem_removed:
                resp += f"${len(item)}\r\n{item}\r\n"
            return resp
        elif "BLPOP" in command:
            value_list = self.map.get(split_cmd[1], [])
            value_list_len = len(value_list)

            if value_list_len > 0:
                el = value_list.pop(0)
                self.map[split_cmd[1]] = value_list
                return f"*2\r\n${len(split_cmd[1])}\r\n{split_cmd[1]}\r\n${len(el)}\r\n{el}\r\n"
            
            if int(split_cmd[2]) == 0:
                while not len(self.map.get(split_cmd[1], [])):
                    await asyncio.sleep(0.1)
            else:
                start = datetime.now()
                while (datetime.now() - start).total_seconds() < int(split_cmd[2]):
                    if self.map.get(split_cmd[1]):
                        break
                    await asyncio.sleep(0.1)
            
            value_list = self.map.get(split_cmd[1], [])
            if value_list:
                el = value_list.pop(0)
                self.map[split_cmd[1]] = value_list
                return f"*2\r\n${len(split_cmd[1])}\r\n{split_cmd[1]}\r\n${len(el)}\r\n{el}\r\n"
            else:
                return EMPTY_RES
        elif "TYPE" in command:
            value = self.map.get(split_cmd[1], None)
            if value is None:
                return "none\r\n"
            elif isinstance(value, list):
                if isinstance(value[0], dict) and value[0].get("id", None):
                    return "stream\r\n"
                return "list\r\n"
            elif isinstance(value, dict) and "val" in value:
                return "string\r\n"
            else:
                return "none\r\n"
        elif "XADD" in command:
            key = split_cmd[1]
            valid_id = self.validate_stream_id(command)
            if not valid_id:
                return "(error) ERR The ID specified in XADD is equal or smaller than the target stream top item"

            value = self.map.get(key, [])
            curr_obj = { "id": valid_id }
            for idx in range(3, len(split_cmd), 2):
                curr_obj[split_cmd[idx]] = split_cmd[idx+1]

            value.append(curr_obj)
            self.map[key] = value
            return f"{len(valid_id)}\r\n{valid_id}\r\n"
        elif "XRANGE" in command:
            values = self.map.get(split_cmd[1], [])
            if values is None:
                return "*0\r\n"
            start_id = split_cmd[2]
            end_id = split_cmd[3]

            res = []
            for val in values:
                val_id = val.get("id").split("-")
                val_timestamp = int(val_id[0])
                val_seq = int(val_id[1])

                if start_id == "-":
                    start_match = True
                else:
                    start_part = start_id.split("-")
                    start_timestamp = int(start_part[0])
                    start_seq = int(start_part[1]) if len(start_part) > 1 else 0
                    start_match = (val_timestamp > start_timestamp or (val_timestamp == start_timestamp and val_seq >= start_seq))
                    
                if end_id == "+":
                    end_match = True
                else:
                    end_parts = end_id.split('-')
                    end_timestamp = int(end_parts[0])
                    end_seq = int(end_parts[1]) if len(end_parts) > 1 else float('inf')
                    end_match = (val_timestamp < end_timestamp or 
                                (val_timestamp == end_timestamp and val_seq <= end_seq))

                if start_match and end_match:
                    res.append(val)

            resp = f"*{len(res)}\r\n"
            for item in res:
                resp += f"*2\r\n${len(item['id'])}\r\n{item['id']}\r\n"
                field_count = len(item) - 1
                resp += f"*{field_count * 2}\r\n"
                for key, value in item.items():
                    if key != 'id':
                        resp += f"${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"
            return resp
        elif "XREAD" in command:
            block_ms = None
            streams_idx = split_cmd.index("STREAMS")

            if "BLOCK" in split_cmd:
                block_idx = split_cmd.index("BLOCK")
                block_ms = int(split_cmd[block_idx + 1])

            keys = split_cmd[streams_idx + 1:streams_idx + 1 + (len(split_cmd) - streams_idx - 1) // 2]
            ids = split_cmd[streams_idx + 1 + len(keys):]
            
            result = []
            start_time = datetime.now()
            while True:
                for key_idx, key in enumerate(keys):
                    values = self.map.get(key, [])
                    if values is None:
                        continue
                    
                    if ids[key_idx] == "$":
                        if values:
                            last_id = values[-1].get("id").split("-")
                            start_timestamp = int(last_id[0])
                            start_seq = int(last_id[1])
                        else:
                            start_timestamp = 0
                            start_seq = 0
                    else:
                        start_part = ids[key_idx].split("-")
                        start_timestamp = int(start_part[0])
                        start_seq = int(start_part[1])
                
                    res = []
                    for val in values:
                        val_id = val.get("id").split("-")
                        val_timestamp = int(val_id[0])
                        val_seq = int(val_id[1])

                        if (val_timestamp > start_timestamp or (val_timestamp == start_timestamp and val_seq >= start_seq)):
                            res.append(val)
                    
                    if res:
                        result.append([key, res])
                
                if result or block_ms is None:
                    break

                if block_ms is not None and block_ms > 0:
                    elapsed = (datetime.now() - start_time).total_seconds() * 1000
                    if elapsed >= block_ms:
                        break
                
                await asyncio.sleep(0.1)
            
            resp = f"*{len(result)}\r\n"
            for stream_data in result:
                key, items = stream_data
                resp += f"*2\r\n${len(key)}\r\n{key}\r\n"
                resp += f"*{len(items)}\r\n"
                
                for item in items:
                    resp += f"*2\r\n${len(item['id'])}\r\n{item['id']}\r\n"
                    field_count = len(item) - 1
                    resp += f"*{field_count * 2}\r\n"
                    for field_key, field_value in item.items():
                        if field_key != 'id':
                            resp += f"${len(field_key)}\r\n{field_key}\r\n${len(field_value)}\r\n{field_value}\r\n"
            
            return resp if result else "*0\r\n"
        elif "INCR" in command:
            if split_cmd[1] not in self.map:
                self.map[split_cmd[1]] = 1
            elif split_cmd[1] in self.map and isinstance(self.map[split_cmd[1]], int):
                self.map[split_cmd[1]] = int(self.map[split_cmd[1]]) + 1
            else:
                return "(error) ERR value is not an integer or out of range\r\n"
            
            return f"{self.map[split_cmd[1]]}\r\n"
        elif "MULTI" in command:
            transaction = True
            return "OK\r\n"
        elif "EXEC" in command:
            if not transaction:
                return "(error) ERR EXEC without MULTI\r\n"

            transaction = False
            result = []
            for command in queue:
                result.append(await self.process_command(command, transaction=False, queue=[], writer=writer))
            queue = []
            return result
        elif "DISCARD" in command:
            if not transaction:
                return "(error) ERR DISCARD without MULTI\r\n"

            transaction = False
            queue = []
            return "OK\r\n"
        elif "INFO" in command:
            if self.replica_of is None:
                return f"# Replication\r\nrole:master\r\nmaster_replid:{self.replication_id}\r\nmaster_repl_offset:{self.replication_offset}\r\n"
            else:
                return "# Replication\r\nrole:slave\r\n"
        elif "REPLCONF" in command:
            if self.role == "replica" and "GETACK" in command:
                return f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(self.replication_offset))}\r\n{self.replication_offset}\r\n"
            elif self.role == "master" and "ACK" in command:
                # Replica is acknowledging replication offset
                if writer and len(split_cmd) >= 3:
                    offset = int(split_cmd[2])
                    self.replica_acks[writer] = offset
                return None  # Don't send response to ACK
            elif self.role == "master" and "GETACK" in command:
                await self.propagate_to_replicas(command)
                return None  # Don't send response, wait for ACKs
            else:
                return "+OK\r\n"
        elif "PSYNC" in command:
            if writer:
                self.replicas.append(writer)
                # Send FULLRESYNC response
                response = f"+FULLRESYNC {self.replication_id} {self.replication_offset}\r\n"
                writer.write(response.encode())
                await writer.drain()

                # Send empty RDB file (minimal valid RDB)
                # RDB file format: REDIS<version><databases><EOF>
                empty_rdb = bytes.fromhex(
                    "524544495330303131"  # "REDIS0011" - RDB version 11
                    "fa0972656469732d76657205372e322e30"  # Redis version metadata
                    "fa0a72656469732d62697473c040"  # Redis bits metadata
                    "fa056374696d65c26d08bc65"  # Creation time metadata
                    "fa08757365642d6d656dc2b0c41000"  # Used memory metadata
                    "fa08616f662d62617365c000"  # AOF base metadata
                    "ff"  # EOF marker
                    "f06e3bfec0ff5aa2"  # CRC64 checksum
                )
                writer.write(f"${len(empty_rdb)}\r\n".encode())
                writer.write(empty_rdb)
                await writer.drain()
            return None  # Don't send additional response
        elif "WAIT" in command:
            num_replicas = int(split_cmd[1])
            timeout_ms = int(split_cmd[2])

            # If no replicas connected, return 0
            if len(self.replicas) == 0:
                return ":0\r\n"

            # If no writes have been made (offset is 0), all replicas are in sync
            if self.replication_offset == 0:
                return f":{len(self.replicas)}\r\n"

            # Send REPLCONF GETACK * to all replicas
            getack_cmd = "REPLCONF GETACK *"
            for replica in self.replicas:
                replica.write((getack_cmd + "\r\n").encode())
                await replica.drain()

            # Wait for ACKs from replicas
            start_time = datetime.now()
            ack_count = 0

            while True:
                # Count how many replicas have acknowledged
                ack_count = sum(1 for offset in self.replica_acks.values() if offset >= self.replication_offset)

                # If we have enough ACKs, return
                if ack_count >= num_replicas:
                    return f":{ack_count}\r\n"

                # Check timeout
                elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
                if elapsed_ms >= timeout_ms:
                    return f":{ack_count}\r\n"

                # Wait a bit before checking again
                await asyncio.sleep(0.01)
        else:
            return EMPTY_RES
             
    def validate_command(self, command: str):
        split_command = command.split()

        if "PING" in command or "MULTI" in command or "EXEC" in command or "DISCARD" in command or "INFO" in command:
            if len(split_command) != 1:
                return "Missing parameters"
        elif "SET" in command and ("EX" in command or "PX" in command):
            if len(split_command) != 5:
                return "Missing parameters"
        elif "SET" in command and ("EX" not in command or "PX" not in command):
            if len(split_command) != 3:
                return "Missing parameters"
        elif "GET" in command or "LLEN" in command or "ECHO" in command or "TYPE" in command or "INCR" in command:
            if len(split_command) != 2:
                return "Missing parameters"
        elif "RPUSH" in command or "LPUSH" in command  or "LPOP" in command or "BLPOP" in command or "XADD" in command or "XRANGE" in command or "REPLCONF" in command or "PSYNC" in command:
            if len(split_command) < 3:
                return "Missing parameters"
        elif "LRANGE" in command:
            if len(split_command) != 4:
                return "Missing parameters"
        elif "WAIT" in command:
            if len(split_command) != 3:
                return "Missing parameters"
        elif "XREAD" in command:
            if "STREAMS" not in split_command:
                return "Missing STREAMS keyword"
            streams_idx = split_command.index("STREAMS")
            num_keys = (len(split_command) - streams_idx - 1) // 2
            if num_keys == 0:
                return "Missing keys and IDs"
        else:
            return "Not valid command"
        
        return False

    def validate_stream_id(self, command: str):
        split_command = command.split()
        curr_split_id = split_command[2].split('-')
        value = self.map.get(split_command[1], None)
        
        def generate_id(curr_id, timestamp, last_id=None):
            if len(curr_id) == 1 and curr_id[0] == "*":
                return f"{timestamp}-1"
            elif len(curr_id) == 2:
                first = timestamp if curr_id[0] == "*" else int(curr_id[0])
                if curr_id[1] == "*":
                    if last_id and int(last_id[0]) == first:
                        second = int(last_id[1]) + 1
                    else:
                        second = 1
                else:
                    second = int(curr_id[1])
                return f"{first}-{second}"
            else:
                return split_command[2]
        
        timestamp = datetime.now().microsecond // 1000
        
        if value is None:
            return generate_id(curr_split_id, timestamp)
        
        sorted_val = sorted(value, key=lambda val: val.get("id"))
        last_id = sorted_val[-1].get("id", "").split('-')
        
        new_id = generate_id(curr_split_id, timestamp, last_id).split('-')
        
        if int(new_id[0]) < int(last_id[0]) or \
        (int(new_id[0]) == int(last_id[0]) and int(new_id[1]) <= int(last_id[1])):
            return False
        
        return generate_id(curr_split_id, timestamp, last_id)

    async def start(self):
        server = await asyncio.start_server(self.handleTask, self.host, self.port)
        print(f"Redis running on {self.host}:{self.port} as {self.role}")

        # start handshake if replica
        if self.role == "replica":
            asyncio.create_task(self.connect_to_master())

        async with server:
            await server.serve_forever()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Redis Server')
    parser.add_argument('--port', type=int, default=6378, help='Port to run the server on (default: 6379)')
    parser.add_argument('--replica-of', type=str, default=None, help='Master server to replicate from (default: None)')
    args = parser.parse_args()

    redis_server = RedisServer(port=args.port, replica_of=args.replica_of)
    try:
        asyncio.run(redis_server.start())
    except KeyboardInterrupt:
        pass