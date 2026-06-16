import asyncio
import argparse
import os
from datetime import datetime, timedelta

EMPTY_RES = "$-1\r\n"

class RedisServer:
    def __init__(self, host="localhost", port=6378, replica_of=None, dir="/tmp/redis-data", dbfilename="dump.rdb"):
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
        self.replica_acks = {}
        self.dir = dir
        self.dbfile_name = dbfilename
        self.load_rdb()
        self.channels = {}

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
        subscribed_channels = set()
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break

                if line.startswith(b'*'):
                    # RESP array
                    count = int(line[1:].rstrip(b'\r\n'))
                    parts = []
                    for _ in range(count):
                        length_line = await reader.readline()
                        length = int(length_line[1:].rstrip(b'\r\n'))
                        data = await reader.readexactly(length + 2)
                        parts.append(data[:length].decode())
                    command = ' '.join(parts)
                else:
                    command = line.decode().strip()

                response = await self.process_command(command, transaction, queue, writer, subscribed_channels)
                if response:
                    writer.write(response.encode())
                    await writer.drain()
            print(self.map)
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
        except Exception as e:
            if not isinstance(e, (ConnectionResetError, BrokenPipeError)):
                print(e)
        finally:
            for ch in list(subscribed_channels):
                if ch in self.channels and writer in self.channels[ch]:
                    self.channels[ch].remove(writer)
            writer.close()
            await writer.wait_closed()
    
    async def process_command(self, command: str, transaction: bool, queue: list, writer: asyncio.StreamWriter = None, subscribed_channels: set = None):
        issue = self.validate_command(command)

        if issue:
            return issue

        if transaction:
            queue.append(command)
            return "QUEUED\r\n"

        split_cmd = command.split()
        if not split_cmd:
            return EMPTY_RES
        if subscribed_channels and split_cmd[0] not in ("SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT"):
            return "-ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context\r\n"
        if split_cmd[0] == "PING":
            return "+PONG\r\n"
        elif split_cmd[0] == "ECHO":
            val = split_cmd[1]
            return f"${len(val)}\r\n{val}\r\n"
        elif split_cmd[0] == "SET":
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
            return "+OK\r\n"
        elif split_cmd[0] == "GET":
            if split_cmd[1] in self.map:
                value_obj = self.map[split_cmd[1]]

                if isinstance(value_obj, list):
                    return "-ERR wrong kind of value\r\n"
                if value_obj.get("exp", -1) != -1 and datetime.now() > value_obj.get("exp"):
                    return EMPTY_RES
                else:
                    val = value_obj.get("val")
                    return f"${len(val)}\r\n{val}\r\n"
            else:
                return EMPTY_RES
        elif split_cmd[0] in ("RPUSH", "LPUSH"):
            for idx in range(2, len(split_cmd)):
                if split_cmd[1] not in self.map:
                    self.map[split_cmd[1]] = [split_cmd[idx]]
                else:
                    if split_cmd[0] == "RPUSH":
                        self.map[split_cmd[1]].append(split_cmd[idx])
                    else:
                        self.map[split_cmd[1]] = [split_cmd[idx]] + self.map.get(split_cmd[1], [])

            return f":{len(self.map[split_cmd[1]])}\r\n"
        elif split_cmd[0] == "LRANGE":
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
        elif split_cmd[0] == "LLEN":
            value_list = self.map.get(split_cmd[1], [])
            return f":{len(value_list)}\r\n"
        elif split_cmd[0] == "LPOP":
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
        elif split_cmd[0] == "BLPOP":
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
        elif split_cmd[0] == "TYPE":
            value = self.map.get(split_cmd[1], None)
            if value is None:
                return "+none\r\n"
            elif isinstance(value, list):
                if isinstance(value[0], dict) and value[0].get("id", None):
                    return "+stream\r\n"
                return "+list\r\n"
            elif isinstance(value, dict) and "val" in value:
                return "+string\r\n"
            else:
                return "+none\r\n"
        elif split_cmd[0] == "XADD":
            key = split_cmd[1]
            valid_id = self.validate_stream_id(command)
            if not valid_id:
                return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"

            value = self.map.get(key, [])
            curr_obj = { "id": valid_id }
            for idx in range(3, len(split_cmd), 2):
                curr_obj[split_cmd[idx]] = split_cmd[idx+1]

            value.append(curr_obj)
            self.map[key] = value
            return f"${len(valid_id)}\r\n{valid_id}\r\n"
        elif split_cmd[0] == "XRANGE":
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
        elif split_cmd[0] == "XREAD":
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
        elif split_cmd[0] == "INCR":
            if split_cmd[1] not in self.map:
                self.map[split_cmd[1]] = 1
            elif split_cmd[1] in self.map and isinstance(self.map[split_cmd[1]], int):
                self.map[split_cmd[1]] = int(self.map[split_cmd[1]]) + 1
            else:
                return "-ERR value is not an integer or out of range\r\n"

            return f":{self.map[split_cmd[1]]}\r\n"
        elif split_cmd[0] == "MULTI":
            transaction = True
            return "+OK\r\n"
        elif split_cmd[0] == "EXEC":
            if not transaction:
                return "-ERR EXEC without MULTI\r\n"

            transaction = False
            result = []
            for command in queue:
                result.append(await self.process_command(command, transaction=False, queue=[], writer=writer))
            queue = []
            resp = f"*{len(result)}\r\n"
            for item in result:
                resp += item
            return resp
        elif split_cmd[0] == "DISCARD":
            if not transaction:
                return "-ERR DISCARD without MULTI\r\n"

            transaction = False
            queue = []
            return "+OK\r\n"
        elif split_cmd[0] == "INFO":
            if self.replica_of is None:
                info = f"# Replication\r\nrole:master\r\nmaster_replid:{self.replication_id}\r\nmaster_repl_offset:{self.replication_offset}\r\n"
            else:
                info = "# Replication\r\nrole:slave\r\n"
            return f"${len(info)}\r\n{info}\r\n"
        elif split_cmd[0] == "REPLCONF":
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
        elif split_cmd[0] == "PSYNC":
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
        elif split_cmd[0] == "WAIT":
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
        elif split_cmd[0] == "CONFIG":
            if split_cmd[1] == 'GET':
                result = []
                for i in range(2, len(split_cmd)):
                    if split_cmd[i] == 'dir':
                        result.append(('dir', self.dir))
                    elif split_cmd[i] == 'dbfilename':
                        result.append(('dbfilename', self.dbfile_name))
                resp = f"*{len(result) * 2}\r\n"
                for key, value in result:
                    resp += f"${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"
                return resp
            else:
                return EMPTY_RES
        elif split_cmd[0] == "SUBSCRIBE":
            if subscribed_channels is None:
                subscribed_channels = set()
            for ch in split_cmd[1:]:
                if ch not in self.channels:
                    self.channels[ch] = []
                if writer not in self.channels[ch]:
                    self.channels[ch].append(writer)
                subscribed_channels.add(ch)
                resp = f"*3\r\n$9\r\nsubscribe\r\n${len(ch)}\r\n{ch}\r\n:{len(subscribed_channels)}\r\n"
                writer.write(resp.encode())
                await writer.drain()
            return None
        elif split_cmd[0] == "PUBLISH":
            ch = split_cmd[1]
            msg = ' '.join(split_cmd[2:])
            subscribers = self.channels.get(ch, [])
            resp = f"*3\r\n$7\r\nmessage\r\n${len(ch)}\r\n{ch}\r\n${len(msg)}\r\n{msg}\r\n"
            for sub_writer in subscribers:
                try:
                    sub_writer.write(resp.encode())
                    await sub_writer.drain()
                except Exception:
                    pass
            return f":{len(subscribers)}\r\n"
        elif split_cmd[0] == "UNSUBSCRIBE":
            if subscribed_channels is None:
                subscribed_channels = set()
            targets = split_cmd[1:] if len(split_cmd) > 1 else list(subscribed_channels)
            for ch in targets:
                if ch in subscribed_channels:
                    subscribed_channels.remove(ch)
                    if ch in self.channels and writer in self.channels[ch]:
                        self.channels[ch].remove(writer)
                resp = f"*3\r\n$11\r\nunsubscribe\r\n${len(ch)}\r\n{ch}\r\n:{len(subscribed_channels)}\r\n"
                writer.write(resp.encode())
                await writer.drain()
            return None
        else:
            return EMPTY_RES
             
    def validate_command(self, command: str):
        split_command = command.split()

        if split_command[0] in ("PING", "MULTI", "EXEC", "DISCARD", "INFO"):
            if len(split_command) != 1:
                return "-ERR Missing parameters\r\n"
        elif split_command[0] == "SET" and ("EX" in split_command or "PX" in split_command):
            if len(split_command) != 5:
                return "-ERR Missing parameters\r\n"
        elif split_command[0] == "SET" and "EX" not in split_command and "PX" not in split_command:
            if len(split_command) != 3:
                return "-ERR Missing parameters\r\n"
        elif split_command[0] in ("GET", "LLEN", "ECHO", "TYPE", "INCR"):
            if len(split_command) != 2:
                return "-ERR Missing parameters\r\n"
        elif split_command[0] == "SUBSCRIBE":
            if len(split_command) < 2:
                return "-ERR Missing parameters\r\n"
        elif split_command[0] == "PUBLISH":
            if len(split_command) < 3:
                return "-ERR Missing parameters\r\n"
        elif split_command[0] in ("RPUSH", "LPUSH", "LPOP", "BLPOP", "XADD", "XRANGE", "REPLCONF", "PSYNC", "CONFIG"):
            if len(split_command) < 3:
                return "-ERR Missing parameters\r\n"
        elif split_command[0] == "LRANGE":
            if len(split_command) != 4:
                return "-ERR Missing parameters\r\n"
        elif split_command[0] == "UNSUBSCRIBE":
            if len(split_command) < 1:
                return "-ERR Missing parameters\r\n"
        elif split_command[0] == "WAIT":
            if len(split_command) != 3:
                return "-ERR Missing parameters\r\n"
        elif split_command[0] == "XREAD":
            if "STREAMS" not in split_command:
                return "-ERR Missing STREAMS keyword\r\n"
            streams_idx = split_command.index("STREAMS")
            num_keys = (len(split_command) - streams_idx - 1) // 2
            if num_keys == 0:
                return "-ERR Missing keys and IDs\r\n"
        else:
            return "-ERR Not valid command\r\n"
        
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

    def load_rdb(self):
        rdb_path = os.path.join(self.dir, self.dbfile_name)
        if not os.path.exists(rdb_path):
            return
        try:
            with open(rdb_path, 'rb') as f:
                self.parse_rdb(f.read())
        except Exception as e:
            print(f"Error loading RDB: {e}")

    def _read_length_encoded_integer(self, data: bytes, idx: int):
        """Read a length-encoded integer from data at idx. Returns (value, new_idx)."""
        byte = data[idx]
        prefix = (byte >> 6) & 0x03
        if prefix == 0:
            return byte & 0x3F, idx + 1
        elif prefix == 1:
            value = ((byte & 0x3F) << 8) | data[idx + 1]
            return value, idx + 2
        elif prefix == 2:
            value = int.from_bytes(data[idx + 1:idx + 5], byteorder='big')
            return value, idx + 5
        else:
            # Special encoding - shouldn't happen for lengths
            return 0, idx + 1

    def _read_length_encoded_string(self, data: bytes, idx: int):
        """Read a length-encoded string from data at idx. Returns (string, new_idx)."""
        byte = data[idx]
        prefix = (byte >> 6) & 0x03
        if prefix == 0:
            length = byte & 0x3F
            return data[idx + 1:idx + 1 + length].decode(), idx + 1 + length
        elif prefix == 1:
            length = ((byte & 0x3F) << 8) | data[idx + 1]
            return data[idx + 2:idx + 2 + length].decode(), idx + 2 + length
        elif prefix == 2:
            length = int.from_bytes(data[idx + 1:idx + 5], byteorder='big')
            return data[idx + 5:idx + 5 + length].decode(), idx + 5 + length
        else:
            # Special encoding
            enc_type = byte & 0x3F
            if enc_type == 0:
                value = int.from_bytes(data[idx + 1:idx + 2], byteorder='little', signed=True)
                return str(value), idx + 2
            elif enc_type == 1:
                value = int.from_bytes(data[idx + 1:idx + 3], byteorder='little', signed=True)
                return str(value), idx + 3
            elif enc_type == 2:
                value = int.from_bytes(data[idx + 1:idx + 5], byteorder='little', signed=True)
                return str(value), idx + 5
            else:
                # Skip unknown special encoding
                return "", idx + 1

    def parse_rdb(self, data: bytes):
        """Parse RDB file data and populate self.map."""
        if len(data) < 9 or not data.startswith(b'REDIS'):
            return

        idx = 9  # Skip "REDIS" + 4 bytes version

        while idx < len(data):
            byte = data[idx]

            if byte == 0xFA:
                # Metadata
                idx += 1
                key, idx = self._read_length_encoded_string(data, idx)
                value, idx = self._read_length_encoded_string(data, idx)

            elif byte == 0xFE:
                # Database selector
                idx += 1
                db_index, idx = self._read_length_encoded_integer(data, idx)

            elif byte == 0xFB:
                # Hash table size info
                idx += 1
                ht_size, idx = self._read_length_encoded_integer(data, idx)
                expire_ht_size, idx = self._read_length_encoded_integer(data, idx)

            elif byte == 0xFC:
                # Expiry in milliseconds (8 bytes, little-endian)
                idx += 1
                expiry_ms = int.from_bytes(data[idx:idx + 8], byteorder='little')
                idx += 8
                expiry = datetime.fromtimestamp(expiry_ms / 1000)
                val_type = data[idx]
                idx += 1
                if val_type == 0x00:
                    key, idx = self._read_length_encoded_string(data, idx)
                    value, idx = self._read_length_encoded_string(data, idx)
                    if datetime.now() < expiry:
                        self.map[key] = {"val": value, "exp": expiry}

            elif byte == 0xFD:
                # Expiry in seconds (4 bytes, little-endian)
                idx += 1
                expiry_s = int.from_bytes(data[idx:idx + 4], byteorder='little')
                idx += 4
                expiry = datetime.fromtimestamp(expiry_s)
                val_type = data[idx]
                idx += 1
                if val_type == 0x00:
                    key, idx = self._read_length_encoded_string(data, idx)
                    value, idx = self._read_length_encoded_string(data, idx)
                    if datetime.now() < expiry:
                        self.map[key] = {"val": value, "exp": expiry}

            elif byte == 0xFF:
                # End of file
                break

            elif byte == 0x00:
                # String type
                idx += 1
                key, idx = self._read_length_encoded_string(data, idx)
                value, idx = self._read_length_encoded_string(data, idx)
                self.map[key] = {"val": value, "exp": -1}

            else:
                # Unknown type - skip to avoid infinite loop
                idx += 1

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
    parser.add_argument('--dir', type=str, default='/tmp/redis-data', help='Directory for RDB files')
    parser.add_argument('--dbfilename', type=str, default='dump.rdb', help='RDB filename')
    args = parser.parse_args()

    redis_server = RedisServer(port=args.port, replica_of=args.replica_of, dir=args.dir, dbfilename=args.dbfilename)
    try:
        asyncio.run(redis_server.start())
    except KeyboardInterrupt:
        pass