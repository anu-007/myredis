import asyncio
from datetime import datetime, timedelta

EMPTY_RES = "$-1\r\n"

class RedisServer:
    def __init__(self, host="localhost", port=6378):
        self.host = host
        self.port = port
        self.map = {}
    
    async def handleTask(self, reader:asyncio.StreamReader, writer:asyncio.StreamWriter):
        queue = []
        transaction = False
        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    break

                response = await self.process_command(data.decode(), transaction, queue)
                if response:
                    writer.write(response.encode())
                    writer.close()
            print(self.map)
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
        except Exception as e:
            if not isinstance(e, (ConnectionResetError, BrokenPipeError)):
                print(e)
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def process_command(self, command: str, transaction: bool, queue: list):
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
                result.append(await self.process_command(command, transaction=False, queue=[]))
            queue = []
            return result
        elif "DISCARD" in command:
            if not transaction:
                return "(error) ERR DISCARD without MULTI\r\n"

            transaction = False
            queue = []
            return "OK\r\n"
        else:
            return EMPTY_RES
             
    def validate_command(self, command: str):
        split_command = command.split()

        if "PING" in command or "MULTI" in command or "EXEC" in command or "DISCARD" in command:
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
        elif "RPUSH" in command or "LPUSH" in command  or "LPOP" in command or "BLPOP" in command or "XADD" in command or "XRANGE" in command:
            if len(split_command) < 3:
                return "Missing parameters"
        elif "LRANGE" in command:
            if len(split_command) != 4:
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
        await server.serve_forever()

if __name__ == "__main__":
    redis_server = RedisServer()
    try:
        asyncio.run(redis_server.start())
    except KeyboardInterrupt:
        pass


# TODO: RESP parser, command validator