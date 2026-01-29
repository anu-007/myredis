import asyncio
from datetime import datetime, timedelta

EMPTY_RES = "$-1\r\n"

class RedisServer:
    def __init__(self, host="localhost", port=6378):
        self.host = host
        self.port = port
        self.map = {}
    
    async def handleTask(self, reader:asyncio.StreamReader, writer:asyncio.StreamWriter):
        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    break

                response = await self.process_command(data.decode())
                if response:
                    writer.write(response.encode())
                    writer.close()
        except Exception as e:
            print(e)
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def process_command(self, command: str):
        issue = self.validate_command(command)

        if issue:
            return issue

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
        elif "RPUSH" in command:
            for idx in range(2, len(split_cmd)):
                if split_cmd[1] not in self.map:
                    self.map[split_cmd[1]] = [split_cmd[idx]]
                else:
                    self.map[split_cmd[1]].append(split_cmd[idx])
            
            return f"{len(self.map[split_cmd[1]])}\r\n"
        elif "LRANGE" in command:
            value_list = self.map.get(split_cmd[1], [])
            value_list_len = len(value_list)
            lb = int(split_cmd[2])
            ub = int(split_cmd[3])
            if lb >= value_list_len or lb > ub:
                return EMPTY_RES
            elif ub >= value_list_len and lb <= value_list_len:
                return f"{value_list[lb:value_list_len]}\r\n"
            else:
                return f"{value_list[lb:ub]}\r\n"
        else:
            return EMPTY_RES
    
    def validate_command(self, command: str):
        split_command = command.split()

        if "PING" in command:
            if len(split_command) != 1:
                return "Missing parameters"
        elif "ECHO" in command:
            if len(split_command) != 2:
                return "Missing parameters"
        elif "SET" in command and ("EX" in command or "PX" in command):
            if len(split_command) != 5:
                return "Missing parameters"
        elif "SET" in command and ("EX" not in command or "PX" not in command):
            if len(split_command) != 3:
                return "Missing parameters"
        elif "GET" in command:
            if len(split_command) != 2:
                return "Missing parameters"
        elif "RPUSH" in command:
            if len(split_command) < 3:
                return "Missing parameters"
        elif "LRANGE" in command:
            if len(split_command) != 4:
                return "Missing parameters"
        else:
            return "Not valid command"
        
        return False

    async def start(self):
        server = await asyncio.start_server(self.handleTask, self.host, self.port)
        await server.serve_forever()

if __name__ == "__main__":
    redis_server = RedisServer()
    asyncio.run(redis_server.start())
