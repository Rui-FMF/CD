import sys
import argparse
import middleware

class Consumer:
    def __init__(self, datatype, mech):
        self.type = datatype
        if mech == "json":
            self.queue = middleware.JSONQueue(f"/{self.type}")
        elif mech == "xml":
            self.queue = middleware.XMLQueue(f"/{self.type}")
        elif mech == "pickle":
            self.queue = middleware.PickleQueue(f"/{self.type}")

    @classmethod
    def datatypes(self):
        return ["temp", "msg", "weather"]
    @classmethod
    def mechtypes(self):
        return ["json", "xml", "pickle"]    

    def run(self, length=10):
        while True:
            topic, data = self.queue.pull()
            print(topic, data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", help="type of producer: [temp, msg, weather]", default="temp")
    parser.add_argument("--mech", help="serialization mechanism: [json, xml, pickle]", default="json")
    args = parser.parse_args()

    if args.type not in Consumer.datatypes():
        print("Error: not a valid consumer type")
        sys.exit(1)

    if args.mech not in Consumer.mechtypes():
        print("Error: not a valid serialization mechanism")
        sys.exit(1)

    p = Consumer(args.type, args.mech)

    p.run()