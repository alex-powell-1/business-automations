import concurrent.futures
import requests
import threading
import time

class ObjectProcessor:
    def __init__(self, objects: list = [], speed: int = 20):
        self.objects = objects
        self.thread_local = threading.local()
        self.speed = speed

    def process(self):
        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.speed) as executor:
            executor.map(self.process_object, self.objects)
        
        print(f"Processed {len(self.objects)} objects in {time.time() - start_time} seconds.")

    def get_session(self):
        if not hasattr(self.thread_local, "session"):
            self.thread_local.session = requests.Session()
        return self.thread_local.session

    def process_object(self, obj):
        session = self.get_session()
        obj.process(session=session)