import concurrent.futures
import requests
import threading
import time

from integration.utilities import VirtualRateLimiter

class ObjectProcessor:
    def __init__(self, objects: list = [], speed: int = 50):
        self.objects = objects
        self.thread_local = threading.local()
        self.speed = speed

    def process(self):
        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.speed) as executor:
            for obj in self.objects:
                time.sleep(0.1)
                # VirtualRateLimiter().limit()
                # while VirtualRateLimiter.is_paused():
                #     time.sleep(0.1)
                executor.submit(self.process_object, obj)
        
        print(f"Processed {len(self.objects)} objects in {time.time() - start_time} seconds.")

    def get_session(self):
        if not hasattr(self.thread_local, "session"):
            self.thread_local.session = requests.Session()

        return self.thread_local.session

    def process_object(self, obj):
        session = requests.Session()

        def post(*args, **kwargs):
            VirtualRateLimiter().limit()
            while VirtualRateLimiter.is_paused():
                time.sleep(0.1)
            return self.get_session().post(*args, **kwargs)
        
        def get(*args, **kwargs):
            VirtualRateLimiter().limit()
            while VirtualRateLimiter.is_paused():
                time.sleep(0.1)
            return self.get_session().get(*args, **kwargs)
        
        def put(*args, **kwargs):
            VirtualRateLimiter().limit()
            while VirtualRateLimiter.is_paused():
                time.sleep(0.1)
            return self.get_session().put(*args, **kwargs)
        
        def delete(*args, **kwargs):
            VirtualRateLimiter().limit()
            while VirtualRateLimiter.is_paused():
                time.sleep(0.1)
            return self.get_session().delete(*args, **kwargs)
        
        session.post = post
        session.get = get
        session.put = put
        session.delete = delete

        obj.process(session=session)