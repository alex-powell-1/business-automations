from typing import Callable
from database import Database as db


# WIP
class Query:
    def __init__(self, query, cb: Callable, commit=False):
        self.query = query
        self.commit = commit
        self.cb = cb

    def run_query(self):
        r = db.query(self.query, commit=self.commit)
        if self.cb:
            self.cb(r)


# WIP
class QueryQueue:
    def __init__(self):
        self.queue: list[Query] = []
        self.running = False

    def run_queue(self):
        self.running = True
        for i, query in enumerate(self.queue):
            query.run_query()
            self.queue.pop(i)

        self.running = False

    def add_query(self, query, cb: Callable, commit=False):
        self.queue.append(Query(query=query, cb=cb, commit=commit))
        if not self.running:
            self.run_queue()

    def query_db(self, query, commit=False):
        def cb(r):
            return r

        self.add_query(query=query, cb=cb, commit=commit)
