import asyncio
import time


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class APIThrottle:
    def __init__(self, api_limit: int, interval: int):
        self.limit = api_limit
        self.interval = interval
        self.jobs = {}  # (job_hash, start_time) -> end_time

    async def insert_job(self, job_hash):
        self.flush_old_jobs()

        while not self.okay_to_insert():
            await asyncio.sleep(0.01)
            self.flush_old_jobs()

        st = time.time()
        self.jobs[(job_hash, st)] = -1  # -1 means the job is not done yet

        return job_hash, st

    def job_done(self, job_hash, st):
        self.jobs[(job_hash, st)] = time.time()

    def okay_to_insert(self) -> bool:
        if len(self.jobs) < self.limit:
            return True
        else:
            return False

    def flush_old_jobs(self):
        ct = time.time()
        self.jobs = {
            job_key: end_time
            for job_key, end_time in self.jobs.items()
            if (end_time == -1) or (ct - end_time < self.interval)
        }


class UpbitOrderAPISecondlyThrottle(APIThrottle, metaclass=Singleton):
    def __init__(self):
        super().__init__(7, 1)


class UpbitOrderAPIMinutelyThrottle(APIThrottle, metaclass=Singleton):
    def __init__(self):
        super().__init__(195, 60)


class UpbitQuotationAPISecondlyThrottle(APIThrottle, metaclass=Singleton):
    def __init__(self):
        super().__init__(9, 1)


class UpbitExchangeAPISecondlyThrottle(APIThrottle, metaclass=Singleton):
    def __init__(self):
        super().__init__(29, 1)
