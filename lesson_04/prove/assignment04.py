"""
Course    : CSE 351
Assignment: 04
Student   : Spencer Gamero

Instructions:
    - review instructions in the course

In order to retrieve a weather record from the server, Use the URL:

f'{TOP_API_URL}/record/{name}/{recno}

where:

name: name of the city
recno: record number starting from 0

"""
import random
import threading
import time
from common import *

from cse351 import *

THREADS = 200                # TODO - set for your program
WORKERS = 5                 # TODO - set for your program
RECORDS_TO_RETRIEVE = 5000  # Don't change

QUEUE_MAX = 10

# ---------------------------------------------------------------------------
def retrieve_weather_data(cmd_q, cmd_lock,cmd_slots,cmd_items,data_q,data_lock,data_slots,data_items):
    # TODO - fill out this thread function (and arguments)
    while True:
        cmd_items.acquire()
        with cmd_lock:
            command = cmd_q.get()
        cmd_slots.release()

        if command is None:
            break

        city, recno = command
        record = get_data_from_server(f'{TOP_API_URL}/record/{city}/{recno}')

        data_slots.acquire()
        with data_lock:
            data_q.put((city, record['date'], record['temp']))
        data_items.release()

# ---------------------------------------------------------------------------
# TODO - Create Worker threaded class
class Worker(threading.Thread):
    def __init__(self, data_q, data_lock, data_slots, data_items, noaa):
        super().__init__()
        self.data_q = data_q
        self.data_lock = data_lock
        self.data_slots = data_slots
        self.data_items = data_items
        self.noaa = noaa

    def run(self):
        while True:
            self.data_items.acquire()
            with self.data_lock:
                item = self.data_q.get()
            self.data_slots.release()

            if item is None:
                break
            city, date, temp = item
            self.noaa.add_record(city, date, temp)

# ---------------------------------------------------------------------------
# TODO - Complete this class
class NOAA:

    def __init__(self):
        self.cities = {}
        self.lock = threading.Lock()

    def add_record(self, city, date, temp):
        with self.lock:
            if city not in self.cities:
                self.cities[city] = []
            self.cities[city].append((date, temp))

    def get_temp_details(self, city):
        records = self.cities.get(city, [])
        if len(records) == 0:
            return 0.0
        return sum(temp for _, temp in records) / len(records)


# ---------------------------------------------------------------------------
class Queue351():
    """ This is the queue object to use for this class. Do not modify!! """

    def __init__(self):
        self.__items = []
   
    def put(self, item):
        assert len(self.__items) <= 10
        self.__items.append(item)

    def get(self):
        return self.__items.pop(0)

    def get_size(self):
        """ Return the size of the queue like queue.Queue does -> Approx size """
        extra = 1 if random.randint(1, 50) == 1 else 0
        if extra > 0:
            extra *= -1 if random.randint(1, 2) == 1 else 1
        return len(self.__items) + extra


# ---------------------------------------------------------------------------
def verify_noaa_results(noaa):

    answers = {
        'sandiego': 14.5004,
        'philadelphia': 14.865,
        'san_antonio': 14.638,
        'san_jose': 14.5756,
        'new_york': 14.6472,
        'houston': 14.591,
        'dallas': 14.835,
        'chicago': 14.6584,
        'los_angeles': 15.2346,
        'phoenix': 12.4404,
    }

    print()
    print('NOAA Results: Verifying Results')
    print('===================================')
    for name in CITIES:
        answer = answers[name]
        avg = noaa.get_temp_details(name)

        if abs(avg - answer) > 0.00001:
            msg = f'FAILED  Expected {answer}'
        else:
            msg = f'PASSED'
        print(f'{name:>15}: {avg:<10} {msg}')
    print('===================================')


# ---------------------------------------------------------------------------
def main():

    log = Log(show_terminal=True, filename_log='assignment.log')
    log.start_timer()

    noaa = NOAA()

    # Start server
    data = get_data_from_server(f'{TOP_API_URL}/start')

    # Get all cities number of records
    print('Retrieving city details')
    city_details = {}
    name = 'City'
    print(f'{name:>15}: Records')
    print('===================================')
    for name in CITIES:
        city_details[name] = get_data_from_server(f'{TOP_API_URL}/city/{name}')
        print(f'{name:>15}: Records = {city_details[name]['records']:,}')
    print('===================================')

    records = RECORDS_TO_RETRIEVE

    # TODO - Create any queues, semaphores, locks or barriers you need
    cmd_q = Queue351()
    cmd_lock = threading.Lock()
    cmd_slots = threading.Semaphore(QUEUE_MAX)
    cmd_items = threading.Semaphore(0)

    data_q = Queue351()
    data_lock = threading.Lock()
    data_slots = threading.Semaphore(QUEUE_MAX)
    data_items = threading.Semaphore(0)

    retrievers = []
    for _ in range(THREADS):
        t = threading.Thread(target=retrieve_weather_data,
                             args=(cmd_q, cmd_lock, cmd_slots, cmd_items, data_q, data_lock, data_slots, data_items))
        retrievers.append(t)
        t.start()

    workers = []
    for _ in range(WORKERS):
        w = Worker(data_q, data_lock, data_slots, data_items, noaa)
        workers.append(w)
        w.start()

    for city in CITIES:
        for recno in range(records):
            cmd_slots.acquire()
            with cmd_lock:
                cmd_q.put((city, recno))
            cmd_items.release()            

    for _ in range(THREADS):
        cmd_slots.acquire()
        with cmd_lock:
            cmd_q.put(None)
        cmd_items.release()
    for t in retrievers:
        t.join()

    for _ in range(WORKERS):
        data_slots.acquire()
        with data_lock:
            data_q.put(None)
        data_items.release()
    for w in workers:
        w.join()


    # End server - don't change below
    data = get_data_from_server(f'{TOP_API_URL}/end')
    print(data)

    verify_noaa_results(noaa)

    log.stop_timer('Run time: ')


if __name__ == '__main__':
    main()

