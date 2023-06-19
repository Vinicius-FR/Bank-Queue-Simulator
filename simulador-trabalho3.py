'''Run some bank teller queue processing simulations in Batch.'''

from queue import Queue, LifoQueue
from abc import ABC, abstractmethod
import random
from collections import namedtuple

from desimul import Calendar, Event, Server

# Auxiliary simulation classes

class Task:
    '''Implements methods to verify waiting times for each task sent by a task.'''

    def __init__(self, work):
        '''Create a new task with the specified amount of work.'''
        self._arrival_time = None
        self._departure_time = None
        self._total_work = work

    def arrival(self, time):
        '''Called when task arrives.'''
        self._arrival_time = time

    def departure(self, time):
        '''Called when task departs.'''
        self._departure_time = time

    def report(self):
        '''Report on arrival and departure times (for statistics).'''
        return self._arrival_time, self._departure_time

    def work(self):
        '''Inform total work needed to process the task.'''
        return self._total_work
    
    def __lt__(self, other): # NEW
        '''Introduces a comparation method based on the total work of the tasks'''
        return self._total_work < other._total_work


# Server classes

class QueueingSystem(ABC, Server):
    '''Abstract base class for all task queueing systems.'''

    def __init__(self, calendar):
        '''Creates a queue associated with the given calendar.'''
        Server.__init__(self, calendar)
        self._free_tellers = Queue()  # This stores the free tellers.

    def new_task(self, task):
        '''A new task to attend. Either send it to a free teller (if
        available) or to the waiting queue.'''
        if self._free_tellers.empty():
            # No free tellers. Put task on the queue.
            self.enqueue(task)
        else:
            # There is a free teller. Send task to them.
            teller = self._free_tellers.get()
            cal = self.calendar()
            now = cal.current_time()
            event = TaskToTellerEvent(now, teller, task)
            cal.put(event)

    def free_teller(self, teller):
        '''A new free teller. Send it a task (if one is waiting) or put it in
        the waiting queue.'''
        if self.has_waiting_task(teller):
            # There is a task waiting.
            task = self.get_next_task(teller)
            cal = self.calendar()
            now = cal.current_time()
            event = TaskToTellerEvent(now, teller, task)
            cal.put(event)
        else:
            # No task waiting. Put teller in the free tellers queue.
            self._free_tellers.put(teller)

    @abstractmethod
    def enqueue(self, task):
        '''Inserts the task in the queue, according to queueing policy.'''
        pass

    @abstractmethod
    def has_waiting_task(self, teller):
        '''Verify if the teller has a waiting task according to queueing
        policy.'''
        pass

    @abstractmethod
    def get_next_task(self, teller):
        '''Get the next task for the given teller, according do queueing
        policy.'''
        pass

class RandomQueue(QueueingSystem):
    '''A task queueing system that chooses randomly from waiting tasks.'''

    def __init__(self, calendar):
        QueueingSystem.__init__(self, calendar)
        self._waiting_tasks = []

    def enqueue(self, task):
        # Put task at the end of the queue.
        self._waiting_tasks.append(task)

    def has_waiting_task(self, teller):
        return len(self._waiting_tasks) > 0

    def get_next_task(self, teller):
        '''Gets a random task from the queue. Queue must not be empty.'''
        i = random.randrange(len(self._waiting_tasks))
        task = self._waiting_tasks.pop(i)
        return task

class FIFO_Queue(QueueingSystem):
    '''A task queueing system that chooses the first task to arrive first from waiting tasks.'''

    def __init__(self, calendar):
        QueueingSystem.__init__(self, calendar)
        self._queue = Queue()

    def enqueue(self, task):
        '''Put the task at the back of the queue.'''
        self._queue.put(task)

    def has_waiting_task(self, teller):
        return not self._queue.empty()

    def get_next_task(self, teller):
        '''Gets the task at the front of the queue.'''
        return self._queue.get()
    
class LIFO_Queue(FIFO_Queue):
    '''A task queueing system that chooses first the last task to arrive from waiting tasks.'''
    
    def __init__(self, calendar):
        QueueingSystem.__init__(self, calendar)
        self._queue = LifoQueue()
    
class LessWorkFirstQueue(RandomQueue):
    '''A task queueing system that chooses the task with the smallest amount of work.'''
    
    def enqueue(self, task):
        # Put task at the queue based on the amount of work.
        self._waiting_tasks.append(task)
        self._waiting_tasks.sort()

    def get_next_task(self, teller):
        '''Gets the task from the queue with the least amount of work. Queue must not be empty.'''
        task = self._waiting_tasks.pop(0)
        return task


class Teller(Server):
    '''Tellers know how to attend a task.'''

    def __init__(self, calendar, queue, work_rate):
        '''Create a teller server associated with the given calendar and queue,
        and working at a given work_rate.'''
        Server.__init__(self, calendar)
        self._queue = queue
        self._work_rate = work_rate
        self._free_time = []
        self._last_attending = 0.0

    def attend_task(self, task):
        '''Do the work required by the task (takes time). Afterwards, notify
        queue about free status.'''
        curr_time = self.calendar().current_time()
        if curr_time > self._last_attending:
            self._free_time.append(curr_time - self._last_attending)
        time_to_finish = task.work() / self._work_rate
        finish_time = curr_time + time_to_finish
        task.departure(finish_time)
        event = TellerFreeEvent(finish_time, self._queue, self)
        self.calendar().put(event)
        self._last_attending = finish_time

    def free_times(self):
        '''Return a list of all idle interval lengths.'''
        return self._free_time

 
# Event types

class TaskArrivalEvent(Event):
    '''A task has arrived.'''

    def __init__(self, time, queue, task):
        '''Creates an event of the given task arriving at the given queue at
        the given time'''
        Event.__init__(self, time, queue)
        self._task = task

    def process(self):
        '''Record arrival time in the task and insert it in the queue.'''
        self._task.arrival(self.time())
        self.server().new_task(self._task)


class TaskToTellerEvent(Event):
    '''Task goes to a free teller.'''

    def __init__(self, time, teller, task):
        '''Create an event of a given teller starting to attend for a given
        task at a given time.'''
        Event.__init__(self, time, teller)
        self._task = task

    def process(self):
        '''Teller should attend to task.'''
        self.server().attend_task(self._task)


class TellerFreeEvent(Event):
    '''A teller has become free.'''

    def __init__(self, time, queue, teller):
        '''Creates an event of a given teller becoming free.'''
        Event.__init__(self, time, queue)
        self._free_teller = teller

    def process(self):
        '''Notify queueing system of the free teller.'''
        self.server().free_teller(self._free_teller)


# Simulations

Task_par = namedtuple('Task_par', ['number', 'work', 'arrival'])
Teller_par = namedtuple('Teller_par', ['number', 'rate'])

# Auxiliary writing functions


def write_task_data(filename, tasks):
    '''Writes task timing information to file with filename prefix.'''

    with open(filename + '_wait.dat', 'w') as outfile:
        for task in tasks:
            arrival, departure = task.report()
            print(task.work(), arrival, departure, file=outfile)


def write_free_times(filename, tellers):
    '''"Write free teller time information to file with filename prefix.'''

    with open(filename + '_free.dat', 'w') as outfile:
        for teller in tellers:
            freetimes = teller.free_times()
            for free_t in freetimes:
                print(free_t, end=' ', file=outfile)
            print(file=outfile)


# Perform a simulation and save results

def simple_simulation(filename, queueing_system, tellers_p, tasks_p):
    '''Perform simulation for the given queueing system, teller and task
    parameters. Save the results in file with filename as prefix.'''

    # Create simulation infrastructure.
    calendar = Calendar()
    queue = queueing_system(calendar)

    # Create all tellers.
    tellers = [Teller(calendar, queue, tellers_p.rate(i))
               for i in range(tellers_p.number)]

    # Insert initial events of free teller for all tellers (ready to work).
    for teller in tellers:
        calendar.put(TellerFreeEvent(0.0, queue, teller))

    # Create all tasks.
    tasks = [Task(tasks_p.work(i)) for i in range(tasks_p.number)]

    # Create the events of task arrival for all tasks.
    for i, task in enumerate(tasks):
        calendar.put(TaskArrivalEvent(tasks_p.arrival(i), queue, task))

    # Process all events until finished.
    calendar.process_all_events()

    # Write results to files.
    write_task_data(filename, tasks)
    write_free_times(filename, tellers)

    print('Total simulation time for', filename, 'is', calendar.current_time())


# Code to run

if __name__ == '__main__':

    # Define a set of 3 simulations for each of the 4 Queueing Systems, with associated file names.
    # Each task arrive at uniform time intervals 1000/N, where N is the total of tasks
    SIMUL_PAR = [
        ('random_100tasks',
         RandomQueue,
         Teller_par(4, lambda i: 1),
         Task_par(100, lambda i: random.expovariate(0.1),
                    lambda i: i*1000/100),
         ),
        ('random_250tasks',
         RandomQueue,
         Teller_par(4, lambda i: 1),
         Task_par(250, lambda i: random.expovariate(0.1),
                    lambda i: i*1000/250),
         ),
        ('random_500tasks',
         RandomQueue,
         Teller_par(4, lambda i: 1),
         Task_par(500, lambda i: random.expovariate(0.1),
                    lambda i: i*1000/500),
         ),
        ('first_in_first_out_100tasks',
         FIFO_Queue,
         Teller_par(4, lambda i: 1),
         Task_par(100, lambda i: random.expovariate(0.1),
                    lambda i: i*1000/100),
         ),
        ('first_in_first_out_250tasks',
         FIFO_Queue,
         Teller_par(4, lambda i: 1),
         Task_par(250, lambda i: random.expovariate(0.1),
                    lambda i: i*1000/250),
         ),
        ('first_in_first_out_500tasks',
         FIFO_Queue,
         Teller_par(4, lambda i: 1),
         Task_par(500, lambda i: random.expovariate(0.1),
                    lambda i: i*1000/500),
         ),
        ('last_in_first_out_100tasks',
         LIFO_Queue,
         Teller_par(4, lambda i: 1),
         Task_par(100, lambda i: random.expovariate(0.1),
                    lambda i: i*1000/100),
         ),
        ('last_in_first_out_250tasks',
         LIFO_Queue,
         Teller_par(4, lambda i: 1),
         Task_par(250, lambda i: random.expovariate(0.1),
                    lambda i: i*1000/250),
         ),
        ('last_in_first_out_500tasks',
         LIFO_Queue,
         Teller_par(4, lambda i: 1),
         Task_par(500, lambda i: random.expovariate(0.1),
                    lambda i: i*1000/500),
         ),
        ('less_work_first_100tasks',
         LessWorkFirstQueue,
         Teller_par(4, lambda i: 1),
         Task_par(100, lambda i: random.expovariate(0.1),
                    lambda i: i*1000/100),
         ),
        ('less_work_first_250tasks',
         LessWorkFirstQueue,
         Teller_par(4, lambda i: 1),
         Task_par(250, lambda i: random.expovariate(0.1),
                    lambda i: i*1000/250),
         ),
        ('less_work_first_500tasks',
         LessWorkFirstQueue,
         Teller_par(4, lambda i: 1),
         Task_par(500, lambda i: random.expovariate(0.1),
                    lambda i: i*1000/500),
         ),
    ]

    # Run all simulation configurations.
    for par in SIMUL_PAR:
        simple_simulation(*par)