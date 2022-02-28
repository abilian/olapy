from libcpp.deque cimport deque
from libcpp.vector cimport vector
from libcpp.atomic cimport atomic
from libc.stdio cimport printf
from libc.stdlib cimport rand
from posix.unistd cimport sysconf
from pthread.pthreads cimport *
from pthread.semaphore cimport *

cdef extern from "<unistd.h>" nogil:
    enum: _SC_NPROCESSORS_ONLN  # Seems to not be included in "posix.unistd".


cdef cypclass Scheduler
cdef cypclass Worker

# The 'inline' qualifier on this function is a hack to convince Cython
# to allow a definition in a .pxd file.
# The C compiler will dismiss it because we pass the function pointer
# to create a thread which prevents inlining.
cdef inline void * worker_function(void * arg) nogil:
    worker = <lock Worker> arg
    sch = <Scheduler> <void*> worker.scheduler
    # Wait until all the workers are ready.
    pthread_barrier_wait(&sch.barrier)
    while 1:
        # Wait until a queue becomes available.
        sem_wait(&sch.num_free_queues)
        # If the scheduler is done there is nothing to do anymore.
        if sch.is_done:
            return <void*> 0
        # Pop or steal a queue.
        queue = worker.get_queue()
        with wlocked queue:
            # Do one task on the queue.
            queue.activate()
            if queue.is_empty():
                # Mark the empty queue as not assigned to any worker.
                queue.has_worker = False
                # Decrement the number of non-completed queues.
                if sch.num_pending_queues.fetch_sub(1) == 1:
                    # Signal that there are no more queues.
                    sem_post(&sch.done)
                # Discard the empty queue and continue the main loop.
                continue
        # The queue is not empty: reinsert it in this worker's queues.
        with wlocked worker:
            worker.queues.push_back(queue)
        # Signal that the queue is available.
        sem_post(&sch.num_free_queues)


cdef cypclass Worker:
    deque[lock SequentialMailBox] queues
    lock Scheduler scheduler
    pthread_t thread

    lock Worker __new__(alloc, lock Scheduler scheduler):
        instance = consume alloc()
        instance.scheduler = scheduler
        locked_instance = <lock Worker> consume instance
        if not pthread_create(
                &locked_instance.thread, NULL, worker_function,
                <void *> locked_instance):
            return locked_instance
        printf("pthread_create() failed\n")

    lock SequentialMailBox get_queue(lock self):
        # Get the next queue in the worker's list or steal one.
        with wlocked self:
            if not self.queues.empty():
                queue = self.queues.front()
                self.queues.pop_front()
                return queue
        return self.steal_queue()

    lock SequentialMailBox steal_queue(lock self):
        # Steal a queue from another worker:
        # - inspect each worker in order starting at a random offset
        # - skip any worker with an empty queue list
        # - return the last queue of the first worker with a non-empty
        #   list
        # - continue looping until a queue is found
        cdef int i, index, num_workers, random_offset
        sch = <Scheduler> <void*> self.scheduler
        num_workers = <int> sch.workers.size()
        index = rand() % num_workers
        while True:
            victim = sch.workers[index]
            with wlocked victim:
                if not victim.queues.empty():
                    stolen_queue = victim.queues.back()
                    victim.queues.pop_back()
                    return stolen_queue
            index += 1
            if index >= num_workers:
                index = 0

    int join(self):
        # Join the worker thread.
        return pthread_join(self.thread, NULL)


cdef cypclass Scheduler:
    vector[lock Worker] workers
    pthread_barrier_t barrier
    sem_t num_free_queues
    atomic[int] num_pending_queues
    sem_t done
    volatile bint is_done
    int num_workers

    lock Scheduler __new__(alloc, int num_workers=0):
        self = <lock Scheduler> consume alloc()
        if num_workers == 0: num_workers = sysconf(_SC_NPROCESSORS_ONLN)
        self.num_workers = num_workers
        sem_init(&self.num_free_queues, 0, 0)
        sem_init(&self.done, 0, 0)
        self.num_pending_queues.store(0)
        if pthread_barrier_init(&self.barrier, NULL, num_workers + 1):
            printf("Could not allocate memory for the thread barrier\n")
            # Signal that no work will be done.
            sem_post(&self.done)
            return self
        self.is_done = False
        self.workers.reserve(num_workers)
        for i in range(num_workers):
            worker = Worker(self)
            if worker is NULL:
                # Signal that no work will be done.
                sem_post(&self.done)
                return self
            self.workers.push_back(worker)
        # Wait until all the worker threads are ready.
        pthread_barrier_wait(&self.barrier)
        return self

    __dealloc__(self):
        pthread_barrier_destroy(&self.barrier)
        sem_destroy(&self.num_free_queues)
        sem_destroy(&self.done)

    void post_queue(lock self, lock SequentialMailBox queue):
        cdef int num_workers, random_offset
        sch = <Scheduler> <void*> self
        # Add a queue to a random worker.
        num_workers = <int> sch.workers.size()
        random_offset = rand() % num_workers
        receiver = sch.workers[random_offset]
        with wlocked receiver:
            queue.has_worker = True
            receiver.queues.push_back(queue)
        # Increment the number of non-completed queues.
        sch.num_pending_queues.fetch_add(1)
        # Signal that a queue is available.
        sem_post(&sch.num_free_queues)

    void finish(lock self):
        # Wait until there is no more work.
        done = &self.done
        sem_wait(done)
        # Signal the worker threads that there is no more work.
        self.is_done = True
        # Pretend that there are new queues to wake up the workers.
        num_free_queues = &self.num_free_queues
        for worker in self.workers:
            sem_post(num_free_queues)
        # Clear the workers to break reference cycles.
        self.workers.clear()


cdef cypclass SequentialMailBox(ActhonQueueInterface):
    deque[ActhonMessageInterface] messages
    lock Scheduler scheduler
    bint has_worker

    __init__(self, lock Scheduler scheduler):
        self.scheduler = scheduler
        self.has_worker = False

    bint is_empty(const self):
        return self.messages.empty()

    void push(locked self, ActhonMessageInterface message):
        # Add a task to the queue.
        self.messages.push_back(message)
        if message._sync_method is not NULL:
            message._sync_method.insertActivity()
        # If no worker is already assigned this queue
        # register it with the scheduler.
        if not self.has_worker:
            self.scheduler.post_queue(self)

    bint activate(self):
        # Try to process the first message in the queue.
        cdef bint one_message_processed
        if self.messages.empty():
            return False
        next_message = self.messages.front()
        self.messages.pop_front()
        one_message_processed = next_message.activate()
        if one_message_processed:
            if next_message._sync_method is not NULL:
                next_message._sync_method.removeActivity()
        else:
            printf("Pushed front message to back :/\n")
            self.messages.push_back(next_message)
        return one_message_processed


cdef cypclass BatchMailBox(SequentialMailBox):
    bint activate(self):
        # Process as many messages as possible.
        while not self.messages.empty():
            next_message = self.messages.front()
            self.messages.pop_front()
            if not next_message.activate():
                printf("Pushed front message to back :/\n")
                self.messages.push_back(next_message)
                return False
            if next_message._sync_method is not NULL:
                next_message._sync_method.removeActivity()
        return True


cdef inline ActhonResultInterface NullResult() nogil:
    return NULL


# Taken from:
# https://lab.nexedi.com/nexedi/cython/blob/3.0a6-cypclass/tests/run/cypclass_acthon.pyx#L66
cdef cypclass WaitResult(ActhonResultInterface):
    union result_t:
        int int_val
        void* ptr
    result_t result
    sem_t semaphore

    __init__(self):
        self.result.ptr = NULL
        sem_init(&self.semaphore, 0, 0)

    __dealloc__(self):
        sem_destroy(&self.semaphore)

    @staticmethod
    ActhonResultInterface construct():
        return WaitResult()

    void pushVoidStarResult(self, void* result):
        self.result.ptr = result
        sem_post(&self.semaphore)

    void pushIntResult(self, int result):
        self.result.int_val = result
        sem_post(&self.semaphore)

    result_t _getRawResult(const self):
        # We must ensure a result exists, but we can let others access
        # it immediately
        # The cast here is a way of const-casting (we're modifying the
        # semaphore in a const method)
        sem_wait(<sem_t*> &self.semaphore)
        sem_post(<sem_t*> &self.semaphore)
        return self.result

    void* getVoidStarResult(const self):
        res = self._getRawResult()
        return res.ptr

    int getIntResult(const self):
        res = self._getRawResult()
        return res.int_val
