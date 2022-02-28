cdef extern from "<semaphore.h>" nogil:
    ctypedef struct sem_t:
        pass
    int sem_init(sem_t *sem, int pshared, unsigned int value)
    int sem_wait(sem_t *sem)
    int sem_post(sem_t *sem)
    int sem_getvalue(sem_t *, int *)
    int sem_destroy(sem_t* sem)
