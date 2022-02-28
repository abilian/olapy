cdef extern from "<sys/types.h>" nogil:
    ctypedef long unsigned int pthread_t

    ctypedef union pthread_attr_t:
        pass
    ctypedef union pthread_mutex_t:
        pass
    ctypedef union pthread_mutexattr_t:
        pass
    ctypedef union pthread_barrier_t:
        pass
    ctypedef union pthread_barrierattr_t:
        pass
    ctypedef union pthread_cond_t:
        pass
    ctypedef union pthread_condattr_t:
        pass


cdef extern from "<pthread.h>" nogil:
    int pthread_create(pthread_t *, const pthread_attr_t *, void *(*)(void *), void *)
    void pthread_exit(void *)
    int pthread_join(pthread_t, void **)
    int pthread_cancel(pthread_t thread)
    int pthread_attr_init(pthread_attr_t *)
    int pthread_attr_setdetachstate(pthread_attr_t *, int)
    int pthread_attr_destroy(pthread_attr_t *)

    int pthread_mutex_init(pthread_mutex_t *, const pthread_mutexattr_t *)
    int pthread_mutex_destroy(pthread_mutex_t *)
    int pthread_mutex_lock(pthread_mutex_t *)
    int pthread_mutex_unlock(pthread_mutex_t *)
    int pthread_mutex_trylock(pthread_mutex_t *)

    int pthread_barrier_init(pthread_barrier_t *, const pthread_barrierattr_t *, unsigned int)
    int pthread_barrier_destroy(pthread_barrier_t *)
    int pthread_barrier_wait(pthread_barrier_t *)

    int pthread_cond_init(pthread_cond_t * cond, const pthread_condattr_t * attr)
    int pthread_cond_destroy(pthread_cond_t *cond)
    int pthread_cond_wait(pthread_cond_t * cond, pthread_mutex_t * mutex)
    int pthread_cond_broadcast(pthread_cond_t *cond)
    int pthread_cond_signal(pthread_cond_t *cond)

    enum: PTHREAD_CREATE_JOINABLE
