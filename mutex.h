
#undef TRACEPOINT_PROVIDER
#define TRACEPOINT_PROVIDER rocksdb

#undef TRACEPOINT_INCLUDE
#define TRACEPOINT_INCLUDE "./mutex.h"

#if !defined(MUTEX_H) || defined(TRACEPOINT_HEADER_MULTI_READ)
#define MUTEX_H

#include <lttng/tracepoint.h>

TRACEPOINT_EVENT(rocksdb, db_mutex_lock_acq,
	TP_ARGS(void *, mutex, unsigned long long, tid),
	TP_FIELDS(
		ctf_integer_hex(void *, mutex, mutex)
    ctf_integer_hex(unsigned long long, tid, tid)
	)
)

TRACEPOINT_EVENT(rocksdb, db_mutex_unlock,
	TP_ARGS(void *, mutex, unsigned long long, tid),
	TP_FIELDS(
		ctf_integer_hex(void *, mutex, mutex)
    ctf_integer_hex(unsigned long long, tid, tid)
	)
)

#endif /* MUTEX_H */

#include <lttng/tracepoint-event.h>
