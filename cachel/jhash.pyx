cimport cython


@cython.cdivision(True)
cpdef long jhash(unsigned long long key, long buckets) nogil:
    cdef long b = -1
    cdef long long j = 0
    while j < buckets:
        b = j
        key = key * 2862933555777941757ULL + 1
        j = <long long>((b + 1) * <double>(0x80000000) / <double>((key >> 33) + 1.0))

    return b
