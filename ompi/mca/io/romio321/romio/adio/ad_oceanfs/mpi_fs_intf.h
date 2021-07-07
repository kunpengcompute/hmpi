#ifndef _MPI_FS_INTERFACE_H_
#define _MPI_FS_INTERFACE_H_
#include <unistd.h>
#include <dirent.h>
#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <linux/types.h>
#include <linux/uuid.h>
#include <linux/ioctl.h>

typedef unsigned int u32;

/*
#define EPERM     1    Operation not permitted 
#define ENOEN     2    No such file or directory 
#define ESRCH     3    No such process 
#define EINTR     4    Interrupted system call 
#define EIO       5    I/O error 
#define ENXIO     6    No such device or address 
#define E2BIG     7    Argument list too long 
#define ENOEX     8    Exec format error 
#define EBADF     9    Bad file number 
#define ECHIL    10    No child processes 
#define EAGAI    11    Try again 
#define ENOME    12    Out of memory 
#define EACCE    13    Permission denied 
#define EFAUL    14    Bad address 
#define ENOTB    15    Block device required 
#define EBUSY    16    Device or resource busy 
#define EEXIS    17    File exists 
#define EXDEV    18    Cross-device link 
#define ENODE    19    No such device 
#define ENOTD    20    Not a directory 
#define EISDI    21    Is a directory 
#define EINVA    22    Invalid argument 
#define ENFIL    23    File table overflow 
#define EMFIL    24    Too many open files 
#define ENOTT    25    Not a typewriter 
#define ETXTB    26    Text file busy 
#define EFBIG    27    File too large 
#define ENOSP    28    No space left on device 
#define ESPIP    29    Illegal seek 
#define EROFS    30    Read-only file system 
#define EMLIN    31    Too many links 
#define EPIPE    32    Broken pipe 
#define EDOM     33    Math argument out of domain of func 
#define ERANG    34    Math result not representable
*/

#define FS_MAX_XATTR_NAME_LEN 256UL
#define FS_MAX_NAME_LEN 1024UL
#define FS_MAX_PATH_LENGTH 4096UL
#define FS_MAX_LONG_NAME_LEN 1024UL

int mpi_fs_open(const char *pathname, int flags, mode_t mode);

int mpi_fs_pread(int fd, void *buf, size_t count, off_t offset);

int mpi_fs_pwrite(int fd, const void *buf, size_t count, off_t offset);

int mpi_fs_stat(const char *pathname, struct stat *buf);

int mpi_fs_close(int fd);

int mpi_fs_ftruncate(int fd, off_t length);

off_t mpi_fs_lseek(int fd, off_t offset, int whence);

int mpi_fs_set_fileview(int fd, off_t offset, u32 count, u32 *blocklens, off_t *blockoffs, off_t ub_off);

int mpi_fs_view_read(int fd, u32 iovcnt, struct iovec *iov, off_t offset);

int mpi_fs_get_group_id(int fd, uint64_t *group_id);

int mpi_fs_set_group_id(int fd, uint64_t group_id);

#endif
