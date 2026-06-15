#define _GNU_SOURCE
#include <mpi.h>
#include <omp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <linux/limits.h>
#include <dirent.h>
#include <pthread.h>

#define TASK_GRAIN 8
#define BATCH_CAP 4096
#define INIT_CAP 256
#define MAX_WORK 65536
#define PATHLEN 512
#define FANOUT_DEPTH 3

struct linux_dirent64 {
ino64_t d_ino;
off64_t d_off;
unsigned short d_reclen;
unsigned char d_type;
char d_name[];
};

static inline long xgetdents(int fd, void *buf, size_t n) {
return syscall(SYS_getdents64, fd, buf, n);
}

typedef struct { char **v; int n, cap; } StrVec;

static void sv_init(StrVec *s, int cap) {
s->v = malloc((size_t)cap * sizeof(char *));
s->n = 0;
s->cap = cap;
}

static void sv_push(StrVec s, const char p) {
if (s->n == s->cap) {
s->cap *= 2;
s->v = realloc(s->v, (size_t)s->cap * sizeof(char *));
}

s->v[s->n++] = strdup(p);
}

static void sv_free(StrVec *s) {
for (int i = 0; i < s->n; i++) free(s->v[i]);
free(s->v);
s->n = s->cap = 0;
}

static void list_dir(const char path, StrVec dirs, StrVec *files) {
int fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
if (fd < 0) return;
enum { DBUF_SZ = 131072 };
char *dbuf = malloc(DBUF_SZ);
if (!dbuf) { close(fd); return; }
char child[PATH_MAX];
long nb;

while ((nb = xgetdents(fd, dbuf, DBUF_SZ)) > 0) {
for (long off = 0; off < nb; ) {
struct linux_dirent64 de = (struct linux_dirent64 )(dbuf + off);
off += de->d_reclen;
const char *nm = de->d_name;

if (nm[0] == ‘.’ && (nm[1] == ‘\0’ || (nm[1] == ‘.’ && nm[2] == ‘\0’)))
continue;

int clen = snprintf(child, PATH_MAX, “%s/%s”, path, nm);
if (clen <= 0 || clen >= PATH_MAX) continue;
unsigned char dt = de->d_type;

if (dt == DT_UNKNOWN) {
struct stat st;
if (stat(child, &st) != 0) continue;
dt = S_ISDIR(st.st_mode) ? DT_DIR : DT_REG;
}

if (dt == DT_DIR && dirs) sv_push(dirs, child);
else if (dt != DT_DIR && files) sv_push(files, child);
}
}
free(dbuf);
close(fd);
}

typedef struct {
char **paths;
int head;
int tail;
int cap;
int done;
pthread_mutex_t mu;
pthread_cond_t cv;
} WorkQueue;

static void wq_init(WorkQueue *q, int cap) {
q->paths = malloc((size_t)cap * sizeof(char *));
q->head = 0;
q->tail = 0;
q->cap = cap;
q->done = 0;
pthread_mutex_init(&q->mu, NULL);
pthread_cond_init(&q->cv, NULL);
}

static void wq_push(WorkQueue q, char path) {
pthread_mutex_lock(&q->mu);
if (q->tail < q->cap)
q->paths[q->tail++] = path;
else
free(path);
pthread_mutex_unlock(&q->mu);
pthread_cond_signal(&q->cv);
}

static char wq_pop(WorkQueue q) {
pthread_mutex_lock(&q->mu);
while (q->head == q->tail && !q->done)
pthread_cond_wait(&q->cv, &q->mu);
char *p = (q->head < q->tail) ? q->paths[q->head++] : NULL;
pthread_mutex_unlock(&q->mu);
return p;
}

static void wq_close(WorkQueue *q) {
pthread_mutex_lock(&q->mu);
q->done = 1;
pthread_mutex_unlock(&q->mu);
pthread_cond_broadcast(&q->cv);
}

static void wq_free(WorkQueue *q) {
for (int i = q->head; i < q->tail; i++) free(q->paths[i]);
free(q->paths);
pthread_mutex_destroy(&q->mu);
pthread_cond_destroy(&q->cv);
}

static void fanout(const char path, int depth, WorkQueue q) {
if (depth == 0) {
wq_push(q, strdup(path));
return;
}

StrVec dirs;
sv_init(&dirs, INIT_CAP);
list_dir(path, &dirs, NULL);
if (dirs.n == 0) {
wq_push(q, strdup(path));
} else {
for (int i = 0; i < dirs.n; i++)
fanout(dirs.v[i], depth - 1, q);
}
sv_free(&dirs);
}

typedef struct { WorkQueue q; const char root; } FanoutArg;

static void fanout_thread(void arg) {
FanoutArg *fa = arg;
fanout(fa->root, FANOUT_DEPTH, fa->q);
wq_close(fa->q);
return NULL;
}

static int build_work(const char *root, char (**out)[PATHLEN]) {
char (buf)[PATHLEN] = malloc((size_t)MAX_WORK sizeof(*buf));
if (!buf) return -1;
WorkQueue q;
wq_init(&q, MAX_WORK);
FanoutArg fa = { .q = &q, .root = root };
pthread_t tid;
pthread_create(&tid, NULL, fanout_thread, &fa);
int total = 0;
char *p;

while ((p = wq_pop(&q)) != NULL) {
if (total < MAX_WORK)
snprintf(buf[total++], PATHLEN, “%s”, p);
free(p);
}

pthread_join(tid, NULL);
wq_free(&q);
*out = buf;
return total;
}

typedef struct { char buf[BATCH_CAP]; int len; } PrintBuf;

static void pb_flush(PrintBuf *pb) {
if (pb->len > 0) {
fwrite(pb->buf, 1, (size_t)pb->len, stdout);
pb->len = 0;
}
}

static void pb_append(PrintBuf pb, int rank, int tid, const char path) {
int n = snprintf(pb->buf + pb->len, (size_t)(BATCH_CAP - pb->len), “[MPI %d OMP %d] %s”, rank, tid, path);
if (n <= 0) return;
pb->len += n;

if (pb->len > BATCH_CAP - 256) pb_flush(pb);
}

typedef struct { const char *pat; int rank; } Ctx;

static void search_node(const char path, const Ctx ctx, PrintBuf *pb) {
StrVec dirs, files;
sv_init(&dirs, INIT_CAP);
sv_init(&files, INIT_CAP);
list_dir(path, &dirs, &files);
for (int i = 0; i < files.n; i++) {
const char *base = strrchr(files.v[i], ‘/’);
base = base ? base + 1 : files.v[i];
if (strstr(base, ctx->pat))
pb_append(pb, ctx->rank, omp_get_thread_num(), files.v[i]);
}

sv_free(&files);

for (int i = 0; i < dirs.n; i++) {
char *sub = dirs.v[i];
if (dirs.n >= TASK_GRAIN) {
#pragma omp task firstprivate(sub) shared(ctx)
{
PrintBuf tpb; tpb.len = 0;
search_node(sub, ctx, &tpb);
pb_flush(&tpb);
free(sub);
}
dirs.v[i] = NULL;

} else {
search_node(sub, ctx, pb);
}
}

#pragma omp taskwait

for (int i = 0; i < dirs.n; i++) free(dirs.v[i]);
free(dirs.v);
}

static void search_root(const char root, const Ctx ctx) {
#pragma omp parallel
#pragma omp single nowait
{
PrintBuf pb; pb.len = 0;
search_node(root, ctx, &pb);
pb_flush(&pb);
}
}

int main(int argc, char **argv) {

int provided;
MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);
int rank, size;
MPI_Comm_rank(MPI_COMM_WORLD, &rank);
MPI_Comm_size(MPI_COMM_WORLD, &size);
if (argc < 3) {
if (rank == 0)
fprintf(stderr, “Usage: %s ”, argv[0]);
MPI_Finalize();
return 1;
}

const char *root = argv[1];
const char *pattern = argv[2];
char (*work)[PATHLEN] = NULL;
int nwork = 0;
if (rank == 0) {
nwork = build_work(root, &work);
if (nwork < 0) {
fprintf(stderr, “rank 0: build_work failed”);
nwork = 0;
}
}

MPI_Bcast(&nwork, 1, MPI_INT, 0, MPI_COMM_WORLD);
char (my_work)[PATHLEN] = malloc(sizeof(my_work));
int my_count = 0;
if (nwork > 0) {
int sendcounts = NULL, displs = NULL;
if (rank == 0) {
sendcounts = calloc((size_t)size, sizeof(int));
displs = calloc((size_t)size, sizeof(int));
for (int i = 0; i < nwork; i++)
sendcounts[i % size]++;
int off = 0;
for (int i = 0; i < size; i++) {
displs[i] = off * PATHLEN;
off += sendcounts[i];
sendcounts[i] *= PATHLEN;
}
}

int my_bytes = 0;
MPI_Scatter(sendcounts, 1, MPI_INT,
&my_bytes, 1, MPI_INT,
0, MPI_COMM_WORLD);
my_count = my_bytes / PATHLEN;

if (my_count > 0) {
free(my_work);
my_work = malloc((size_t)my_count * sizeof(*my_work));
}

MPI_Scatterv(work, sendcounts, displs, MPI_BYTE, my_work, my_bytes, MPI_BYTE, 0, MPI_COMM_WORLD);

if (rank == 0) { free(sendcounts); free(displs); }
}

if (rank == 0) free(work);
Ctx ctx = { .pat = pattern, .rank = rank };
for (int i = 0; i < my_count; i++)
search_root(my_work[i], &ctx);
free(my_work);
MPI_Barrier(MPI_COMM_WORLD);

if (rank == 0)
printf(“[done] search for "%s" in "%s" complete across %d ranks.”, pattern, root, size);
MPI_Finalize();

return 0;
}
