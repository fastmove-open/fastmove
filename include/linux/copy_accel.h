/* SPDX-License-Identifier: GPL-2.0 */
#ifndef _LINUX_COPY_ACCEL_H
#define _LINUX_COPY_ACCEL_H
#include <linux/list.h>
#include <linux/kfifo.h>
#include <linux/completion.h>
#include <linux/threads.h>
#include <linux/time.h>
#include <linux/iomap.h>
#include <linux/types.h>


#define WORKER_FIFO_SIZE (1 << 7)
#define FREE_REQS_NUM 256
#define FREE_OP_ITERS_NUM 2048

/* for sysctl: */
extern unsigned long copy_accel_dbg_mask;
extern unsigned long copy_accel_max_dma_concurrency;
extern int copy_accel_ddio_enabled;
extern int copy_accel_sync_wait;
extern int copy_accel_enabled_nodes;
extern unsigned long copy_accel_local_read_threshold;
extern unsigned long copy_accel_remote_read_threshold;
extern unsigned long copy_accel_local_write_threshold;
extern unsigned long copy_accel_remote_write_threshold;

#define COPY_DBGMASK_TIME_STAT	(0x00000001)
#define COPY_DBGMASK_VERBOSE	(0x00000002)
#define COPY_DBGMASK_MEM		(0x00000004)
#define COPY_DBGMASK_SIZE_STAT	(0x00000010)

#define copy_info(s, args ...) pr_info(s, ## args)
#define copy_err(s, args ...) pr_err(s, ## args)
#define copy_dbg(s, args ...) ((copy_accel_dbg_mask & COPY_DBGMASK_VERBOSE) ? pr_info(s, ## args) : 0)
#define copy_dbgm(s, args ...) ((copy_accel_dbg_mask & COPY_DBGMASK_MEM) ? pr_info(s, ## args) : 0)



struct copy_info;

enum copy_type {
	COPY_SDMA,
	COPY_DMA
};

enum copy_mem_type {
	COPY_MEM_READ,
	COPY_MEM_WRITE,
	COPY_MEM_READ_ITER,
	COPY_MEM_WRITE_ITER,
};

struct copy_write_info {
	bool hole_fill;
	unsigned long blocknr;
	int allocated;
};

struct copy_op {
	struct list_head ptr;
	enum copy_mem_type type;
	const void* src;
	void* dst;
	size_t nr;
	int ret;
	void *data;
};

enum copy_req_type {
	COPY_REQ_NORMAL,
	COPY_REQ_ITER,
};

struct copy_req {
	struct list_head op_head;
	struct mm_struct *mm;
	ktime_t time;
	int dma_thread_id;
	int node;
	ssize_t ret;
	union {
		struct completion done;
		struct {
			struct kiocb *iocb;
			struct iomap map;
			/* can be extracted from iocb */
			/* struct inode *inode; */
			/* can be calculated */
			/* ssize_t written; */
			loff_t pos;
			loff_t flags;
			loff_t length;
			int (*iomap_end)(struct inode *inode, loff_t pos, loff_t length, ssize_t written, unsigned flags, struct iomap *iomap);
			void (*finish)(struct copy_req *, void *);
			void * data;
		};
	};
	enum copy_req_type type;
};

struct copy_node_info {
	struct copy_info *ci;
	/* dma info */
	struct task_struct **sdma_threads;
	struct copy_sdma_ctx *sdma_thread_ctxs;
	/* per node dma threads number */
	atomic_t sdma_thread_idx;

	int sdma_thd_per_node;

	int node;
};

struct copy_sdma_ctx {
	struct copy_node_info *ni;
	DECLARE_KFIFO_PTR(fifo, struct copy_req*);
	spinlock_t fifo_lock;
	DECLARE_KFIFO_PTR(reqs_fifo, struct copy_req*);
	spinlock_t reqs_lock;
	struct copy_req **req_pool;
	DECLARE_KFIFO_PTR(op_iters_fifo, struct copy_op*);
	spinlock_t op_iters_lock;
	struct copy_op **op_iter_pool;
	wait_queue_head_t sdma_thread_wait;
};

struct copy_info {
	unsigned sdma_thd_per_node;
	struct copy_node_info **node_infos;
	struct dax_device *dax_dev;
	int node;
	int node_num;
};



int copy_init(void);
int copy_destroy(void);
ssize_t copy_iomap_rw_sync(struct kiocb *iocb, struct iov_iter *iter, const struct iomap_ops *ops);
size_t dma_copy_from_user(struct block_device*, void *dst, const void __user *src, unsigned long size);
size_t dma_copy_to_user(struct block_device*, void *dst, const void *src, unsigned long size, bool async);
size_t dma_copy_from_iter(struct block_device *bdev, void *addr, size_t bytes, struct iov_iter *i);
size_t dma_copy_to_iter(struct block_device *bdev, const void *addr, size_t bytes, struct iov_iter *i);
int
copy_dma_run_bench_sgs(struct scatterlist *dst_sgl, struct scatterlist *src_sgl, int rw,
			int num_req, unsigned int chan_node, unsigned int chan_idx);
#endif
