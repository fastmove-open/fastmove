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


#define MUTIWORK_OFFSET (1<<16)
#define WORKER_FIFO_SIZE (1 << 7)
#define FREE_REQS_NUM 256
#define FREE_OP_ITERS_NUM 2048

/* for sysctl: */
#define SYSCTL_FASTMOVE_CPU (0)
#define SYSCTL_FASTMOVE_DMA (1)
#define SYSCTL_FASTMOVE_DM (2)

enum copy_type {
	FM_CPU,
	FM_DMA,
	FM_CAT_NUM
};

enum copy_directions {
	dir_local_read_t,
	dir_local_write_t,
	dir_remote_read_t,
	dir_remote_write_t,
	COPY_DIR_NUM
};

extern struct sysctl_fastmove {
	int		enable;
	int		memcg;
	int		mode;
	int		show_stats;
	int		ddio;
	int		scatter;
	int		sync_wait;
	int		num_nodes;
	int		main_switch; /* 0 for dma; 1 for cpu */
	int		worker_switch; /* 0 for dma; 1 for cpu */
	unsigned long	dbg_mask;
	unsigned long	max_user_num;
	unsigned long	chunks;
	unsigned long   inflight;
	unsigned long	local_read;
	unsigned long	remote_read;
	unsigned long	local_write;
	unsigned long	remote_write;
} sysctl_fastmove;

#define PLUGGED_CAPACITY (8)
#define PLUGGED_MASK (~((1 << 3) - 1))
#define POLICY_LOCAL (0)
#define POLICY_RANDOM (1)

struct plugged_device {
	struct dma_chan *chans[PLUGGED_CAPACITY];
	atomic64_t inflight_task_num;
	atomic64_t user_num;
};

extern struct fastmove {
	struct dma_chan **chan_map;
	struct plugged_device **dev_map;
	struct plugged_device *devices;
	atomic64_t index;
	__u64 num_nodes;
	__u64 capacity;
} fastmove;


#define FASTMOVE_DBGMASK_TIME_STAT	(0x00000001)
#define FASTMOVE_DBGMASK_VERBOSE	(0x00000002)
#define FASTMOVE_DBGMASK_MEM		(0x00000004)
#define FASTMOVE_DBGMASK_SIZE_STAT	(0x00000010)

#define fm_info(s, args ...) pr_info(s, ## args)
#define fm_err(s, args ...) pr_err(s, ## args)
#define fm_dbg(s, args ...) ((sysctl_fastmove.dbg_mask & FASTMOVE_DBGMASK_VERBOSE) ? pr_info(s, ## args) : 0)
#define fm_dbgm(s, args ...) ((sysctl_fastmove.dbg_mask & FASTMOVE_DBGMASK_MEM) ? pr_info(s, ## args) : 0)

enum fastmove_type {
	FASTMOVE_MEM_READ,
	FASTMOVE_MEM_WRITE,
	FASTMOVE_MEM_READ_ITER,
	FASTMOVE_MEM_WRITE_ITER,
};

struct fastmove_op {
	struct list_head ptr;
	enum fastmove_type type;
	const void* src;
	void* dst;
	size_t nr;
	int ret;
	void *data;
};

enum fastmove_req_type {
	FASTMOVE_REQ_NORMAL,
	FASTMOVE_REQ_ITER,
};

struct fastmove_req {
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
			int (*iomap_end)(struct inode *inode, loff_t pos,
					 loff_t length, ssize_t written,
					 unsigned flags, struct iomap *iomap);
			void (*finish)(struct fastmove_req *, void *);
			void * data;
		};
	};
	enum fastmove_req_type type;
};

extern int sysctl_dma_page_migration(struct ctl_table *table, int write,
				     void __user *buffer, size_t *lenp,
				     loff_t *ppos);
extern int sysctl_fastmove_control(struct ctl_table *table, int write,
				void __user *buffer, size_t *lenp,
				loff_t *ppos);
extern int sysctl_memcg_control(struct ctl_table *table, int write,
				void __user *buffer, size_t *lenp,
				loff_t *ppos);
extern int sysctl_fastmove_stats(struct ctl_table *table, int write,
			      void __user *buffer, size_t *lenp, loff_t *ppos);
extern ssize_t dax_iomap_rw_fastmove(struct kiocb *iocb, struct iov_iter *iter,
				  const struct iomap_ops *ops);
extern size_t memcpy_to_pmem_nocache_fastmove(struct block_device *, void *dst,
				   const void __user *src, unsigned long size);
extern size_t memcpy_mcsafe_fastmove(struct block_device *, void *dst,
				 const void *src, unsigned long size,
				 bool async);
extern size_t dax_copy_from_iter_fastmove(struct block_device *bdev, void *addr,
				       size_t bytes, struct iov_iter *i);
extern size_t dax_copy_to_iter_fastmove(struct block_device *bdev,
				     const void *addr, size_t bytes,
				     struct iov_iter *i);
extern int copy_page_fastmove(struct page *to, struct page *from, int nr_pages);
extern int copy_page_lists_fastmove(struct page **to, struct page **from,
				      int nr_pages);
extern int copy_page_lists_dma_always(struct page **to, struct page **from,
				      int nr_items);
extern int copy_page_lists_mt(struct page **to, struct page **from,
			      int nr_pages);
#endif
