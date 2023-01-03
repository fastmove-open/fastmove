#define pr_fmt(fmt) "%s: " fmt, __func__
#include <linux/copy_accel.h>
#include <linux/slab.h>
#include <linux/uio.h>
#include <linux/kthread.h>
#include <linux/sched/mm.h>
#include <linux/sched/signal.h>
#include <linux/mmu_context.h>
#include <linux/dma-mapping.h>
#include <linux/dmaengine.h>
#include <linux/dax.h>
#include <linux/wait.h>
#include <linux/freezer.h>
#include <linux/proc_fs.h>
#include <linux/seq_file.h>
#include <linux/percpu-defs.h>
#include <linux/libnvdimm.h>
#include <asm/uaccess.h>

/* procfs variables */
const char *proc_dirname = "fs/copy_accel";
struct proc_dir_entry *copy_proc_root;

/* kmem_cache for copy op and copy req */
static struct kmem_cache	*cop_cachep;
static struct kmem_cache	*creq_cachep;
/*------ sysctl variables----*/
unsigned long copy_accel_dbg_mask = 0;
unsigned long copy_accel_max_dma_concurrency = 4;
int copy_accel_ddio_enabled = 1;
int copy_accel_sync_wait = 0;
int copy_accel_enabled_nodes = 0;
unsigned long copy_accel_local_read_threshold = 1 << 16;
unsigned long copy_accel_remote_read_threshold = 1 << 16;
unsigned long copy_accel_local_write_threshold = 1 << 16;
unsigned long copy_accel_remote_write_threshold = 1 << 14;
/*----end sysctl variables---*/

/*----- stats begin -----*/
enum copy_hist_cats {
	hist_read_t,
	hist_read_bytes_t,
	hist_aligned_t,
	hist_aligned_bytes_t,
	hist_unaligned_t,
	hist_unaligned_bytes_t,
	hist_cpu_read_t,
	hist_cpu_read_bytes_t,
	hist_cpu_write_t,
	hist_cpu_write_bytes_t,
	HIST_CAT_NUM
};

const char *hist_strs[HIST_CAT_NUM] = {
	"read",
	"read_bytes",
	"aligned_write",
	"aligned_bytes",
	"unaligned_write",
	"unaligned_bytes",
	"cpu_read",
	"cpu_read_bytes",
	"cpu_write",
	"cpu_write_bytes"
};

enum copy_time_cats {
	copy_read_t,
	copy_write_t,
	copy_dma_pin_t,
	copy_dma_submit_t,
	copy_dma_wait_t,
	copy_dma_unpin_t,
	copy_huge_read_t,
	copy_huge_write_t,
	copy_read_thresh,
	copy_read_busy,
	copy_write_thresh,
	copy_write_busy,
	TIME_CAT_NUM
};

const char *time_strs[TIME_CAT_NUM] = {
	"memcpy_read",
	"memcpy_write",
	"copy_dma_pin",
	"copy_dma_submit",
	"copy_dma_wait",
	"copy_dma_unpin",
	"copy_dma_huge_read",
	"copy_dma_huge_write",
	"copy_read_thresh",
	"copy_read_busy",
	"copy_write_thresh",
	"copy_write_busy"
};


#define COPY_INLINE_SIZE	(PAGE_SIZE*2)
#define COPY_INLINE_PAGES	(COPY_INLINE_SIZE / sizeof(struct scatterlist) / 2)
// 64B - 256K
#define COPY_HIST_BINS 14
u64 IOhist[2 * COPY_HIST_BINS * HIST_CAT_NUM];
DEFINE_PER_CPU(u64[2 * COPY_HIST_BINS * HIST_CAT_NUM], IOhist_percpu);
u64 time_stats[TIME_CAT_NUM];
DEFINE_PER_CPU(u64[TIME_CAT_NUM], time_stats_percpu);
u64 cnt_stats[TIME_CAT_NUM];
DEFINE_PER_CPU(u64[TIME_CAT_NUM], cnt_stats_percpu);
DEFINE_PER_CPU(u64[3], bufs_percpu);

#define COPY_START_TIME_STAT(name, start) \
	{if (copy_accel_dbg_mask & COPY_DBGMASK_TIME_STAT) start = ktime_get(); }

#define COPY_END_TIME_STAT(name, start) { \
		if (copy_accel_dbg_mask & COPY_DBGMASK_TIME_STAT) { \
			ktime_t __temp = ktime_get(); \
			__this_cpu_add(time_stats_percpu[name], ktime_to_ns(ktime_sub(__temp, start))); \
			start = __temp; \
		} \
		__this_cpu_add(cnt_stats_percpu[name], 1); \
	}

int copy_hist_bin(unsigned long v) {
	int r = __fls(v);
	if (r < 6) {
		return 0;
	} else if (r > COPY_HIST_BINS + 5) {
		return COPY_HIST_BINS - 1;
	} else {
		return r - 6;
	}
}

#define COPY_HIST_ADD_READ(numa, value) \
	if (copy_accel_dbg_mask & COPY_DBGMASK_SIZE_STAT) \
	{int bin = copy_hist_bin(value); \
	 __this_cpu_add(IOhist_percpu[bin + hist_read_t * COPY_HIST_BINS + numa * COPY_HIST_BINS * HIST_CAT_NUM], 1); \
	 __this_cpu_add(IOhist_percpu[bin + hist_read_bytes_t * COPY_HIST_BINS + numa * COPY_HIST_BINS * HIST_CAT_NUM], value); }

#define COPY_HIST_ADD_WRITE_ALIGNED(numa, value) \
	if (copy_accel_dbg_mask & COPY_DBGMASK_SIZE_STAT) \
	{int bin = copy_hist_bin(value); \
	 __this_cpu_add(IOhist_percpu[bin + hist_aligned_t * COPY_HIST_BINS + numa * COPY_HIST_BINS * HIST_CAT_NUM], 1); \
	 __this_cpu_add(IOhist_percpu[bin + hist_aligned_bytes_t * COPY_HIST_BINS + numa * COPY_HIST_BINS * HIST_CAT_NUM], value); }

#define COPY_HIST_ADD_WRITE_UNALIGNED(numa, value) \
	if (copy_accel_dbg_mask & COPY_DBGMASK_SIZE_STAT) \
	{int bin = copy_hist_bin(value); \
	 __this_cpu_add(IOhist_percpu[bin + hist_unaligned_t * COPY_HIST_BINS + numa * COPY_HIST_BINS * HIST_CAT_NUM], 1); \
	 __this_cpu_add(IOhist_percpu[bin + hist_unaligned_bytes_t * COPY_HIST_BINS + numa * COPY_HIST_BINS * HIST_CAT_NUM], value); }

#define COPY_HIST_ADD_CPU_READ(numa, value) \
	if (copy_accel_dbg_mask & COPY_DBGMASK_SIZE_STAT) \
	{int bin = copy_hist_bin(value); \
	 __this_cpu_add(IOhist_percpu[bin + hist_cpu_read_t * COPY_HIST_BINS + numa * COPY_HIST_BINS * HIST_CAT_NUM], 1); \
	 __this_cpu_add(IOhist_percpu[bin + hist_cpu_read_bytes_t * COPY_HIST_BINS + numa * COPY_HIST_BINS * HIST_CAT_NUM], value); }

#define COPY_HIST_ADD_CPU_WRITE(numa, value) \
	if (copy_accel_dbg_mask & COPY_DBGMASK_SIZE_STAT) \
	{int bin = copy_hist_bin(value); \
	 __this_cpu_add(IOhist_percpu[bin + hist_cpu_write_t * COPY_HIST_BINS + numa * COPY_HIST_BINS * HIST_CAT_NUM], 1); \
	 __this_cpu_add(IOhist_percpu[bin + hist_cpu_write_bytes_t * COPY_HIST_BINS + numa * COPY_HIST_BINS * HIST_CAT_NUM], value); }

static void copy_get_hist_stat(void)
{
	int i;
	int cpu;

	for (i = 0; i < 2 * COPY_HIST_BINS * HIST_CAT_NUM; i++) {
		IOhist[i] = 0;
		for_each_possible_cpu(cpu)
			IOhist[i] += per_cpu(IOhist_percpu[i], cpu);
	}
}

/*
static void copy_print_hist(void)
{
	int j = 0;
	copy_info("=========== copy_accel hist ===========\n");
	for (j = 0;j < HIST_CAT_NUM;j++) {
		int i = j * COPY_HIST_BINS;
		copy_info("%s: [%llu, %llu, %llu, %llu, %llu, %llu,"
				"%llu, %llu, %llu, %llu, %llu, %llu, %llu]\n",
				hist_strs[j], IOhist[i + 0], IOhist[i + 1], IOhist[i + 2],
				IOhist[i + 3], IOhist[i + 4], IOhist[i + 5], IOhist[i + 6],
				IOhist[i + 7], IOhist[i + 8], IOhist[i + 9], IOhist[i + 10],
				IOhist[i + 11], IOhist[i + 12]);
	}
}
*/

static void copy_clear_hist(void)
{
	int i;
	int cpu;

	for (i = 0; i < 2 * COPY_HIST_BINS * HIST_CAT_NUM; i++) {
		IOhist[i] = 0;
		for_each_possible_cpu(cpu)
			per_cpu(IOhist_percpu[i], cpu) = 0;
	}
}

static void copy_get_time_stat(void)
{
	int i;
	int cpu;

	for (i = 0; i < TIME_CAT_NUM; i++) {
		time_stats[i] = 0;
		cnt_stats[i] = 0;
		for_each_possible_cpu(cpu) {
			time_stats[i] += per_cpu(time_stats_percpu[i], cpu);
			cnt_stats[i] += per_cpu(cnt_stats_percpu[i], cpu);
		}
	}
}

/*
static void copy_print_time(void)
{
	int j = 0;
	copy_info("=========== copy_accel time ===========\n");
	for (j = 0;j < TIME_CAT_NUM;j++) {
		if (copy_accel_dbg_mask & COPY_DBGMASK_TIME_STAT) {
			copy_info("%s: count  %llu, time %llu, average %llu\n",
				time_strs[j], cnt_stats[j], time_stats[j],
				cnt_stats[j] ? time_stats[j] / cnt_stats[j] : 0);
		} else {
			copy_info("%s: count %llu\n", time_strs[j], cnt_stats[j]);
		}
	}
}
*/

static void copy_clear_time_stat(void)
{
	int i;
	int cpu;

	for (i = 0; i < TIME_CAT_NUM; i++) {
		for_each_possible_cpu(cpu) {
			per_cpu(time_stats_percpu[i], cpu) = 0;
			per_cpu(cnt_stats_percpu[i], cpu) = 0;
		}
	}
}

static int copy_seq_stats_show(struct seq_file *seq, void *v) {
	int j = 0;
	int offset = HIST_CAT_NUM * COPY_HIST_BINS;
	copy_get_time_stat();
	copy_get_hist_stat();
	seq_puts(seq, "=========== copy_accel hist (local) ===========\n");
	for (j = 0;j < HIST_CAT_NUM;j++) {
		int k, i = j * COPY_HIST_BINS;
		seq_printf(seq, "%s: [", hist_strs[j]);
		for (k = 0;k < COPY_HIST_BINS;k++) {
			seq_printf(seq, "%llu", IOhist[i + k]);
			if (k != COPY_HIST_BINS - 1)
				seq_printf(seq, ", ");
			else
				seq_printf(seq, "]\n");
		}
	}
	seq_puts(seq, "=========== copy_accel hist (remote) ===========\n");
	for (j = 0;j < HIST_CAT_NUM;j++) {
		int k, i = j * COPY_HIST_BINS;
		seq_printf(seq, "%s: [", hist_strs[j]);
		for (k = 0;k < COPY_HIST_BINS;k++) {
			seq_printf(seq, "%llu", IOhist[offset + i + k]);
			if (k != COPY_HIST_BINS - 1)
				seq_printf(seq, ", ");
			else
				seq_printf(seq, "]\n");
		}
	}
	seq_puts(seq, "=========== copy_accel time ===========\n");
	for (j = 0;j < TIME_CAT_NUM;j++) {
		if (copy_accel_dbg_mask & COPY_DBGMASK_TIME_STAT) {
			seq_printf(seq, "%s: count  %llu, time %llu, average %llu\n",
				time_strs[j], cnt_stats[j], time_stats[j],
				cnt_stats[j] ? time_stats[j] / cnt_stats[j] : 0);
		} else {
			seq_printf(seq, "%s: count %llu\n", time_strs[j], cnt_stats[j]);
		}
	}
	return 0;
}

static int copy_seq_stats_open(struct inode *inode, struct file *file)
{
	return single_open(file, copy_seq_stats_show, NULL);
}

static ssize_t copy_seq_stats_clear(struct file *filp, const char __user *buf,
	size_t len, loff_t *ppos) {
	copy_clear_hist();
	copy_clear_time_stat();
	return len;
}

static const struct proc_ops copy_seq_stats_fops = {
	.proc_open		= copy_seq_stats_open,
	.proc_read		= seq_read,
	.proc_write		= copy_seq_stats_clear,
	.proc_lseek		= seq_lseek,
	.proc_release	= single_release,
};

static ssize_t copy_control(struct file *filp, const char __user *buf,
	size_t len, loff_t *ppos) {
	char a;
	if (copy_from_user(&a, buf, 1) == 1) {
		copy_err("read command error!\n");
		return len;
	}
	if (a == '1') {
		copy_init();
	} else if (a == '0') {
		copy_destroy();
	} else {
		copy_err("Unknown control command: %c\n", a);
	}
	return len;
}

static const struct proc_ops copy_control_fops = {
	.proc_write		= copy_control
};

/*------ stats end ------*/

static int __init copy_accel_init(void)
{
	int cpu;
	cop_cachep = KMEM_CACHE(copy_op, SLAB_HWCACHE_ALIGN|SLAB_PANIC);
	creq_cachep = KMEM_CACHE(copy_req,SLAB_HWCACHE_ALIGN|SLAB_PANIC);
	copy_proc_root = proc_mkdir(proc_dirname, NULL);
	if (copy_proc_root) {
		proc_create_data("IO_stats", 0444, copy_proc_root, &copy_seq_stats_fops, NULL);
		proc_create_data("enable", 0444, copy_proc_root, &copy_control_fops, NULL);
	}
	for_each_possible_cpu(cpu) {
		per_cpu(bufs_percpu[0], cpu) = (u64)kmalloc_array(COPY_INLINE_PAGES, sizeof(struct page *), GFP_KERNEL);
		per_cpu(bufs_percpu[1], cpu) = (u64)kmalloc(2 * COPY_INLINE_SIZE, GFP_KERNEL);
		per_cpu(bufs_percpu[2], cpu) = 0;
	}
	return 0;
}
__initcall(copy_accel_init);

static void __exit copy_accel_exit(void)
{
	int cpu;
	kmem_cache_destroy(cop_cachep);
	kmem_cache_destroy(creq_cachep);
	copy_proc_root = proc_mkdir(proc_dirname, NULL);
	if (copy_proc_root) {
		remove_proc_entry("IO_stats", copy_proc_root);
		remove_proc_entry("enable", copy_proc_root);
		copy_proc_root = NULL;
	}
	for_each_possible_cpu(cpu) {
		kfree((void *)per_cpu(bufs_percpu[0], cpu));
		kfree((void *)per_cpu(bufs_percpu[1], cpu));
	}
}
__exitcall(copy_accel_exit);

#define iterate_and_advance(i, n, v, I) { \
	if (unlikely(i->count < n)) \
		n = i->count; \
	if (i->count) { \
		size_t skip = i->iov_offset; \
		const struct iovec *iov; \
		struct iovec v; \
		size_t left; \
		size_t wanted = n; \
		iov = i->iov; \
		v.iov_len = min(n, iov->iov_len - skip); \
		if (likely(v.iov_len)) { \
			v.iov_base = iov->iov_base + skip; \
			left = (I); \
			v.iov_len -= left; \
			skip += v.iov_len; \
			n -= v.iov_len; \
		} else { \
			left = 0; \
		} \
		while (unlikely(!left && n)) { \
			iov++; \
			v.iov_len = min(n, iov->iov_len); \
			if (unlikely(!v.iov_len)) \
				continue; \
			v.iov_base = iov->iov_base; \
			left = (I); \
			v.iov_len -= left; \
			skip = v.iov_len; \
			n -= v.iov_len; \
		} \
		n = wanted - n; \
		if (skip == iov->iov_len) { \
			iov++; \
			skip = 0; \
		} \
		i->nr_segs -= iov - i->iov; \
		i->iov = iov; \
		i->count -= n; \
		i->iov_offset = skip; \
	} \
}

static sector_t copy_iomap_sector(struct iomap *iomap, loff_t pos)
{
	return (iomap->addr + (pos & PAGE_MASK) - iomap->offset) >> 9;
}


/* dma part start */

#define COPY_CHAN_PER_NODE	(8)

struct copy_dma_device {
	struct dma_chan		*chans[COPY_CHAN_PER_NODE];
	atomic64_t			seqid;
	atomic64_t			active;
};

/**
 * struct copy_dma_engine - ship dma channel information.           
 */
static struct copy_dma_engine {
	struct copy_dma_device *devices;
	int num_nodes;
} copy_engine;



/*
 * IO submission data structure (Submission Queue Entry)
 */
struct copy_dma_req {
	__u64				uid;
	__u64				cid;

	struct {
		u64				virt;	// virtual address
		struct sg_table	tbl;	// sg table
	} dst, src;

	struct scatterlist	*sgl;	// temporary scatterlist
	unsigned long		len;	// data length
	atomic_t			finish;

	/* dma channel session */
	bool				sync;
	bool				ddio_enabled;
	bool				read;
	struct dma_chan		*chan;
	unsigned int		dma_req_nums;
	struct completion	done;
};

/*
 * For the data that needs to be copied, the operations within the range of src 
 * and dst are processed page by page in units of pages. If src and dst occupy 
 * multiple pages, the data in one page of src may involve two pages in dst, so 
 * it is necessary to divide the range of src and dst according to the page size 
 * to ensure byte-by-byte mapping with page granularity
 */
static int copy_dma_pin_pair_sgtable(struct copy_dma_req *sqe)
{
	unsigned int gap;
	typeof(sqe->dst) *user_info, *pmem_info;
	unsigned long pg_shift, pg_size, pg_mask;
	int i;
	int num, ret;
	unsigned int gup_flags = 0;
	struct vm_area_struct *vma;
	unsigned long page_link = NULL;

	if (sqe->read) {
		user_info = &sqe->dst;
		pmem_info = &sqe->src;
		gup_flags |= FOLL_WRITE;
	} else {
		user_info = &sqe->src;
		pmem_info = &sqe->dst;
	}

	vma = find_vma(current->mm, (unsigned long)user_info->virt);
	if (!vma) {
		copy_err("cannot find vma of user address!\n");
		return -EINVAL;
	}

	if (is_vm_hugetlb_page(vma)) {
		pg_size = vma->vm_ops->pagesize(vma);
		if (pg_size == HPAGE_PMD_SIZE) {
			pg_shift = HPAGE_PMD_SHIFT;
			pg_mask = HPAGE_PMD_MASK;
		} else if (pg_size == HPAGE_PUD_SIZE) {
			pg_shift = HPAGE_PUD_SHIFT;
			pg_mask = HPAGE_PUD_MASK;
		} else {
			copy_err("found unrecognized huge page size: %lu\n", pg_size);
		}
		if (copy_accel_dbg_mask & COPY_DBGMASK_TIME_STAT) {
			if (sqe->read) {
				__this_cpu_add(cnt_stats_percpu[copy_huge_read_t], 1);
				__this_cpu_add(time_stats_percpu[copy_huge_read_t], sqe->len);
			} else {
				__this_cpu_add(cnt_stats_percpu[copy_huge_write_t], 1);
				__this_cpu_add(time_stats_percpu[copy_huge_write_t], sqe->len);
			}
		}
	} else {
		pg_shift = PAGE_SHIFT;
		pg_size = PAGE_SIZE;
		pg_mask = PAGE_MASK;
	}

	user_info->tbl.orig_nents = ((sqe->len + ((unsigned long)(user_info->virt) & ~pg_mask) - 1) >> pg_shift) + 1;
	pmem_info->tbl.orig_nents = user_info->tbl.orig_nents;
	num = user_info->tbl.orig_nents;
	sqe->sgl = kmalloc_array(4 * num, sizeof(struct scatterlist), GFP_KERNEL);

	sqe->dst.tbl.sgl = sqe->sgl;
	sqe->src.tbl.sgl = sqe->sgl + 2 * num;
	
	// ret = pin_user_pages_fast((unsigned long)user_info->virt, num, gup_flags, sqe->pages);
	// if (ret <= 0) {
	// 	copy_err("pin user pages err: ret: %d num: %d virt:%p\n", ret, num, (void *)user_info->virt);
	// 	return -EINVAL;
	// }

	for (i = 0; i < 2 * num && sqe->len > 0; i++) {
		user_info->tbl.sgl[i].offset = user_info->virt & ~pg_mask;
		if (page_link == NULL) {
			int rc = 0;
			if ((rc = pin_user_pages((unsigned long)user_info->virt, 1, gup_flags, (struct page **)&page_link, NULL)) != 1) {
				pr_warn_once("%s: pin_user_pages error: %d\n", __func__, rc);
				break;
			}
		}
		user_info->tbl.sgl[i].page_link = page_link;
		pmem_info->tbl.sgl[i].offset = pmem_info->virt & ~pg_mask;
		pmem_info->tbl.sgl[i].page_link = (unsigned long) virt_to_page(pmem_info->virt);
		gap = min(pg_size - user_info->tbl.sgl[i].offset, min(sqe->len, pg_size - pmem_info->tbl.sgl[i].offset));
		if (gap == pg_size - user_info->tbl.sgl[i].offset) page_link = NULL;
		user_info->tbl.sgl[i].length = gap;
		user_info->tbl.nents++;
		user_info->virt += gap;
		pmem_info->tbl.sgl[i].length = gap;
		pmem_info->tbl.nents++;
		pmem_info->virt += gap;
		sqe->len -= gap;
	}

	return i;
}

static void copy_dma_unpin_sgtable(struct copy_dma_req *sqe, unsigned sg_num)
{
	int i;
	struct page *p = NULL;
	typeof(sqe->dst) *user_info;
	if (sqe->read)
		user_info = &sqe->dst;
	else
		user_info = &sqe->src;

	for (i = 0;i < sg_num; i++) {
		if (p != (struct page *)user_info->tbl.sgl[i].page_link) {
			p = (struct page *)user_info->tbl.sgl[i].page_link;
			unpin_user_page(p);
		}
	}
}

static int copy_dma_unmap_sgs(struct copy_dma_req *sqe)
{
	struct dma_device *dma_dev;
	int i;

	dma_dev = sqe->chan->device;

	for (i = 0; i < sqe->dma_req_nums; i++) {
		dma_unmap_page(dma_dev->dev, sqe->dst.tbl.sgl[i].dma_address,
			       sqe->dst.tbl.sgl[i].length, DMA_TO_DEVICE);
		dma_unmap_page(dma_dev->dev, sqe->src.tbl.sgl[i].dma_address,
			       sqe->src.tbl.sgl[i].length, DMA_FROM_DEVICE);
	}

	return 0;
}

static inline void copy_dma_callback(void *param)
{
    struct copy_dma_req *sqe = (struct copy_dma_req *)param;
	if (!sqe->read && sqe->ddio_enabled) {
		long cnt = atomic_fetch_add(1, &sqe->finish);
		arch_invalidate_pmem(page_to_virt((struct page*)sqe->dst.tbl.sgl[cnt].page_link) + sqe->dst.tbl.sgl[cnt].offset, sqe->dst.tbl.sgl[cnt].length);
		// arch_wb_cache_pmem(page_to_virt((struct page*)sqe->dst.tbl.sgl[cnt].page_link) + sqe->dst.tbl.sgl[cnt].offset, sqe->dst.tbl.sgl[cnt].length);
	}
}

static void copy_dma_last_callback(void *param)
{
    struct copy_dma_req *sqe = (struct copy_dma_req *)param;
	copy_dma_callback(param);
	complete(&sqe->done);
}



/*
 * The streaming DMA mapping routines can be called from interrupt
 * context. There are two versions of each map/unmap, one which will
 * map/unmap a single memory region, and one which will map/unmap a
 * scatterlist. (Now, we only suppory map/unmap a single memory page
 * because of ioat dma driver disable dma_{map/unmap}_single)
 */
static int copy_dma_map_submit_sgs(struct copy_dma_req *sqe, unsigned sg_num)
{
	struct dma_async_tx_descriptor *desc;
	dma_cookie_t cookie;
	struct dma_device *dma_dev;
	unsigned long flag_last = DMA_PREP_INTERRUPT | DMA_CTRL_ACK;
	unsigned long flag = DMA_CTRL_ACK;
	int i, last;
	int need_cb = sqe->ddio_enabled && !sqe->read;

	if (need_cb) {
		flag |= DMA_PREP_INTERRUPT;
	}

	dma_dev = sqe->chan->device;
	sqe->dma_req_nums = sg_num;

	last = sg_num - 1;
	for (i = 0; i < sg_num; i++) {
		sqe->dst.tbl.sgl[i].dma_address = dma_map_page(
			dma_dev->dev, (struct page *)sqe->dst.tbl.sgl[i].page_link,
			sqe->dst.tbl.sgl[i].offset, sqe->dst.tbl.sgl[i].length, DMA_TO_DEVICE);
		if (dma_mapping_error(dma_dev->dev, sqe->dst.tbl.sgl[i].dma_address)) {
			copy_err("failed to prepare dst address\n");
			goto map_err1;
		}

		sqe->src.tbl.sgl[i].dma_address = dma_map_page(
			dma_dev->dev, (struct page *)sqe->src.tbl.sgl[i].page_link,
			sqe->src.tbl.sgl[i].offset, sqe->src.tbl.sgl[i].length, DMA_FROM_DEVICE);
		if (dma_mapping_error(dma_dev->dev, sqe->src.tbl.sgl[i].dma_address)) {
			copy_err("failed to prepare src address\n");
			goto map_err2;
		}

		desc = dma_dev->device_prep_dma_memcpy(
			sqe->chan, sqe->dst.tbl.sgl[i].dma_address,
			sqe->src.tbl.sgl[i].dma_address, sqe->src.tbl.sgl[i].length,
			i == last ? flag_last : flag);
		if (!desc) {
			copy_err("failed to prepare DMA memcpy\n");
			goto prep_err;
		}

		if (i == last) {
			desc->callback = copy_dma_last_callback;
			desc->callback_param = (void *) sqe;
		} else if (need_cb) {
			desc->callback = copy_dma_callback;
			desc->callback_param = (void *) sqe;
		}
		cookie = dmaengine_submit(desc);
		if (dma_submit_error(cookie)) {
			copy_err("failed to do DMA tx_submit, page_pins: %u, i: %d, len: %u\n", sg_num, i, sqe->src.tbl.sgl[i].length);
			goto submit_err;
		}
	}
	if (i < sg_num) {
submit_err:
		dmaengine_desc_free(desc);
prep_err:
		dma_unmap_page(dma_dev->dev, sqe->src.tbl.sgl[i].dma_address,
			       sqe->src.tbl.sgl[i].length, DMA_FROM_DEVICE);
map_err2:
		dma_unmap_page(dma_dev->dev, sqe->dst.tbl.sgl[i].dma_address,
			       sqe->dst.tbl.sgl[i].length, DMA_TO_DEVICE);

map_err1:
		sqe->dma_req_nums = i;
		while (i < sg_num) {
			unsigned length = sqe->src.tbl.sgl[sg_num - 1].length;
			sqe->src.virt -= length;
			sqe->dst.virt -= length;
			sqe->len += length;
			sg_num -= 1;
		}
		copy_err("error result: %d\n", i);
	}
	return i;
}

static size_t
run_dma_copy(void *dst, const void *src, unsigned long size, bool read,
				int threshold, struct copy_dma_device *copy_dev)
{
	struct copy_dma_req sqe;
	int ret = 0;
	ktime_t time;
	int use_cache = 0;
	int cpu;
	if (size == 0)
		return 0;
	atomic64_inc(&copy_dev->active);
	sqe.len = size;
	sqe.dst.virt = (u64)dst;
	sqe.src.virt = (u64)src;
	cpu = smp_processor_id();
	sqe.sync = copy_accel_sync_wait;
	sqe.ddio_enabled = copy_accel_ddio_enabled;
	sqe.read = read;
	init_completion(&sqe.done);
	sqe.uid = atomic64_inc_return(&copy_dev->seqid);
	sqe.cid = sqe.uid % COPY_CHAN_PER_NODE;
	sqe.chan = copy_dev->chans[sqe.cid];

	while (sqe.len) {
		unsigned sg_num;
		if (sqe.len < threshold) {
			goto alloc_err;
		}
		atomic_set(&sqe.finish, 0);
		reinit_completion(&sqe.done);
		sqe.dst.tbl.nents = 0;
		sqe.src.tbl.nents = 0;

		COPY_START_TIME_STAT(copy_dma_pin_t, time);
		ret = copy_dma_pin_pair_sgtable(&sqe);
		COPY_END_TIME_STAT(copy_dma_pin_t, time);
		if (ret <= 0)
			goto pin_err;
		sg_num = ret;

		/* submit message then wait asynchronously */
		ret = copy_dma_map_submit_sgs(&sqe, sg_num);
		COPY_END_TIME_STAT(copy_dma_submit_t, time);
		if (ret == 0) {
			copy_err("copy_dma_map_submit error: page_pins: %u\n", sg_num);
			goto submit_err;
		}
		dma_async_issue_pending(copy_dev->chans[sqe.cid]);
		if (sqe.sync) {
			while (!try_wait_for_completion(&sqe.done)) cpu_relax();
		} else {
			wait_for_completion(&sqe.done);
		}
		COPY_END_TIME_STAT(copy_dma_wait_t, time);

		/* wind up for messages describing */
		copy_dma_unmap_sgs(&sqe);

	submit_err:
		copy_dma_unpin_sgtable(&sqe, sg_num);
		kfree(sqe.sgl);
		COPY_END_TIME_STAT(copy_dma_unpin_t, time);
		if (ret == 0)
			break;
		cond_resched();
	}
pin_err:
	if (ret <= 0)
		copy_err("ERROR in dma copy!\n");
alloc_err:
	atomic64_dec(&copy_dev->active);
	return sqe.len;
}

static struct copy_dma_device*
copy_select_channel(int pm_node, unsigned long length, int threshold, int rw) {
	struct copy_dma_device *copy_dev = NULL;
	int i;

	if (length < threshold || copy_accel_max_dma_concurrency == 0) {
		__this_cpu_add(cnt_stats_percpu[rw ? copy_read_thresh : copy_write_thresh], 1);
		return NULL;
	}

	if (atomic64_read(&copy_engine.devices[pm_node].active) < copy_accel_max_dma_concurrency) {
		copy_dev = copy_engine.devices + pm_node;
	} else {
		for (i = 0;i < copy_engine.num_nodes;i++) {
			if (i != pm_node && atomic64_read(&copy_engine.devices[i].active) < copy_accel_max_dma_concurrency) {
				copy_dev = copy_engine.devices + i;
				break;
			}
		}
	}

	if (copy_dev == NULL)
		__this_cpu_add(cnt_stats_percpu[rw ? copy_read_busy: copy_write_busy], 1);

	return copy_dev;
}

size_t dma_copy_from_user(struct block_device *bdev, void *dst, const void __user *src, unsigned long size)
{
	int write_threshold;
	struct copy_dma_device *copy_dev;
	int node = dev_to_node(disk_to_dev(bdev->bd_disk));
	int numa = 0;
	if (node == numa_node_id()) {
		write_threshold = copy_accel_local_write_threshold;
	} else {
		numa = 1;
		write_threshold = copy_accel_remote_write_threshold;
	}

	copy_dev = copy_select_channel(node, size, write_threshold, false);

	if (copy_dev == NULL) {
		COPY_HIST_ADD_CPU_WRITE(numa, size);
		return __copy_from_user_flushcache(dst, src, size);
	}

	might_fault();
	if (likely(access_ok(src, size))) {
		long rc = 0;
		unsigned long gap = 0;
		int aligned = false;
	    kasan_check_write(dst, size);
	    check_object_size(dst, size, true);

		if (size > 0)
			rc = run_dma_copy(dst, src, size, 0, write_threshold, copy_dev);

		if (rc > 0)
			rc = __copy_from_user_flushcache(dst + size - rc, src + size - rc, rc);

		return rc;
	}

	return size;
}
EXPORT_SYMBOL(dma_copy_from_user);

size_t dma_copy_to_user(struct block_device *bdev, void *dst, const void *src, unsigned long size, bool async)
{
	int read_threshold;
	struct copy_dma_device *copy_dev = NULL;
	int node = dev_to_node(disk_to_dev(bdev->bd_disk));
	int numa = 0;
	if (node == numa_node_id()) {
		read_threshold = copy_accel_local_read_threshold;
	} else {
		numa = 1;
		read_threshold = copy_accel_remote_read_threshold;
	}

	copy_dev = copy_select_channel(node, size, read_threshold, true);
	
	if (copy_dev == NULL) {
		COPY_HIST_ADD_CPU_READ(numa, size);
		if (async)
			return copy_to_user_mcsafe(dst, src, size);
		else
			return __copy_to_user(dst, src, size);
	}

	might_fault();
	if (likely(access_ok(dst, size))) {
		long rc;
	    kasan_check_read(src, size);
	    check_object_size(src, size, true);
	    rc = run_dma_copy(dst, src, size, 1, read_threshold, copy_dev);
		if (rc > 0) {
			if (async)
				rc = copy_to_user_mcsafe(dst + size - rc, src + size - rc, rc);
			else
				rc = __copy_to_user(dst + size - rc, src + size - rc, rc);
		}
		COPY_HIST_ADD_READ(numa, size);
		return rc;
	}

	return size;
}
EXPORT_SYMBOL(dma_copy_to_user);

int copy_init(void)
{
	dma_cap_mask_t mask;
	int idx, node;
	struct copy_dma_device *copy_dev;
	copy_engine.num_nodes = copy_accel_enabled_nodes == 0 ? num_online_nodes() : copy_accel_enabled_nodes;

	dma_cap_zero(mask);
	dma_cap_set(DMA_MEMCPY, mask);

	copy_engine.devices = kmalloc(sizeof(struct copy_dma_device) * copy_engine.num_nodes, GFP_KERNEL);

	for (idx = 0; idx < copy_engine.num_nodes;idx++)
		copy_engine.devices[idx].seqid.counter = 0;


	for (idx = 0; idx < copy_engine.num_nodes * COPY_CHAN_PER_NODE; idx++) {
		struct dma_chan *chan;

		chan = dma_request_channel(mask, NULL, NULL);
		if (chan) {
			node = dev_to_node(chan->device->dev);
			copy_dev = copy_engine.devices + node;
			copy_dev->chans[copy_dev->seqid.counter] = chan;
			copy_dev->seqid.counter++;
			dmaengine_pause(chan);
			printk(KERN_INFO "request chan[%d] node=%d\n", idx, node);
		} else {
			printk(KERN_INFO "request channel failed\n");
			break; /* add_channel failed, punt */
		}
	}

	for (idx = 0; idx < copy_engine.num_nodes;idx++) {
		atomic64_set(&copy_engine.devices[idx].seqid, 0);
		atomic64_set(&copy_engine.devices[idx].active, 0);
	}
	return 0;
}
EXPORT_SYMBOL(copy_init);

int copy_destroy(void)
{
	int i, j;
	struct copy_dma_device *copy_dev;
	struct dma_chan *chan;

	for (i = 0;i < copy_engine.num_nodes;i++) {
		copy_dev = copy_engine.devices + i;
		for (j = 0;j < COPY_CHAN_PER_NODE;j++) {
			chan = copy_dev->chans[j];
			dmaengine_terminate_sync(chan);
			printk(KERN_INFO "terminating channel %s\n", dma_chan_name(chan));
			dma_release_channel(chan);
		}
	}

	kfree(copy_engine.devices);
	return 0;
}
EXPORT_SYMBOL(copy_destroy);

#ifdef CONFIG_ARCH_HAS_UACCESS_MCSAFE
static __always_inline __must_check unsigned long
dma_copy_to_user_mcsafe(struct block_device *bdev, void *to, const void *from, unsigned len)
{
	unsigned long ret;

	/*
	 * Note, __memcpy_mcsafe() is explicitly used since it can
	 * handle exceptions / faults.  memcpy_mcsafe() may fall back to
	 * memcpy() which lacks this handling.
	 * 
	 * Return 0 for success, or number of bytes not copied if there was an
	 * exception.
	 */
	// ret = __memcpy_mcsafe(to, from, len);
	ret = dma_copy_to_user(bdev, to, from, len, true);
	return ret;
}

static int dma_copyout_mcsafe(struct block_device *bdev, void __user *to, const void *from, size_t n)
{
	if (access_ok(to, n)) {
		kasan_check_read(from, n);
		n = dma_copy_to_user_mcsafe(bdev, (__force void *) to, from, n);
	}
	return n;
}

#endif


static int dma_copy_from_user_flushcache(struct block_device *bdev, void *dst, const void __user *src, unsigned size)
{
	kasan_check_write(dst, size);
	return dma_copy_from_user(bdev, dst, src, size);
}

size_t
dma_copy_to_iter(struct block_device *bdev, const void *addr, size_t bytes, struct iov_iter *i)
{
	const char *from = addr;
	if (iter_is_iovec(i))
		might_fault();
	iterate_and_advance(i, bytes, v,
		dma_copyout_mcsafe(bdev, v.iov_base, (from += v.iov_len) - v.iov_len, v.iov_len));

	return bytes;
}
EXPORT_SYMBOL_GPL(dma_copy_to_iter);

size_t
dma_copy_from_iter(struct block_device *bdev, void *addr, size_t bytes, struct iov_iter *i)
{
	char *to = addr;
	iterate_and_advance(i, bytes, v,
		dma_copy_from_user_flushcache(bdev, (to += v.iov_len) - v.iov_len, 
					 v.iov_base, v.iov_len));

	return bytes;
}
EXPORT_SYMBOL_GPL(dma_copy_from_iter);

/* dma part end */

static loff_t
copy_iomap_actor(struct inode *inode, loff_t pos, loff_t length, void *data,
		struct iomap *iomap, struct iomap *srcmap)
{
	struct block_device *bdev = iomap->bdev;
	struct dax_device *dax_dev = iomap->dax_dev;
	struct iov_iter *iter = data;
	loff_t end = pos + length, done = 0;
	ssize_t ret = 0;
	size_t xfer;
	int id;

	if (iov_iter_rw(iter) == READ) {
		end = min(end, i_size_read(inode));
		if (pos >= end)
			return 0;

		if (iomap->type == IOMAP_HOLE || iomap->type == IOMAP_UNWRITTEN)
			return iov_iter_zero(min(length, end - pos), iter);
	}

	if (WARN_ON_ONCE(iomap->type != IOMAP_MAPPED))
		return -EIO;

	/*
	 * Write can allocate block for an area which has a hole page mapped
	 * into page tables. We have to tear down these mappings so that data
	 * written by write(2) is visible in mmap.
	 */
	if (iomap->flags & IOMAP_F_NEW) {
		invalidate_inode_pages2_range(inode->i_mapping,
					      pos >> PAGE_SHIFT,
					      (end - 1) >> PAGE_SHIFT);
	}

	id = dax_read_lock();
	while (pos < end) {
		unsigned offset = pos & (PAGE_SIZE - 1);
		const size_t size = ALIGN(length + offset, PAGE_SIZE);
		const sector_t sector = copy_iomap_sector(iomap, pos);
		ssize_t map_len;
		pgoff_t pgoff;
		void *kaddr;

		if (fatal_signal_pending(current)) {
			ret = -EINTR;
			break;
		}

		ret = bdev_dax_pgoff(bdev, sector, size, &pgoff);
		if (ret)
			break;

		map_len = dax_direct_access(dax_dev, pgoff, PHYS_PFN(size),
				&kaddr, NULL);
		if (map_len < 0) {
			ret = map_len;
			break;
		}

		map_len = PFN_PHYS(map_len);
		kaddr += offset;
		map_len -= offset;
		if (map_len > end - pos)
			map_len = end - pos;

		/*
		 * The userspace address for the memory copy has already been
		 * validated via access_ok() in either vfs_read() or
		 * vfs_write(), depending on which operation we are doing.
		 */
		if (iov_iter_rw(iter) == WRITE)
			xfer = dma_copy_from_iter(bdev, kaddr, (size_t)map_len, iter);
		else
			xfer = dma_copy_to_iter(bdev, kaddr, (size_t)map_len, iter);

		pos += xfer;
		length -= xfer;
		done += xfer;

		if (xfer == 0)
			ret = -EFAULT;
		if (xfer < map_len)
			break;
	}
	dax_read_unlock(id);

	return done ? done : ret;
}

/*
 * Execute a iomap write on a segment of the mapping that spans a
 * contiguous range of pages that have identical block mapping state.
 *
 * This avoids the need to map pages individually, do individual allocations
 * for each page and most importantly avoid the need for filesystem specific
 * locking per page. Instead, all the operations are amortised over the entire
 * range of pages. It is assumed that the filesystems will lock whatever
 * resources they require in the iomap_begin call, and release them in the
 * iomap_end call.
 */
loff_t
copy_iomap_apply(struct inode *inode, loff_t pos, loff_t length, unsigned flags,
		const struct iomap_ops *ops, void *data, iomap_actor_t actor)
{
	struct iomap iomap = { .type = IOMAP_HOLE };
	struct iomap srcmap = { .type = IOMAP_HOLE };
	loff_t written = 0, ret;
	u64 end;

	/*
	 * Need to map a range from start position for length bytes. This can
	 * span multiple pages - it is only guaranteed to return a range of a
	 * single type of pages (e.g. all into a hole, all mapped or all
	 * unwritten). Failure at this point has nothing to undo.
	 *
	 * If allocation is required for this range, reserve the space now so
	 * that the allocation is guaranteed to succeed later on. Once we copy
	 * the data into the page cache pages, then we cannot fail otherwise we
	 * expose transient stale data. If the reserve fails, we can safely
	 * back out at this point as there is nothing to undo.
	 */
	ret = ops->iomap_begin(inode, pos, length, flags, &iomap, &srcmap);
	if (ret)
		return ret;
	if (WARN_ON(iomap.offset > pos)) {
		written = -EIO;
		goto out;
	}
	if (WARN_ON(iomap.length == 0)) {
		written = -EIO;
		goto out;
	}

	/*
	 * Cut down the length to the one actually provided by the filesystem,
	 * as it might not be able to give us the whole size that we requested.
	 */
	end = iomap.offset + iomap.length;
	if (srcmap.type != IOMAP_HOLE)
		end = min(end, srcmap.offset + srcmap.length);
	if (pos + length > end)
		length = end - pos;

	/*
	 * Now that we have guaranteed that the space allocation will succeed,
	 * we can do the copy-in page by page without having to worry about
	 * failures exposing transient data.
	 *
	 * To support COW operations, we read in data for partially blocks from
	 * the srcmap if the file system filled it in.  In that case we the
	 * length needs to be limited to the earlier of the ends of the iomaps.
	 * If the file system did not provide a srcmap we pass in the normal
	 * iomap into the actors so that they don't need to have special
	 * handling for the two cases.
	 */
	written = actor(inode, pos, length, data, &iomap,
			srcmap.type != IOMAP_HOLE ? &srcmap : &iomap);

out:
	/*
	 * Now the data has been copied, commit the range we've copied.  This
	 * should not fail unless the filesystem has had a fatal error.
	 */
	if (ops->iomap_end) {
		ret = ops->iomap_end(inode, pos, length,
				     written > 0 ? written : 0,
				     flags, &iomap);
	}

	return written ? written : ret;
}

/**
 * copy_iomap_rw_sync - Perform I/O to a DAX file
 * @iocb:	The control block for this I/O
 * @iter:	The addresses to do I/O from or to
 * @ops:	iomap ops passed from the file system
 *
 * This function performs read and write operations to directly mapped
 * persistent memory.  The callers needs to take care of read/write exclusion
 * and evicting any page cache pages in the region under I/O.
 */
ssize_t
copy_iomap_rw_sync(struct kiocb *iocb, struct iov_iter *iter,
		const struct iomap_ops *ops)
{
	struct address_space *mapping = iocb->ki_filp->f_mapping;
	struct inode *inode = mapping->host;
	loff_t pos = iocb->ki_pos, ret = 0, done = 0;
	unsigned flags = 0;

	if (iov_iter_rw(iter) == WRITE) {
		lockdep_assert_held_write(&inode->i_rwsem);
		flags |= IOMAP_WRITE;
	} else {
		lockdep_assert_held(&inode->i_rwsem);
	}

	if (iocb->ki_flags & IOCB_NOWAIT)
		flags |= IOMAP_NOWAIT;

	while (iov_iter_count(iter)) {
		ret = copy_iomap_apply(inode, pos, iov_iter_count(iter), flags, ops,
				iter, copy_iomap_actor);
		if (ret <= 0)
			break;
		pos += ret;
		done += ret;
	}

	iocb->ki_pos += done;
	return done ? done : ret;
}
EXPORT_SYMBOL_GPL(copy_iomap_rw_sync);

/*
* return the number of successful requests
*/
int
copy_dma_run_bench_sgs(struct scatterlist *dst_sgl, struct scatterlist *src_sgl, int rw,
			int num_req, unsigned int chan_node, unsigned int chan_idx)
{
    struct copy_dma_req sqe;
	int ret = 0;
	sqe.chan = copy_engine.devices[chan_node].chans[chan_idx];
	sqe.sync = copy_accel_sync_wait;
	sqe.ddio_enabled = copy_accel_ddio_enabled;
	sqe.read = !rw;
	init_completion(&sqe.done);
	sqe.src.tbl.sgl = src_sgl;
	sqe.dst.tbl.sgl = dst_sgl;
	ret = copy_dma_map_submit_sgs(&sqe, num_req);
	if (ret == 0) {
		copy_err("copy_dma_map_submit error: num_req: %d\n", num_req);
		goto submit_err;
	}
	dma_async_issue_pending(sqe.chan);
	if (sqe.sync) {
		while (!try_wait_for_completion(&sqe.done)) cpu_relax();
	} else {
		wait_for_completion(&sqe.done);
	}
	copy_dma_unmap_sgs(&sqe);
submit_err:
	return ret;
}
EXPORT_SYMBOL_GPL(copy_dma_run_bench_sgs);
