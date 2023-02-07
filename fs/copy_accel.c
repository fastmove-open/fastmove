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
#include <linux/prandom.h>
#include <linux/copy_accel.h>
#include <linux/dma-direct.h>
#include <linux/hrtimer.h>
#include <linux/delay.h>
#include <asm/uaccess.h>



/* procfs variables */
const char *proc_dirname = "fs/fastmove";
struct proc_dir_entry *proc_dir = NULL;

/* kmem_cache for fastmove op and req */
static struct kmem_cache	*op_cachep;
static struct kmem_cache	*req_cachep;

struct fastmove fastmove = {
	.chan_map = NULL,
	.dev_map = NULL,
	.devices = NULL,
	.num_nodes = 0,
	.capacity = 0,
};
EXPORT_SYMBOL(fastmove);

struct sysctl_fastmove sysctl_fastmove = {
	.enable = 0,
	.memcg = 0,
	.mode = 0,
	.show_stats = 0,
	.dbg_mask = 0,
	.max_user_num = 8,
	.chunks = 4,
	.scatter = 1,
	.ddio = 0,
	.sync_wait = 0,
	.num_nodes = 1,
	.local_read = (1 << 14),
	.remote_read = (1 << 14),
	.local_write = (1 << 14),
	.remote_write = (1 << 14),
};
EXPORT_SYMBOL(sysctl_fastmove);

enum proc_hist_cats {
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
	hist_split_dma_ratio_t,
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
	"cpu_write_bytes",
	"split_dma_ratio",
};

enum proc_time_cats {
	main_init_t,
	main_pin_t,
	main_split_t,
	main_submit_t,
	main_execute_t,
	main_wait_t,
	main_unmap_t,
	main_unpin_t,
	dma_callback_t,
	huge_read_t,
	huge_write_t,
	read_thresh,
	read_busy,
	write_thresh,
	write_busy,
	split_t,
	non_split_t,
	TIME_CAT_NUM
};

const char *time_strs[TIME_CAT_NUM] = {
	"fastmove_main_init",
	"fastmove_main_pin",
	"fastmove_main_split",
	"fastmove_main_submit",
	"fastmove_main_execute",
	"fastmove_main_wait",
	"fastmove_main_unmap",
	"fastmove_main_unpin",
	"dma_callback",
	"huge_read",
	"huge_write",
	"read_thresh",
	"read_busy",
	"write_thresh",
	"write_busy",
	"split",
	"non_split",
};


#define COPY_INLINE_SIZE	(PAGE_SIZE)
#define COPY_INLINE_PAGES	(COPY_INLINE_SIZE / sizeof(struct scatterlist))
// 64B - 256K
#define HIST_BINS 14
u64 IOhist[2 * HIST_BINS * HIST_CAT_NUM];
DEFINE_PER_CPU(u64[2 * HIST_BINS * HIST_CAT_NUM], IOhist_percpu);
u64 time_stats[TIME_CAT_NUM];
DEFINE_PER_CPU(u64[TIME_CAT_NUM], time_stats_percpu);
u64 cnt_stats[TIME_CAT_NUM];
DEFINE_PER_CPU(u64[TIME_CAT_NUM], cnt_stats_percpu);
DEFINE_PER_CPU(u64[3], bufs_percpu);

#define PROC_START_TIME_STAT(name, start) \
	{if (sysctl_fastmove.dbg_mask & FASTMOVE_DBGMASK_TIME_STAT) start = ktime_get(); }

#define PROC_END_TIME_STAT(name, start) ({ \
		u64 elapse = 0; \
		if (sysctl_fastmove.dbg_mask & FASTMOVE_DBGMASK_TIME_STAT) { \
			ktime_t __temp = ktime_get(); \
			elapse = ktime_to_ns(ktime_sub(__temp, start)); \
			__this_cpu_add(time_stats_percpu[name], elapse); \
			start = __temp; \
		} \
		__this_cpu_add(cnt_stats_percpu[name], 1); \
		elapse; \
	})

int proc_hist_bin(unsigned long v)
{
	int r = __fls(v);
	if (r < 6) {
		return 0;
	} else if (r > HIST_BINS + 5) {
		return HIST_BINS - 1;
	} else {
		return r - 6;
	}
}

#define PROC_HIST_ADD_READ(numa, value) \
	if (sysctl_fastmove.dbg_mask & FASTMOVE_DBGMASK_SIZE_STAT) \
	{int bin = proc_hist_bin(value); \
	 __this_cpu_add(IOhist_percpu[bin + hist_read_t * HIST_BINS + numa * HIST_BINS * HIST_CAT_NUM], 1); \
	 __this_cpu_add(IOhist_percpu[bin + hist_read_bytes_t * HIST_BINS + numa * HIST_BINS * HIST_CAT_NUM], value); }

#define PROC_HIST_ADD_WRITE_ALIGNED(numa, value) \
	if (sysctl_fastmove.dbg_mask & FASTMOVE_DBGMASK_SIZE_STAT) \
	{int bin = proc_hist_bin(value); \
	 __this_cpu_add(IOhist_percpu[bin + hist_aligned_t * HIST_BINS + numa * HIST_BINS * HIST_CAT_NUM], 1); \
	 __this_cpu_add(IOhist_percpu[bin + hist_aligned_bytes_t * HIST_BINS + numa * HIST_BINS * HIST_CAT_NUM], value); }

#define PROC_HIST_ADD_WRITE_UNALIGNED(numa, value) \
	if (sysctl_fastmove.dbg_mask & FASTMOVE_DBGMASK_SIZE_STAT) \
	{int bin = proc_hist_bin(value); \
	 __this_cpu_add(IOhist_percpu[bin + hist_unaligned_t * HIST_BINS + numa * HIST_BINS * HIST_CAT_NUM], 1); \
	 __this_cpu_add(IOhist_percpu[bin + hist_unaligned_bytes_t * HIST_BINS + numa * HIST_BINS * HIST_CAT_NUM], value); }

#define PROC_HIST_ADD_CPU_READ(numa, value) \
	if (sysctl_fastmove.dbg_mask & FASTMOVE_DBGMASK_SIZE_STAT) \
	{int bin = proc_hist_bin(value); \
	 __this_cpu_add(IOhist_percpu[bin + hist_cpu_read_t * HIST_BINS + numa * HIST_BINS * HIST_CAT_NUM], 1); \
	 __this_cpu_add(IOhist_percpu[bin + hist_cpu_read_bytes_t * HIST_BINS + numa * HIST_BINS * HIST_CAT_NUM], value); }

#define PROC_HIST_ADD_CPU_WRITE(numa, value) \
	if (sysctl_fastmove.dbg_mask & FASTMOVE_DBGMASK_SIZE_STAT) \
	{int bin = proc_hist_bin(value); \
	 __this_cpu_add(IOhist_percpu[bin + hist_cpu_write_t * HIST_BINS + numa * HIST_BINS * HIST_CAT_NUM], 1); \
	 __this_cpu_add(IOhist_percpu[bin + hist_cpu_write_bytes_t * HIST_BINS + numa * HIST_BINS * HIST_CAT_NUM], value); }

#define PROC_HIST_ADD_SPLIT_DMA_RATIO(numa, bin) \
	if (sysctl_fastmove.dbg_mask & FASTMOVE_DBGMASK_TIME_STAT) \
	{ __this_cpu_add(IOhist_percpu[bin + hist_split_dma_ratio_t * HIST_BINS + numa * HIST_BINS * HIST_CAT_NUM], 1);}

/* adaptive tune threshold start */
enum fastmove_estimate_stat_types {
	STAT_BYTE,
	STAT_TIME,
	STAT_CNTS,
	FM_STAT_CAT_NUM
};

const char *copy_dir_strs[COPY_DIR_NUM] = {
	"local_read",
	"local_write",
	"remote_read",
	"remote_write"
};

#define FM_ESTIMATE_BIN_NUM 5UL /* [16K, 32K), [32K, 64K), [64K, 128K), [128K, 256K), [256K, inf) */

/* tuner stats, bandwidth unit is MB/s */
static unsigned long total_bw[FM_CAT_NUM];
static unsigned long average_bw[FM_CAT_NUM];
static unsigned long dma_bw[COPY_DIR_NUM][FM_ESTIMATE_BIN_NUM];
static atomic64_t dma_bw_stats[COPY_DIR_NUM][FM_ESTIMATE_BIN_NUM][FM_STAT_CAT_NUM];
static unsigned long dma_bw_stats_snap[COPY_DIR_NUM][FM_ESTIMATE_BIN_NUM][FM_STAT_CAT_NUM];
static unsigned long cpu_bw[COPY_DIR_NUM];
static atomic64_t cpu_bw_stats[COPY_DIR_NUM][FM_STAT_CAT_NUM];
static unsigned long cpu_bw_stats_snap[COPY_DIR_NUM][FM_STAT_CAT_NUM];

static unsigned long dma_bw_stats_tmp[COPY_DIR_NUM][FM_ESTIMATE_BIN_NUM][FM_STAT_CAT_NUM];
static unsigned long cpu_bw_stats_tmp[COPY_DIR_NUM][FM_STAT_CAT_NUM];
static unsigned long cnt_stat[FM_CAT_NUM][COPY_DIR_NUM];

/* ms */
#define FM_ESTIMATE_PERIOD 50
struct hrtimer fastmove_estimate_timer;
static ktime_t fastmove_estimate_last_timestamp;
static ktime_t fastmove_estimate_last_duration;

#define GET_BW_DIR(lr, rw) (lr * 2 + rw)
static inline unsigned get_bw_bin(unsigned long size) {
	unsigned long bin = __fls((size >> 14) | 1);
	return min(bin, FM_ESTIMATE_BIN_NUM - 1);
}
#define COPY_UPDATE_BW_STAT(ctype, bwdir, size, time, bin) do { \
	if (sysctl_fastmove.dbg_mask & FASTMOVE_DBGMASK_TIME_STAT) { \
		if ((ctype) == FM_CPU) {\
			atomic64_add((size), &cpu_bw_stats[(bwdir)][STAT_BYTE]);\
			atomic64_add((time), &cpu_bw_stats[(bwdir)][STAT_TIME]);\
			atomic64_add(1, &cpu_bw_stats[(bwdir)][STAT_CNTS]);\
		} else { \
			atomic64_add((size), &dma_bw_stats[(bwdir)][bin][STAT_BYTE]); \
			atomic64_add((time), &dma_bw_stats[(bwdir)][bin][STAT_TIME]); \
			atomic64_add(1, &dma_bw_stats[(bwdir)][bin][STAT_CNTS]); \
		}\
	}\
} while (0);

#define COPY_UPDATE_BW(bw, old_stat, new_stat) \
	if ((old_stat)[STAT_CNTS] != (new_stat)[STAT_CNTS]) {\
		bw = ((bw + 3 * 1000 * ((new_stat)[STAT_BYTE] - (old_stat)[STAT_BYTE]) /\
			((new_stat)[STAT_TIME] - (old_stat)[STAT_TIME])) >> 2);\
	}

static enum hrtimer_restart fastmove_estimate_run(struct hrtimer *timer) {
	int dir, bin;
	unsigned long total_stats[FM_CAT_NUM][FM_STAT_CAT_NUM] = {0};
	ktime_t cur;
	unsigned long duration;
	for (dir = 0;dir < COPY_DIR_NUM;dir++) {
		cpu_bw_stats_tmp[dir][STAT_BYTE] = atomic64_read(&cpu_bw_stats[dir][STAT_BYTE]);
		cpu_bw_stats_tmp[dir][STAT_TIME] = atomic64_read(&cpu_bw_stats[dir][STAT_TIME]);
		cpu_bw_stats_tmp[dir][STAT_CNTS] = atomic64_read(&cpu_bw_stats[dir][STAT_CNTS]);
		cnt_stat[FM_CPU][dir] = cpu_bw_stats_tmp[dir][STAT_CNTS] - cpu_bw_stats_snap[dir][STAT_CNTS];
		total_stats[FM_CPU][STAT_CNTS] += cnt_stat[FM_CPU][dir];
		total_stats[FM_CPU][STAT_TIME] += cpu_bw_stats_tmp[dir][STAT_TIME] - cpu_bw_stats_snap[dir][STAT_TIME];
		total_stats[FM_CPU][STAT_BYTE] += cpu_bw_stats_tmp[dir][STAT_BYTE] - cpu_bw_stats_snap[dir][STAT_BYTE];
		cnt_stat[FM_DMA][dir] = 0;
		for (bin = 0;bin < FM_ESTIMATE_BIN_NUM;bin++) {
			dma_bw_stats_tmp[dir][bin][STAT_BYTE] = atomic64_read(&dma_bw_stats[dir][bin][STAT_BYTE]);
			dma_bw_stats_tmp[dir][bin][STAT_TIME] = atomic64_read(&dma_bw_stats[dir][bin][STAT_TIME]);
			dma_bw_stats_tmp[dir][bin][STAT_CNTS] = atomic64_read(&dma_bw_stats[dir][bin][STAT_CNTS]);
			cnt_stat[FM_DMA][dir] += dma_bw_stats_tmp[dir][bin][STAT_CNTS] - dma_bw_stats_snap[dir][bin][STAT_CNTS];
			total_stats[FM_DMA][STAT_BYTE] += dma_bw_stats_tmp[dir][bin][STAT_BYTE] - dma_bw_stats_snap[dir][bin][STAT_BYTE];
			total_stats[FM_DMA][STAT_TIME] += dma_bw_stats_tmp[dir][bin][STAT_TIME] - dma_bw_stats_snap[dir][bin][STAT_TIME];
		}
		total_stats[FM_DMA][STAT_CNTS] += cnt_stat[FM_DMA][dir];
	}

	for (dir = 0;dir < COPY_DIR_NUM;dir++) {
		COPY_UPDATE_BW(cpu_bw[dir], cpu_bw_stats_snap[dir], cpu_bw_stats_tmp[dir]);
		for (bin = 0;bin < FM_ESTIMATE_BIN_NUM;bin++)
			COPY_UPDATE_BW(dma_bw[dir][bin], dma_bw_stats_snap[dir][bin], dma_bw_stats_tmp[dir][bin]);
	}
	cur = ktime_get();
	duration = ktime_to_ns(ktime_sub(cur, fastmove_estimate_last_timestamp));
	fastmove_estimate_last_timestamp = cur;
	fastmove_estimate_last_duration = duration;
	/* MB/s */
	total_bw[FM_CPU] = total_stats[FM_CPU][STAT_BYTE] * 1000 / duration;
	total_bw[FM_DMA] = total_stats[FM_DMA][STAT_BYTE] * 1000 / duration;
	if (total_stats[FM_CPU][STAT_TIME] != 0)
		average_bw[FM_CPU] = total_stats[FM_CPU][STAT_BYTE] * 1000 / total_stats[FM_CPU][STAT_TIME];
	if (total_stats[FM_DMA][STAT_TIME] != 0)
		average_bw[FM_DMA] = total_stats[FM_DMA][STAT_BYTE] * 1000 / total_stats[FM_DMA][STAT_TIME];

	memcpy(cpu_bw_stats_snap, cpu_bw_stats_tmp, sizeof(cpu_bw_stats_snap));
	memcpy(dma_bw_stats_snap, dma_bw_stats_tmp, sizeof(dma_bw_stats_snap));

	hrtimer_forward_now(timer, ms_to_ktime(FM_ESTIMATE_PERIOD));
	return HRTIMER_RESTART;
}

static void fastmove_start_estimate(void) {
	int i, j;
	memset(dma_bw_stats_snap, 0, sizeof(dma_bw_stats_snap));
	memset(dma_bw_stats, 0, sizeof(dma_bw_stats));
	memset(cpu_bw_stats_snap, 0, sizeof(cpu_bw_stats_snap));
	memset(cpu_bw_stats, 0, sizeof(cpu_bw_stats));
	for (i = 0;i < COPY_DIR_NUM;i++) {
		cpu_bw[i] = 1000;
		for (j = 0;j < FM_ESTIMATE_BIN_NUM;j++) {
			dma_bw[i][j] = 1000;
		}
	}
	hrtimer_init(&fastmove_estimate_timer, CLOCK_MONOTONIC, HRTIMER_MODE_REL);
	fastmove_estimate_timer.function = fastmove_estimate_run;
	fastmove_estimate_last_timestamp = ktime_get();
	hrtimer_start(&fastmove_estimate_timer, ms_to_ktime(FM_ESTIMATE_PERIOD), HRTIMER_MODE_REL);
}

static void fastmove_end_estimate(void) {
	hrtimer_cancel(&fastmove_estimate_timer);
}
/* adaptive tune threshold end */

static void proc_get_hist_stat(void)
{
	int i;
	int cpu;

	for (i = 0; i < 2 * HIST_BINS * HIST_CAT_NUM; i++) {
		IOhist[i] = 0;
		for_each_possible_cpu (cpu)
			IOhist[i] += per_cpu(IOhist_percpu[i], cpu);
	}
}

static void proc_clear_hist(void)
{
	int i;
	int cpu;

	for (i = 0; i < 2 * HIST_BINS * HIST_CAT_NUM; i++) {
		IOhist[i] = 0;
		for_each_possible_cpu (cpu)
			per_cpu(IOhist_percpu[i], cpu) = 0;
	}
}

static void proc_get_time_stat(void)
{
	int i;
	int cpu;

	for (i = 0; i < TIME_CAT_NUM; i++) {
		time_stats[i] = 0;
		cnt_stats[i] = 0;
		for_each_possible_cpu (cpu) {
			time_stats[i] += per_cpu(time_stats_percpu[i], cpu);
			cnt_stats[i] += per_cpu(cnt_stats_percpu[i], cpu);
		}
	}
}


static void proc_clear_time_stat(void)
{
	int i;
	int cpu;

	for (i = 0; i < TIME_CAT_NUM; i++) {
		for_each_possible_cpu (cpu) {
			per_cpu(time_stats_percpu[i], cpu) = 0;
			per_cpu(cnt_stats_percpu[i], cpu) = 0;
		}
	}
}

static int proc_stats_show(struct seq_file *seq, void *v)
{
	int j = 0;
	int offset = HIST_CAT_NUM * HIST_BINS;
	proc_get_time_stat();
	proc_get_hist_stat();
	seq_puts(seq, "=========== fastmove hist (local) ===========\n");
	for (j = 0; j < HIST_CAT_NUM; j++) {
		int k, i = j * HIST_BINS;
		seq_printf(seq, "%s: [", hist_strs[j]);
		for (k = 0; k < HIST_BINS; k++) {
			seq_printf(seq, "%llu", IOhist[i + k]);
			if (k != HIST_BINS - 1)
				seq_printf(seq, ", ");
			else
				seq_printf(seq, "]\n");
		}
	}
	seq_puts(seq, "=========== fastmove hist (remote) ===========\n");
	for (j = 0; j < HIST_CAT_NUM; j++) {
		int k, i = j * HIST_BINS;
		seq_printf(seq, "%s: [", hist_strs[j]);
		for (k = 0; k < HIST_BINS; k++) {
			seq_printf(seq, "%llu", IOhist[offset + i + k]);
			if (k != HIST_BINS - 1)
				seq_printf(seq, ", ");
			else
				seq_printf(seq, "]\n");
		}
	}
	seq_puts(seq, "=========== fastmove time ===========\n");
	for (j = 0; j < TIME_CAT_NUM; j++) {
		if (sysctl_fastmove.dbg_mask & FASTMOVE_DBGMASK_TIME_STAT) {
			seq_printf(seq,
				   "%s: count  %llu, time %llu, average %llu\n",
				   time_strs[j], cnt_stats[j], time_stats[j],
				   cnt_stats[j] ? time_stats[j] / cnt_stats[j] :
						  0);
		} else {
			seq_printf(seq, "%s: count %llu\n", time_strs[j],
				   cnt_stats[j]);
		}
	}
	seq_puts(seq, "=========== fastmove runtime data ===========\n");
	for (j = 0; j < fastmove.num_nodes; j++) {
		struct plugged_device *dev = fastmove.devices + j;
		seq_printf(seq, "Node %d: user_num: %lld task_num: %lld\n", j,
			atomic64_read(&dev->user_num), atomic64_read(&dev->inflight_task_num));
	}
	for (j = 0;j < COPY_DIR_NUM;j++) {
		int i;
		seq_printf(seq, "dir %d cpu: %lu MB/s ", j, cpu_bw[j]);
		for (i = 0;i < FM_ESTIMATE_BIN_NUM;i++) {
			seq_printf(seq, "bin %d: %lu MB/s ", i, dma_bw[j][i]);
		}
		seq_printf(seq, "\n");
	}
	seq_printf(seq, "aggregated bandwidth: DMA %lu MB/s CPU %lu MB/s\n", total_bw[FM_DMA], total_bw[FM_CPU]);
	seq_printf(seq, "average bandwidth: DMA %lu MB/s CPU %lu MB/s\n", average_bw[FM_DMA], average_bw[FM_CPU]);
	return 0;
}

static int proc_stats_open(struct inode *inode, struct file *file)
{
	return single_open(file, proc_stats_show, NULL);
}

static ssize_t proc_stats_clear(struct file *filp, const char __user *buf,
				size_t len, loff_t *ppos)
{
	proc_clear_hist();
	proc_clear_time_stat();
	return len;
}

static const struct proc_ops proc_stats = {
	.proc_open		= proc_stats_open,
	.proc_read		= seq_read,
	.proc_write		= proc_stats_clear,
	.proc_lseek		= seq_lseek,
	.proc_release	= single_release,
};

/**
 * sysctl_fastmove_stats - create fastmove engine's stats in /proc/fs/fastmove/stats
 * @table: the sysctl table
 * @write: %TRUE if this is a write to the sysctl file
 * @buffer: the user buffer
 * @lenp: the size of the user buffer
 * @ppos: file position
 */
int sysctl_fastmove_stats(struct ctl_table *table, int write,
			 void __user *buffer, size_t *lenp, loff_t *ppos)
{
	int prior_show_val = sysctl_fastmove.show_stats;
	int err = 0;

	if (write && !capable(CAP_SYS_ADMIN))
		return -EPERM;

	err = proc_dointvec_minmax(table, write, buffer, lenp, ppos);
	if (err < 0)
		return err;

	if (prior_show_val == 0 && sysctl_fastmove.show_stats == 1) {
		proc_dir = proc_mkdir(proc_dirname, NULL);
		if (proc_dir) {
			proc_create_data("stats", 0644, proc_dir, &proc_stats, NULL);
			printk(KERN_INFO "redirect shown in /proc/fs/fastmove/stats\n");
		}
	} else if (prior_show_val == 1 && sysctl_fastmove.show_stats == 0) {
		if (proc_dir) {
			remove_proc_entry("stats", proc_dir);
			proc_remove(proc_dir);
			proc_dir = NULL;
		}
	}

	return err;
}

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

/**
 * sysctl_fastmove_control - grab/release DMA channels
 * @table: the sysctl table
 * @write: %TRUE if this is a write to the sysctl file
 * @buffer: the user buffer
 * @lenp: the size of the user buffer
 * @ppos: file position
 */
int sysctl_fastmove_control(struct ctl_table *table, int write,
			 void __user *buffer, size_t *lenp, loff_t *ppos)
{
	dma_cap_mask_t mask;
	struct plugged_device *device;
	struct dma_chan *chan;
	int prior_enable_val = sysctl_fastmove.enable;
	__u64 i;
	int err = 0;

	if (write && !capable(CAP_SYS_ADMIN))
		return -EPERM;

	err = proc_dointvec_minmax(table, write, buffer, lenp, ppos);
	if (err < 0)
		return err;

	fastmove.num_nodes = sysctl_fastmove.num_nodes;
	fastmove.capacity = fastmove.num_nodes * PLUGGED_CAPACITY;

	/* grab DMA channels  */
	if (prior_enable_val == 0 && sysctl_fastmove.enable == 1) {
		dma_cap_zero(mask);
		dma_cap_set(DMA_MEMCPY, mask);

		fastmove.chan_map = kmalloc(fastmove.capacity * sizeof(struct dma_chan *), GFP_KERNEL);
		fastmove.dev_map = kmalloc(fastmove.capacity * sizeof(struct plugged_device *), GFP_KERNEL);
		fastmove.devices = kmalloc(fastmove.num_nodes * sizeof(struct plugged_device), GFP_KERNEL);

		for (i = 0; i < fastmove.capacity; i++) {
			chan = dma_request_channel(mask, NULL, NULL);
			if (chan) {
				fastmove.chan_map[i] = chan;
				device = fastmove.devices + dev_to_node(chan->device->dev);
				fastmove.dev_map[i] = device;
				device->chans[i & ~PLUGGED_MASK] = chan;
				dmaengine_pause(chan);
				printk(KERN_INFO "request chan[%lld] node=%d\n", i, dev_to_node(chan->device->dev));
			} else {
				printk(KERN_INFO "request channel failed\n");
				break; /* add_channel failed, punt */
			}
		}

		for (i = 0; i < fastmove.num_nodes; i++) {
			atomic64_set(&fastmove.devices[i].user_num, 0);
			atomic64_set(&fastmove.devices[i].inflight_task_num, 0);
		}
		
		atomic64_set(&fastmove.index, 0);
		fastmove_start_estimate();
	}
	/* release DMA channels  */
	else if (prior_enable_val == 1 && sysctl_fastmove.enable == 0) {
		fastmove_end_estimate();
		for (i = 0; i < fastmove.capacity; i++) {
			chan = fastmove.chan_map[i];
			dmaengine_terminate_sync(chan);
			printk(KERN_INFO "terminate chan %s\n", dma_chan_name(chan));
			dma_release_channel(chan);
		}
		kfree(fastmove.chan_map);
		kfree(fastmove.dev_map);
		kfree(fastmove.devices);
	}

	return err;
}

static int __init fastmove_engine_init(void)
{
	int cpu;
	op_cachep = KMEM_CACHE(fastmove_op, SLAB_HWCACHE_ALIGN | SLAB_PANIC);
	req_cachep = KMEM_CACHE(fastmove_req, SLAB_HWCACHE_ALIGN | SLAB_PANIC);

	for_each_possible_cpu(cpu) {
		per_cpu(bufs_percpu[0], cpu) = (u64)kmalloc(COPY_INLINE_PAGES * sizeof(struct page *), GFP_KERNEL);
		per_cpu(bufs_percpu[1], cpu) = (u64)kmalloc(2 * COPY_INLINE_SIZE, GFP_KERNEL);
		per_cpu(bufs_percpu[2], cpu) = 0;
	}
	return 0;
}
__initcall(fastmove_engine_init);

static void __exit fastmove_engine_exit(void)
{
	int cpu;
	kmem_cache_destroy(op_cachep);
	kmem_cache_destroy(req_cachep);

	for_each_possible_cpu(cpu) {
		kfree((void *)per_cpu(bufs_percpu[0], cpu));
		kfree((void *)per_cpu(bufs_percpu[1], cpu));
	}
}
__exitcall(fastmove_engine_exit);


struct address_desc {
	__u64			virt; // virtual address
	struct sg_table		tbl; // sg table
};

struct mission_task {
	struct work_struct mission_work;
	__u64 sgl_offset; /* only useful to worker */
	__u64 virt_offset;
	__u64 chunk_length;
	__u64 maps;
	ktime_t time;
	struct dma_chan		*chan;
	struct mission_desc *mission;
};

struct mission_desc {
	/* basic info */
	__u64			len;	// data length
	__u64			maps;  // pair maps ready for transfer
	struct page		**pages;// pages for pin_user_pages
	struct scatterlist	*sgl;	// temporary scatterlist
	struct address_desc	mem;
	u64				pmem_virt;
	u64				mem_virt;

	/* task descriptor */
	int				bw_type;
	int				bw_bin;
	struct mission_task tasks[PLUGGED_CAPACITY];
	struct completion	completion;

	/* operation config */
	bool			sync;
	bool			ddio;
	bool			read;
	bool			scatter;
	u64				chunks;
	ktime_t			time;
};

/*
 * For the data that needs to be copied, the operations within the range of src 
 * and dst are processed page by page in units of pages. If src and dst occupy 
 * multiple pages, the data in one page of src may involve two pages in dst, so 
 * it is necessary to divide the range of src and dst according to the page size 
 * to ensure byte-by-byte mapping with page granularity
 */
static int dynamic_pin_user_pages(struct mission_desc *mission, u64 request_len)
{
	unsigned int gap;
	struct address_desc *user_info;
	__u64 pg_shift, pg_size, pg_mask;
	__u64 i, j;
	int page_pins, num;
	unsigned int gup_flags = FOLL_LONGTERM;
	struct vm_area_struct *vma;
    int nents = 0;
	u64 pmem_virt = mission->pmem_virt;

	if (mission->read) {
		gup_flags |= FOLL_WRITE;
	}
	user_info = &mission->mem;

	vma = find_vma(current->mm, (unsigned long)user_info->virt);
	if (!vma) {
		fm_err("cannot find vma of user address!\n");
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
			fm_err("found unrecognized huge page size: %llu\n", pg_size);
		}
		if (sysctl_fastmove.dbg_mask & FASTMOVE_DBGMASK_TIME_STAT) {
			if (mission->read) {
				__this_cpu_add(cnt_stats_percpu[huge_read_t], 1);
				__this_cpu_add(time_stats_percpu[huge_read_t], request_len);
			} else {
				__this_cpu_add(cnt_stats_percpu[huge_write_t], 1);
				__this_cpu_add(time_stats_percpu[huge_write_t], request_len);
			}
		}
	} else {
		pg_shift = PAGE_SHIFT;
		pg_size = PAGE_SIZE;
		pg_mask = PAGE_MASK;
	}

	user_info->tbl.orig_nents = ((request_len + ((unsigned long)(user_info->virt) & ~pg_mask) - 1) >> pg_shift) + 1;
	num = min(user_info->tbl.orig_nents, (unsigned int)COPY_INLINE_PAGES);

	mission->mem.tbl.sgl = mission->sgl;

	page_pins = pin_user_pages_fast((unsigned long)user_info->virt, num, gup_flags, mission->pages);
	if (page_pins <= 0) {
		fm_err("pin user pages err: ret: %d num: %d virt:%p\n", page_pins, num, (void *)user_info->virt);
		return -EINVAL;
	}

	num = user_info->tbl.orig_nents = page_pins;
	j = 0;
	for (i = 0; i < 2 * num && request_len > 0 && j < num; i++) {
		unsigned long pmem_offset = pmem_virt & ~pg_mask;
		user_info->tbl.sgl[i].offset = user_info->virt & ~pg_mask;
		user_info->tbl.sgl[i].page_link = (unsigned long) mission->pages[j];
		gap = min(pg_size - user_info->tbl.sgl[i].offset, min(request_len, pg_size - pmem_offset));
		if (user_info->tbl.sgl[i].offset + gap == pg_size) j++;
		user_info->tbl.sgl[i].length = gap;
		user_info->virt += gap;
		pmem_virt += gap;
		request_len -= gap;
        nents++;
		mission->tasks[FM_DMA].chunk_length += gap;
	}
	user_info->tbl.nents = nents;

	return num;
}

static int dynamic_pin_user_pages_sg(struct mission_desc *mission, u64 request_len)
{
	unsigned int gap;
	struct address_desc *user_info;
	__u64 pg_shift, pg_size, pg_mask;
	__u64 i;
	int page_pins, num;
	unsigned int gup_flags = FOLL_LONGTERM;
	struct vm_area_struct *vma;
    int nents = 0;

	if (mission->read) {
		gup_flags |= FOLL_WRITE;
	}
	user_info = &mission->mem;

	vma = find_vma(current->mm, (unsigned long)user_info->virt);
	if (!vma) {
		fm_err("cannot find vma of user address!\n");
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
			fm_err("found unrecognized huge page size: %llu\n", pg_size);
		}
		if (sysctl_fastmove.dbg_mask & FASTMOVE_DBGMASK_TIME_STAT) {
			if (mission->read) {
				__this_cpu_add(cnt_stats_percpu[huge_read_t], 1);
				__this_cpu_add(time_stats_percpu[huge_read_t], request_len);
			} else {
				__this_cpu_add(cnt_stats_percpu[huge_write_t], 1);
				__this_cpu_add(time_stats_percpu[huge_write_t], request_len);
			}
		}
	} else {
		pg_shift = PAGE_SHIFT;
		pg_size = PAGE_SIZE;
		pg_mask = PAGE_MASK;
	}

	user_info->tbl.orig_nents = ((request_len + ((unsigned long)(user_info->virt) & ~pg_mask) - 1) >> pg_shift) + 1;
	num = min(user_info->tbl.orig_nents, (unsigned int)COPY_INLINE_PAGES);

	mission->mem.tbl.sgl = mission->sgl;

	page_pins = pin_user_pages_fast((unsigned long)user_info->virt, num, gup_flags, mission->pages);
	if (page_pins <= 0) {
		fm_err("pin user pages err: ret: %d num: %d virt:%p\n", page_pins, num, (void *)user_info->virt);
		return -EINVAL;
	}

	num = page_pins;
	for (i = 0; i < num && request_len > 0;) {
		__u64 hpg_size, hpg_mask;
		struct page *page = compound_head((struct page *)mission->pages[i]);
		hpg_size = page_size(page);
		hpg_mask = ~(hpg_size - 1);
		user_info->tbl.sgl[nents].offset = user_info->virt & ~hpg_mask;
		user_info->tbl.sgl[nents].page_link = (unsigned long) page;
		gap = min(hpg_size - user_info->tbl.sgl[nents].offset, min(request_len, hpg_size));
		user_info->tbl.sgl[nents].length = gap;
		i += (((unsigned long)(user_info->virt) & ~pg_mask) + gap + (pg_size - 1)) >> pg_shift;
		user_info->virt += gap;
		request_len -= gap;
		nents++;
		mission->tasks[FM_DMA].chunk_length += gap;
	}
	if (nents == 0)
		fm_err("%s: no sgl entries: page_pins %d request_len %llx orig_nents: %u", __func__, page_pins, request_len, user_info->tbl.orig_nents);
	user_info->tbl.nents = nents;

	return num;
}

static void dynamic_unpin_user_pages(struct mission_desc *mission, __u64 page_pins)
{
	unpin_user_pages(mission->pages, page_pins);
}

static __u64 dynamic_unmap_sg_user_pages(struct mission_task *task, __u64 sgl_offset, __u64 page_maps)
{
	struct dma_device *dma_dev;
	struct mission_desc *mission = task->mission;
	__u64 i;

	dma_dev = task->chan->device;

	dma_unmap_sg(dma_dev->dev, mission->mem.tbl.sgl + sgl_offset, page_maps, mission->read ? DMA_TO_DEVICE : DMA_FROM_DEVICE);

	return 0;
}

static __u64 dynamic_unmap_single_user_pages(struct mission_task *task, __u64 sgl_offset, __u64 page_maps)
{
	struct dma_device *dma_dev;
	struct mission_desc *mission = task->mission;
	__u64 i;

	dma_dev = task->chan->device;

	for (i = sgl_offset; i < sgl_offset + page_maps; i++) {
		dma_unmap_page(dma_dev->dev,
			       mission->mem.tbl.sgl[i].dma_address,
			       mission->mem.tbl.sgl[i].length,
			       mission->read ? DMA_TO_DEVICE : DMA_FROM_DEVICE);
	}

	return 0;
}

static void fastmove_device_last_callback(void *param)
{
	struct mission_task *task = (struct mission_task *)param;
	struct mission_desc *mission = task->mission;
	bool need_writeback = (mission->ddio && !mission->read);
	u64 elapse = PROC_END_TIME_STAT(dma_callback_t, task->time);

	if (need_writeback)
		arch_invalidate_pmem((void *)(mission->pmem_virt + task->virt_offset), task->chunk_length);
	COPY_UPDATE_BW_STAT(FM_DMA, task->mission->bw_type, task->chunk_length,
		elapse, task->mission->bw_bin);
	complete(&mission->completion);
}

/*
 * The streaming DMA mapping routines can be called from interrupt
 * context. There are two versions of each map/unmap, one which will
 * map/unmap a single memory region, and one which will map/unmap a
 * scatterlist. (Now, we only suppory map/unmap a single memory page
 * because of ioat dma driver disable dma_{map/unmap}_single)
 */
static __u64 dynamic_map_sg_user_pages(struct mission_task *task, __u64 sgl_offset, __u64 page_maps)
{
	struct mission_desc *mission = task->mission;
	struct dma_async_tx_descriptor *desc;
	dma_cookie_t cookie;
	struct dma_device *dma_dev;
	struct dma_chan	*chan;
	unsigned long flag_last = DMA_PREP_INTERRUPT | DMA_CTRL_ACK;
	unsigned long flag = DMA_CTRL_ACK;
	__u64 i, length;
	bool need_callback = (mission->ddio && !mission->read);
	dma_addr_t pmem_dma_addr;

	chan = task->chan;
	dma_dev = chan->device;

	if (need_callback) {
		flag |= DMA_PREP_INTERRUPT;
	}

	dma_map_sg(dma_dev->dev, mission->mem.tbl.sgl + sgl_offset, page_maps, mission->read ? DMA_TO_DEVICE : DMA_FROM_DEVICE);

	pmem_dma_addr = phys_to_dma(dma_dev->dev, page_to_phys(virt_to_page(mission->pmem_virt)) + offset_in_page(mission->pmem_virt));
	desc = dma_dev->device_prep_dma_memcpy_sg(
		chan,
		mission->mem.tbl.sgl + sgl_offset,
		page_maps,
		pmem_dma_addr + task->virt_offset,
		mission->read,
		flag_last);
	if (!desc) {
		fm_err("failed to prepare DMA memcpy: pagenum %llu pmem_dma_addr %llx\n", page_maps, pmem_dma_addr);
		goto prep_err;
	}
	desc->callback = fastmove_device_last_callback;
	desc->callback_param = (void *)task;

	task->time = ktime_get();
	cookie = dmaengine_submit(desc);
	if (dma_submit_error(cookie)) {
		fm_err("failed to do DMA tx_submit sgl_offset = %llu", sgl_offset);
		goto submit_err;
	}

	return page_maps;

submit_err:
	dmaengine_desc_free(desc);
prep_err:
	dma_unmap_sg(dma_dev->dev, mission->mem.tbl.sgl + sgl_offset, page_maps, mission->read ? DMA_TO_DEVICE : DMA_FROM_DEVICE);
	return 0;
}

static __u64 dynamic_map_single_user_pages(struct mission_task *task, __u64 sgl_offset, __u64 page_maps)
{
	struct mission_desc *mission = task->mission;
	struct dma_async_tx_descriptor *desc;
	dma_cookie_t cookie;
	struct dma_device *dma_dev;
	struct dma_chan	*chan;
	unsigned long flag_last = DMA_PREP_INTERRUPT | DMA_CTRL_ACK;
	unsigned long flag = DMA_CTRL_ACK;
	__u64 i, offset, sentinel;
	bool need_callback = (mission->ddio && !mission->read);
	dma_addr_t pmem_dma_addr;

	chan = task->chan;
	dma_dev = chan->device;

	if (need_callback) {
		flag |= DMA_PREP_INTERRUPT;
	}

	sentinel = sgl_offset + page_maps - 1;
	offset = 0;
	for (i = sgl_offset; i < sgl_offset + page_maps; i++) {
		mission->mem.tbl.sgl[i].dma_address = dma_map_page(dma_dev->dev,
			(struct page *)mission->mem.tbl.sgl[i].page_link,
			mission->mem.tbl.sgl[i].offset,
			mission->mem.tbl.sgl[i].length, 
			mission->read ? DMA_TO_DEVICE : DMA_FROM_DEVICE);
		if (dma_mapping_error(dma_dev->dev, mission->mem.tbl.sgl[i].dma_address)) {
			fm_err("failed to prepare mem address\n");
			goto map_mem_err;
		}

		pmem_dma_addr = phys_to_dma(dma_dev->dev, page_to_phys(virt_to_page(mission->pmem_virt + offset)) + offset_in_page(mission->pmem_virt + offset));
		offset += mission->mem.tbl.sgl[i].length;

		desc = dma_dev->device_prep_dma_memcpy(chan,
			mission->read ? mission->mem.tbl.sgl[i].dma_address : pmem_dma_addr,
			mission->read ? pmem_dma_addr : mission->mem.tbl.sgl[i].dma_address,
			mission->mem.tbl.sgl[i].length,
			i == sentinel ? flag_last : flag);
		if (!desc) {
			fm_err("failed to prepare DMA memcpy\n");
			goto map_prep_err;
		}

		if (i == sentinel) {
			desc->callback = fastmove_device_last_callback;
			desc->callback_param = (void *) task;
		}
		cookie = dmaengine_submit(desc);
		if (dma_submit_error(cookie)) {
			fm_err("failed to do DMA tx_submit, pairs: %llu, sgl_offset: %llu, len: %u\n", page_maps, i, mission->mem.tbl.sgl[i].length);
			goto submit_err;
		}
	}

	task->time = ktime_get();

	if (i < sgl_offset + page_maps) {
submit_err:
		dmaengine_desc_free(desc);
map_prep_err:
		i = i + 1;
map_mem_err:
		for (i = i - 1; i > sgl_offset; i--) {
			dma_unmap_page(dma_dev->dev,
				mission->mem.tbl.sgl[i].dma_address,
				mission->mem.tbl.sgl[i].length,
				mission->read ? DMA_TO_DEVICE : DMA_FROM_DEVICE);
		}
	}

	return i - sgl_offset;
}

static size_t run_user_page_fastmove(void *dst, const void *src, unsigned long size,
	bool rw, bool numa, int threshold, int node)
{
	struct mission_desc mission;
	struct mission_task *task;
	int master_cpu, use_cache = 0;
	__u64 pinned_pages, mapped_pages;
	struct plugged_device *fm_dev;
	u64 coords, i;
	bool pinned, use_cpu;
	int bw_type = GET_BW_DIR(numa, rw);
	mission.bw_type = bw_type;

	fm_dev = fastmove.devices + node;

	/* initialize all necessary mission descriptor */
	PROC_START_TIME_STAT(main_init_t, mission.time);
	if (size == 0)
		return 0;
	mission.len = size;
	mission.mem_virt = mission.mem.virt = rw ? (u64)dst : (u64)src;
	mission.pmem_virt = rw ? (u64)src : (u64) dst;
	master_cpu = smp_processor_id();
	use_cache = !cmpxchg(&per_cpu(bufs_percpu[2], master_cpu), 0, 1);
	if (use_cache) {
		mission.pages = (struct page **)per_cpu(bufs_percpu[0], master_cpu);
		mission.sgl = (struct scatterlist *)per_cpu(bufs_percpu[1], master_cpu);
	} else {
		mission.pages = kmalloc(COPY_INLINE_PAGES * sizeof(struct page *), GFP_KERNEL);
		mission.sgl = kmalloc(2 * COPY_INLINE_SIZE, GFP_KERNEL);
	}
	if (!mission.sgl)
		goto alloc_err;
	mission.sync = sysctl_fastmove.sync_wait;
	mission.ddio = sysctl_fastmove.ddio;
	mission.read = rw;
	mission.scatter = sysctl_fastmove.scatter;
	mission.chunks = sysctl_fastmove.chunks;
	task = mission.tasks;

	atomic64_inc(&fm_dev->user_num);
	init_completion(&mission.completion);

	coords = (u64) atomic64_fetch_add_relaxed(1, &fastmove.index);
	task[FM_DMA].chan = fm_dev->chans[coords % PLUGGED_CAPACITY];

	PROC_END_TIME_STAT(main_init_t, mission.time);

	while (mission.len) {
		__u64 scope, total_length = 0;
		__u64 dma_len, cpu_len, local_dma_bw, local_cpu_bw;
		mission.bw_bin = get_bw_bin(mission.len);
		if (mission.len < threshold) {
			goto cleanup;
		}

		pinned = false;
		use_cpu = false;

		reinit_completion(&mission.completion);

		/* chunks round down to closest capacity value */
		scope = min_t(__u64, min_t(__u64, mission.chunks, mission.len >> (PAGE_SHIFT + 2)), PLUGGED_CAPACITY);
		if (scope == 0)
			scope = 1;
		
		local_dma_bw = dma_bw[bw_type][mission.bw_bin];
		local_cpu_bw = cpu_bw[bw_type];
		if (mission.read && scope != 1) {
			__this_cpu_add(cnt_stats_percpu[split_t], 1);
			PROC_HIST_ADD_SPLIT_DMA_RATIO(numa, 10 * local_dma_bw / (local_dma_bw + local_cpu_bw));
			use_cpu = true;
			dma_len = mission.len * local_dma_bw / (local_dma_bw + local_cpu_bw);
		} else {
			if (mission.read)
				__this_cpu_add(cnt_stats_percpu[non_split_t], 1);
			dma_len = mission.len;
		}

		if (dma_len == 0) {
			fm_info("%s: zero dma length: mission length %llu local_dma_bw %llu local_cpu_bw %llu bw_type %d bw_bin %d",
				__func__, mission.len, local_dma_bw, local_cpu_bw, bw_type, mission.bw_bin);
		}

		task[FM_DMA].chunk_length = 0;

		if (dma_len >= (1 << 13)) {
			/* batch pin all user pages */
			pinned = true;
			pinned_pages = mission.scatter
				? dynamic_pin_user_pages_sg(&mission, dma_len)
				: dynamic_pin_user_pages(&mission, dma_len);
			PROC_END_TIME_STAT(main_pin_t, mission.time);
			if (pinned_pages <= 0) {
				goto pin_err;
			}
			mission.maps = mission.mem.tbl.nents;
			task[FM_DMA].mission = &mission;
			task[FM_DMA].sgl_offset = 0;
			task[FM_DMA].maps = mission.maps;
			task[FM_DMA].virt_offset = 0;
			mapped_pages = mission.scatter ?
					dynamic_map_sg_user_pages(&task[FM_DMA], 0, mission.maps) :
					dynamic_map_single_user_pages(&task[FM_DMA], 0, mission.maps);
			if (mapped_pages == 0) {
				fm_err("copy_dma_map_submit error: %llu mapped_pages\n", mapped_pages);
				goto submit_err;
			}
			dma_async_issue_pending(task[FM_DMA].chan);
			PROC_END_TIME_STAT(main_submit_t, mission.time);

			total_length += task[FM_DMA].chunk_length;
		} else {
			dma_len = 0;
			use_cpu = true;
		}

		// fm_info("dma length = %llx\n", task[FM_DMA].chunk_length);

		if (use_cpu) {
			void *mem_addr;
			void *pmem_addr;
			u64 execute_time;

			task[FM_CPU].virt_offset = task[FM_DMA].chunk_length;

			/* chunk_length is limited to ~512K */
			if (task[FM_DMA].chunk_length < dma_len)
				task[FM_CPU].chunk_length = task[FM_DMA].chunk_length * local_cpu_bw / local_dma_bw;
			else
				task[FM_CPU].chunk_length = mission.len - total_length;

			if (task[FM_CPU].chunk_length > mission.len - total_length)
				task[FM_CPU].chunk_length = mission.len - total_length;

			mem_addr = (void *)mission.mem_virt + task[FM_CPU].virt_offset;
			pmem_addr = (void *)mission.pmem_virt + task[FM_CPU].virt_offset;

			if (mission.read)
				__copy_to_user(mem_addr, pmem_addr, task[FM_CPU].chunk_length);
			else
				__copy_from_user_flushcache(pmem_addr, mem_addr, task[FM_CPU].chunk_length);
			execute_time = PROC_END_TIME_STAT(main_execute_t, mission.time);
			COPY_UPDATE_BW_STAT(FM_CPU, bw_type, task[FM_CPU].chunk_length, execute_time, 0);
			total_length += task[FM_CPU].chunk_length;
		}
		if (dma_len > 0) {
			if (mission.sync) {
				while (!try_wait_for_completion(&mission.completion)) cpu_relax();
			} else {
				unsigned long wait_time = dma_len / local_dma_bw;
				if (wait_time < 10){
					while (!try_wait_for_completion(&mission.completion)) cpu_relax();
				} else {
					usleep_range(wait_time - 1, wait_time + 2);
					while (!try_wait_for_completion(&mission.completion)) cpu_relax();
				}
			}
		}
		PROC_END_TIME_STAT(main_wait_t, mission.time);
		mission.len -= total_length;
		mission.pmem_virt += total_length;
		mission.mem_virt += total_length;
		mission.mem.virt = mission.mem_virt;

		/* I don't think its necessary in Intel platform */
		// dynamic_unmap_user_pages(task, split ? sentinel * bundle : 0, split ? bundle + remain : mission.maps);
		PROC_END_TIME_STAT(main_unmap_t, mission.time);

submit_err:
		/* batch unpin all user pages */
		if (pinned && pinned_pages > 0)
			dynamic_unpin_user_pages(&mission, pinned_pages);
		PROC_END_TIME_STAT(main_unpin_t, mission.time);
	}
pin_err:
	if (pinned && pinned_pages <= 0)
		fm_err("ERROR in pin pages!\n");
cleanup:
	if (use_cache) {
		per_cpu(bufs_percpu[2], master_cpu) = 0;
	} else {
		kfree(mission.sgl);
		kfree(mission.pages);
	}
alloc_err:
	atomic64_dec(&fm_dev->user_num);

	return mission.len;
}

int alloc_plugged_device(int pm_node, unsigned long length, int threshold, bool rw)
{
	int node, enroll = -1;

	if (length < threshold || sysctl_fastmove.max_user_num == 0) {
		__this_cpu_add(cnt_stats_percpu[rw ? read_thresh : write_thresh], 1);
		return -1;
	}

	if (atomic64_read(&fastmove.devices[pm_node].user_num) < sysctl_fastmove.max_user_num && pm_node < fastmove.num_nodes) {
		enroll = pm_node;
	} else {
		for (node = 0; node < fastmove.num_nodes; node++) {
			if (node != pm_node && atomic64_read(&fastmove.devices[node].user_num) < sysctl_fastmove.max_user_num) {
				enroll = node;
				break;
			}
		}
	}

	if (enroll == -1)
		__this_cpu_add(cnt_stats_percpu[rw ? read_busy: write_busy], 1);

	return enroll;
}

size_t memcpy_to_pmem_nocache_fastmove(struct block_device *bdev, void *dst,
			    const void __user *src, unsigned long size)
{
	int write_threshold;
	int node = -1;
	int numa = 0;
	int bw_type;
	int rc;
	ktime_t time;
	if (dev_to_node(disk_to_dev(bdev->bd_disk)) == numa_node_id()) {
		write_threshold = sysctl_fastmove.local_write;
	} else {
		numa = 1;
		write_threshold = sysctl_fastmove.remote_write;
	}
	bw_type = GET_BW_DIR(numa, 1);

	node = alloc_plugged_device(dev_to_node(disk_to_dev(bdev->bd_disk)), size, write_threshold, false);

	if (node == -1) {
		PROC_HIST_ADD_CPU_WRITE(numa, size);
		if (size >= write_threshold)
			time = ktime_get();
		rc = __copy_from_user_flushcache(dst, src, size);
		if (size >= write_threshold)
			COPY_UPDATE_BW_STAT(FM_CPU, bw_type, size - rc,
				ktime_to_ns(ktime_sub(ktime_get(), time)), 0);
		return rc;
	}

	might_fault();
	if (likely(access_ok(src, size))) {
		long rc = 0;
		unsigned long gap = 0;
		int aligned = false;
	    kasan_check_write(dst, size);
	    check_object_size(dst, size, true);

		// cacheline alignment
		gap = (void *)L1_CACHE_ALIGN((phys_addr_t)src) - src;
		if (gap == (void *)L1_CACHE_ALIGN((phys_addr_t) dst) - dst) {
			aligned = true;
			gap = min(gap, size);
			__copy_from_user_flushcache(dst, src, gap);
			dst += gap;
			src += gap;
			size -= gap;
		} else {
			aligned = false;
		}

		if (aligned) {
			if (size > 0)
				rc = run_user_page_fastmove(dst, src, size, false, numa, write_threshold, node);
		} else {
			rc = __copy_from_user_flushcache(dst, src, size);
		}

		if (rc > 0)
			rc = __copy_from_user_flushcache(dst + size - rc, src + size - rc, rc);

		if (aligned) {
			PROC_HIST_ADD_WRITE_ALIGNED(numa, size);
		} else {
			PROC_HIST_ADD_WRITE_UNALIGNED(numa, size);
		}
		return rc;
	}

	return size;
}
EXPORT_SYMBOL(memcpy_to_pmem_nocache_fastmove);

size_t memcpy_mcsafe_fastmove(struct block_device *bdev, void *dst, const void *src,
			  unsigned long size, bool async)
{
	int read_threshold;
	int node = -1;
	int numa = 0;
	int bw_type;
	int rc;
	ktime_t time;
	if (dev_to_node(disk_to_dev(bdev->bd_disk)) == numa_node_id()) {
		read_threshold = sysctl_fastmove.local_read;
	} else {
		numa = 1;
		read_threshold = sysctl_fastmove.remote_read;
	}
	bw_type = GET_BW_DIR(numa, 0);

	node = alloc_plugged_device(dev_to_node(disk_to_dev(bdev->bd_disk)), size, read_threshold, true);

	if (node == -1) {
		PROC_HIST_ADD_CPU_READ(numa, size);
		if (size >= read_threshold)
			time = ktime_get();
		if (async)
			rc = copy_to_user_mcsafe(dst, src, size);
		else
			rc = __copy_to_user(dst, src, size);
		if (size >= read_threshold)
			COPY_UPDATE_BW_STAT(FM_CPU, bw_type, size - rc,
				ktime_to_ns(ktime_sub(ktime_get(), time)), 0);
		return rc;
	}

	might_fault();
	if (likely(access_ok(dst, size))) {
		long rc;
	    kasan_check_read(src, size);
	    check_object_size(src, size, true);
	    rc = run_user_page_fastmove(dst, src, size, true, numa, read_threshold, node);
		if (rc > 0) {
			if (async)
				rc = copy_to_user_mcsafe(dst + size - rc, src + size - rc, rc);
			else
				rc = __copy_to_user(dst + size - rc, src + size - rc, rc);
		}
		PROC_HIST_ADD_READ(numa, size);
		return rc;
	}

	return size;
}
EXPORT_SYMBOL(memcpy_mcsafe_fastmove);

#ifdef CONFIG_ARCH_HAS_UACCESS_MCSAFE
static __always_inline __must_check unsigned long
copy_to_user_mcsafe_fastmove(struct block_device *bdev, void *to, const void *from,
			  unsigned len)
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
	ret = memcpy_mcsafe_fastmove(bdev, to, from, len, true);
	return ret;
}

static int copyout_mcsafe_fastmove(struct block_device *bdev, void __user *to,
				const void *from, size_t n)
{
	if (access_ok(to, n)) {
		kasan_check_read(from, n);
		n = copy_to_user_mcsafe_fastmove(bdev, (__force void *) to, from, n);
	}
	return n;
}
#endif

static int copy_from_user_flushcache_fastmove(struct block_device *bdev, void *dst,
					   const void __user *src,
					   unsigned size)
{
	kasan_check_write(dst, size);
	return memcpy_to_pmem_nocache_fastmove(bdev, dst, src, size);
}

size_t dax_copy_to_iter_fastmove(struct block_device *bdev, const void *addr,
			      size_t bytes, struct iov_iter *i)
{
	const char *from = addr;
	if (iter_is_iovec(i))
		might_fault();
	iterate_and_advance(i, bytes, v,
		copyout_mcsafe_fastmove(bdev, v.iov_base, (from += v.iov_len) - v.iov_len, v.iov_len));

	return bytes;
}
EXPORT_SYMBOL_GPL(dax_copy_to_iter_fastmove);

size_t dax_copy_from_iter_fastmove(struct block_device *bdev, void *addr,
				size_t bytes, struct iov_iter *i)
{
	char *to = addr;
	iterate_and_advance(i, bytes, v,
		copy_from_user_flushcache_fastmove(bdev, (to += v.iov_len) - v.iov_len, 
					 v.iov_base, v.iov_len));

	return bytes;
}
EXPORT_SYMBOL_GPL(dax_copy_from_iter_fastmove);


static sector_t dax_iomap_sector(struct iomap *iomap, loff_t pos)
{
	return (iomap->addr + (pos & PAGE_MASK) - iomap->offset) >> 9;
}

static loff_t dax_iomap_actor_fastmove(struct inode *inode, loff_t pos,
				    loff_t length, void *data,
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
		const sector_t sector = dax_iomap_sector(iomap, pos);
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
			xfer = dax_copy_from_iter_fastmove(bdev, kaddr, (size_t)map_len, iter);
		else
			xfer = dax_copy_to_iter_fastmove(bdev, kaddr, (size_t)map_len, iter);

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
loff_t dax_iomap_apply_fastmove(struct inode *inode, loff_t pos, loff_t length,
			     unsigned flags, const struct iomap_ops *ops,
			     void *data, iomap_actor_t actor)
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
 * dax_iomap_rw_fastmove - Perform I/O to a DAX file
 * @iocb:	The control block for this I/O
 * @iter:	The addresses to do I/O from or to
 * @ops:	iomap ops passed from the file system
 *
 * This function performs read and write operations to directly mapped
 * persistent memory.  The callers needs to take care of read/write exclusion
 * and evicting any page cache pages in the region under I/O.
 */
ssize_t dax_iomap_rw_fastmove(struct kiocb *iocb, struct iov_iter *iter,
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
		ret = dax_iomap_apply_fastmove(inode, pos, iov_iter_count(iter), flags, ops,
				iter, dax_iomap_actor_fastmove);
		if (ret <= 0)
			break;
		pos += ret;
		done += ret;
	}

	iocb->ki_pos += done;
	return done ? done : ret;
}
EXPORT_SYMBOL_GPL(dax_iomap_rw_fastmove);
