#include "nova.h"
#include <linux/scatterlist.h>
#include <linux/copy_accel.h>

/* run 30 seconds */
#define BENCH_TIME 30

int nova_bench(struct super_block *sb, unsigned int use_dma,
    unsigned int rw, unsigned int page_type, unsigned int size,
    unsigned int chan_node, unsigned int chan_idx) {
    int ret = 0;
    struct page **page_pool = NULL;
    size_t page_pool_len = 0;
    unsigned int pg_num = 1 << (PUD_SHIFT - PAGE_SHIFT);
    unsigned int hpg_num = 1 << (PUD_SHIFT - PMD_SHIFT);
    unsigned long pg_size = page_type ? PMD_SIZE : PAGE_SIZE;
    unsigned long pg_mask = page_type ? PMD_MASK : PAGE_MASK;
    unsigned long pg_order = page_type ? HPAGE_PMD_ORDER : 0;
    unsigned long pg_shift = page_type ? PMD_SHIFT : PAGE_SHIFT;
    unsigned long gfp_flags = page_type ? GFP_TRANSHUGE : GFP_KERNEL;
    struct nova_inode_info_header bench_sih;
    void *pmem_buf = NULL;
    unsigned long blocknr;
    int i = 0;
    ktime_t start, begin;
    u64 time_stat = 0;
    void *dst, *src; /* temp variable for memcpy */
    void **pmem_addrp, **mem_addrp;
    struct scatterlist *dst_sgl = NULL, *src_sgl = NULL;
    struct scatterlist **pmem_sglp = NULL, **mem_sglp= NULL;
    
    if (size < 1 || size > 2048) {
        nova_err(sb, "%s: invalid IO size %u!\n", __func__, size);
        ret = -EFAULT;
        goto out;
    }

    /* change to bytes */
    size = size * 1024;

    /* current buf pool size is 1G */
    if (page_type)
        page_pool_len = hpg_num;
    else
        page_pool_len = pg_num;

    page_pool = kvmalloc(sizeof(struct page *) * page_pool_len, GFP_KERNEL);
    if (!page_pool) {
        ret = -ENOMEM;
        goto out;
    }

    for (i = 0;i < page_pool_len;i++) {
        page_pool[i] = alloc_pages_node(numa_node_id(), gfp_flags, pg_order);
        cond_resched();
    }

    if (use_dma) {
        src_sgl = kvmalloc((page_type ? 1 : DIV_ROUND_UP(size, pg_size))
                    * sizeof(struct scatterlist), GFP_KERNEL);

        dst_sgl = kvmalloc((page_type ? 1 : DIV_ROUND_UP(size, pg_size))
                    * sizeof(struct scatterlist), GFP_KERNEL);
        if (!src_sgl || !dst_sgl) {
            ret = -ENOMEM;
            goto cleanup;
        }
    }

   /* alloc pmem buffer */
    bench_sih.ino = NOVA_TEST_PERF_INO;
    bench_sih.i_blk_type = NOVA_BLOCK_TYPE_4K;
    bench_sih.log_head = 0;
    bench_sih.log_tail = 0;

    i = nova_new_data_blocks(sb, &bench_sih, &blocknr, 0,
        pg_num, ALLOC_NO_INIT, smp_processor_id(),
        ALLOC_FROM_HEAD);
    
    if (i < pg_num) {
        nova_err(sb, "%s: allocated pmem blocks %d < requested pmem blocks %u\n", __func__, i, pg_num);
        if (i > 0)
            nova_free_data_blocks(sb, &bench_sih, blocknr, i);
        blocknr = 0;
        goto cleanup;
    }

    pmem_buf = nova_get_block(sb, nova_get_block_off(sb, blocknr, NOVA_BLOCK_TYPE_4K));
    
    if (rw) {
        pmem_addrp = &dst;
        mem_addrp = &src;
        pmem_sglp = &dst_sgl;
        mem_sglp = &src_sgl;
    } else {
        pmem_addrp = &src;
        mem_addrp = &dst;
        pmem_sglp = &src_sgl;
        mem_sglp = &dst_sgl;
    }

    i = 0;
    begin = ktime_get();
    while (true) {
        int j = 0;
        int ret = 0;
        ktime_t end;
        unsigned long offset;
        /* prepare next request */
        offset = (i * size) % PUD_SIZE;
        if (use_dma) {
            unsigned long remain = size;
            unsigned long gap;
            while (remain != 0) {
                (*pmem_sglp)[j].offset = (unsigned long)(pmem_buf + offset) & ~pg_mask;
                (*pmem_sglp)[j].page_link = (unsigned long)virt_to_page(pmem_buf + offset);
                (*mem_sglp)[j].offset = offset & ~pg_mask;
                (*mem_sglp)[j].page_link = (unsigned long)page_pool[offset >> pg_shift];
                gap = min(pg_size - (*mem_sglp)[j].offset, min(remain, pg_size));
                src_sgl[j].length = dst_sgl[j].length = gap;
                offset += gap;
                remain -= gap;
                j++;
            }
        }
        start = ktime_get();

        if (use_dma) {
            ret = copy_dma_run_bench_sgs(dst_sgl, src_sgl, rw, j, chan_node, chan_idx);
            if (ret != j)
                copy_err("%s: ret %d < request req num %d!\n", __func__, ret, j);
        } else {
            unsigned long remain = size;
            unsigned long gap;
            while (remain != 0) {
                *pmem_addrp = pmem_buf + offset;
                *mem_addrp = page_address(page_pool[offset >> pg_shift]) + (offset & ~pg_mask);
                gap = min(remain, (pg_size - (offset & ~pg_mask)));
                if (rw)
                    __memcpy_flushcache(dst, src, gap);
                else
                    __memcpy(dst, src, gap);
                remain -= gap;
                offset += gap;
            }
        }
       
        end = ktime_get();
        i++;
        time_stat += ktime_to_ns(ktime_sub(end, start));

        if (ktime_to_ns(ktime_sub(end, begin)) > BENCH_TIME * NSEC_PER_SEC)
            break;
        cond_resched();
    }

    nova_info("%s: %s %s %s %u chan-%u-%u lat: %llu\n", __func__, use_dma ? "DMA": "CPU",
        rw ? "write" : "read", page_type ? "HugePage" : "NormalPage",
        size, chan_node, chan_idx, time_stat / (u64)i);
cleanup:

    if (blocknr) {
        nova_free_data_blocks(sb, &bench_sih, blocknr, pg_num);
        blocknr = 0;
    }

    if (use_dma) {
       if (src_sgl) {
            kvfree(src_sgl);
            src_sgl = NULL;
        }

        if (dst_sgl) {
            kvfree(dst_sgl);
            dst_sgl = NULL;
        }
    }

    for (i = 0;i < page_pool_len;i++) {
        __free_pages(page_pool[i], pg_order);
    }
    kvfree(page_pool);
    page_pool = NULL;

out:
    return ret;
}