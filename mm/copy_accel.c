#include <linux/sysctl.h>
#include <linux/highmem.h>
#include <linux/workqueue.h>
#include <linux/slab.h>
#include <linux/freezer.h>
#include <linux/dmaengine.h>
#include <linux/dma-mapping.h>
#include <linux/prandom.h>
#include <linux/copy_accel.h>


#define FASTMOVE_ISSUE_DMA_ROUTINE (1<<1)
#define FASTMOVE_ISSUE_CPU_ROUTINE (1<<2)
#define FASTMOVE_ISSUE_NONE	(1<<3)

struct issue_item {
	char *to;
	char *from;
	unsigned long chunk_size;
};

struct issue_task {
	struct work_struct issue_work;
	struct plugged_device *device;
	struct dmaengine_unmap_data *unmap;
	dma_cookie_t cookie;
	struct dma_chan *chan;

	int type;
	unsigned long num_items;
	struct issue_item item[0];
};

static void fastmove_issue_routine(struct work_struct *work)
{
	struct issue_task *issue = (struct issue_task *)work;
	int i;

	if(issue->type == FASTMOVE_ISSUE_DMA_ROUTINE) {
		dma_async_issue_pending(issue->chan);
		if (dma_sync_wait(issue->chan, issue->cookie) != DMA_COMPLETE) {
			pr_err("%s: dma does not complete at chunk %s\n", __func__, dma_chan_name(issue->chan));
		}

		if (issue->unmap){
			dmaengine_unmap_put(issue->unmap);
			atomic64_dec(&issue->device->user_num);
		}
	} else if(issue->type == FASTMOVE_ISSUE_CPU_ROUTINE) {
		for(i = 0; i < issue->num_items; i++) {
			memcpy(issue->item[i].to,
						  issue->item[i].from,
						  issue->item[i].chunk_size);
		}
	}
}

int copy_page_fastmove(struct page *to, struct page *from, int nr_pages)
{
	struct plugged_device *device;
	struct dma_chan *chan;
	struct device *dev;
	struct dma_async_tx_descriptor *tx ;
	dma_cookie_t cookie;
	enum dma_ctrl_flags flags;
	struct dmaengine_unmap_data *unmap;
	struct issue_task *issue[40] = {0};
	int cpu, node, issue_type;
	int cpu_id_list[40] = {0};
	int err = 0;
	int idx, chunks = sysctl_fastmove.chunks;
	size_t page_offset;

	BUG_ON(thp_nr_pages(from) != nr_pages);
	BUG_ON(thp_nr_pages(to) != nr_pages);

	/* round down to closest 2^x value  */
	chunks = 1<<ilog2(chunks);

	chunks = min_t(int, chunks, nr_pages);

	if ((nr_pages != 1) && (nr_pages % chunks != 0))
		return -5;

	idx = 0;
	for_each_cpu (cpu, cpumask_of_node(page_to_nid(to))) {
		if (idx >= chunks)
			break;
		cpu_id_list[idx] = cpu;
		idx++;
	}

	for (idx = 0; idx < chunks; idx++) {
		// perferable numa node
		node = max_t(int, page_to_nid(from), page_to_nid(to)) % 4;
		chan = NULL;
		issue_type = FASTMOVE_ISSUE_NONE;
		if (atomic64_read(&fastmove.devices[node].user_num) < sysctl_fastmove.max_user_num) {
			device = fastmove.devices + node;
			atomic64_inc(&device->user_num);
			chan = device->chans[prandom_u32_max(PLUGGED_CAPACITY)];
			dev = chan->device->dev;
			issue_type = FASTMOVE_ISSUE_DMA_ROUTINE;
		} else {
			// other numa node
			for(node = 0; node < sysctl_fastmove.num_nodes; node++){
				if (atomic64_read(&fastmove.devices[node].user_num) < sysctl_fastmove.max_user_num) {
					device = fastmove.devices + node;
					atomic64_inc(&device->user_num);
					chan = device->chans[prandom_u32_max(PLUGGED_CAPACITY)];
					dev = chan->device->dev;
					issue_type = FASTMOVE_ISSUE_DMA_ROUTINE;
					break;
				}
			}
			// CPU collaboration
			if(!chan && node == sysctl_fastmove.num_nodes)
				issue_type = FASTMOVE_ISSUE_CPU_ROUTINE;
		}

		if (issue_type == FASTMOVE_ISSUE_DMA_ROUTINE) {
			unmap = dmaengine_get_unmap_data(dev, 2, GFP_NOWAIT);
			if (!unmap) {
				pr_err("%s: no unmap data at chan %s\n", __func__, dma_chan_name(chan));
				err = -3;
				goto unmap_dma;
			}

			if (nr_pages == 1) {
				page_offset = PAGE_SIZE / chunks;

				unmap->to_cnt = 1;
				unmap->addr[0] = dma_map_page(dev, from, page_offset * idx, page_offset, DMA_TO_DEVICE);
				unmap->from_cnt = 1;
				unmap->addr[1] = dma_map_page(dev, to, page_offset * idx, page_offset, DMA_FROM_DEVICE);
				unmap->len = page_offset;
			} else {
				page_offset = nr_pages / chunks;

				unmap->to_cnt = 1;
				unmap->addr[0] = dma_map_page(dev, from + page_offset * idx, 0, PAGE_SIZE * page_offset, DMA_TO_DEVICE);
				unmap->from_cnt = 1;
				unmap->addr[1] = dma_map_page(dev, to + page_offset * idx, 0, PAGE_SIZE * page_offset, DMA_FROM_DEVICE);
				unmap->len = PAGE_SIZE * page_offset;
			}

			tx = chan->device->device_prep_dma_memcpy(chan, unmap->addr[1], unmap->addr[0], unmap->len, flags);
			if (!tx) {
				pr_err("%s: no tx descriptor at chan %s\n", __func__, dma_chan_name(chan));
				err = -4;
				goto unmap_dma;
			}

			cookie = tx->tx_submit(tx);

			if (dma_submit_error(cookie)) {
				pr_err("%s: submission error at chunk %s\n", __func__, dma_chan_name(chan));
				err = -5;
				goto unmap_dma;
			}

			issue[idx] = kzalloc(sizeof(struct issue_task), GFP_KERNEL);
			if (!issue[idx]) {
				err = -ENOMEM;
				goto free_issue;
			}

			INIT_WORK((struct work_struct *)issue[idx], fastmove_issue_routine);
			issue[idx]->type = issue_type;
		
			issue[idx]->device = device;
			issue[idx]->chan = chan;
			issue[idx]->unmap = unmap;
			issue[idx]->cookie = cookie;
			// pr_warn("%s: pages:%d idx: %d: Moving from %p to %p, chunk size: %lx, scheme: %s\n", __func__, nr_pages, idx,
			// 			unmap->addr[1], unmap->addr[0], unmap->len,
			// 			issue_type == FASTMOVE_ISSUE_DMA_ROUTINE ? "DMA" : "CPU");
			queue_work_on(cpu_id_list[idx], system_highpri_wq, (struct work_struct *)issue[idx]);
		} else if (issue_type == FASTMOVE_ISSUE_CPU_ROUTINE) {
			issue[idx] = kzalloc(sizeof(struct issue_task) + sizeof(struct issue_item), GFP_KERNEL);
			if (!issue[idx]) {
				err = -ENOMEM;
				goto free_issue;
			}
			
			INIT_WORK((struct work_struct *)issue[idx], fastmove_issue_routine);
			issue[idx]->type = issue_type;
			issue[idx]->num_items = 1;

			if (nr_pages == 1) {
				page_offset = PAGE_SIZE / chunks;
				
				issue[idx]->item->to = kmap(to) + idx * page_offset;
				issue[idx]->item->from = kmap(from) + idx * page_offset;
				issue[idx]->item->chunk_size = page_offset;
			} else {
				page_offset = nr_pages / chunks;

				issue[idx]->item->to = kmap(to + idx * page_offset);
				issue[idx]->item->from = kmap(from + idx * page_offset);
				issue[idx]->item->chunk_size = PAGE_SIZE * page_offset;
			}

			// pr_warn("%s: pages:%d idx: %d: Moving from %p to %p, chunk size: %lx, scheme: %s\n", __func__, nr_pages, idx,
			// 			issue[idx]->item->from, issue[idx]->item->to, issue[idx]->item->chunk_size,
			// 			issue_type == FASTMOVE_ISSUE_DMA_ROUTINE ? "DMA" : "CPU");
			queue_work_on(cpu_id_list[idx], system_highpri_wq, (struct work_struct *)issue[idx]);
		} else {
			pr_warn("%s: !! Unhandled issue NONE", __func__);
		}
	}

	/* Wait until all issue routine finishes  */
	for (idx = 0; idx < chunks; idx++) {
		flush_work((struct work_struct *)issue[idx]);
		if(issue[idx]->type == FASTMOVE_ISSUE_CPU_ROUTINE) {
			if (nr_pages == 1) {
				kunmap(from);
				kunmap(to);
			} else {
				page_offset = nr_pages / chunks;
				kunmap(from + idx * page_offset);
				kunmap(to + idx * page_offset);
			}
		}
	}

free_issue:
	for (idx = 0; idx < chunks; idx++){
		if (issue[idx])
			kfree(issue[idx]);
	}

	return err;

unmap_dma:
	if (unmap){
		dmaengine_unmap_put(unmap);
		atomic64_dec(&device->user_num);
	}

	return err;
}

/*
 * Use DMA copy a list of pages to a new location
 *
 * Just put each page into individual DMA channel.
 *
 * */
int copy_page_lists_fastmove(struct page **to, struct page **from, int nr_items)
{
	struct plugged_device *device;
	struct dma_chan *chan;
	struct device *dev;
	struct dma_async_tx_descriptor *tx;
	dma_cookie_t cookie;
	enum dma_ctrl_flags flags;
	struct dmaengine_unmap_data *unmap;
	struct issue_task *issue[40] = {0};
	int cpu, node, issue_type;
	int cpu_id_list[40] = {0};
	int err = 0;
	int chunks = sysctl_fastmove.chunks;
	int chk_idx, xfer_idx, page_idx = 0;

	/* round down to closest 2^x value  */
	chunks = 1<<ilog2(chunks);

	chunks = min_t(int, chunks, nr_items);

	for (chk_idx = 0; chk_idx < chunks; chk_idx++) {
		int batch_size = nr_items / chunks;

		if (chk_idx < (nr_items % chunks))
			batch_size += 1;

		if (batch_size > 128) {
			err = -ENOMEM;
			pr_err("%s: too many pages to be transferred\n", __func__);
			return err;
		}

		xfer_idx = 0;
		for_each_cpu (cpu, cpumask_of_node(page_to_nid(to[page_idx]))) {
			if (xfer_idx >= 1)
				break;
			cpu_id_list[xfer_idx] = cpu;
			xfer_idx++;
		}

		// perferable numa node
		node = max_t(int, page_to_nid(from[page_idx]), page_to_nid(to[page_idx])) % 4;
		chan = NULL;
		if (atomic64_read(&fastmove.devices[node].user_num) < sysctl_fastmove.max_user_num) {
			device = fastmove.devices + node;
			atomic64_inc(&device->user_num);
			chan = device->chans[prandom_u32_max(PLUGGED_CAPACITY)];
			dev = chan->device->dev;
			issue_type = FASTMOVE_ISSUE_DMA_ROUTINE;
		} else {
			// other numa node
			for(node = 0; node < sysctl_fastmove.num_nodes; node++){
				if (atomic64_read(&fastmove.devices[node].user_num) < sysctl_fastmove.max_user_num) {
					device = fastmove.devices + node;
					atomic64_inc(&device->user_num);
					chan = device->chans[prandom_u32_max(PLUGGED_CAPACITY)];
					dev = chan->device->dev;
					issue_type = FASTMOVE_ISSUE_DMA_ROUTINE;
					break;
				}
			}
			// CPU collaboration
			if(!chan && node == sysctl_fastmove.num_nodes)
				issue_type = FASTMOVE_ISSUE_CPU_ROUTINE;
		}

		if (issue_type == FASTMOVE_ISSUE_DMA_ROUTINE) {
			unmap = dmaengine_get_unmap_data(dev, 2 * batch_size, GFP_NOWAIT);
			if (!unmap) {
				pr_err("%s: no unmap data at chan %s\n", __func__, dma_chan_name(chan));
				err = -ENODEV;
				goto unmap_dma;
			}

			unmap->to_cnt = batch_size;
			unmap->from_cnt = batch_size;
			unmap->len = thp_nr_pages(from[chk_idx]) * PAGE_SIZE;

			for (xfer_idx = 0; xfer_idx < batch_size; xfer_idx++, page_idx++) {
				size_t page_len = thp_nr_pages(from[page_idx]) * PAGE_SIZE;

				BUG_ON(page_len != thp_nr_pages(to[page_idx]) * PAGE_SIZE);
				BUG_ON(unmap->len != page_len);

				unmap->addr[xfer_idx] = dma_map_page(dev, from[page_idx], 0, page_len, DMA_TO_DEVICE);
				unmap->addr[xfer_idx+batch_size] = dma_map_page(dev, to[page_idx], 0, page_len, DMA_FROM_DEVICE);

				tx = chan->device->device_prep_dma_memcpy(chan, unmap->addr[xfer_idx + batch_size], unmap->addr[xfer_idx], unmap->len, flags);
				if (!tx) {
					pr_err("%s: no tx descriptor at chan %s xfer %d\n", __func__, dma_chan_name(chan), xfer_idx);
					err = -ENODEV;
					goto unmap_dma;
				}

				cookie = tx->tx_submit(tx);

				if (dma_submit_error(cookie)) {
					pr_err("%s: submission error at chan %s xfer %d\n", __func__, dma_chan_name(chan), xfer_idx);
					err = -ENODEV;
					goto unmap_dma;
				}
				// pr_warn("%s: items:%d chk_idx: %d: Moving from %p to %p, chunk size: %lx, scheme: %s\n", __func__, nr_items, chk_idx,
				// 		unmap->addr[xfer_idx + batch_size], unmap->addr[xfer_idx], unmap->len,
				// 		issue_type == FASTMOVE_ISSUE_DMA_ROUTINE ? "DMA" : "CPU");
			}

			issue[chk_idx] = kzalloc(sizeof(struct issue_task), GFP_KERNEL);
			if (!issue[chk_idx]) {
				err = -ENOMEM;
				goto free_issue;
			}
			INIT_WORK((struct work_struct *)issue[chk_idx], fastmove_issue_routine);
			issue[chk_idx]->type = issue_type;
			issue[chk_idx]->device = device;
			issue[chk_idx]->chan = chan;
			issue[chk_idx]->unmap = unmap;
			issue[chk_idx]->cookie = cookie;
			queue_work_on(cpu_id_list[chk_idx], system_highpri_wq, (struct work_struct *)issue[chk_idx]);
		} else {
			issue[chk_idx] = kzalloc(sizeof(struct issue_task) + sizeof(struct issue_item) * batch_size, GFP_KERNEL);
			if (!issue[chk_idx]) {
				err = -ENOMEM;
				goto free_issue;
			}
			INIT_WORK((struct work_struct *)issue[chk_idx], fastmove_issue_routine);
			issue[chk_idx]->type = issue_type;
			issue[chk_idx]->num_items = batch_size;
			
			for (xfer_idx = 0; xfer_idx < batch_size; xfer_idx++, page_idx++) {
				issue[chk_idx]->item[xfer_idx].to = kmap(to[page_idx]);
				issue[chk_idx]->item[xfer_idx].from = kmap(from[page_idx]);
				issue[chk_idx]->item[xfer_idx].chunk_size = PAGE_SIZE * thp_nr_pages(from[page_idx]);
				
				BUG_ON(thp_nr_pages(to[page_idx]) != thp_nr_pages(from[page_idx]));
			}

			// pr_warn("%s: items:%d chk_idx: %d: Moving from %p to %p, chunk size: %lx, scheme: %s\n", __func__, nr_items, chk_idx,
			// 			issue[chk_idx]->item->from, issue[chk_idx]->item->to, issue[chk_idx]->item->chunk_size,
			// 			issue_type == FASTMOVE_ISSUE_DMA_ROUTINE ? "DMA" : "CPU");
			queue_work_on(cpu_id_list[chk_idx], system_highpri_wq, (struct work_struct *)issue[chk_idx]);
		}

		if (xfer_idx != batch_size)
			pr_err("%s: only %d out of %d pages are transferred\n", __func__, xfer_idx - 1, batch_size);
	}

	/* Wait until all issue routine finishes  */
	for (chk_idx = 0; chk_idx < chunks; chk_idx++){
		if(issue[chk_idx]) {
			flush_work((struct work_struct *)issue[chk_idx]);
		}
	}

free_issue:
	for (chk_idx = 0; chk_idx < chunks; chk_idx++){
		if (issue[chk_idx])
			kfree(issue[chk_idx]);
	}

	return err;

unmap_dma:
	if (unmap){
		dmaengine_unmap_put(unmap);
		atomic64_dec(&device->user_num);
	}

	return err;
}
