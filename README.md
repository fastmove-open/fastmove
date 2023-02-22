# Revitalizing the Forgotten On-Chip DMA to Expedite Data Movement in NVM-based Storage Systems
## Description

This repository contains a modified Linux-5.9 kernel, Fastmove implementation,
NOVA codes. Bench scripts can be found in [fastmove_bench](https://github.com/fastmove-open/fastmove_bench). Following the commands below to evaluate Fastmove.

## How to test Fastmove

### 0. Pre-requisite

Make sure the testbed supports Intel I/OAT Technology.

If you don't have Intel Optane PMM, you can emulate using kernel options ([Tutorial](https://pmem.io/blog/2016/02/how-to-emulate-persistent-memory/)). However, emulated PMs has significant different characteristics from real ones.

### 1. Build and install the modified kernel

Run `make menuconfig` and enables LIBNVDIMM (`CONFIG_LIBNVDIMM`), PMEM (`CONFIG_BLK_DEV_PMEM`), DAX (`CONFIG_FS_DAX`) supports under *Device Drivers* section. Then, enable NOVA (`CONFIG_NOVA_FS`, `CONFIG_NOVA_DM_FS`) in **modules (m)** under *File Systems* section.  Build and install as usual. 

### 2. Install applications and generate datasets.

Our evaluation involves the following benchmark tools / applications:

1. [fio](https://github.com/axboe/fio)
2. [MySQL](https://www.mysql.com/)
3. [filebench](https://github.com/filebench/filebench)
4. [GraphWalker](https://github.com/ustcadsl/GraphWalker)

Please follow the instructions in their documents to install these tools / applications.

To evaluate MySQL or GraphWalker, we should first generate a dataset.

#### MySQL

This test uses [tpcc-mysql](https://github.com/Percona-Lab/tpcc-mysql). Please follow the instructions there to set up this test. Here is the brief process:

After compile the tpcc-mysql, you need to generate the database:

```bash
# Create a database named tpcc5000
mysqladmin create tpcc5000
mysql tpcc5000 < create_table.sql
mysql tpcc5000 < add_fkey_idx.sql

# Populate data, warehouse 5000
tpcc_load -h127.0.0.1 -d tpcc5000 -u root -p "" -w 5000
```

You may also save the database for future use. The database directory structure should look like this. Assuming your database is located at $TPCC_DB_DIR:

```bash
$TPCC_DB_DIR
├── auto.cnf
├── ca-key.pem
├── ca.pem
├── client-cert.pem
├── client-key.pem
├── ib_buffer_pool
├── ibdata1
├── ib_logfile0
├── ib_logfile1
├── ibtmp1
├── mysql
├── mysql.sock.lock
├── performance_schema
├── private_key.pem
├── public_key.pem
├── server-cert.pem
├── server-key.pem
├── sys
└── tpcc5000
```

Please first set the corresponding variable in `fn_mysql`:

```bash
TPCC_DIR="/root/tpcc-mysql/"
TPCC_DB_DIR="/mnt/nvme/tpcc-p16-5000"
```

You might also want to modify the auto generated MySQL configuration to make it recognize the correct Tpcc test database location.

#### GraphWalker

Download the [generator](https://github.com/rwang067/graph500-3.0).
After compile it, use the following commands to generate the dataset:

```sh
# generating 2^31 vertices with average vertex degree of 2 * 32 = 64
# you can control the dataset size by change these two factors
./graph500_reference_bfs 31 32 kron31_32.txt

# split the big file into small files, with each small file 33554432 lines
split -l 33554432 kron31_32.txt kron31_32.txt-split-

# sort the smaller files
for X in kron31_32.txt-split-*;
do
    sort -n -t' ' -k1 < $X > $X-sorted;
    rm $X
done

# merge the sorted smaller files
sort -n -t' ' -k1 -T ./ -m kron31_32.txt-split-*-sorted > kron31_32-sorted.txt

# Clean-up:
rm kron31_32.txt-split*
```

### 3. Run bench scripts

Boot to the installed kernel, download bench scripts:
```sh
git clone https://github.com/fastmove-open/fastmove_bench
```
#### Benchmark base structure

The structure of the benchmark directory is as follows:

```
bench_scripts/fastmove
├-- .
|
├-- bench_all.sh
|
├-- bench_setting
|
├-- profile/
|   ├-- bench_profile_1
|   └-- bench_profile_2
|
|   *** Workload Directory ***
|
├-- workload1/
|   └-- fn_workload1
|
├-- workload2/
|   ├-- fn_workload2
|   └-- helper_fn
|
└-- ....
```

**Workload Directory**:

The workload directories each defines a workload to run, if the workload is named `$WL`,
then there should be a bash file that can be sourced named `fn_$WL` in that folder that
will later be sourced from `bench_all.sh`.

**Profile Directory**:

The profile directory defines presets that contain workloads to run and system configuration to use.

**bench_all.sh**

The main benchmark script. It will try to run all workloads found the profile. For a specific benchmark profile,
it will first setup the system based on the profile, then run the benchmarks specified by the profile.

**bench_setting**

This is a bash script that describes all profiles to be used at one bench run.

#### Running a workload

To run a workload, for example, `fio`, write a profile in the `profile` directory which looks like this:

```bash
#!/bin/bash
# fio_profile

## System configuration
setting_bench="fio"
setting_fs="nova"
setting_skt_setup="SS"
setting_method_setup="DMA"

## Fastmove configuration
setting_write_thresh="16384"
setting_read_thresh="32768"
setting_user_nums="4"
setting_chunk="2"
setting_scatter="1"
setting_remote_write_thresh="16384"
setting_remote_read_thresh="32768"
```

And in the `bench_setting` file, set the `setting_bench_profile` variable to instruct the bench script to load the
profile.

```bash
#!/bin/bash

export PROFILE_BASE_DIR=profile

setting_bench_profile=(
        "mysql_ss"
)
```

If you want to run multiple profiles at one time, make it a list.

Then run the script:

```bash
./bench_all.sh
```

The result will be in a folder named `result_$(date --iso-8601=seconds)/${profile}`

The predefined profiles are as follows:

- `breakdown`: Filebench breakdown, workload 1
- `breakdown_filebench_r`: Filebench breakdown, workload 2
- `filebench_ss`: Filebench single socket
- `filebench_ds`: Filebench dual socket
- `graphwalker_ss`: Graphwalker single socket
- `graphwalker_ds`: Graphwalker dual socket
- `mysql_ss`: Mysql single socket
- `mysql_ds`: Mysql dual socket

#### Defining a workload

To add a workload, declare a `fn_${BENCH_NAME}` file in a folder named `${BENCH_NAME}`, and define the following
functions:

1. `setup_single_bench`:
  - Setup profiling / other things before benchmark begins
  - Arguments:
    - `$1`: The output filename
    - `$2`: 0 for single socket, 1 for dual socket

2. `bench_single_main` (optional):
  - Run the actual workload
  - Arguments:
    - `$1`: The output filename
    - `$2`: 0 for single socket, 1 for dual socket

3. `stop_single_bench` (optional):
  - Stop profiling / other cleanups after the benchmark
  - Arguments:
    - `$1`: The output filename
    - `$2`: 0 for single socket, 1 for dual socket

4. `init_single_bench` (optional):
  - A run-once function that is run only once before everything starts
  - No arguments

There are also 3 variables that are defined for you to use:

1. `$BENCH_DIR` is the current workload directory
2. `$MOUNT_DIR` is the file system mount point
3. `$numactl_cmd` contains the right `numactl` command for the system configuration




```bash
cd fastmove-bench
./bench_all.sh
```

And the result will be in the directory named: `result_$(date --iso-8601=seconds)/fio_profile/fio`

The fio output will be files with ".log" extension. The last 3 fields in the filename indicate the
fio read/write pattern, bs size and thread number, respectively.

To acquire bandwidth and latency information, check fio's [minimal output field description](https://www.andypeace.com/fio_minimal.html),
for example, to get the latency information from the fio log named: `SS-CPU-16-nova-16384-32768-2-1-1-1-1-write-s256k-t1.log`,
one can use the following command:

```bash
# Get mean total latency
cat SS-CPU-16-nova-16384-32768-2-1-1-1-1-write-s256k-t1.log | cut -d ';' -f 81

# Get mean total bandwidth
cat SS-CPU-16-nova-16384-32768-2-1-1-1-1-write-s256k-t1.log | cut -d ';' -f 86
```
