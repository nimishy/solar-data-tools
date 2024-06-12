"""Script for testing the Dask Runner tool with the LocalFiles DataPlug. The 
script takes in arguments for number of workers, threads and amount of memory 
allocated to each worker.

Options:
    -w, --workers
            Number of Workers spawned in the Dask Client.
            Default is 4
    
    -t, --threads
            Number of threads allocated to each worker.
            Default is 2.

    -m, --memory
            Amount of memory in GB to allocate per worker.
            Default is 6.

    -n, --number_files
            Number of files in directory to perform computations on. 0 indicates
            all files in the directory path will be included.
            Default is 0.
            Optional

    -v, --verbose
            Enabling verbose for the Dask Runner Tool functions.
            Default is False.
            Optional.

    -p, --perf_report
            Enables performance report generation for the computations.
            Default is False.
            Optional

    -f, --file
            Directory path to the folder containing the files for computation.

    -r, --result
            Directory path to store the results and reports.
            Default is `../results/`.

Usage Examples:
    python rev_loc_base_dask.py -w 2 -t 2 -m 6 -p -d ../example_data/ -r ../results/
    python rev_loc_base_dask.py -w 2 -t 2 -m 6 -v -d ../example_data/
"""
import glob, os, argparse
from sdt_dask.dataplugs.csv_plug import LocalFiles
from sdt_dask.clients.local_client import LocalClient
from sdt_dask.dask_tool.runner import Runner

parser = argparse.ArgumentParser()

parser.add_argument(
    "-w",
    "--workers",
    default=4,
    help=(
        "Number of Workers to spawn in the Dask Client. "
        "Example: --workers 3', default=4"),
)

parser.add_argument(
    "-t",
    "--threads",
    default=2,
    help=(
        "Number of threads allocated to each worker. "
        "Example: --threads 3', default=2"),
)

parser.add_argument(
    "-m",
    "--memory",
    default=6,
    help=(
        "Amount of memory in GB to allocate per worker. "
        "Example: --memory 5', default=5"),
)

parser.add_argument(
    "-n",
    "--number_files", 
    default=0,
    help=(
        "Number of files in directory to perform computations on. 0 indicates \
            all files in the directory path will be included. "
        "Example: -n 10 --number_files 10, default=0"),
)

parser.add_argument(
    "-v",
    "--verbose",
    default=False,
    action='store_true',
    help=(
        "Enabling verbose for the Dask Runner Tool functions. "
        "Example --verbose"),
)

parser.add_argument(
    "-p",
    "--perf_report",
    default=False, 
    action='store_true', 
    help=(
        "Enables performance report generation for the computations.  "
        "Example -p"
    )
)

parser.add_argument(
    "-d",
    "--dir_path",
    default=None,
    help=(
        "Directory path to the folder containing the files for computation. "
        "Example --file ../results/', default=None"),
)

parser.add_argument(
    "-r",
    "--report_dir",
    default="../results/",
    help=(
        "Directory path to store the results and reports."
        "Example --result ../results/', default='../results/'"),
)

# Assigns Arguments to variables, configures the DataPlug and the Dask Client.
# Calls Runner and configures the Dask task graph using Runner.set_up(). 
# Executes the task graph on the Dask Client depending on the users preference 
# for a performance report.
if __name__ == '__main__':

    options = parser.parse_args()

    # Argument variable declaration
    WORKERS = int(options.workers)
    THREADS = int(options.threads)
    MEMORY = float(options.memory)
    NUMBER_FILES = int(options.number_files)
    VERBOSE = bool(options.verbose)
    PERF_REPORT = bool(options.perf_report)
    FILE_PATH = options.dir_path
    REPORT_DIR = options.report_dir

    # Defined LocalFiles DataPlug to read local CSV files
    data_plug = LocalFiles(path_to_files=FILE_PATH)
    KEYS = [(os.path.basename(fname)[:-4],) for fname in glob.glob(FILE_PATH + "*")]
    
    if NUMBER_FILES > 0: 
        KEYS = KEYS[:NUMBER_FILES]

    # Dask Local client Setup
    client_setup = LocalClient(workers=WORKERS,
                               threads=THREADS,
                               memory=MEMORY)
    # Dask Local Client Initialization
    client = client_setup.init_client()

    # Dask Tool initialization and set up
    dask_tool = Runner(client=client)
    dask_tool.set_up(KEYS, data_plug, fix_shifts=True, verbose=VERBOSE)

    # Dask Tool Task Compute and client shutdown
    df = dask_tool.compute(report=PERF_REPORT, output_path=REPORT_DIR)

    print(df)
    client.shutdown()
