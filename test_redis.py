import concurrent
import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, Future
from typing import List
import logging
import redis

from azure.storage.blob import BlockBlobService, Blob

from pynotstdlib.logging import default_logging

container_name = "myblobs"
account_name = "jgoddingblob"
account_key = "rzfvY+HXTZGKJs5MMon3PHHKKtdBL9eZnKBw3tuav62xczvPNpJjp2D2Qg/bSyucTdVwNYbSu1veAF293ufWWQ=="

default_logging(logging.INFO)


def process_file(folder_blob: str):
    root = logging.getLogger()
    root.setLevel(logging.ERROR)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s - [%(filename)s:%(lineno)s]')
    ch.setFormatter(formatter)
    root.addHandler(ch)

    print("Importing polymer")
    sys.stdout.flush()
    from polymer.main import run_atm_corr, Level1, Level2
    from polymer.level2 import default_datasets

    print("Processing: {0}".format(folder_blob))
    block_blob_service = BlockBlobService(account_name = account_name, account_key = account_key)

    # Copy the file down from azure
    input_file_name = tempfile.gettempdir()
    print("Downloading blob to " + input_file_name)

    target_root_folder = os.path.join(input_file_name, folder_blob)
    if target_root_folder[-1] == "/":
        # Remove the final / if it exists.
        target_root_folder = target_root_folder[:-1]

    if not os.path.exists(target_root_folder):
        os.makedirs(target_root_folder, exist_ok = True)
    else:
        import shutil
        shutil.rmtree(target_root_folder)
        os.makedirs(target_root_folder, exist_ok = True)

    import psutil

    def download_file(sub_blob_file: Blob):
        print(psutil.disk_usage("/"))
        print("Downloading: {0}".format(sub_blob_file.name))

        target_output_file = os.path.join(input_file_name, sub_blob_file.name)

        import time
        retry = 0

        while not os.path.exists(target_output_file):
            try:
                block_blob_service.get_blob_to_path(container_name,
                                                    blob_name = sub_blob_file.name,
                                                    file_path = target_output_file,
                                                    validate_content = True)
            except Exception:
                time.sleep(2 ** retry)
                retry += 1

        print("Downloaded: {0}".format(sub_blob_file.name))

    print("Target_root_folder: {0}".format(target_root_folder))
    # Download them in parallel, 30 at a time (so it's faster).
    with ThreadPoolExecutor(max_workers = 5) as executor:
        task_list: List[Future] = []
        for sub_blob_file in block_blob_service.list_blobs(container_name, prefix = folder_blob):
            # download_file(sub_blob_file)
            task_list.append(executor.submit(download_file, sub_blob_file))

    concurrent.futures.wait(task_list)

    print("Finished downloading files")
    sys.stdout.flush()

    # Get a new temp file and run the function
    output_dir = os.path.join(input_file_name, "output")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok = True)

    print("output_dir: " + output_dir)

    # Clean previous run
    output_file = list(filter(lambda f: f.startswith(target_root_folder.split("/")[-1]), os.listdir(output_dir)))
    if len(output_file) != 0:
        # We already have a file, delete it
        os.remove(os.path.join(output_dir, output_file[0]))

    print(psutil.virtual_memory())

    level1 = Level1(target_root_folder)
    level2 = Level2(outdir = output_dir,
                    # level2 filename determined from level1 name, if outdir is not provided it
                    # will go to the same folder as level1
                    fmt = "netcdf4",
                    datasets = default_datasets + ['SPM'])

    print("'running' polymer")

    # 0 for single threaded
    # > 1 for that many threads
    # -1 for all the threads on the machine.
    run_atm_corr(level1, level2, multiprocessing = -1)

    # The output file is the last thing in the path of the target_root_folder
    output_file = list(filter(lambda f: f.startswith(target_root_folder.split("/")[-1]), os.listdir(output_dir)))[0]

    print("Uploading to Blob storage as blob: {0} <-> {1}".format(output_dir, output_file))

    # Upload the created file, use local_file_name for the blob name
    block_blob_service.create_blob_from_path(container_name,
                                             blob_name = os.path.join("output", output_file),
                                             file_path = os.path.join(output_dir, output_file),
                                             validate_content = True)


def update_paths():
    import os
    import shutil
    import sys

    print(os.environ)

    cwd = os.getcwd()
    open("__init__.py", 'a').close()

    if os.path.exists("/tmp/raw"):
        print(os.listdir("/tmp/raw"))
        sys.stdout.flush()

    for folder in ["polymer", "build", "auxdata", "tools"]:
        if os.path.exists(os.path.join(cwd, folder)):
            shutil.rmtree(os.path.join(cwd, folder))
        shutil.copytree("/polymer-v4.9/" + folder, os.path.join(cwd, folder))

    sys.path.append(cwd)
    import polymer.main


def run_worker():
    update_paths()

    redis_conn = redis.StrictRedis(host = os.environ["SCHEDULER_HOST_PORT"], port = 6379, encoding = "utf-8")

    while True:
        folder_to_process = redis_conn.blpop("geo-queue", timeout = 0)
        logging.info("Got folder: {0}".format(folder_to_process))
        process_file(folder_to_process)


if __name__ == "__main__":
    run_worker()
