import concurrent
import os
import pickle
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, Future
from multiprocessing.dummy import Process
from queue import Queue
from typing import List
import logging
import redis
from azure.storage.blob import BlockBlobService, Blob
from pynotstdlib.logging import default_logging
import shutil
import psutil
import time

from polymer.main import run_atm_corr, Level1, Level2
from polymer.level2 import default_datasets

container_name = "myblobs"
account_name = "jgoddingblob"
account_key = "rzfvY+HXTZGKJs5MMon3PHHKKtdBL9eZnKBw3tuav62xczvPNpJjp2D2Qg/bSyucTdVwNYbSu1veAF293ufWWQ=="

default_logging(logging.ERROR)

download_file_queue = Queue(maxsize = 1)
run_polymer_queue = Queue(maxsize = 1)
upload_output_queue = Queue(maxsize = 1)


def get_blob_service():
    block_blob_service = BlockBlobService(account_name = account_name, account_key = account_key)
    return block_blob_service


def create_output_dirs(folder_blob):
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
        shutil.rmtree(target_root_folder)
        os.makedirs(target_root_folder, exist_ok = True)

    return input_file_name, target_root_folder


def grab_file(folder_blob, block_blob_service):
    input_file_name, target_root_folder = create_output_dirs(folder_blob)

    def download_file(sub_blob_file: Blob):
        #print(psutil.disk_usage("/"))
        #print("Downloading: {0}".format(sub_blob_file.name))

        target_output_file = os.path.join(input_file_name, sub_blob_file.name)

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

        #print("Downloaded: {0}".format(sub_blob_file.name))

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

    return target_root_folder, output_dir


def run_polymer(target_root_folder, output_dir, next_folder, block_blob_service):
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
    try:
        run_atm_corr(level1, level2, multiprocessing = 0)
    except FileNotFoundError:
        grab_file(folder_blob = next_folder, block_blob_service = block_blob_service)
        run_atm_corr(level1, level2, multiprocessing = 0)
    except OSError:
        grab_file(folder_blob = next_folder, block_blob_service = block_blob_service)
        run_atm_corr(level1, level2, multiprocessing = 0)

    # The output file is the last thing in the path of the target_root_folder
    output_file = list(filter(lambda f: f.startswith(target_root_folder.split("/")[-1]), os.listdir(output_dir)))[0]
    return output_dir, output_file


def upload_output(output_dir, output_file, block_blob_service, target_root_folder, next_folder):
    print("Uploading to Blob storage as blob: {0} <-> {1}".format(output_dir, output_file))

    upload_file = os.path.join(output_dir, output_file)

    if not os.path.exists(upload_file):
        run_polymer(target_root_folder, output_dir, next_folder, block_blob_service)

    # Upload the created file, use local_file_name for the blob name
    block_blob_service.create_blob_from_path(container_name,
                                             blob_name = os.path.join("output", output_file),
                                             file_path = os.path.join(output_dir, output_file),
                                             validate_content = True)

    # TODO: Cleanup the directories after.

    # remove output
    os.remove(os.path.join(output_dir, output_file))
    shutil.rmtree(target_root_folder)


# def update_paths():
#     import os
#     import shutil
#     import sys
#
#     print(os.environ)
#
#     cwd = os.getcwd()
#     open("__init__.py", 'a').close()
#
#     if os.path.exists("/tmp/raw"):
#         print(os.listdir("/tmp/raw"))
#         sys.stdout.flush()
#
#     for folder in ["polymer", "build", "auxdata", "tools"]:
#         if os.path.exists(os.path.join(cwd, folder)):
#             shutil.rmtree(os.path.join(cwd, folder))
#         shutil.copytree("/polymer-v4.9/" + folder, os.path.join(cwd, folder))
#
#     sys.path.append(cwd)


def download_file_process():
    block_blob_service = get_blob_service()

    while True:
        print("Download Qsize: {0}".format(download_file_queue.qsize()))
        next_folder = download_file_queue.get(block = True, timeout = 1)
        if next_folder is None:
            continue

        target_root_folder, output_dir = grab_file(folder_blob = next_folder, block_blob_service = block_blob_service)
        print("Run Polymer Qsize: {0}".format(run_polymer_queue.qsize()))
        run_polymer_queue.put((target_root_folder, output_dir, next_folder), block = True, timeout = None)

        # with open("/download_queue.pkl", "w") as f:
            # pickle.dump(list(download_file_queue.queue), f)


def process_polymer_process():
    block_blob_service = get_blob_service()

    while True:
        next_folder = run_polymer_queue.get(block = True, timeout = 1)

        if next_folder is None:
            continue

        target_root_folder, output_dir, next_folder = next_folder
        output_dir, output_file = run_polymer(target_root_folder, output_dir, next_folder, block_blob_service)
        upload_output_queue.put((output_dir, output_file, target_root_folder, next_folder), block = True, timeout = None)

        # Save run_polymer_queue
        # with open("/run_polymer_queue.pkl", "w") as f:
        #     pickle.dump(list(run_polymer_queue.queue), f)


def upload_file_process():
    block_blob_service = get_blob_service()
    redis_conn = redis.StrictRedis(host = os.environ["SCHEDULER_SERVICE_HOST"], port = 6379, encoding = "utf-8")

    while True:
        next_file = upload_output_queue.get(block = True, timeout = 1)

        if next_file is None:
            continue

        output_dir, output_file, target_root_folder, next_folder = next_file
        upload_output(output_dir, output_file, block_blob_service, target_root_folder, next_folder)
        # with open("/upload_queue.pkl", "w") as f:
        #     pickle.dump(list(upload_output_queue.queue), f)

        redis_conn.rpush("geo-queue-done", next_folder)


def run_worker():
    #update_paths()

    redis_conn = redis.StrictRedis(host = os.environ["SCHEDULER_SERVICE_HOST"], port = 6379, encoding = "utf-8")

    download_thread = Process(target = download_file_process)
    polymer_thread = Process(target = process_polymer_process)
    upload_thread = Process(target = upload_file_process)

    download_thread.start()
    polymer_thread.start()
    upload_thread.start()

    # Read from pickle files
    if os.path.exists("/download_queue.pkl"):
        with open("/download_queue.pkl", "r") as f:
            old_data = pickle.load(f)
            for o in old_data:
                download_file_queue.put(o)

    if os.path.exists("/run_polymer_queue.pkl"):
        with open("/run_polymer_queue.pkl", "r") as f:
            old_data = pickle.load(f)
            for o in old_data:
                run_polymer_queue.put(o)

    if os.path.exists("/upload_queue.pkl"):
        with open("/upload_queue.pkl", "r") as f:
            old_data = pickle.load(f)
            for o in old_data:
                upload_output_queue.put(o)

    while True:
        next_folder = redis_conn.blpop("geo-queue", timeout = 1)

        if next_folder is None:
            continue

        queue_name, folder_to_process_raw = next_folder
        folder_to_process = folder_to_process_raw.decode("utf-8")
        logging.info("Got folder: {0}".format(folder_to_process))
        print("Putting in Download: {0}".format(download_file_queue.qsize()))
        download_file_queue.put(folder_to_process, block = True, timeout = None)


if __name__ == "__main__":
    run_worker()
