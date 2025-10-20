import os
import signal
import time
from multiprocessing import Process, Queue, Event
from typing import List, Dict, Optional

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import BulkWriteError


def load_env() -> None:
    # Load local env first if present, then global config
    for path in (".env_local", "config.env"):
        if os.path.exists(path):
            load_dotenv(path, override=True)


def create_client(uri_env: str) -> MongoClient:
    uri = os.getenv(uri_env)
    if not uri:
        raise RuntimeError(f"Missing env var: {uri_env}")
    return MongoClient(uri)


def reader(
    stop_evt: Event,
    out_queue: Queue,
    cosmos_uri: str,
    db_name: str,
    coll_name: str,
    batch_size: int,
    projection: Optional[Dict] = None,
) -> None:
    """Single-process reader: streams batches from Cosmos DB into a multiprocessing queue."""
    try:
        cosmos = MongoClient(cosmos_uri)
        coll = cosmos[db_name][coll_name]
        cursor = coll.find({}, projection).sort("_id", 1).batch_size(batch_size)

        batch: List[Dict] = []
        for doc in cursor:
            if stop_evt.is_set():
                break
            batch.append(doc)
            if len(batch) >= batch_size:
                out_queue.put(batch)
                batch = []

        if batch:
            out_queue.put(batch)
    finally:
        # Signal completion to consumers
        # Put one sentinel per writer to ensure all exit
        writers = int(os.getenv("SIMPLE_PROCESSES", "20"))
        for _ in range(writers):
            out_queue.put(None)


def writer(
    stop_evt: Event,
    in_queue: Queue,
    atlas_uri: str,
    db_name: str,
    coll_name: str,
) -> None:
    """Writer process: consumes batches from the queue and inserts into Atlas."""
    client = MongoClient(atlas_uri)
    coll = client[db_name][coll_name]

    while not stop_evt.is_set():
        batch = in_queue.get()
        if batch is None:
            break
        if not batch:
            continue
        try:
            coll.insert_many(batch, ordered=False)
        except BulkWriteError:
            # Ignore duplicates or transient errors; continue
            continue
        except Exception:
            # Non-fatal; continue to next batch
            continue


def main() -> None:
    load_env()

    # Configuration (env-driven)
    batch_size = int(os.getenv("SIMPLE_BATCH_SIZE", "1000"))
    num_writers = int(os.getenv("SIMPLE_PROCESSES", "8"))
    queue_size = int(os.getenv("SIMPLE_QUEUE_SIZE", "100"))

    # Get actual connection strings and names
    cosmos_uri = os.getenv("COSMOS_DB_CONNECTION_STRING")
    atlas_uri = os.getenv("MONGODB_ATLAS_CONNECTION_STRING")
    db_name = os.getenv("COSMOS_DB_NAME")
    coll_name = os.getenv("COSMOS_DB_COLLECTION")

    if not all([cosmos_uri, atlas_uri, db_name, coll_name]):
        raise RuntimeError("Missing required environment variables")

    # CRITICAL: Set environment variables for child processes
    # Multiprocessing doesn't inherit dotenv-loaded variables
    os.environ["COSMOS_DB_CONNECTION_STRING"] = cosmos_uri
    os.environ["MONGODB_ATLAS_CONNECTION_STRING"] = atlas_uri
    os.environ["COSMOS_DB_NAME"] = db_name
    os.environ["COSMOS_DB_COLLECTION"] = coll_name

    print(f"🚀 Starting simple migration:")
    print(f"   📊 Batch size: {batch_size}")
    print(f"   📊 Writers: {num_writers}")
    print(f"   📊 Queue size: {queue_size}")
    print(f"   📊 DB: {db_name}")
    print(f"   📊 Collection: {coll_name}")

    # Shared queue and stop event
    stop_evt = Event()
    q: Queue = Queue(maxsize=queue_size)

    # Graceful shutdown
    def handle_sigint(signum, frame):
        stop_evt.set()

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    # Start reader (main process) and writers (child processes)
    writers: List[Process] = []
    for _ in range(num_writers):
        p = Process(target=writer, args=(stop_evt, q, atlas_uri, db_name, coll_name))
        p.daemon = True
        p.start()
        writers.append(p)

    # Run reader in main process to keep cursor single-threaded
    reader(stop_evt, q, cosmos_uri, db_name, coll_name, batch_size)

    # Wait for writers
    for p in writers:
        p.join()


if __name__ == "__main__":
    main()


