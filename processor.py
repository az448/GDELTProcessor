from collections import defaultdict
import sys
import multiprocessing
import os
import time

def map(file_path, num_mappers):
    # Divides the input file into chunks and processes tem in parallel using mappers
    file_size = os.path.getsize(file_path)
    chunk_size = file_size // num_mappers
    processes = []
    intermediate_outputs = multiprocessing.Queue()

    for i in range(num_mappers):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i < num_mappers - 1 else file_size
        #print(f"Starting mapper process {i} for bytes {start}-{end}")
        process = multiprocessing.Process(target = map_worker, args = (file_path, start, end, intermediate_outputs))
        processes.append(process)
        process.start()
    
    intermediate_data = []
    for i in range(num_mappers):
        #print(f"Waiting for mapper process {i} to finish and put data in queue...")
        item = intermediate_outputs.get()
        #print(f"Received item from mapper {i}: {len(item)} results")
        intermediate_data.extend(item)

    for i, process in enumerate(processes):
        process.join()
        #print(f"Mapper process {i} joined.")

    return intermediate_data

def map_worker(file_path, start, end, output_queue):
    # Workers for the map phase
    #print(f"Mapper worker started for bytes {start}-{end}")
    intermediate_results = defaultdict(int)
    try:
        with open(file_path, 'r') as f:
            #print(f"Mapper worker successfully opened file for chunk {start}-{end}")
            f.seek(start)
            chunk = f.read(end - start)
            #print(f"Mapper worker successfully read chunk of size {len(chunk)} for {start}-{end}")
            # words = re.findall(r'\b\w+\b', chunk.lower())
            words = chunk.lower().split()
            #print(f"Mapper worker found {len(words)} words in chunk {start}-{end}")
            for word in words:
                intermediate_results[word] += 1
        output_queue.put(list(intermediate_results.items()))
        #print(f"got here!!")
    except Exception as e:
        print(f"Mapper worker error on chunk {start}-{end}: {e}", file=sys.stderr)
        output_queue.put([])

def reduce(intermediate_data, num_reducers):
    # Reduces the intermediate data
    word_dict = defaultdict(list)
    for word, count in intermediate_data:
        word_dict[word].append(count)
    
    pool = multiprocessing.Pool(processes=num_reducers)
    final_counts = pool.map(reduce_worker, word_dict.items())
    pool.close()
    pool.join()

    #return dict(final_counts)
    final_word_counts = dict(final_counts)
    sorted_counts = sorted(final_word_counts.items(), key=lambda item: item[1], reverse=True)
    return sorted_counts

def reduce_worker(item):
    # Workers for the reduce phase
    word, counts = item
    return word, sum(counts)

if __name__ == '__main__':
    #print("Script started")
    if len(sys.argv) != 4:
        print("Usage: python3 processor.py <input_file> <num_mappers> <num_reducers>")
        sys.exit()

    input_file = sys.argv[1]
    num_mappers = int(sys.argv[2])
    num_reducers = int(sys.argv[3])

    start_time_map = time.time()
    print("Starting Map Phase...")
    intermediate_data = map(input_file, num_mappers)
    end_time_map = time.time()
    map_duration = end_time_map - start_time_map
    print("Map Phase Complete")

    start_time_reduce = time.time()
    print("Starting Reduce Phase...")
    final_word_counts = reduce(intermediate_data, num_reducers)
    end_time_reduce = time.time()
    reduce_duration = end_time_reduce - start_time_reduce
    print("Reduce Phase Complete")

    for word, count in final_word_counts:
        if count > 100000:
            print(f"{word}\t{count}")

    total_duration = map_duration + reduce_duration
    print(f"Map phase with {num_mappers} mappers complete in {map_duration:.2f} seconds.")
    print(f"Reduce phase with {num_reducers} reducers complete in {reduce_duration:.2f} seconds.")
    print(f"Total processing time: {total_duration:.2f} seconds.")