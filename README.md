# GDELTProcessor

Instructions to Run:

python3 processor.py <input_file> <num_mappers> <num_reducers>

For example: % python3 processor.py 1979Data.txt 8 4

On the dataset provided in this respository, the processor should complete in less than a second. However, this processor is built to handle much larger files than can be uploaded to GitHub. The larger files that I tested with can be downloaded from the GDELT Project (https://www.gdeltproject.org/data.html#rawdatafiles). Once you find the optimal number of mappers and reducers to use based on your machine, processing the larger data files should take 20-25 seconds. The output will be a list of the most frequently occurring words in the dataset and a report on the amount of time it took to process.
