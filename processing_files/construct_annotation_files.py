import os
from argparse import ArgumentParser, Namespace

data_path = ""
parser = ArgumentParser()
parser.add_argument('--base_path', type=str, help='Directory where training/evaluation dataset is present.', required=True)
parser.add_argument('--dataset_name', type=str, help='dataset json name', required=True)
parser.add_argument('--dataset_start_index', type=int, help='non zero integer', required=True)
parser.add_argument('--dataset_end_index', type=int, help='non zero integer',  required=True)
def create_annotation_files(data_path, dataset_name, dataset_start_index, dataset_end_index):
    import time
    t = int(time.time())
    print(f"save final dataset to dir started. time {t}")
    os.chdir(data_path)
    import json
    dataset = {}
    with open(f"{data_path}/{dataset_name}", "r") as rds:
        dataset = json.load(rds)

    filtered_ds = []
    #ct = 1
    for i in dataset:
        #if (i['index']>=dataset_start_index) and (i['index']<=dataset_end_index) and (i['uri'] not in file_names_prefixes):
        if (i['index']>=dataset_start_index) and (i['index']<=dataset_end_index):
            filtered_ds.append(i)
    if len(filtered_ds) == 0:
        print("nothing to do, returning")
        return 0
    import multiprocessing
    num_workers = multiprocessing.cpu_count()  # Use all available CPU cores
    pool = multiprocessing.Pool(processes=num_workers)

    # Use the pool to apply the job processing function to the list of jobs
    results = pool.map(process_each_question, filtered_ds)
    # Close the pool to release resources
    pool.close()
    pool.join()
    # for result in results:
    #     uri = result['uri']
    #     payload = result['txt']
    #     with open(f"{data_path}/{uri}.txt", "w") as txt:
    #         txt.write(payload)
    #     with open(f"{data_path}/{uri}.ann", "w") as txt:
    #         txt.write("")
    print(f"done creating {len(results)} files")
    print(f"creation of annotation ended. time {int(time.time())}")

def process_each_question(question):

    uri = question['uri']
    print(f"here {uri}")
    payload={}
    txt = []
    #txt.append(f"index: {question['index']}")
    txt.append(f"uri: {uri}")
    txt.append(f"question: {question['subject']}")
    content =  question['content'] if 'content' in question else ""
    txt.append(f"context: {content}")
    answers = question["nbestanswers"]
    for j in range(len(answers)):
        answer = answers[j].replace("\n", "")
        txt.append(f"answer_{j}: {answer}")
    payload['uri'] = uri
    payload['txt'] = "\n".join(txt)
    data_path = os.getcwd()
    with open(f"{data_path}/{uri}.txt", "w") as txt:
        txt.write(payload['txt'])
    with open(f"{data_path}/{uri}.ann", "w") as txt:
        txt.write("")
    return payload

data_path = ""
if __name__ == '__main__':
    args, _ = parser.parse_known_args()
    data_path = args.base_path
    dataset_name = args.dataset_name
    dataset_start_index = args.dataset_start_index
    dataset_end_index = args.dataset_end_index
    if data_path and dataset_name and dataset_start_index and dataset_end_index and (dataset_start_index<dataset_end_index):
        create_annotation_files(data_path, dataset_name, dataset_start_index, dataset_end_index)
    else:
        raise ValueError(f"errors with the args passed to the funnction args : {args}")