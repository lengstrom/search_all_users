import pandas as pd, math, boto3
from subprocess import Popen
s3 = boto3.resource('s3')


def get_df(path):
    store = pd.HDFStore(path, 'r')
    df = store['df']
    store.close()
    return df

if __name__ == "__main__":
    _, tokens_path, repos_list, aws_ul = sys.argv
    tokens = get_df(tokens_path)
    repos_list = get_df(repos_list)

    num_tokens = tokens.shape[0]
    num_repos = repos_list.shape[0]
    num_per_subgroup = math.ceil(num_repos/num_tokens)

    subgroups = [repos_list.iloc[i * num_per_subgroup:(i+1) * num_per_subgroup] for i in range(num_tokens)]

    # s3
    bucket = s3.Bucket('ghscraping')
    keys = []
    for i in xrange(0, len(subgroups)):
        store_path = 'staging/set_%s.h5' % i
        store = pd.HDFStore(store_path)
        store['df'] = subgroups[i]
        store.close(store_path)
        if aws_ul == "1":
            data = open(store_path, 'rb')
            key = "repos_" + str(i)
            keys.append(key)
            bucket.put_object(key=(key), Body=data)

    job_params = zip(tokens['user'].values, tokens['token'].values, keys)
    print job_params
