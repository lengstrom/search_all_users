import pandas as pd, math, boto3, sys, os, subprocess, pdb
from subprocess import Popen
s3 = boto3.resource('s3')

def get_df(path):
    store = pd.HDFStore(path, 'r')
    df = store['df']
    store.close()
    return df

if __name__ == "__main__":
    _, tokens_path, author_list_path, aws_ul = sys.argv
    tokens = get_df(tokens_path)
    df = get_df(author_list_path)
    author_list = df[df['fork'] == 0]['author'].drop_duplicates()
    print "author_list Shape: %s" % author_list.shape

    num_tokens = tokens.shape[0]
    num_authors = author_list.shape[0]
    num_per_subgroup = int(math.ceil(float(num_authors)/num_tokens))

    subgroups = [author_list.iloc[i * num_per_subgroup:(i+1) * num_per_subgroup] for i in range(num_tokens)]

    # s3
    bucket = s3.Bucket('ghscraping')
    keys = []
    for i in xrange(0, len(subgroups)):
        store_path = 'staging/set_%s.h5' % i
        if os.path.exists(store_path):
            os.remove(store_path)
        store = pd.HDFStore(store_path)
        store['df'] = subgroups[i]
        print "%s shape : %s" % (i, subgroups[i].shape)
        store.close()
        if aws_ul == "1":
            data = open(store_path, 'rb')
            key = "authors_" + str(i)
            keys.append(key)
            bucket.put_object(Key=(key), Body=data)

    job_params = zip(tokens['user'].values, tokens['token'].values, keys)
