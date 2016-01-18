import pandas as pd, math
from subprocess import Popen

def get_df(path):
    store = pd.HDFStore(path, 'r')
    df = store['df']
    store.close()
    return df

if __name__ == "__main__":
    _, tokens_path, repos_list = sys.argv
    tokens = get_df(tokens_path)
    repos_list = get_df(repos_list)

    num_tokens = tokens.shape[0]
    num_repos = repos_list.shape[0]
    num_per_subgroup = math.ceil(num_repos/num_tokens)

    subgroups = [repos_list.iloc[i * num_per_subgroup:(i+1) * num_per_subgroup] for i in range(num_tokens)]
    for i in xrange(0, len(subgroups)):
        store = pd.HDFStore('staging/set_%s.h5' % i)
        store['df'] = subgroups[i]
        store.close()
    

    job_params = zip(tokens['user'].values, tokens['token'].values, subgroups)
    
