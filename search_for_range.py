import pandas as pd, math, boto3, sys, os, subprocess, pdb, time, requests
from subprocess import Popen
s3 = boto3.resource('s3')

def extract_data(json):
    records = []
    for item in json['items']:
        record = []
        record.append(str(item['html_url']))
        record.append(str(item['git_url']))
        record.append(str(item['repository']['full_name']))
        record.append(str(item['repository']['fork']))
        records.append(record)
    return records

def archive_data(Key, bucket, search_res):
    df =  pd.DataFrame(search_res, columns=['html', 'api', 'name', 'fork'])
    assert len(search_res) == df.shape[0]
    csv = df.to_csv()
    with open(Key, 'w') as f:
        f.write(csv)

    bucket.put_object(Key=Key, Body=csv)

def process_does_user_exist(r):
    if r.headers['status'] == "200 OK":
        print ">>> User exists!"
        return (True, True)
    if r.headers['status'] == '404 Not Found':
        print ">>> User doesn't exist!"
        return (False, True)
    raise Exception("Bad GitHub request!")

def process_search_request(r):
    json = r.json()
    if r.headers['status'] == '200 OK':
        print "    >>> 200! OK search results"
        return (extract_data(json), True)
    if 'API rate limit' in json['message']:
        print "    >>> search results: error msg: %s" % (json,)
        return (False, False)
    if 'Validation Failed' in json['message']:
        print "    >>> validation failed!"
        return ([], True)
        
def complete_request(request_str, user, token, fn):
    request_complete = False
    while not request_complete:
        r = requests.get(request_str, auth=(user, token))
        res, request_complete = fn(r)

        if 'x-ratelimit-remaining' in r.headers and int(r.headers['x-ratelimit-remaining']) == 0:
            reset_time = int(r.headers['x-ratelimit-reset'])
            time_to_wait = reset_time - time.time() + 3
            print ">>> No requests left... waiting %s secs until time %s " % (time_to_wait, reset_time)
            time.sleep(time_to_wait)

    return res

if __name__ == "__main__":
    _, user, token, authors_key = sys.argv
    out = authors_key + '.h5'
    s3.Bucket('ghscraping').download_file(authors_key, out)
    ul_prefix = authors_key + '/'
    store = pd.HDFStore(out, 'r')
    df = store['df']
    store.close()
    incomplete = []
    files = {}
    n = 0
    out_dir = './out'
    all_files = os.listdir(ul_prefix)
    if len(all_files) == 0:
        max_so_far = 0
    else:
        max_so_far = max(map(lambda x: int(x), all_files))
    # user : {{user}}, language:bash
    # to get:
    #     [items] -> html_url
    #     [items] -> git_url
    #     [items] -> repository -> 'id'
    #     [items] -> repository -> fork
    # OR
    # "message" == "API rate limit"
    bucket = s3.Bucket('ghfiles')
    search_res = []
    n = 1
    for i in xrange(max_so_far, df.shape[0]):
        if n % 10 == 0: #should be 900
            print "Archiving %s" % n
            archive_data(ul_prefix + str(n), bucket, search_res)
            del search_res[:]
        file_complete = False
        author =  df.iloc[i]
        exists_GET_string = 'https://github.com/%s' % author

        print author
        user_exists = complete_request(exists_GET_string, user, token, process_does_user_exist)
        if user_exists:
            search_GET_string = 'https://api.github.com/search/code?q=language:shell+user:%s' % author
            search_res += complete_request(search_GET_string, user, token, process_search_request)
        n += 1
        if n % 10 == 0:
            archive_data(ul_prefix + str(n), bucket, search_res)

    archive_data(ul_prefix + str(n), bucket, search_res)