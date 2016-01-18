import pandas as pd
df = pd.read_csv('tokens', sep=' ', header=None, names=["user", "token"])
store = pd.HDFStore('tokens.h5')
store['df'] = df
