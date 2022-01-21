import pandas as pd

#read file
raw_data=pd.read_csv('./raw_data/video_data.csv')
raw_df=pd.DataFrame(raw_data[0:10000])

raw_df.to_csv('./raw_data/video_data2.csv',index=False)