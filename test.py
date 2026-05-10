from utils import is_short_video_channel

# bool=is_short_video_channel('UC1gou-jb2Ax0z16MFEAUBlQ')
# print(bool)


import pandas as pd

df = pd.read_csv('weekly_report_20260426.csv', encoding='utf-8-sig')

# 保留两位小数（互动率改为百分比格式）
df['长视频平均播放数'] = df['长视频平均播放数'].round(0).astype(int)
df['长视频互动率'] = (df['长视频互动率'] * 100).round(2).astype(str) + '%'
df['更新频率(1/天)'] = df['更新频率(1/天)'].round(2)

df.to_csv('weekly_report_cleaned2.csv', index=False, encoding='utf-8-sig')