import unittest
import pandas as pd

from idbt.client import DBTRPCClient


class TestDBTRPCClient(unittest.TestCase):
    def setUp(self):
        self.client = DBTRPCClient('http://localhost:8580')

    def test_list(self):
        sql = """
with aggs as (
  select
    channel_id,
    COUNT(DISTINCT video_id) as sum_video,
    SUM(view_count) as sum_view,
    SUM(like_count) as sum_like,
    SUM(favorite_count) as sum_favorite,
    SUM(comment_count) as sum_comment,

  from {{ ref('base_videos') }}

  group by channel_id
)

select
  c.title,
  c.subscriber_count,
  c.video_count,
  v.sum_video,
  c.view_count,
  v.sum_view,
  v.sum_like,
  v.sum_favorite,
  v.sum_comment,

from {{ ref('base_channels') }} c
left join aggs v using(channel_id)
order by channel_id
        """
        r = self.client.run(sql)
        rs = r.json()['result']['results'][0]['table']
        df = pd.DataFrame(rs['rows'], columns=rs['column_names'])
        print(df)
        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()
