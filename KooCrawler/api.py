import requests # https://docs.python-requests.org/en/latest/api/
import json

class API:
    def get_trending_hashtags(self, limit, offset):
        url = 'https://www.kooapp.com/apiV1/ku/explore'
        payload = {
            'limit': limit,
            'offset': offset,
            'categories': 'trending-tags'
        }
        return requests.get(url=url, params=payload).text

    def get_feed(self, limit, offset):
        url = 'https://www.kooapp.com/apiV1/consumer_feed'
        payload = {
            'limit': limit,
            'offset': offset,
            'isBulkFollow': 'true',
            'mixedReactions': 'true',
            'showSampleFeed': 'true',
            'showPoll': 'true'
        }
        return requests.get(url, payload).text

    def get_feed_by_hashtag(self, limit, offset, hashtag):
        url = 'https://www.kooapp.com/apiV1/consumer_feed'
        payload = {
            'limit': limit,
            'offset': offset,
            'hashTag': hashtag
        }
        return requests.get(url=url, params=payload).text

    def get_trending_feed(self, limit, offset):
        url = 'https://www.kooapp.com/apiV1/consumer_feed/explore'
        payload = {
            'limit': limit,
            'offset': offset
        }
        return requests.get(url=url, params=payload).text

    def get_new_feed(self, limit, offset):
        url = 'https://www.kooapp.com/apiV1/consumer_feed/moderation'
        payload = {
            'limit': limit,
            'offset': offset,
            'feedFetchType': 0
        }
        return requests.get(url=url, params=payload).text

    def get_exclusive_feed(self, limit, offset):
        url = 'https://www.kooapp.com/apiV1/consumer_feed'
        payload = {
            'limit': limit,
            'offset': offset,
            'feedType': 'exclusive',
            'isBulkFollow': 'true',
            'mixedReactions': 'true',
            'showPoll': 'true',
            'showBanner': 'true',
            'showCeleb': 'true',
            'showSampleFeed': 'true',
            'showTimeline': 'true',
            'showHashTag': 'true',
            'showGif': 'true',
            'showTrendingKooButton': 'true',
            'showTrendingKoo': 'true',
            'showInActiveKooers': 'true',
            'showProfessionCarousel': 'true',
            'showPlainKoo': 'false',
            'feedFetchType': 2,
            'feedAlgoType': 1,
            'isFirstCall': 'false'
        }
        return requests.get(url=url, params=payload).text

    def get_user_by_handle(self, handle):
        url = 'https://www.kooapp.com/apiV1/users/handle/' + handle
        return requests.get(url=url).text

    def get_koo_by_user_id(self, user_id, limit, offset):
        url = 'https://www.kooapp.com/apiV1/users/created/ku/' + user_id
        payload = {
            'limit': limit,
            'offset': offset,
            'showPoll': 'true',
            'showMultiLangKoo': 'true'
        }   
        return requests.get(url=url, params=payload).text

    def get_followers_by_id(self, user_id, limit, offset):
        url = 'https://www.kooapp.com/apiV1/users/' + user_id + '/followers'
        payload = {
            'limit': limit,
            'offset': offset
        }
        return requests.get(url=url, params=payload).text
    
    def get_following_by_id(self, user_id, limit, offset):
        url = 'https://www.kooapp.com/apiV1/users/' + user_id + '/following'
        payload = {
            'limit': limit,
            'offset': offset
        }
        return requests.get(url=url, params=payload).text

    def get_user_id_by_handle(self, handle):
        req = json.loads(self.get_user_by_handle(handle))
        return req['id']

    def test(self):
        print('Working!')


if __name__ == '__main__':
    api = API()
    # print(api.get_followers_by_id(user_id='1de93793-7db9-44cf-9842-a6ea5bd22a2e', limit = 5, offset=0))
    print(api.get_feed(2,0))
