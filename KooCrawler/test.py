import api
import json
import datetime
import math

obj = api.API()


# res = obj.get_trending_feed(limit = 10000, offset = 0)

# res_json = json.loads(res)
# has_more_content = res_json['meta']['hasMoreContent']

def func(offset=0):
    res = obj.get_trending_feed(limit=1000, offset=offset)
    res_json = json.loads(res)
    has_more_content = res_json['meta']['hasMoreContent']
    next_offset = res_json['meta']['nextOffset']

    datetime1 = datetime.datetime.utcnow()
    d1 = str(datetime1).split(".")[0].replace("-", "").replace(":", "").replace(" ", "_")
    filename = str(d1)+"-tf.json"

    with open(filename, 'w', encoding='utf8') as fd:
        json.dump(json.loads(res)['feed'], fd, indent=4, ensure_ascii=False)

    if has_more_content:
        func(offset=next_offset)

def func_followers(handle, limit=10, offset=0):
    res = obj.get_user_by_handle(handle=handle)
    res_json = json.loads(res)
    followers_count = res_json['followerCount']
    user_id = res_json['id']
    # user_id = '7e948711-4711-46ce-8aba-75cf3c1c14a3'
    print(followers_count)
    print(user_id)

    with open(handle+".json", 'a', encoding='utf8') as fd:
        for i in range(math.floor(followers_count/limit)):
            res = obj.get_followers_by_id(user_id=user_id, limit = limit, offset=offset)
            offset=offset+limit
            json.dump(json.loads(res), fd, indent=4, ensure_ascii=False)

    # res = obj.get_followers_by_id(user_id=user_id, limit = limit, offset=offset)
    
    # with open(handle+".json", 'a', encoding='utf8') as fd:
    #     json.dump(json.loads(res), fd, indent=4, ensure_ascii=False)

    # if(offset < followers_count):
    #     func_followers(handle,limit,offset=offset+limit)


if __name__ == '__main__':
    func_followers('Tannu_9761')
    # print(obj.get_user_id_by_handle('virat.kohli'))