import time
import redis

REDIS_RANKING = redis.StrictRedis(host="localhost", port=6305, db=0)


class Ranking:

    _h_player_info_key_pattern = 'ranking:play_info:{season}:{idx}'

    _s_temp_idx_key_pattern = 'ranking:ranking_temp_idx:{season}'

    _z_ranking_list_key_pattern = 'ranking:ranking_list:{season}'

    def __init__(self, season):
        self.season = season
        self._s_temp_idx_key = self._s_temp_idx_key_pattern.format(season=season)
        self._z_ranking_list_key = self._z_ranking_list_key_pattern.format(season=season)

    # 将玩家分数更新到hash
    def update_score(self, player_id, score):
        player_info_key = self.get_player_info_key()
        modified_score = self.modify_score_with_time(score)
        REDIS_RANKING.hset(player_info_key, player_id, modified_score)

    # 把hash更新到sset 同时建立新的hash
    def build_ranking_list(self):
        player_info_key = self.get_player_info_key()
        self.incr_current_temp_idx()
        player_info_dict = REDIS_RANKING.hgetall(player_info_key)

        pp = REDIS_RANKING.pipeline()
        for play_id, score in player_info_dict.items():
            pp.sadd(self._z_ranking_list_key, score, play_id)
        pp.delete(player_info_key)
        pp.execute()

    def get_ranking_list_around_player(self, player_id):
        res = []
        place = REDIS_RANKING.zrevrank(self._z_ranking_list_key, player_id)
        if place is not None:
            start = max(place - 10, 0)
            stop = place + 10
            res = REDIS_RANKING.zrevrange(self._z_ranking_list_key_pattern, start, stop, withscores=True)

        return res

    def get_player_info_key(self):
        idx = self.get_current_temp_idx()
        return self._h_player_info_key_pattern.format(season=self.season, idx=idx)

    def get_current_temp_idx(self):
        res = REDIS_RANKING.get(self._s_temp_idx_key)
        return int(res) if res else 0

    def incr_current_temp_idx(self):
        REDIS_RANKING.incr(self._s_temp_idx_key)

    # 取后10位存9999999999-时间戳
    @staticmethod
    def modify_score_with_time(score):
        now = int(time.time())
        digital_count = 10
        return score * (10 ** digital_count) + (10 ** digital_count - 1 - now)

