#include "match.h"
#include "trade.h"
#include <memory>
#include <string.h>  
#include <iostream>
#include "utils.h"
#include "redis_utils.h"
using namespace std;

string switch_f_to_s(long double f){
	char st[1024];
	snprintf(st, sizeof(st), "%.*Lf", VALUE_DECIMAL, f);
	string ans = st;
	return ans;
}

long double switch_s_to_f(string s){
	return strtold(s.c_str(), NULL);
}

bool CheckDecimal(long double f, int decimal){
	char st[1024];
	snprintf(st, sizeof(st), "%.*Lf", decimal, f);
	long double _f = strtold(st, NULL);
	if (_f - f < -EPS || _f - f > EPS){
		return false;
	}else{
		return true;
	}
}

bool Match::DiffPrice(long double p1, long double p2){
	if (p2 < EPS){
		return false;
	}else{
		long double diff = p1 / p2 - 1;
		if (diff > m_order_price_limit_high + EPS || diff < m_order_price_limit_low - EPS){
			return false;
		}else{
			return true;
		}
	}
}

Match::Match(Trade* trade) {
	m_trade = trade;
	Init();
}

Match::~Match() {
}

void Match::PreLock() {
	string str = "lock_account_pre";
	lock_key_all.insert(str);
	LOG(INFO) << " lock " << str;
	Lock(str);
	return;
}

void Match::AccountLock(int user_id) {
	char tmp[1024];
	snprintf(tmp, sizeof(tmp), "lock_account_%d", user_id);
	string str = tmp;
	lock_key_all.insert(str);
	LOG(INFO) << " lock " << str;
	Lock(str);
	return;
}

void Match::PreUnlock() {
	string str = "lock_account_pre";
	Unlock(str);
	lock_key_all.erase(str);
	LOG(INFO) << " unlock " << str;
	return;
}

void Match::AccountUnlock(int user_id) {
	char tmp[1024];
	snprintf(tmp, sizeof(tmp), "lock_account_%d", user_id);
	string str = tmp;
	Unlock(str);
	lock_key_all.erase(str);
	LOG(INFO) << " unlock " << str;
	return;
}

void Match::AllUnlock() {
	for (set<string>::iterator it = lock_key_all.begin(); it != lock_key_all.end(); it++){
		string str = *it;
		Unlock(str);
	}
}

bool Match::Init() {
	Config config;

	{
		const Json::Value redis_config = config["redis"];
		const Json::Value sentinel_config = redis_config["sentinels"];
		std::vector<std::pair<std::string, int> > sentinels;
		for (unsigned i = 0; i < sentinel_config.size(); ++i) {
			sentinels.push_back(std::make_pair(sentinel_config[i]["host"].asString(), sentinel_config[i]["port"].asInt()));
		}
		std::string encode_password = redis_config["password"].asString();
		//std::string password = real_password(encode_password);
		std::string password = encode_password;

		//m_redis = SentinelRedisConnect(sentinels, redis_config["master_name"].asCString(), password.c_str(), redis_config["database"].asInt());
		for (auto it = sentinels.begin(); it != sentinels.end(); ++it) {
            //m_redis = redisConnect(it->first.c_str(), it->second) ; 
            m_redis = redis_connect(it->first.c_str(), it->second, password.c_str()) ;          
        }

        if (m_redis == NULL) {
            LOG(ERROR) << "m_redis connect faild";
            exit(1);
        }else{
            //std::cout << "m_redis connect ok" << std::endl;
            LOG(INFO) << "m_redis connect ok";
        }
	}
	
	{
		const Json::Value redis_config = config["statRedis"];
		const Json::Value sentinel_config = redis_config["sentinels"];
		std::vector<std::pair<std::string, int> > sentinels;
		for (unsigned i = 0; i < sentinel_config.size(); ++i) {
			sentinels.push_back(std::make_pair(sentinel_config[i]["host"].asString(), sentinel_config[i]["port"].asInt()));
		}
		std::string encode_password = redis_config["password"].asString();
		//std::string password = real_password(encode_password);
		std::string password = encode_password;
		
		//m_stat_redis = SentinelRedisConnect(sentinels, redis_config["master_name"].asCString(), password.c_str(), redis_config["database"].asInt());
        for (auto it = sentinels.begin(); it != sentinels.end(); ++it) {
            //m_stat_redis = redisConnect(it->first.c_str(), it->second) ;
        	m_stat_redis = redis_connect(it->first.c_str(), it->second, password.c_str()) ; 
        }

        if (m_stat_redis == NULL) {
            LOG(ERROR) << "m_stat_redis connect faild";
            exit(1);
        }else{
            //std::cout << "m_stat_redis connect ok" << std::endl;
            LOG(INFO) << "m_stat_redis connect ok";
        }
	}

	{
		const Json::Value redis_config = config["indexRedis"];
		const Json::Value sentinel_config = redis_config["sentinels"];
		std::vector<std::pair<std::string, int> > sentinels;
		for (unsigned i = 0; i < sentinel_config.size(); ++i) {
			sentinels.push_back(std::make_pair(sentinel_config[i]["host"].asString(), sentinel_config[i]["port"].asInt()));
		}
		std::string encode_password = redis_config["password"].asString();
		//std::string password = real_password(encode_password);
		std::string password = encode_password;

		//m_index_redis = SentinelRedisConnect(sentinels, redis_config["master_name"].asCString(), password.c_str(), redis_config["database"].asInt());
        for (auto it = sentinels.begin(); it != sentinels.end(); ++it) {
            //m_index_redis = redisConnect(it->first.c_str(), it->second) ; 
            m_index_redis = redis_connect(it->first.c_str(), it->second, password.c_str()) ;          
        }

        if (m_index_redis == NULL) {
            LOG(ERROR) << "m_index_redis connect faild";
            exit(1);
        }else{
            //std::cout << "m_index_redis connect ok" << std::endl;
            LOG(INFO) << "m_index_redis connect ok";
        }
	}
	
	LOG(INFO) << "redis ok";

	m_usd_cny_value = 6.5L;
	m_usd_cny_timestamp = 0;
    return true;
}

bool Match::Reposition(){
	m_redis_cmd_list.clear();
	m_trade_list.resize(0);
	m_order_result_list.resize(0);
	m_trade_users_set.clear();
	m_trade_users_list.clear();
	Json::Value m_user_account = Json::Value::null;
	m_time_now = time(0);
	lock_key_all.clear();
	m_executed_quote_amount = 0L;
	m_order_user_id = -1;
	m_statistics = Json::Value::null;
	m_spot_price = 0;
	
	return true;
}

bool Match::InitOrder(string order_str){
	Json::CharReaderBuilder rbuilder;
	std::unique_ptr<Json::CharReader> const reader(rbuilder.newCharReader());
	JSONCPP_STRING error;
	
	Json::Value order_json = Json::Value::null;
	bool ret = reader->parse(order_str.c_str(), order_str.c_str() + order_str.size(), &order_json, &error);
	
	if (!(ret && error.size() == 0)) {
		LOG(ERROR) << "json error";
		return false;
	}
	
	if (!order_json.isMember("msg_type")){
		LOG(ERROR) << "json no msg_type";
		return false;
	}
	
	m_msg_type = order_json["msg_type"].asString();
	if (m_msg_type == "order"){
		if (!order_json.isMember("order_id")){
			LOG(ERROR) << "json no order_id";
			return false;
		}
		if (!order_json.isMember("user_id")){
			LOG(ERROR) << "json no user_id";
			return false;
		}
		if (!order_json.isMember("base_asset")){
			LOG(ERROR) << "json no base_asset";
			return false;
		}
		if (!order_json.isMember("quote_asset")){
			LOG(ERROR) << "json no quote_asset";
			return false;
		}
		if (!order_json.isMember("order_type")){
			LOG(ERROR) << "json no order_type";
			return false;
		}
		if (!order_json.isMember("order_op")){
			LOG(ERROR) << "json no order_op";
			return false;
		}
		if (!order_json.isMember("origin_amount")){
			LOG(ERROR) << "json no origin_amount";
			return false;
		}
		if (!order_json.isMember("ip")){
			LOG(ERROR) << "json no ip";
			return false;
		}
		if (!order_json.isMember("source")){
			LOG(ERROR) << "json no source";
			return false;
		}
		m_order_id = order_json["order_id"].asString();
		m_order_user_id = order_json["user_id"].asInt();
		m_order_base_asset = order_json["base_asset"].asString();
		m_order_quote_asset = order_json["quote_asset"].asString();
		m_order_type = order_json["order_type"].asInt();
		m_order_op = order_json["order_op"].asInt();
		m_order_amt = strtold(order_json["origin_amount"].asString().c_str(), NULL);
		m_order_amt_str = switch_f_to_s(m_order_amt);
		m_order_ip = order_json["ip"].asString();
		m_order_source = order_json["source"].asInt();
		
		if (m_order_type == ORDER_TYPE_LIMIT) {
			if (!order_json.isMember("price")){
				LOG(ERROR) << "json no price";
				return false;
			}
			m_order_price = strtold(order_json["price"].asString().c_str(), NULL);
			m_order_price_str = switch_f_to_s(m_order_price);
			if (m_order_price <= EPS){
				LOG(ERROR) << "m_order_id:" << m_order_id << " m_order_price error";
				return false;
			}
		} else if (m_order_type == ORDER_TYPE_MARKET) {
			m_order_price = 0;
			m_order_price_str = switch_f_to_s(m_order_price);
		} else {
			LOG(ERROR) << "m_order_id:" << m_order_id << " m_order_type error";
			return false;
		}
		if (m_order_op != ORDER_SIDE_BUY && m_order_op != ORDER_SIDE_SELL){
			LOG(ERROR) << "m_order_id:" << m_order_id << " m_order_op error";
			return false;
		}
		if (m_order_amt <= EPS){
			LOG(ERROR) << "m_order_id:" << m_order_id << " m_order_amt error";
			return false;
		}
		if (GetOrderInfo()){
			LOG(INFO) << "m_order_id:" << m_order_id << " order repeat";
			return false;
		}
		if (!m_redis) {
			return false;
		}
	}else if (m_msg_type == "cancel"){
		if (!order_json.isMember("order_id")){
			LOG(ERROR) << "json no order_id";
			return false;
		}
		if (!order_json.isMember("user_id")){
			LOG(ERROR) << "json no user_id";
			return false;
		}
		m_order_id = order_json["order_id"].asString();
		m_order_user_id = order_json["user_id"].asInt();
		if (!GetOrderInfo()){
			LOG(INFO) << "m_order_id:" << m_order_id << " no this order";
			return false;
		}
		if (!m_redis) {
			return false;
		}
	}else{
		return false;
	}
	
	m_statistics["trade"]["base_asset"] = m_order_base_asset;
	m_statistics["trade"]["quote_asset"] = m_order_quote_asset;
	m_statistics["trade"]["list"].resize(0);
	
	m_statistics["order_book"]["base_asset"] = m_order_base_asset;
	m_statistics["order_book"]["quote_asset"] = m_order_quote_asset;
	m_statistics["order_book"]["buy"].resize(0);
	m_statistics["order_book"]["sell"].resize(0);
	
	m_trade_list.resize(0);
	
	return true;
}

bool Match::PrepareForOrder(){
	//查询挂单用户余额
	LOG(INFO) << "m_order_id:" << m_order_id << " CheckAmount start";
	
	AccountLock(m_order_user_id);
	
	Json::CharReaderBuilder rbuilder;
	std::unique_ptr<Json::CharReader> const reader(rbuilder.newCharReader());
	JSONCPP_STRING error;

	if (m_time_now - m_usd_cny_timestamp > 60){
		redisReply* reply = NULL;
	
		reply = (redisReply*) redisCommand(m_stat_redis, "GET RATE_USD_CNY");
		if (reply == NULL){
			LOG(ERROR) << "redis reply null";
			redisFree(m_stat_redis);
			m_stat_redis = NULL;
			return false;
		}
		if (reply->type != REDIS_REPLY_STRING && reply->type != REDIS_REPLY_NIL){
			LOG(ERROR) << " redis type error:" << reply->type;
			freeReplyObject(reply);
			return false;
		}
		if (reply->type == REDIS_REPLY_STRING){
			string tmp_str = reply->str;
			m_usd_cny_value = switch_s_to_f(tmp_str);
		}
		m_usd_cny_timestamp = m_time_now;
		freeReplyObject(reply);

		reply = (redisReply*) redisCommand(m_stat_redis, "HMGET global_match_config order_price_limit_high order_price_limit_low");
		if (reply == NULL){
			LOG(ERROR) << "redis reply null";
			redisFree(m_stat_redis);
			m_stat_redis = NULL;
			return false;
		}
		if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 2){
			if (reply->element[0]->type == REDIS_REPLY_STRING){
				m_order_price_limit_high = strtold(reply->element[0]->str, NULL);
			}
			if (reply->element[1]->type == REDIS_REPLY_STRING){
				m_order_price_limit_low = strtold(reply->element[1]->str, NULL);
			}
		}
		freeReplyObject(reply);
	}

	//查询现货指数价格
	LOG(INFO) << "m_order_id:" << m_order_id << " query limit price start";

	redisReply* reply = NULL;
	redisAppendCommand(m_index_redis, "LRANGE trade_binance_%s_%s 0 0", m_order_base_asset.c_str(), m_order_quote_asset.c_str());
	redisGetReply(m_index_redis, (void**)&reply);
	if (reply == NULL) {
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
		redisFree(m_index_redis);
		m_index_redis = NULL;
		return false;
	}

	m_spot_price = 0.0L;
	if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 1){
		std::string kline_str = reply->element[0]->str;
		Json::Value kline_json = Json::Value::null;
		bool ret = reader->parse(kline_str.c_str(), kline_str.c_str() + kline_str.size(), &kline_json, &error);
		if (!(ret && error.size() == 0)) {
			LOG(ERROR) << "kline_str:" << kline_str << " json error";
			return false;
		}
		int timestamp = kline_json["timestamp"].asInt();
		if (m_time_now - timestamp < 120) {
			m_spot_price = switch_s_to_f(kline_json["price"].asString());
		}
	}
	LOG(INFO) << "m_spot_price:" << m_spot_price;
	freeReplyObject(reply);

	if (m_order_type == ORDER_TYPE_LIMIT && m_spot_price > EPS){
		if (ORDER_PRICE_LIMIT_SWITCH && !DiffPrice(m_order_price, m_spot_price)){
			if (m_order_op == ORDER_SIDE_BUY && m_order_price > m_spot_price) {
				LOG(INFO) << " price diff much m_order_price: " << m_order_price << " m_spot_price: " << m_spot_price;
				return false;
			}
			if (m_order_op == ORDER_SIDE_SELL && m_order_price < m_spot_price) {
				LOG(INFO) << " price diff much m_order_price: " << m_order_price << " m_spot_price: " << m_spot_price;
				return false;
			}
		}
	}

	LOG(INFO) << "m_order_id:" << m_order_id << " query limit price end";

	redisReply* reply1 = NULL;
	redisReply* reply2 = NULL;
	redisReply* reply3 = NULL;
	
	redisAppendCommand(m_redis, "GET account_user_%d", m_order_user_id);
	redisAppendCommand(m_stat_redis, "HMGET trade_config_%s_%s min_amt max_amt amt_decimal price_decimal maker_fee taker_fee state", m_order_base_asset.c_str(), m_order_quote_asset.c_str());
	redisAppendCommand(m_redis, "HMGET last_trade_price %s_bitCNY %s_bitCNY KK_bitCNY %s_ETH %s_ETH KK_ETH %s_%s ETH_%s ETH_bitCNY ETH_USDT", m_order_base_asset.c_str(), m_order_quote_asset.c_str(), m_order_base_asset.c_str(), m_order_quote_asset.c_str(), m_order_base_asset.c_str(), m_order_quote_asset.c_str(), m_order_quote_asset.c_str());
	
	redisGetReply(m_redis, (void**)&reply1);
	redisGetReply(m_stat_redis, (void**)&reply2);
	redisGetReply(m_redis, (void**)&reply3);
	Json::Value order_user_account_json = Json::Value::null;
	if (reply1 == NULL){
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
		freeReplyObject(reply2);
		freeReplyObject(reply3);
		redisFree(m_redis);
		m_redis = NULL;
		return false;
	}
	if (reply1->type != REDIS_REPLY_STRING){
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis type error:" << reply1->type;
		freeReplyObject(reply1);
		freeReplyObject(reply2);
		freeReplyObject(reply3);
		return false;
	}
	string order_user_account_str = reply1->str;
	freeReplyObject(reply1);
	
	bool ret = reader->parse(order_user_account_str.c_str(), order_user_account_str.c_str() + order_user_account_str.size(), &order_user_account_json, &error);
	if (!(ret && error.size() == 0)) {
		LOG(ERROR) << "m_order_id:" << m_order_id << " json error";
		freeReplyObject(reply2);
		freeReplyObject(reply3);
		return false;
	}

	 LOG(INFO) << "order_user_account_str.c_str(): " << order_user_account_str.c_str();
	 LOG(INFO) << "m_order_quote_asset: " << m_order_quote_asset;

	if (m_order_op == ORDER_SIDE_BUY) {
		if (!order_user_account_json.isMember(m_order_quote_asset)){
			LOG(ERROR) << "m_order_id:" << m_order_id << " available not enough";
			freeReplyObject(reply2);
			freeReplyObject(reply3);
			return false;
		}
		string available = order_user_account_json[m_order_quote_asset]["available"].asString();
		string encode_available = order_user_account_json[m_order_quote_asset]["encode_available"].asString();
		if (m_order_type == ORDER_TYPE_LIMIT) {
			if (strtold(available.c_str(), NULL) + EPS < m_order_price * m_order_amt){
				LOG(ERROR) << "m_order_id:" << m_order_id << " available not enough";
				freeReplyObject(reply2);
				freeReplyObject(reply3);
				return false;
			}
		} else if (m_order_type == ORDER_TYPE_MARKET) {
			int start = 0;
			int step = 10;
			long double remain_amount = m_order_amt;
			long double available_amt = switch_s_to_f(available.c_str());
			long double quote_amt = 0;
			while (true) {
				char sortsetkey[128];
				snprintf(sortsetkey, sizeof(sortsetkey), "order_book_%s_%s_sell", m_order_base_asset.c_str(), m_order_quote_asset.c_str());
				redisReply* reply = (redisReply*) redisCommand(m_redis, "ZRANGE %s %d %d WITHSCORES", sortsetkey, start, start + step - 1);
				start += step;
				if (reply == NULL) {
					LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
					freeReplyObject(reply2);
					freeReplyObject(reply3);
					redisFree(m_redis);
					m_redis = NULL;
					return false;
				}
				if (reply->type != REDIS_REPLY_ARRAY) {
					LOG(ERROR) << "m_order_id:" << m_order_id << " redis type error:" << reply->type;
					freeReplyObject(reply);
					freeReplyObject(reply2);
					freeReplyObject(reply3);
					return false;
				}
				if (reply->type == REDIS_REPLY_ARRAY) {
					int len = reply->elements;
					if (!(len % 2 == 0)) {
						LOG(ERROR) << "m_order_id:" << m_order_id << " reply->elements not even: " << len;
						freeReplyObject(reply);
						freeReplyObject(reply2);
						freeReplyObject(reply3);
						return false;
					}

					for (int i = 0; i < len - 1; i = i + 2) {
						if (remain_amount <= EPS) {
							break;
						}

						string l_sortset_str = reply->element[i]->str;
						Json::Value l_sortset_json;
						bool ret = reader->parse(l_sortset_str.c_str(), l_sortset_str.c_str() + l_sortset_str.size(), &l_sortset_json, &error);
						if (!(ret && error.size() == 0)) {
							LOG(ERROR) << "m_order_id:" << m_order_id << " json error";
							freeReplyObject(reply);
							freeReplyObject(reply2);
							freeReplyObject(reply3);
							return false;
						}

						string l_price_str = switch_f_to_s(switch_s_to_f(reply->element[i + 1]->str));
						long double l_price = switch_s_to_f(l_price_str);

						int j;
						for (j = 0; j < (int)l_sortset_json["orders"].size(); j++) {
							long double tmp_order_amount = switch_s_to_f(l_sortset_json["orders"][j]["amount"].asString());
							long double trade_amount;
							if (remain_amount <= tmp_order_amount){
								trade_amount = remain_amount;
							}else{
								trade_amount = tmp_order_amount;
							}
							if (available_amt + EPS < quote_amt + trade_amount * l_price) {
								LOG(ERROR) << "m_order_id:" << m_order_id << " available not enough";
								freeReplyObject(reply);
								freeReplyObject(reply2);
								freeReplyObject(reply3);
								return false;
							}
							remain_amount -= trade_amount;
							quote_amt += trade_amount * l_price;
							if (remain_amount <= EPS) {
								m_order_price_str = l_price_str;
								m_order_price = l_price;
								break;
							}
						}

						if (j > 0) {
							m_order_price_str = l_price_str;
							m_order_price = l_price;
						}
					}
					freeReplyObject(reply);

					if (remain_amount <= EPS) {
						break;
					}
					if (len < step * 2) {
						break;
					}
				}
			}
		} else {
			freeReplyObject(reply2);
			freeReplyObject(reply3);
			return false;
		}
		if (HmacSha256Encode(to_string(m_order_user_id) + m_order_quote_asset + available) != encode_available){
			LOG(ERROR) << "m_order_id:" << m_order_id << " encode_available error";
			freeReplyObject(reply2);
			freeReplyObject(reply3);
			return false;
		}
	}else{
		if (!order_user_account_json.isMember(m_order_base_asset)){
			LOG(ERROR) << "m_order_id:" << m_order_id << " available not enough";
			freeReplyObject(reply2);
			freeReplyObject(reply3);
			return false;
		}
		string available = order_user_account_json[m_order_base_asset]["available"].asString();
		string encode_available = order_user_account_json[m_order_base_asset]["encode_available"].asString();
		if (strtold(available.c_str(), NULL) + EPS < m_order_amt){
			LOG(ERROR) << "m_order_id:" << m_order_id << " available not enough";
			freeReplyObject(reply2);
			freeReplyObject(reply3);
			return false;
		}
		if (m_order_type == ORDER_TYPE_MARKET) {
			int start = 0;
			int step = 10;
			long double remain_amount = m_order_amt;
			while (true) {
				char sortsetkey[128];
				snprintf(sortsetkey, sizeof(sortsetkey), "order_book_%s_%s_buy", m_order_base_asset.c_str(), m_order_quote_asset.c_str());
				redisReply* reply = (redisReply*) redisCommand(m_redis, "ZREVRANGE %s %d %d WITHSCORES", sortsetkey, start, start + step - 1);
				start += step;
				if (reply == NULL) {
					LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
					freeReplyObject(reply2);
					freeReplyObject(reply3);
					redisFree(m_redis);
					m_redis = NULL;
					return false;
				}
				if (reply->type != REDIS_REPLY_ARRAY) {
					LOG(ERROR) << "m_order_id:" << m_order_id << " redis type error:" << reply->type;
					freeReplyObject(reply);
					freeReplyObject(reply2);
					freeReplyObject(reply3);
					return false;
				}
				if (reply->type == REDIS_REPLY_ARRAY) {
					int len = reply->elements;
					if (!(len % 2 == 0)) {
						LOG(ERROR) << "m_order_id:" << m_order_id << " reply->elements not even: " << len;
						freeReplyObject(reply);
						freeReplyObject(reply2);
						freeReplyObject(reply3);
						return false;
					}

					for (int i = 0; i < len - 1; i = i + 2) {
						if (remain_amount <= EPS) {
							break;
						}

						string l_sortset_str = reply->element[i]->str;
						Json::Value l_sortset_json;
						bool ret = reader->parse(l_sortset_str.c_str(), l_sortset_str.c_str() + l_sortset_str.size(), &l_sortset_json, &error);
						if (!(ret && error.size() == 0)) {
							LOG(ERROR) << "m_order_id:" << m_order_id << " json error";
							freeReplyObject(reply);
							freeReplyObject(reply2);
							freeReplyObject(reply3);
							return false;
						}

						string l_price_str = switch_f_to_s(switch_s_to_f(reply->element[i + 1]->str));
						long double l_price = switch_s_to_f(l_price_str);

						for (int j = 0; j < (int)l_sortset_json["orders"].size(); j++) {
							long double tmp_order_amount = switch_s_to_f(l_sortset_json["orders"][j]["amount"].asString());
							long double trade_amount;
							if (remain_amount <= tmp_order_amount){
								trade_amount = remain_amount;
							}else{
								trade_amount = tmp_order_amount;
							}
							remain_amount -= trade_amount;
							if (remain_amount <= EPS) {
								break;
							}
						}

						m_order_price_str = l_price_str;
						m_order_price = l_price;
					}
					freeReplyObject(reply);

					if (remain_amount <= EPS) {
						break;
					}
					if (len < step * 2) {
						break;
					}
				}
			}
		}
		if (HmacSha256Encode(to_string(m_order_user_id) + m_order_base_asset + available) != encode_available){
			LOG(ERROR) << "m_order_id:" << m_order_id << " encode_available error";
			freeReplyObject(reply2);
			freeReplyObject(reply3);
			return false;
		}
	}
	LOG(INFO) << "m_order_id:" << m_order_id << " CheckAmount end";
	
	//查询交易对状态及手续费
	LOG(INFO) << "m_order_id:" << m_order_id << " GetFee start";
	
	m_maker_fee = 0L;
	m_taker_fee = 0L;
	if (reply2 == NULL){
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
		freeReplyObject(reply3);
		redisFree(m_stat_redis);
		m_stat_redis = NULL;
		return false;
	}
	if (reply2->type != REDIS_REPLY_ARRAY || reply2->elements != 7){
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis type error:" << reply2->type;
		freeReplyObject(reply2);
		freeReplyObject(reply3);
		return false;
	}
	
	for (int i = 0; i < 7; i++){
		if (reply2->element[i]->type != REDIS_REPLY_STRING){
			LOG(ERROR) << "m_order_id:" << m_order_id << " i: " << i << " redis type error: i:" << reply2->element[i]->type;
			freeReplyObject(reply2);
			freeReplyObject(reply3);
			return false;
		}
	}
	
	m_min_order_amount = strtold(reply2->element[0]->str, NULL);
	m_max_order_amount = strtold(reply2->element[1]->str, NULL);
	m_amount_decimal = atoi(reply2->element[2]->str);
	m_price_decimal = atoi(reply2->element[3]->str);
	m_maker_fee = switch_s_to_f(switch_f_to_s(strtold(reply2->element[4]->str, NULL)));
	m_taker_fee = switch_s_to_f(switch_f_to_s(strtold(reply2->element[5]->str, NULL)));
	int pair_state = atoi(reply2->element[6]->str);
	freeReplyObject(reply2);
	
	if (pair_state != TRADE_PAIR_STATE_ON){
		LOG(ERROR) << "m_order_id:" << m_order_id << " pair_state error:" << pair_state;
		freeReplyObject(reply3);
		return false;
	}
	
	if (m_order_amt + EPS < m_min_order_amount){
		LOG(ERROR) << "m_order_id:" << m_order_id << " amount: " << m_order_amt << " min_amount: " << m_min_order_amount;
		freeReplyObject(reply3);
		return false;
	}
	
	if (m_order_amt - EPS > m_max_order_amount){
		LOG(ERROR) << "m_order_id:" << m_order_id << " amount: " << m_order_amt << " max_amount: " << m_max_order_amount;
		freeReplyObject(reply3);
		return false;
	}
	
	if (m_maker_fee - EPS > 0.002 || m_maker_fee + EPS < -0.002){
		LOG(INFO) << "m_order_id:" << m_order_id << " m_maker_fee: " << m_maker_fee << " change to 0.001 ";
		m_maker_fee = 0.001L;
	}
	if (m_taker_fee - EPS > 0.002 || m_taker_fee + EPS < -0.002){
		LOG(INFO) << "m_order_id:" << m_order_id << " m_taker_fee: " << m_taker_fee << " change to 0.001 ";
		m_taker_fee = 0.001L;
	}
	
	if (!CheckDecimal(m_order_amt, m_amount_decimal)){
		LOG(ERROR) << "m_order_id:" << m_order_id << " m_order_amt: " << m_order_amt << " m_amount_decimal: " << m_amount_decimal;
		freeReplyObject(reply3);
		return false;
	}
	
	if (!CheckDecimal(m_order_price, m_price_decimal)){
		LOG(ERROR) << "m_order_id:" << m_order_id << " m_order_price: " << m_order_price << " m_price_decimal: " << m_price_decimal;
		freeReplyObject(reply3);
		return false;
	}
	
	LOG(INFO) << "m_maker_fee:" << m_maker_fee;
	LOG(INFO) << "m_taker_fee:" << m_taker_fee;
	LOG(INFO) << "m_order_id:" << m_order_id << " GetFee end";
	
	//查询交易对币种和KK的价格
	LOG(INFO) << "m_order_id:" << m_order_id << " GetAssetPrice start";

	m_price_base_asset_KK = 0L;
	m_price_quote_asset_KK = 0L;
	m_price_base_asset_bitCNY = 0L;
	long double price_base_CNY = 0L;
	long double price_quote_CNY = 0L;
	long double price_KK_CNY = 0L;
	long double price_base_ETH = 0L;
	long double price_quote_ETH = 0L;
	long double price_KK_ETH = 0L;
	long double price_base_quote = 0L;
	long double price_ETH_quote = 0L;
	long double price_ETH_bitCNY = 0L;
	long double price_ETH_USDT = 0L;
	if (m_order_base_asset == "bitCNY"){
		price_base_CNY = 1;
	}
	if (m_order_quote_asset == "bitCNY"){
		price_quote_CNY = 1;
	}
	if (m_order_base_asset == "ETH"){
		price_base_ETH = 1;
	}
	if (m_order_quote_asset == "ETH"){
		price_quote_ETH = 1;
	}
	if (reply3 == NULL){
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
		redisFree(m_redis);
		m_redis = NULL;
		return false;
	}
	if (reply3->type == REDIS_REPLY_ARRAY && reply3->elements == 10){
		if (reply3->element[0]->type == REDIS_REPLY_STRING){
			price_base_CNY = strtold(reply3->element[0]->str, NULL);
		}
		if (reply3->element[1]->type == REDIS_REPLY_STRING){
			price_quote_CNY = strtold(reply3->element[1]->str, NULL);
		}
		if (reply3->element[2]->type == REDIS_REPLY_STRING){
			price_KK_CNY = strtold(reply3->element[2]->str, NULL);
		}
		if (reply3->element[3]->type == REDIS_REPLY_STRING){
			price_base_ETH = strtold(reply3->element[3]->str, NULL);
		}
		if (reply3->element[4]->type == REDIS_REPLY_STRING){
			price_quote_ETH = strtold(reply3->element[4]->str, NULL);
		}
		if (reply3->element[5]->type == REDIS_REPLY_STRING){
			price_KK_ETH = strtold(reply3->element[5]->str, NULL);
		}
		if (reply3->element[6]->type == REDIS_REPLY_STRING){
			price_base_quote = strtold(reply3->element[6]->str, NULL);
		}
		if (reply3->element[7]->type == REDIS_REPLY_STRING){
			price_ETH_quote = strtold(reply3->element[7]->str, NULL);
		}
		if (reply3->element[8]->type == REDIS_REPLY_STRING){
			price_ETH_bitCNY = strtold(reply3->element[8]->str, NULL);
		}
		if (reply3->element[9]->type == REDIS_REPLY_STRING){
			price_ETH_USDT = strtold(reply3->element[9]->str, NULL);
		}
	}
	freeReplyObject(reply3);
	if (price_base_ETH > EPS && price_KK_ETH > EPS){
		m_price_base_asset_KK = price_base_ETH / price_KK_ETH;
	}else if (price_base_quote > EPS && price_ETH_quote > EPS && price_KK_ETH > EPS){
		m_price_base_asset_KK = price_base_quote / price_ETH_quote / price_KK_ETH;
	}else if (price_base_CNY > EPS && price_KK_CNY > EPS){
		m_price_base_asset_KK = price_base_CNY / price_KK_CNY;
	}
	
	if (price_quote_ETH > EPS && price_KK_ETH > EPS){
		m_price_quote_asset_KK = price_quote_ETH / price_KK_ETH;
	}else if (price_ETH_quote > EPS && price_KK_ETH > EPS){
		m_price_quote_asset_KK = 1L / price_ETH_quote / price_KK_ETH;
	}else if (price_quote_CNY > EPS && price_KK_CNY > EPS){
		m_price_quote_asset_KK = price_quote_CNY / price_KK_CNY;
	}
	
	if (m_price_base_asset_KK > EPS && price_KK_ETH > EPS && price_ETH_USDT > EPS){
		m_price_base_asset_bitCNY = m_price_base_asset_KK * price_KK_ETH * price_ETH_USDT * m_usd_cny_value;
	}else if (m_price_base_asset_KK > EPS && price_KK_ETH > EPS && price_ETH_bitCNY > EPS){
		m_price_base_asset_bitCNY = m_price_base_asset_KK * price_KK_ETH * price_ETH_bitCNY;
	}
	
	LOG(INFO) << "m_price_base_asset_KK:" << m_price_base_asset_KK;
	LOG(INFO) << "m_price_quote_asset_KK:" << m_price_quote_asset_KK;
	LOG(INFO) << "m_order_id:" << m_order_id << " GetAssetPrice end";
	
	return true;
}

bool Match::GetTradeId(){
	LOG(INFO) << "m_order_id:" << m_order_id << " GetTradeId start";
	//test
	time_t time_cur;
	time(&time_cur);
	struct tm *time_p;
	time_p = gmtime(&time_cur);
	
	char st[100];
	snprintf(st, sizeof(st), "%04d%02d%02d", time_p->tm_year + 1900, time_p->tm_mon + 1, time_p->tm_mday);
	
	redisReply* reply = NULL;
	
	reply = (redisReply*) redisCommand(m_stat_redis, "rpop list_for_id_%s", st);
	if (reply == NULL){
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
		redisFree(m_stat_redis);
		m_stat_redis = NULL;
		return false;
	}
	if (reply->type != REDIS_REPLY_STRING){
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis type error:" << reply->type;
		freeReplyObject(reply);
		return false;
	}
	m_cur_trade_id = reply->str;
	freeReplyObject(reply);
	
	LOG(INFO) << "m_order_id:" << m_order_id << " GetTradeId end";
	return true;
}

bool Match::MatchOrder(){
	LOG(INFO) << "m_order_id:" << m_order_id << " MatchOrder start";
	m_trade_users_set.insert(m_order_user_id);
	m_remain_amount = m_order_amt;
	
	if (m_order_price > EPS) {
		//获取下单对面方向的orderbook
		Json::StreamWriterBuilder writer;
		writer["indentation"] = "";
		Json::CharReaderBuilder rbuilder;
		std::unique_ptr<Json::CharReader> const reader(rbuilder.newCharReader());
		JSONCPP_STRING error;

		redisReply* reply = NULL;
		char *redis_cmd;

		char sortsetkey[128];
		if (m_order_op == ORDER_SIDE_BUY){
			snprintf(sortsetkey, sizeof(sortsetkey), "order_book_%s_%s_sell", m_order_base_asset.c_str(), m_order_quote_asset.c_str());
			reply = (redisReply*) redisCommand(m_redis, "ZRANGEBYSCORE %s -inf %.9f WITHSCORES", sortsetkey, (double)m_order_price + EPS);
		}else{
			snprintf(sortsetkey, sizeof(sortsetkey), "order_book_%s_%s_buy", m_order_base_asset.c_str(), m_order_quote_asset.c_str());
			reply = (redisReply*) redisCommand(m_redis, "ZREVRANGEBYSCORE %s +inf %.9f WITHSCORES", sortsetkey, (double)m_order_price - EPS);
		}
		if (reply == NULL){
			LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
			redisFree(m_redis);
			m_redis = NULL;
			return false;
		}
		if (reply->type != REDIS_REPLY_ARRAY && reply->type != REDIS_REPLY_NIL){
			LOG(ERROR) << "m_order_id:" << m_order_id << " redis type error:" << reply->type;
			freeReplyObject(reply);
			return false;
		}
		if (reply->type == REDIS_REPLY_ARRAY) {
			int len = reply->elements;
			if (!(len % 2 == 0)){
				LOG(ERROR) << "m_order_id:" << m_order_id << " reply->elements not even: " << len;
				freeReplyObject(reply);
				return false;
			}
			
			for (int i = 0; i < len - 1; i = i + 2){
				string l_sortset_str = reply->element[i]->str;
				long double l_price = switch_s_to_f(reply->element[i + 1]->str);
				string l_price_str = switch_f_to_s(l_price);
				Json::Value l_sortset_json;
				
				bool ret = reader->parse(l_sortset_str.c_str(), l_sortset_str.c_str() + l_sortset_str.size(), &l_sortset_json, &error);
				if (!(ret && error.size() == 0)) {
					LOG(ERROR) << "m_order_id:" << m_order_id << " json error";
					return false;
				}

				if (m_order_type == ORDER_TYPE_MARKET && m_spot_price > EPS){
					if (ORDER_PRICE_LIMIT_SWITCH && !DiffPrice(l_price, m_spot_price)){
						if ((l_price > m_spot_price && m_order_op == ORDER_SIDE_BUY) || (l_price < m_spot_price && m_order_op == ORDER_SIDE_SELL)){
							LOG(ERROR) << "m_order_id:" << m_order_id << " price diff much m_spot_price:" << m_spot_price << " l_price:" << l_price;
							break;
						}
					}
				}

				if (m_remain_amount <= EPS) break;
				if (redisFormatCommand(&redis_cmd, "ZREM %s %s", sortsetkey, l_sortset_str.c_str()) <= 0){
					LOG(ERROR) << "redis format error";
					freeReplyObject(reply);
					return false;
				}
				string temp_str = redis_cmd;
				free(redis_cmd);
				m_redis_cmd_list.push_back(temp_str);
				
				Json::Value temp_json = Json::Value::null;		//新的redis sortset member
				int j;
				for (j = 0; j < (int)l_sortset_json["orders"].size(); j++){
					long double tmp_order_amount = switch_s_to_f(l_sortset_json["orders"][j]["amount"].asString());
					long double trade_amount;
					if (m_remain_amount <= tmp_order_amount){
						trade_amount = m_remain_amount;
					}else{
						trade_amount = tmp_order_amount;
					}
					long double cur_executed_quote_amount = trade_amount * switch_s_to_f(l_price_str);
					long double ori_executed_quote_amount = switch_s_to_f(l_sortset_json["orders"][j]["executed_quote_amount"].asString());
					m_executed_quote_amount += cur_executed_quote_amount;
					
					m_trade_users_set.insert(l_sortset_json["orders"][j]["user_id"].asInt());
					Json::Value temp_trade = Json::Value::null;
					temp_trade["price"] = l_price_str;
					temp_trade["amount"] = switch_f_to_s(trade_amount);
					temp_trade["user_id"] = l_sortset_json["orders"][j]["user_id"].asInt();
					temp_trade["order_id"] = l_sortset_json["orders"][j]["order_id"].asString();
					if (!GetTradeId()){
						freeReplyObject(reply);
						return false;
					}
					temp_trade["id"] = m_cur_trade_id;
					m_trade_list.append(temp_trade);
					
					string cur_order_left_amount;
					if (trade_amount + EPS < tmp_order_amount){
						Json::Value temp_order = Json::Value::null;
						temp_order["user_id"] = l_sortset_json["orders"][j]["user_id"].asInt();
						temp_order["order_id"] = l_sortset_json["orders"][j]["order_id"].asString();
						temp_order["origin_amount"] = l_sortset_json["orders"][j]["origin_amount"].asString();
						temp_order["amount"] = switch_f_to_s(tmp_order_amount - trade_amount);
						temp_order["executed_quote_amount"] = switch_f_to_s(cur_executed_quote_amount + ori_executed_quote_amount);
						temp_json["orders"].append(temp_order);
						temp_json["total_amount"] = switch_f_to_s(tmp_order_amount - trade_amount);
						cur_order_left_amount = temp_order["amount"].asString();
					}else{
						cur_order_left_amount = switch_f_to_s(0);
					}
					
					//添加到 m_order_result_list 队列
					Json::Value tmp_order_result = Json::Value::null;
					tmp_order_result["type"] = "order";
					tmp_order_result["is_new"] = 0;
					tmp_order_result["id"] = l_sortset_json["orders"][j]["order_id"].asString();
					tmp_order_result["user_id"] = l_sortset_json["orders"][j]["user_id"].asInt();
					tmp_order_result["base_asset"] = m_order_base_asset;
					tmp_order_result["quote_asset"] = m_order_quote_asset;
					tmp_order_result["order_type"] = ORDER_TYPE_LIMIT;
					if (m_order_op == ORDER_SIDE_BUY){
						tmp_order_result["order_op"] = ORDER_SIDE_SELL;
					}else{
						tmp_order_result["order_op"] = ORDER_SIDE_BUY;
					}
					tmp_order_result["price"] = l_price_str;
					tmp_order_result["amount"] = cur_order_left_amount;
					tmp_order_result["origin_amount"] = l_sortset_json["orders"][j]["origin_amount"].asString();
					tmp_order_result["executed_quote_amount"] = switch_f_to_s(cur_executed_quote_amount + ori_executed_quote_amount);
					tmp_order_result["update_at"] = m_time_now;
					if (cur_order_left_amount == switch_f_to_s(0)){
						tmp_order_result["status"] = ORDER_STATUS_FILLED;
					}else{
						tmp_order_result["status"] = ORDER_STATUS_PARTIALLY;
					}
					m_order_result_list.append(tmp_order_result);
					m_statistics["order"].append(tmp_order_result);
					
					m_remain_amount -= trade_amount;
					if (m_remain_amount <= EPS){
						break;
					}
				}
				for (int k = j + 1; k < (int)l_sortset_json["orders"].size(); k++){
					temp_json["orders"].append(l_sortset_json["orders"][k]);
					if (!temp_json.isMember("total_amount")){
						temp_json["total_amount"] = "0.00000000";
					}
					temp_json["total_amount"] = switch_f_to_s(switch_s_to_f(temp_json["total_amount"].asString()) + switch_s_to_f(l_sortset_json["orders"][k]["amount"].asString()));
				}
				if (temp_json != Json::Value::null){
					temp_json["price"] = l_price_str;
					string result = Json::writeString(writer, temp_json);
					if (redisFormatCommand(&redis_cmd, "ZADD %s %s %s", sortsetkey, l_price_str.c_str(), result.c_str()) <= 0){
						LOG(ERROR) << "redis format error";
						freeReplyObject(reply);
						return false;
					}
					string redis_cmd_str = redis_cmd;
					free(redis_cmd);
					m_redis_cmd_list.push_back(redis_cmd_str);
					
					Json::Value temp_order_book = Json::Value::null;
					temp_order_book["amount"]  = temp_json["total_amount"].asString();
					temp_order_book["price"]  = temp_json["price"].asString();
					if (m_order_op == ORDER_SIDE_BUY){
						m_statistics["order_book"]["sell"].append(temp_order_book);
					}else{
						m_statistics["order_book"]["buy"].append(temp_order_book);
					}
				}else{
					Json::Value temp_order_book = Json::Value::null;
					temp_order_book["amount"]  = switch_f_to_s(0);
					temp_order_book["price"]  = l_price_str;
					if (m_order_op == ORDER_SIDE_BUY){
						m_statistics["order_book"]["sell"].append(temp_order_book);
					}else{
						m_statistics["order_book"]["buy"].append(temp_order_book);
					}
				}
			}
		}
		freeReplyObject(reply);
	}

	Json::Value tmp_order_result = Json::Value::null;
	tmp_order_result["type"] = "order";
	tmp_order_result["is_new"] = 1;
	tmp_order_result["id"] = m_order_id;
	tmp_order_result["user_id"] = m_order_user_id;
	tmp_order_result["base_asset"] = m_order_base_asset;
	tmp_order_result["quote_asset"] = m_order_quote_asset;
	tmp_order_result["order_type"] = m_order_type;
	tmp_order_result["order_op"] = m_order_op;
	tmp_order_result["amount"] = switch_f_to_s(m_remain_amount);
	tmp_order_result["origin_amount"] = m_order_amt_str;
	tmp_order_result["executed_quote_amount"] = switch_f_to_s(m_executed_quote_amount);
	tmp_order_result["create_at"] = m_time_now;
	tmp_order_result["ip"] = m_order_ip;
	tmp_order_result["source"] = m_order_source;
	if (m_order_type == ORDER_TYPE_LIMIT) {
		tmp_order_result["price"] = m_order_price_str;
		if (m_remain_amount < EPS){
			tmp_order_result["status"] = ORDER_STATUS_FILLED;
		}else if (tmp_order_result["amount"].asString() == tmp_order_result["origin_amount"].asString()){
			tmp_order_result["status"] = ORDER_STATUS_NEW;
		}else{
			tmp_order_result["status"] = ORDER_STATUS_PARTIALLY;
		}
	} else if (m_order_type == ORDER_TYPE_MARKET) {
		tmp_order_result["price"] = switch_f_to_s(0);
		if (m_remain_amount < EPS){
			tmp_order_result["status"] = ORDER_STATUS_FILLED;
		} else {
			tmp_order_result["status"] = ORDER_STATUS_CANCELED;
		}
	} else {
		return false;
	}
	
	m_order_result_list.append(tmp_order_result);
	m_statistics["order"].append(tmp_order_result);
	
	LOG(INFO) << "m_order_id:" << m_order_id << " MatchOrder end";
	return true;
}

bool Match::QueryUserAccount(){
	LOG(INFO) << "m_order_id:" << m_order_id << " QueryUserAccount start";
	
	Json::CharReaderBuilder rbuilder;
	std::unique_ptr<Json::CharReader> const reader(rbuilder.newCharReader());
	JSONCPP_STRING error;
	char *redis_cmd;
	
	for (set<int>::iterator it = m_trade_users_set.begin(); it != m_trade_users_set.end(); it++){
		int trade_user = *it;
		if (trade_user != m_order_user_id) AccountLock(trade_user);
		m_trade_users_list.push_back(trade_user);
	}
	
	bool flag = true;
	for (int i = 0; i < (int)m_trade_users_list.size(); i++){
		int len;
		len = redisFormatCommand(&redis_cmd, "GET account_user_%d", m_trade_users_list[i]);
		if (len <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		redisAppendFormattedCommand(m_redis, redis_cmd, len);
		free(redis_cmd);
	}
	for (int i = 0; i < (int)m_trade_users_list.size(); i++){
		redisReply* temp_reply = NULL;
		redisGetReply(m_redis, (void**)&temp_reply);
		if (temp_reply == NULL){
			LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
			redisFree(m_redis);
			m_redis = NULL;
			return false;
		}
		if (temp_reply->type != REDIS_REPLY_STRING){
			LOG(ERROR) << "m_order_id:" << m_order_id << " redis type error:" << temp_reply->type;
			freeReplyObject(temp_reply);
			flag = false;
			continue;
		}
		string user_account_str = temp_reply->str;
		freeReplyObject(temp_reply);
		Json::Value user_account_json = Json::Value::null;
		bool ret = reader->parse(user_account_str.c_str(), user_account_str.c_str() + user_account_str.size(), &user_account_json, &error);
		if (!(ret && error.size() == 0)) {
			LOG(ERROR) << "m_order_id:" << m_order_id << " json error";
			flag = false;
			continue;
		}
		if (!user_account_json.isMember(m_order_base_asset)){
			user_account_json[m_order_base_asset]["available"] = "0.00000000";
			user_account_json[m_order_base_asset]["frozen"] = "0.00000000";
			user_account_json[m_order_base_asset]["encode_available"] = HmacSha256Encode(to_string(m_trade_users_list[i]) + m_order_base_asset + "0.00000000");
			user_account_json[m_order_base_asset]["encode_frozen"] = HmacSha256Encode(to_string(m_trade_users_list[i]) + m_order_base_asset + "0.00000000");
		}
		if (!user_account_json.isMember(m_order_quote_asset)){
			user_account_json[m_order_quote_asset]["available"] = "0.00000000";
			user_account_json[m_order_quote_asset]["frozen"] = "0.00000000";
			user_account_json[m_order_quote_asset]["encode_available"] = HmacSha256Encode(to_string(m_trade_users_list[i]) + m_order_quote_asset + "0.00000000");
			user_account_json[m_order_quote_asset]["encode_frozen"] = HmacSha256Encode(to_string(m_trade_users_list[i]) + m_order_quote_asset + "0.00000000");
		}
		m_user_account[to_string(m_trade_users_list[i])]["funds"] = user_account_json;
	}
	if (flag == false){
		return false;
	}
	Json::Value default_fee_setting = Json::Value::null;
	default_fee_setting["taker"] = 1;
	default_fee_setting["maker"] = 1;
	for (int i = 0; i < (int)m_trade_users_list.size(); i++){
		int len;
		len = redisFormatCommand(&redis_cmd, "HMGET user_trade_fee_setting %d %s_%s_%d", m_trade_users_list[i], m_order_base_asset.c_str(), m_order_quote_asset.c_str(), m_trade_users_list[i]);
		if (len <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		redisAppendFormattedCommand(m_stat_redis, redis_cmd, len);
		free(redis_cmd);
		
		len = redisFormatCommand(&redis_cmd, "SISMEMBER KK_switch_set %d", m_trade_users_list[i]);
		if (len <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		redisAppendFormattedCommand(m_stat_redis, redis_cmd, len);
		free(redis_cmd);
	}
	for (int i = 0; i < (int)m_trade_users_list.size(); i++){
		redisReply* temp_reply1 = NULL;
		redisGetReply(m_stat_redis, (void**)&temp_reply1);
		redisReply* temp_reply2 = NULL;
		redisGetReply(m_stat_redis, (void**)&temp_reply2);
		if (temp_reply1 == NULL) {
			freeReplyObject(temp_reply2);
			redisFree(m_stat_redis);
			m_stat_redis = NULL;
			return false;
		}
		if (temp_reply1->type != REDIS_REPLY_ARRAY || temp_reply1->elements != 2 || (temp_reply1->element[0]->type == REDIS_REPLY_NIL && temp_reply1->element[1]->type == REDIS_REPLY_NIL)){
			m_user_account[to_string(m_trade_users_list[i])]["fee_setting"] = default_fee_setting;
			freeReplyObject(temp_reply1);
		}else{
			string user_fee_setting_str;
			if (temp_reply1->element[1]->type != REDIS_REPLY_NIL){
				user_fee_setting_str = temp_reply1->element[1]->str;
			}else{
				user_fee_setting_str = temp_reply1->element[0]->str;
			}
			freeReplyObject(temp_reply1);
			Json::Value user_fee_setting_json = Json::Value::null;
			bool ret = reader->parse(user_fee_setting_str.c_str(), user_fee_setting_str.c_str() + user_fee_setting_str.size(), &user_fee_setting_json, &error);
			if (!(ret && error.size() == 0)) {
				LOG(ERROR) << "m_order_id:" << m_order_id << " json error";
				flag = false;
				if (temp_reply2 != NULL) freeReplyObject(temp_reply2);
				continue;
			}
			long double tmp_maker_fee = switch_s_to_f(user_fee_setting_json["maker"].asString());
			long double tmp_taker_fee = switch_s_to_f(user_fee_setting_json["taker"].asString());
			int time_start = 0;
			int time_end = 2000000000;
			if (user_fee_setting_json.isMember("start")){
				time_start = user_fee_setting_json["start"].asInt();
			}
			if (user_fee_setting_json.isMember("end")){
				time_end = user_fee_setting_json["end"].asInt();
			}
			int time_now = time(0);

			if (tmp_maker_fee < 0 || tmp_maker_fee > 1 || time_now < time_start || time_now > time_end){
				user_fee_setting_json["maker"] = 1;
			}
			if (tmp_taker_fee < 0 || tmp_taker_fee > 1 || time_now < time_start || time_now > time_end){
				user_fee_setting_json["taker"] = 1;
			}
			m_user_account[to_string(m_trade_users_list[i])]["fee_setting"] = user_fee_setting_json;
		}
		
		if (temp_reply2 == NULL){
			LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
			redisFree(m_stat_redis);
			m_stat_redis = NULL;
			return false;
		}
		if (temp_reply2->type != REDIS_REPLY_INTEGER){
			LOG(ERROR) << "m_order_id:" << m_order_id << " redis type error:" << temp_reply2->type;
			freeReplyObject(temp_reply2);
			flag = false;
			continue;
		}
		int tmp_switch = temp_reply2->integer;
		m_user_account[to_string(m_trade_users_list[i])]["KK_switch"] = tmp_switch;
		freeReplyObject(temp_reply2);
	}
	if (flag == false){
		return false;
	}
	LOG(INFO) << "m_order_id:" << m_order_id << " QueryUserAccount end";
	return true;
}

bool Match::Frozen(){
	LOG(INFO) << "m_order_id:" << m_order_id << " Frozen start";
	
	if (m_order_type == ORDER_TYPE_LIMIT) {
		Json::Value tmp_order_result = Json::Value::null;
		tmp_order_result["type"] = "account";
		tmp_order_result["id"] = m_order_id;
		tmp_order_result["user_id"] = m_order_user_id;
		tmp_order_result["jnl_type"] = USER_ACCOUNT_JNL_ORDER_FROZEN;
		tmp_order_result["remark"] = "下单冻结";
		tmp_order_result["update_at"] = m_time_now;
		
		if (m_order_op == ORDER_SIDE_BUY){
			long double frozen_amount = m_order_amt * m_order_price;
			string change_available = switch_f_to_s(-frozen_amount);
			string new_available = switch_f_to_s(switch_s_to_f(m_user_account[to_string(m_order_user_id)]["funds"][m_order_quote_asset]["available"].asString()) - frozen_amount);
			string change_frozen = switch_f_to_s(frozen_amount);
			string new_frozen = switch_f_to_s(switch_s_to_f(m_user_account[to_string(m_order_user_id)]["funds"][m_order_quote_asset]["frozen"].asString()) + frozen_amount);
			
			m_user_account[to_string(m_order_user_id)]["funds"][m_order_quote_asset]["available"] = new_available;
			m_user_account[to_string(m_order_user_id)]["funds"][m_order_quote_asset]["frozen"] = new_frozen;
			tmp_order_result["asset"] = m_order_quote_asset;
			tmp_order_result["change_available"] = change_available;
			tmp_order_result["new_available"] = new_available;
			tmp_order_result["change_frozen"] = change_frozen;
			tmp_order_result["new_frozen"] = new_frozen;
			tmp_order_result["new_encode_available"] = HmacSha256Encode(tmp_order_result["user_id"].asString() + m_order_quote_asset + tmp_order_result["new_available"].asString());
			tmp_order_result["new_encode_frozen"] = HmacSha256Encode(tmp_order_result["user_id"].asString() + m_order_quote_asset + tmp_order_result["new_frozen"].asString());
			m_user_account[to_string(m_order_user_id)]["funds"][m_order_quote_asset]["encode_available"] = tmp_order_result["new_encode_available"].asString();
			m_user_account[to_string(m_order_user_id)]["funds"][m_order_quote_asset]["encode_frozen"] = tmp_order_result["new_encode_frozen"].asString();
		}else{
			long double frozen_amount = m_order_amt;
			string change_available = switch_f_to_s(-frozen_amount);
			string new_available = switch_f_to_s(switch_s_to_f(m_user_account[to_string(m_order_user_id)]["funds"][m_order_base_asset]["available"].asString()) - frozen_amount);
			string change_frozen = switch_f_to_s(frozen_amount);
			string new_frozen = switch_f_to_s(switch_s_to_f(m_user_account[to_string(m_order_user_id)]["funds"][m_order_base_asset]["frozen"].asString()) + frozen_amount);
			
			m_user_account[to_string(m_order_user_id)]["funds"][m_order_base_asset]["available"] = new_available;
			m_user_account[to_string(m_order_user_id)]["funds"][m_order_base_asset]["frozen"] = new_frozen;
			tmp_order_result["asset"] = m_order_base_asset;
			tmp_order_result["change_available"] = change_available;
			tmp_order_result["new_available"] = new_available;
			tmp_order_result["change_frozen"] = change_frozen;
			tmp_order_result["new_frozen"] = new_frozen;
			tmp_order_result["new_encode_available"] = HmacSha256Encode(tmp_order_result["user_id"].asString() + m_order_base_asset + tmp_order_result["new_available"].asString());
			tmp_order_result["new_encode_frozen"] = HmacSha256Encode(tmp_order_result["user_id"].asString() + m_order_base_asset + tmp_order_result["new_frozen"].asString());
			m_user_account[to_string(m_order_user_id)]["funds"][m_order_base_asset]["encode_available"] = tmp_order_result["new_encode_available"].asString();
			m_user_account[to_string(m_order_user_id)]["funds"][m_order_base_asset]["encode_frozen"] = tmp_order_result["new_encode_frozen"].asString();
		}
		m_order_result_list.append(tmp_order_result);
		LOG(INFO) << "m_order_id:" << m_order_id << " Frozen end";
	}
	return true;
}

bool Match::SettleTrade(){
	LOG(INFO) << "m_order_id:" << m_order_id << " SettleTrade start";
	
	int trade_num = m_trade_list.size();
	for (int i = 0; i < trade_num; i++){
		string buy_user;
		string sell_user;
		if (m_order_op == ORDER_SIDE_BUY){
			buy_user = to_string(m_order_user_id);
			sell_user = m_trade_list[i]["user_id"].asString();
		}else{
			buy_user = m_trade_list[i]["user_id"].asString();
			sell_user = to_string(m_order_user_id);
		}
		
		Json::Value tmp_obj = Json::Value::null;
		tmp_obj["type"] = "account";
		tmp_obj["id"] = m_trade_list[i]["id"].asString();
		tmp_obj["update_at"] = m_time_now;
		
		//买方基础货币
		tmp_obj["user_id"] = atoi(buy_user.c_str());
		tmp_obj["asset"] = m_order_base_asset;
		tmp_obj["change_available"] = m_trade_list[i]["amount"].asString();
		tmp_obj["change_frozen"] = switch_f_to_s(0);
		tmp_obj["new_available"] = switch_f_to_s(switch_s_to_f(m_user_account[buy_user]["funds"][m_order_base_asset]["available"].asString()) +  switch_s_to_f(tmp_obj["change_available"].asString()));
		tmp_obj["new_frozen"] = switch_f_to_s(switch_s_to_f(m_user_account[buy_user]["funds"][m_order_base_asset]["frozen"].asString()) + switch_s_to_f(tmp_obj["change_frozen"].asString()));
		tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_BUY;
		tmp_obj["remark"] = "买入";
		tmp_obj["new_encode_available"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_order_base_asset + tmp_obj["new_available"].asString());
		tmp_obj["new_encode_frozen"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_order_base_asset + tmp_obj["new_frozen"].asString());
		
		m_user_account[buy_user]["funds"][m_order_base_asset]["available"] = tmp_obj["new_available"].asString();
		m_user_account[buy_user]["funds"][m_order_base_asset]["frozen"] = tmp_obj["new_frozen"].asString();
		m_user_account[buy_user]["funds"][m_order_base_asset]["encode_available"] = tmp_obj["new_encode_available"].asString();
		m_user_account[buy_user]["funds"][m_order_base_asset]["encode_frozen"] = tmp_obj["new_encode_frozen"].asString();
		
		m_order_result_list.append(tmp_obj);
		
		//买方定价货币
		tmp_obj["user_id"] = atoi(buy_user.c_str());
		tmp_obj["asset"] = m_order_quote_asset;
		if (m_order_type == ORDER_TYPE_MARKET && m_order_op == ORDER_SIDE_BUY) {
			tmp_obj["change_frozen"] = switch_f_to_s(0);
			tmp_obj["change_available"] = switch_f_to_s(-switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_trade_list[i]["price"].asString()));
		} else {
			tmp_obj["change_available"] = switch_f_to_s(0);
			tmp_obj["change_frozen"] = switch_f_to_s(-switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_trade_list[i]["price"].asString()));
			if (m_order_op == ORDER_SIDE_BUY && m_order_price > switch_s_to_f(m_trade_list[i]["price"].asString()) + EPS){
				tmp_obj["change_available"] = switch_f_to_s(switch_s_to_f(tmp_obj["change_available"].asString()) + (m_order_price - switch_s_to_f(m_trade_list[i]["price"].asString())) * switch_s_to_f(m_trade_list[i]["amount"].asString()));
				tmp_obj["change_frozen"] = switch_f_to_s(switch_s_to_f(tmp_obj["change_frozen"].asString()) - (m_order_price - switch_s_to_f(m_trade_list[i]["price"].asString())) * switch_s_to_f(m_trade_list[i]["amount"].asString()));
			}
		}
		tmp_obj["new_available"] = switch_f_to_s(switch_s_to_f(m_user_account[buy_user]["funds"][m_order_quote_asset]["available"].asString()) + switch_s_to_f(tmp_obj["change_available"].asString()));
		tmp_obj["new_frozen"] = switch_f_to_s(switch_s_to_f(m_user_account[buy_user]["funds"][m_order_quote_asset]["frozen"].asString()) +  switch_s_to_f(tmp_obj["change_frozen"].asString()));
		tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_SELL;
		tmp_obj["remark"] = "卖出";
		tmp_obj["new_encode_available"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_order_quote_asset + tmp_obj["new_available"].asString());
		tmp_obj["new_encode_frozen"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_order_quote_asset + tmp_obj["new_frozen"].asString());
		
		m_user_account[buy_user]["funds"][m_order_quote_asset]["available"] = tmp_obj["new_available"].asString();
		m_user_account[buy_user]["funds"][m_order_quote_asset]["frozen"] = tmp_obj["new_frozen"].asString();
		m_user_account[buy_user]["funds"][m_order_quote_asset]["encode_available"] = tmp_obj["new_encode_available"].asString();
		m_user_account[buy_user]["funds"][m_order_quote_asset]["encode_frozen"] = tmp_obj["new_encode_frozen"].asString();
		
		m_order_result_list.append(tmp_obj);
		
		//卖方基础货币
		tmp_obj["user_id"] = atoi(sell_user.c_str());
		tmp_obj["asset"] = m_order_base_asset;
		if (m_order_type == ORDER_TYPE_MARKET && m_order_op == ORDER_SIDE_SELL) {
			tmp_obj["change_available"] = switch_f_to_s(-switch_s_to_f(m_trade_list[i]["amount"].asString()));
			tmp_obj["change_frozen"] = switch_f_to_s(0);
		} else {
			tmp_obj["change_available"] = switch_f_to_s(0);
			tmp_obj["change_frozen"] = switch_f_to_s(-switch_s_to_f(m_trade_list[i]["amount"].asString()));
		}
		tmp_obj["new_available"] = switch_f_to_s(switch_s_to_f(m_user_account[sell_user]["funds"][m_order_base_asset]["available"].asString()) + switch_s_to_f(tmp_obj["change_available"].asString()));
		tmp_obj["new_frozen"] = switch_f_to_s(switch_s_to_f(m_user_account[sell_user]["funds"][m_order_base_asset]["frozen"].asString()) + switch_s_to_f(tmp_obj["change_frozen"].asString()));
		tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_SELL;
		tmp_obj["remark"] = "卖出";
		tmp_obj["new_encode_available"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_order_base_asset + tmp_obj["new_available"].asString());
		tmp_obj["new_encode_frozen"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_order_base_asset + tmp_obj["new_frozen"].asString());
		
		m_user_account[sell_user]["funds"][m_order_base_asset]["available"] = tmp_obj["new_available"].asString();
		m_user_account[sell_user]["funds"][m_order_base_asset]["frozen"] = tmp_obj["new_frozen"].asString();
		m_user_account[sell_user]["funds"][m_order_base_asset]["encode_available"] = tmp_obj["new_encode_available"].asString();
		m_user_account[sell_user]["funds"][m_order_base_asset]["encode_frozen"] = tmp_obj["new_encode_frozen"].asString();
		
		m_order_result_list.append(tmp_obj);
		
		//卖方定价货币
		tmp_obj["user_id"] = atoi(sell_user.c_str());
		tmp_obj["asset"] = m_order_quote_asset;
		tmp_obj["change_available"] = switch_f_to_s(switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_trade_list[i]["price"].asString()));
		tmp_obj["change_frozen"] = switch_f_to_s(0);
		tmp_obj["new_available"] = switch_f_to_s(switch_s_to_f(m_user_account[sell_user]["funds"][m_order_quote_asset]["available"].asString()) +  switch_s_to_f(tmp_obj["change_available"].asString()));
		tmp_obj["new_frozen"] = switch_f_to_s(switch_s_to_f(m_user_account[sell_user]["funds"][m_order_quote_asset]["frozen"].asString()) + switch_s_to_f(tmp_obj["change_frozen"].asString()));
		tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_BUY;
		tmp_obj["remark"] = "买入";
		tmp_obj["new_encode_available"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_order_quote_asset + tmp_obj["new_available"].asString());
		tmp_obj["new_encode_frozen"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_order_quote_asset + tmp_obj["new_frozen"].asString());
		
		m_user_account[sell_user]["funds"][m_order_quote_asset]["available"] = tmp_obj["new_available"].asString();
		m_user_account[sell_user]["funds"][m_order_quote_asset]["frozen"] = tmp_obj["new_frozen"].asString();
		m_user_account[sell_user]["funds"][m_order_quote_asset]["encode_available"] = tmp_obj["new_encode_available"].asString();
		m_user_account[sell_user]["funds"][m_order_quote_asset]["encode_frozen"] = tmp_obj["new_encode_frozen"].asString();
		
		m_order_result_list.append(tmp_obj);
		
		//计算手续费
		string buy_fee_coin = m_order_base_asset;
		long double buy_fee_amount = 0L;
		string sell_fee_coin = m_order_quote_asset;
		long double sell_fee_amount = 0L;
		if (m_order_op == ORDER_SIDE_BUY){
			//新下单为买单，orderbook为卖单
			buy_fee_amount = m_taker_fee * switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_user_account[buy_user]["fee_setting"]["taker"].asString());
			sell_fee_amount = m_maker_fee * switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_trade_list[i]["price"].asString()) * switch_s_to_f(m_user_account[sell_user]["fee_setting"]["maker"].asString());
		}else{
			//新下单为卖单，orderbook为买单
			buy_fee_amount = m_maker_fee * switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_user_account[buy_user]["fee_setting"]["maker"].asString());
			sell_fee_amount = m_taker_fee * switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_trade_list[i]["price"].asString()) * switch_s_to_f(m_user_account[sell_user]["fee_setting"]["taker"].asString());
		}
		
		if (m_user_account[buy_user]["funds"].isMember("KK")
			&& m_user_account[buy_user]["KK_switch"].asInt() == 1
			&& m_price_base_asset_KK > EPS
			&& switch_s_to_f(m_user_account[buy_user]["funds"]["KK"]["available"].asString()) > buy_fee_amount * m_price_base_asset_KK / 2){
			buy_fee_coin = "KK";
			buy_fee_amount = buy_fee_amount * m_price_base_asset_KK / 2;
		}
		if (m_user_account[sell_user]["funds"].isMember("KK")
			&& m_user_account[sell_user]["KK_switch"].asInt() == 1
			&& m_price_quote_asset_KK > EPS
			&& switch_s_to_f(m_user_account[sell_user]["funds"]["KK"]["available"].asString()) > sell_fee_amount * m_price_quote_asset_KK / 2){
			sell_fee_coin = "KK";
			sell_fee_amount = sell_fee_amount * m_price_quote_asset_KK / 2;
		}
		
		buy_fee_amount = switch_s_to_f(switch_f_to_s(buy_fee_amount));
		sell_fee_amount = switch_s_to_f(switch_f_to_s(sell_fee_amount));
		
		tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_FEE;
		tmp_obj["remark"] = "手续费";
		
		//买方手续费
		tmp_obj["user_id"] = atoi(buy_user.c_str());
		tmp_obj["asset"] = buy_fee_coin;
		tmp_obj["change_available"] = switch_f_to_s(-buy_fee_amount);
		tmp_obj["new_available"] = switch_f_to_s(switch_s_to_f(m_user_account[buy_user]["funds"][buy_fee_coin]["available"].asString()) - buy_fee_amount);;
		tmp_obj["change_frozen"] = switch_f_to_s(0);
		tmp_obj["new_frozen"] = m_user_account[buy_user]["funds"][buy_fee_coin]["frozen"].asString();
		tmp_obj["new_encode_available"] = HmacSha256Encode(tmp_obj["user_id"].asString() + buy_fee_coin + tmp_obj["new_available"].asString());
		tmp_obj["new_encode_frozen"] = HmacSha256Encode(tmp_obj["user_id"].asString() + buy_fee_coin + tmp_obj["new_frozen"].asString());
		
		m_user_account[buy_user]["funds"][buy_fee_coin]["available"] = tmp_obj["new_available"].asString();
		m_user_account[buy_user]["funds"][buy_fee_coin]["frozen"] = tmp_obj["new_frozen"].asString();
		m_user_account[buy_user]["funds"][buy_fee_coin]["encode_available"] = tmp_obj["new_encode_available"].asString();
		m_user_account[buy_user]["funds"][buy_fee_coin]["encode_frozen"] = tmp_obj["new_encode_frozen"].asString();
		
		m_order_result_list.append(tmp_obj);
		
		//卖方手续费
		tmp_obj["user_id"] = atoi(sell_user.c_str());
		tmp_obj["asset"] = sell_fee_coin;
		tmp_obj["change_available"] = switch_f_to_s(-sell_fee_amount);
		tmp_obj["new_available"] = switch_f_to_s(switch_s_to_f(m_user_account[sell_user]["funds"][sell_fee_coin]["available"].asString()) - sell_fee_amount);;
		tmp_obj["change_frozen"] = switch_f_to_s(0);
		tmp_obj["new_frozen"] = m_user_account[sell_user]["funds"][sell_fee_coin]["frozen"].asString();
		tmp_obj["new_encode_available"] = HmacSha256Encode(tmp_obj["user_id"].asString() + sell_fee_coin + tmp_obj["new_available"].asString());
		tmp_obj["new_encode_frozen"] = HmacSha256Encode(tmp_obj["user_id"].asString() + sell_fee_coin + tmp_obj["new_frozen"].asString());
		
		m_user_account[sell_user]["funds"][sell_fee_coin]["available"] = tmp_obj["new_available"].asString();
		m_user_account[sell_user]["funds"][sell_fee_coin]["frozen"] = tmp_obj["new_frozen"].asString();
		m_user_account[sell_user]["funds"][sell_fee_coin]["encode_available"] = tmp_obj["new_encode_available"].asString();
		m_user_account[sell_user]["funds"][sell_fee_coin]["encode_frozen"] = tmp_obj["new_encode_frozen"].asString();
		
		m_order_result_list.append(tmp_obj);
		
		Json::Value tmp_trade_obj =  Json::Value::null;
		tmp_trade_obj["type"] = "trade";
		tmp_trade_obj["id"] = m_trade_list[i]["id"].asString();
		tmp_trade_obj["order_id"] = m_order_id;
		tmp_trade_obj["user_id"] = m_order_user_id;
		tmp_trade_obj["amount"] = m_trade_list[i]["amount"].asString();
		tmp_trade_obj["quote_amount"] = switch_f_to_s(switch_s_to_f(m_trade_list[i]["price"].asString()) * switch_s_to_f(m_trade_list[i]["amount"].asString()));
		tmp_trade_obj["price"] = m_trade_list[i]["price"].asString();
		tmp_trade_obj["base_asset"] = m_order_base_asset;
		tmp_trade_obj["quote_asset"] = m_order_quote_asset;
		tmp_trade_obj["is_maker"] = 0;
		tmp_trade_obj["create_at"] = m_time_now;
		if (m_order_op == ORDER_SIDE_BUY){
			tmp_trade_obj["is_buy"] = 1;
			tmp_trade_obj["fee_asset"] = buy_fee_coin;
			tmp_trade_obj["fee_amount"] = switch_f_to_s(buy_fee_amount);
		}
		else{
			tmp_trade_obj["is_buy"] = 0;
			tmp_trade_obj["fee_asset"] = sell_fee_coin;
			tmp_trade_obj["fee_amount"] = switch_f_to_s(sell_fee_amount);
		}
		m_order_result_list.append(tmp_trade_obj);
		
		tmp_trade_obj["type"] = "trade";
		tmp_trade_obj["id"] = m_trade_list[i]["id"].asString();
		tmp_trade_obj["order_id"] = m_trade_list[i]["order_id"].asString();
		tmp_trade_obj["user_id"] = m_trade_list[i]["user_id"].asInt();
		tmp_trade_obj["amount"] = m_trade_list[i]["amount"].asString();
		tmp_trade_obj["quote_amount"] = switch_f_to_s(switch_s_to_f(m_trade_list[i]["price"].asString()) * switch_s_to_f(m_trade_list[i]["amount"].asString()));
		tmp_trade_obj["price"] = m_trade_list[i]["price"].asString();
		tmp_trade_obj["base_asset"] = m_order_base_asset;
		tmp_trade_obj["quote_asset"] = m_order_quote_asset;
		tmp_trade_obj["is_maker"] = 1;
		tmp_trade_obj["create_at"] = m_time_now;
		if (m_order_op == ORDER_SIDE_BUY){
			tmp_trade_obj["is_buy"] = 0;
			tmp_trade_obj["fee_asset"] = sell_fee_coin;
			tmp_trade_obj["fee_amount"] = switch_f_to_s(sell_fee_amount);
		}
		else{
			tmp_trade_obj["is_buy"] = 1;
			tmp_trade_obj["fee_asset"] = buy_fee_coin;
			tmp_trade_obj["fee_amount"] = switch_f_to_s(buy_fee_amount);
		}
		m_order_result_list.append(tmp_trade_obj);
		
		tmp_trade_obj =  Json::Value::null;
		tmp_trade_obj["id"] = m_trade_list[i]["id"].asString();
		tmp_trade_obj["price"] = m_trade_list[i]["price"].asString();
		tmp_trade_obj["amount"] = m_trade_list[i]["amount"].asString();
		tmp_trade_obj["quote_amount"] = switch_f_to_s(switch_s_to_f(m_trade_list[i]["price"].asString()) * switch_s_to_f(m_trade_list[i]["amount"].asString()));
		tmp_trade_obj["create_at"] = m_time_now;
		if (m_order_quote_asset == "bitCNY"){
			tmp_trade_obj["price_base_bitCNY"] = m_trade_list[i]["price"].asString();
		}else{
			tmp_trade_obj["price_base_bitCNY"] = switch_f_to_s(m_price_base_asset_bitCNY);
		}
		if (m_order_op == ORDER_SIDE_BUY){
			tmp_trade_obj["maker_is_buyer"] = 0;
		}else{
			tmp_trade_obj["maker_is_buyer"] = 1;
		}
		m_statistics["trade"]["list"].append(tmp_trade_obj);
	}
	if (trade_num > 0){
		char *redis_cmd;
		if (redisFormatCommand(&redis_cmd, "HSET last_trade_price %s_%s %s", m_order_base_asset.c_str(), m_order_quote_asset.c_str(), m_trade_list[trade_num - 1]["price"].asString().c_str()) <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		string redis_cmd_str = redis_cmd;
		free(redis_cmd);
		m_redis_cmd_list.push_back(redis_cmd_str);
	}
	
	LOG(INFO) << "m_order_id:" << m_order_id << " SettleTrade end";
	return true;
}

bool Match::InsertOrder(){
	LOG(INFO) << "m_order_id:" << m_order_id << " InsertOrder start";
	
	Json::StreamWriterBuilder writer;
	writer["indentation"] = "";
	Json::CharReaderBuilder rbuilder;
	std::unique_ptr<Json::CharReader> const reader(rbuilder.newCharReader());
	JSONCPP_STRING error;
	
	redisReply* reply = NULL;
	char *redis_cmd;
	char sortsetkey[128];
	
	if (m_order_type == ORDER_TYPE_LIMIT && m_remain_amount >= EPS){
		//如果单没有被吃完，添加到orderbook
		if (m_order_op == ORDER_SIDE_BUY){
			snprintf(sortsetkey, sizeof(sortsetkey), "order_book_%s_%s_buy", m_order_base_asset.c_str(), m_order_quote_asset.c_str());
		}else{
			snprintf(sortsetkey, sizeof(sortsetkey), "order_book_%s_%s_sell", m_order_base_asset.c_str(), m_order_quote_asset.c_str());
		}
		
		reply = (redisReply*) redisCommand(m_redis, "ZRANGEBYSCORE %s %.9f %.9f", sortsetkey, (double)m_order_price - EPS, (double)m_order_price + EPS);
		if (reply == NULL){
			LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
			redisFree(m_redis);
			m_redis = NULL;
			return false;
		}
		if (reply->type != REDIS_REPLY_ARRAY){
			LOG(ERROR) << "m_order_id:" << m_order_id << " redis type error:" << reply->type;
			freeReplyObject(reply);
			return false;
		}
		if (reply->elements == 0){
			freeReplyObject(reply);
			
			Json::Value temp_json = Json::Value::null;
			temp_json["price"] = m_order_price_str;
			
			Json::Value temp_order = Json::Value::null;
			temp_order["user_id"] = m_order_user_id;
			temp_order["order_id"] = m_order_id;
			temp_order["amount"] = switch_f_to_s(m_remain_amount);
			temp_order["origin_amount"] = m_order_amt_str;
			temp_order["executed_quote_amount"] = switch_f_to_s(m_executed_quote_amount);
			temp_json["orders"].append(temp_order);
			temp_json["total_amount"] = switch_f_to_s(m_remain_amount);
			
			string result = Json::writeString(writer, temp_json);
			if (redisFormatCommand(&redis_cmd, "ZADD %s %s %s", sortsetkey, m_order_price_str.c_str(), result.c_str()) <= 0){
				LOG(ERROR) << "redis format error";
				return false;
			}
			string redis_cmd_str = redis_cmd;
			free(redis_cmd);
			m_redis_cmd_list.push_back(redis_cmd_str);
			
			Json::Value temp_order_book = Json::Value::null;
			temp_order_book["amount"]  = temp_json["total_amount"].asString();
			temp_order_book["price"]  = m_order_price_str;
			if (m_order_op == ORDER_SIDE_BUY){
				m_statistics["order_book"]["buy"].append(temp_order_book);
			}else{
				m_statistics["order_book"]["sell"].append(temp_order_book);
			}
		}else if (reply->elements == 1){
			string temp_str = reply->element[0]->str;
			freeReplyObject(reply);
			Json::Value temp_json;
			
			bool ret = reader->parse(temp_str.c_str(), temp_str.c_str() + temp_str.size(), &temp_json, &error);
			if (!(ret && error.size() == 0)) {
				LOG(ERROR) << "json error";
				return false;
			}
			Json::Value temp_order = Json::Value::null;
			temp_order["user_id"] = m_order_user_id;
			temp_order["order_id"] = m_order_id;
			temp_order["amount"] = switch_f_to_s(m_remain_amount);
			temp_order["origin_amount"] = m_order_amt_str;
			temp_order["executed_quote_amount"] = switch_f_to_s(m_executed_quote_amount);
			temp_json["orders"].append(temp_order);
			temp_json["total_amount"] = switch_f_to_s(switch_s_to_f(temp_json["total_amount"].asString()) + m_remain_amount);
			
			string result = Json::writeString(writer, temp_json);
			if (redisFormatCommand(&redis_cmd, "ZREM %s %s", sortsetkey, temp_str.c_str()) <= 0){
				LOG(ERROR) << "redis format error";
				return false;
			}
			string redis_cmd_str = redis_cmd;
			free(redis_cmd);
			m_redis_cmd_list.push_back(redis_cmd_str);
			
			if (redisFormatCommand(&redis_cmd, "ZADD %s %s %s", sortsetkey, m_order_price_str.c_str(), result.c_str()) <= 0){
				LOG(ERROR) << "redis format error";
				return false;
			}
			redis_cmd_str = redis_cmd;
			free(redis_cmd);
			m_redis_cmd_list.push_back(redis_cmd_str);
			
			Json::Value temp_order_book = Json::Value::null;
			temp_order_book["amount"]  = temp_json["total_amount"].asString();
			temp_order_book["price"]  = m_order_price_str;
			if (m_order_op == ORDER_SIDE_BUY){
				m_statistics["order_book"]["buy"].append(temp_order_book);
			}else{
				m_statistics["order_book"]["sell"].append(temp_order_book);
			}
		}else{
			LOG(ERROR) << "m_order_id:" << m_order_id << " many price";
			freeReplyObject(reply);
			return false;
		}
	}
	LOG(INFO) << "m_order_id:" << m_order_id << " InsertOrder end";
	return true;
}

bool Match::WriteRedis(){
	LOG(INFO) << "m_order_id:" << m_order_id << " WriteRedis start";
	
	Json::StreamWriterBuilder writer;
	writer["indentation"] = "";
	char *redis_cmd;
	
	for (int i = 0; i < (int)m_trade_users_list.size(); i++){
		string result = Json::writeString(writer, m_user_account[to_string(m_trade_users_list[i])]["funds"]);
		if (redisFormatCommand(&redis_cmd, "SET account_user_%d %s", m_trade_users_list[i], result.c_str()) <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		string redis_cmd_str = redis_cmd;
		free(redis_cmd);
		m_redis_cmd_list.push_back(redis_cmd_str);
	}
	
	string result = Json::writeString(writer, m_order_result_list);
	string redis_cmd_str;
	if (redisFormatCommand(&redis_cmd, "LPUSH order_result_list %s", result.c_str()) <= 0){
		LOG(ERROR) << "redis format error";
		return false;
	}
	redis_cmd_str = redis_cmd;
	free(redis_cmd);
	m_redis_cmd_list.push_back(redis_cmd_str);
	
	for (int i = 0; i < (int)m_order_result_list.size(); i++){
		if (m_order_result_list[i]["type"].asString() == "order"){
			if (m_order_result_list[i]["amount"].asString() == switch_f_to_s(0) || m_order_result_list[i]["status"].asInt() == ORDER_STATUS_CANCELED){
				if (redisFormatCommand(&redis_cmd, "DEL order_detail_%s", m_order_result_list[i]["id"].asString().c_str()) <= 0){
					LOG(ERROR) << "redis format error";
					return false;
				}
			}else{
				if (redisFormatCommand(&redis_cmd, "HMSET order_detail_%s id %s user_id %d base_asset %s quote_asset %s m_order_type %d m_order_op %d price %s amount %s origin_amount %s executed_quote_amount %s", m_order_result_list[i]["id"].asString().c_str(), m_order_result_list[i]["id"].asString().c_str(), m_order_result_list[i]["user_id"].asInt(), m_order_result_list[i]["base_asset"].asString().c_str(), m_order_result_list[i]["quote_asset"].asString().c_str(), m_order_result_list[i]["order_type"].asInt(), m_order_result_list[i]["order_op"].asInt(), m_order_result_list[i]["price"].asString().c_str(), m_order_result_list[i]["amount"].asString().c_str(), m_order_result_list[i]["origin_amount"].asString().c_str(), m_order_result_list[i]["executed_quote_amount"].asString().c_str()) <= 0){
					LOG(ERROR) << "redis format error";
					return false;
				}
			}
			redis_cmd_str = redis_cmd;
			free(redis_cmd);
			m_redis_cmd_list.push_back(redis_cmd_str);
		}
	}
	
	result = Json::writeString(writer, m_statistics);
	m_trade->SendMessage(result);
	
	for (int i = 0; i < (int)m_redis_cmd_list.size(); i++){
		//LOG(INFO) << "redis cmd list " << m_redis_cmd_list[i];
	}
	
	redisAppendCommand(m_redis, "MULTI");
	for (int i = 0; i < (int)m_redis_cmd_list.size(); i++){
		redisAppendFormattedCommand(m_redis, m_redis_cmd_list[i].c_str(), m_redis_cmd_list[i].size());
	}
	redisAppendCommand(m_redis, "EXEC");
	redisReply* temp_reply = NULL;
	for (int i = 0; i < (int)m_redis_cmd_list.size() + 2; i++){
		redisGetReply(m_redis, (void**)&temp_reply);
		if (temp_reply == NULL) {
			redisFree(m_redis);
			m_redis = NULL;
			return false;
		}
		if (temp_reply->type == REDIS_REPLY_ERROR){
			LOG(ERROR) << "redis error:" << temp_reply->str;
		}
		freeReplyObject(temp_reply);
		temp_reply = NULL;
	}
	
	for (int i = 0; i < (int)m_trade_users_list.size(); i++){
		AccountUnlock(m_trade_users_list[i]);
	}
	
	LOG(INFO) << "m_order_id:" << m_order_id << " WriteRedis end";
	return true;
}

bool Match::GetOrderInfo(){
	redisReply* reply = NULL;
	
	reply = (redisReply*) redisCommand(m_redis, "HMGET order_detail_%s user_id base_asset quote_asset m_order_type m_order_op price amount origin_amount", m_order_id.c_str());
	if (reply == NULL){
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
		redisFree(m_redis);
		m_redis = NULL;
		return false;
	}
	if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 8){
		for (int i = 0; i < 8; i++){
			if (reply->element[0]->type == REDIS_REPLY_NIL){
				freeReplyObject(reply);
				return false;
			}
		}
		int order_user_id = atoi(reply->element[0]->str);
		if (order_user_id != m_order_user_id){
			LOG(ERROR) << "m_order_id:" << m_order_id << " user_id: " << order_user_id << " cancel user_id: " << m_order_user_id;
			freeReplyObject(reply);
			return false;
		}
		m_order_base_asset = reply->element[1]->str;
		m_order_quote_asset = reply->element[2]->str;
		m_order_type = atoi(reply->element[3]->str);
		m_order_op = atoi(reply->element[4]->str);
		m_order_price_str = reply->element[5]->str;
		m_order_price = strtold(m_order_price_str.c_str(), NULL);
		m_remain_amount = strtold(reply->element[6]->str, NULL);
		m_order_amt_str = reply->element[7]->str;
		m_order_amt = strtold(m_order_amt_str.c_str(), NULL);
		freeReplyObject(reply);
		return true;
	}else{
		freeReplyObject(reply);
		return false;
	}
}

bool Match::DeleteOrder(){
	LOG(INFO) << "m_order_id:" << m_order_id << " DeleteOrder start";
	
	Json::StreamWriterBuilder writer;
	writer["indentation"] = "";
	Json::CharReaderBuilder rbuilder;
	std::unique_ptr<Json::CharReader> const reader(rbuilder.newCharReader());
	JSONCPP_STRING error;
	
	redisReply* reply;
	char *redis_cmd;
	char sortsetkey[128];
	
	//在sortset里删除该订单
	if (m_order_op == ORDER_SIDE_BUY){
		snprintf(sortsetkey, sizeof(sortsetkey), "order_book_%s_%s_buy", m_order_base_asset.c_str(), m_order_quote_asset.c_str());
	}else{
		snprintf(sortsetkey, sizeof(sortsetkey), "order_book_%s_%s_sell", m_order_base_asset.c_str(), m_order_quote_asset.c_str());
	}
	
	reply = (redisReply*) redisCommand(m_redis, "ZRANGEBYSCORE %s %.9f %.9f", sortsetkey, (double)m_order_price - EPS, (double)m_order_price + EPS);
	if (reply == NULL){
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
		redisFree(m_redis);
		m_redis = NULL;
		return false;
	}
	if (reply->type != REDIS_REPLY_ARRAY){
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis type error:" << reply->type;
		freeReplyObject(reply);
		return false;
	}
	if (reply->elements == 0){
		LOG(ERROR) << "m_order_id:" << m_order_id << " no price";
		freeReplyObject(reply);
		return false;
	}else if (reply->elements == 1){
		string temp_str = reply->element[0]->str;
		freeReplyObject(reply);
		Json::Value temp_json;
		bool ret = reader->parse(temp_str.c_str(), temp_str.c_str() + temp_str.size(), &temp_json, &error);
		if (!(ret && error.size() == 0)) {
			LOG(ERROR) << "m_order_id:" << m_order_id << " json error";
			return false;
		}
		
		int pos = -1;
		for (int i = 0; i < (int)temp_json["orders"].size(); i++){
			if (temp_json["orders"][i]["order_id"].asString() == m_order_id){
				pos = i;
				m_executed_quote_amount = switch_s_to_f(temp_json["orders"][i]["executed_quote_amount"].asString());
				break;
			}
		}
		if (pos < 0){
			LOG(ERROR) << "m_order_id:" << m_order_id << " can not find in sortset";
			return false;
		}
		Json::Value delete_order;
		temp_json["orders"].removeIndex(pos, &delete_order);
		temp_json["total_amount"] = switch_f_to_s(switch_s_to_f(temp_json["total_amount"].asString()) - m_remain_amount);
		
		if (redisFormatCommand(&redis_cmd, "ZREM %s %s", sortsetkey, temp_str.c_str()) <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		string redis_cmd_str = redis_cmd;
		free(redis_cmd);
		m_redis_cmd_list.push_back(redis_cmd_str);
		
		if (temp_json["orders"].size() > 0){
			string result = Json::writeString(writer, temp_json);
			if (redisFormatCommand(&redis_cmd, "ZADD %s %s %s", sortsetkey, m_order_price_str.c_str(), result.c_str()) <= 0){
				LOG(ERROR) << "redis format error";
				return false;
			}
			redis_cmd_str = redis_cmd;
			free(redis_cmd);
			m_redis_cmd_list.push_back(redis_cmd_str);
		}
		
		Json::Value temp_order_book = Json::Value::null;
		temp_order_book["amount"]  = temp_json["total_amount"].asString();
		temp_order_book["price"]  = m_order_price_str;
		if (m_order_op == ORDER_SIDE_BUY){
			m_statistics["order_book"]["buy"].append(temp_order_book);
		}else{
			m_statistics["order_book"]["sell"].append(temp_order_book);
		}
	}else{
		LOG(ERROR) << "m_order_id:" << m_order_id << " many price";
		freeReplyObject(reply);
		return false;
	}
	
	Json::Value tmp_obj = Json::Value::null;
	tmp_obj["type"] = "order";
	tmp_obj["is_new"] = 0;
	tmp_obj["id"] = m_order_id;
	tmp_obj["user_id"] = m_order_user_id;
	tmp_obj["base_asset"] = m_order_base_asset;
	tmp_obj["quote_asset"] = m_order_quote_asset;
	tmp_obj["order_type"] = m_order_type;
	tmp_obj["order_op"] = m_order_op;
	tmp_obj["price"] = m_order_price_str;
	tmp_obj["amount"] = switch_f_to_s(m_remain_amount);
	tmp_obj["origin_amount"] = m_order_amt_str;
	tmp_obj["executed_quote_amount"] = switch_f_to_s(m_executed_quote_amount);
	tmp_obj["status"] = ORDER_STATUS_CANCELED;
	tmp_obj["update_at"] = m_time_now;
	
	m_order_result_list.append(tmp_obj);
	m_statistics["order"].append(tmp_obj);
	
	tmp_obj = Json::Value::null;
	
	tmp_obj["type"] = "account";
	tmp_obj["id"] = m_order_id;
	tmp_obj["user_id"] = m_order_user_id;
	tmp_obj["update_at"] = m_time_now;
	if (m_order_op == ORDER_SIDE_BUY){
		tmp_obj["asset"] = m_order_quote_asset;
		tmp_obj["change_available"] = switch_f_to_s(m_remain_amount * m_order_price);
		tmp_obj["change_frozen"] = switch_f_to_s(-m_remain_amount * m_order_price);
	}else{
		tmp_obj["asset"] = m_order_base_asset;
		tmp_obj["change_available"] = switch_f_to_s(m_remain_amount);
		tmp_obj["change_frozen"] = switch_f_to_s(-m_remain_amount);
	}
	tmp_obj["new_available"] = switch_f_to_s(switch_s_to_f(m_user_account[to_string(m_order_user_id)]["funds"][tmp_obj["asset"].asString()]["available"].asString()) +  switch_s_to_f(tmp_obj["change_available"].asString()));
	tmp_obj["new_frozen"] = switch_f_to_s(switch_s_to_f(m_user_account[to_string(m_order_user_id)]["funds"][tmp_obj["asset"].asString()]["frozen"].asString()) + switch_s_to_f(tmp_obj["change_frozen"].asString()));
	tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_CANCEL_ORDER;
	tmp_obj["remark"] = "取消订单解冻";
	tmp_obj["new_encode_available"] = HmacSha256Encode(tmp_obj["user_id"].asString() + tmp_obj["asset"].asString() + tmp_obj["new_available"].asString());
	tmp_obj["new_encode_frozen"] = HmacSha256Encode(tmp_obj["user_id"].asString() + tmp_obj["asset"].asString() + tmp_obj["new_frozen"].asString());
	
	m_user_account[to_string(m_order_user_id)]["funds"][tmp_obj["asset"].asString()]["available"] = tmp_obj["new_available"].asString();
	m_user_account[to_string(m_order_user_id)]["funds"][tmp_obj["asset"].asString()]["frozen"] = tmp_obj["new_frozen"].asString();
	m_user_account[to_string(m_order_user_id)]["funds"][tmp_obj["asset"].asString()]["encode_available"] = tmp_obj["new_encode_available"].asString();
	m_user_account[to_string(m_order_user_id)]["funds"][tmp_obj["asset"].asString()]["encode_frozen"] = tmp_obj["new_encode_frozen"].asString();
	
	m_order_result_list.append(tmp_obj);
	
	LOG(INFO) << "m_order_id:" << m_order_id << " DeleteOrder end";
	return true;
}

bool Match::NewOrder(){

	LOG(INFO)<< "NewOrder!!!!!!!" ;
	
	PreLock();
	if (!PrepareForOrder()) return false;
	
	if (!MatchOrder()) return false;
	
	if (!QueryUserAccount()) return false;
	PreUnlock();
	
	if (!Frozen()) return false;
	
	if (!SettleTrade()) return false;
	
	if (!InsertOrder()) return false;
	
	if (!WriteRedis()) return false;
	
	return true;
}

bool Match::CancelOrder(){
	LOG(INFO) << "m_order_id:" << m_order_id << " cancel start";
	
	m_trade_users_set.insert(m_order_user_id);
	
	AccountLock(m_order_user_id);
	if (!QueryUserAccount()) return false;
	
	if (!DeleteOrder()) return false;
	
	if (!WriteRedis()) return false;
	
	LOG(INFO) << "m_order_id:" << m_order_id << " cancel end";
	return true;
}

bool Match::Msg(string order_str){
	LOG(INFO) << order_str;
	
	if (m_redis == NULL) {
		Config config;
		const Json::Value redis_config = config["redis"];
		const Json::Value sentinel_config = redis_config["sentinels"];
		std::vector<std::pair<std::string, int> > sentinels;
		for (unsigned i = 0; i < sentinel_config.size(); ++i) {
			sentinels.push_back(std::make_pair(sentinel_config[i]["host"].asString(), sentinel_config[i]["port"].asInt()));
		}
		std::string encode_password = redis_config["password"].asString();
		std::string password = real_password(encode_password);

		//m_redis = SentinelRedisConnect(sentinels, redis_config["master_name"].asCString(), password.c_str(), redis_config["database"].asInt());
		for (auto it = sentinels.begin(); it != sentinels.end(); ++it) {
            //m_redis = redisConnect(it->first.c_str(), it->second) ; 
            m_redis = redis_connect(it->first.c_str(), it->second, password.c_str()) ;          
        }

        if (m_redis == NULL) {
            LOG(ERROR) << "m_redis connect faild";
            exit(1);
        }else{
            //std::cout << "m_redis connect ok" << std::endl;
            LOG(INFO) << "m_redis connect ok";
        }
	}
	
	if (m_stat_redis == NULL) {
		Config config;
		const Json::Value redis_config = config["statRedis"];
		const Json::Value sentinel_config = redis_config["sentinels"];
		std::vector<std::pair<std::string, int> > sentinels;
		for (unsigned i = 0; i < sentinel_config.size(); ++i) {
			sentinels.push_back(std::make_pair(sentinel_config[i]["host"].asString(), sentinel_config[i]["port"].asInt()));
		}
		std::string encode_password = redis_config["password"].asString();
		std::string password = real_password(encode_password);

		//m_stat_redis = SentinelRedisConnect(sentinels, redis_config["master_name"].asCString(), password.c_str(), redis_config["database"].asInt());
		for (auto it = sentinels.begin(); it != sentinels.end(); ++it) {
            //m_stat_redis = redisConnect(it->first.c_str(), it->second) ;
        	m_stat_redis = redis_connect(it->first.c_str(), it->second, password.c_str()) ; 
        }

        if (m_stat_redis == NULL) {
            LOG(ERROR) << "m_stat_redis connect faild";
            exit(1);
        }else{
            //std::cout << "m_stat_redis connect ok" << std::endl;
            LOG(INFO) << "m_stat_redis connect ok";
        }
	}

	if (m_index_redis == NULL) {
		Config config;
		const Json::Value redis_config = config["indexRedis"];
		const Json::Value sentinel_config = redis_config["sentinels"];
		std::vector<std::pair<std::string, int> > sentinels;
		for (unsigned i = 0; i < sentinel_config.size(); ++i) {
			sentinels.push_back(std::make_pair(sentinel_config[i]["host"].asString(), sentinel_config[i]["port"].asInt()));
		}
		std::string encode_password = redis_config["password"].asString();
		std::string password = real_password(encode_password);

		//m_index_redis = SentinelRedisConnect(sentinels, redis_config["master_name"].asCString(), password.c_str(), redis_config["database"].asInt());
		for (auto it = sentinels.begin(); it != sentinels.end(); ++it) {
            //m_index_redis = redisConnect(it->first.c_str(), it->second) ; 
            m_index_redis = redis_connect(it->first.c_str(), it->second, password.c_str()) ;          
        }

        if (m_index_redis == NULL) {
            LOG(ERROR) << "m_index_redis connect faild";
            exit(1);
        }else{
            //std::cout << "m_index_redis connect ok" << std::endl;
            LOG(INFO) << "m_index_redis connect ok";
        }
	}

	Reposition();
	if (!InitOrder(order_str)) return false;
	bool res;
	if (m_msg_type == "order"){
		LOG(INFO) << "  m_msg_type == order";
		res = NewOrder();
	}else if (m_msg_type == "cancel"){
		res = CancelOrder();
	}else{
		res = false;
	}
	AllUnlock();
	return res;
}