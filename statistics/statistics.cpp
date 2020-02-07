#include "statistics.h"
#include "trade.h"
#include <memory>
#include "utils.h"
#include "redis_utils.h"

string switch_f_to_s(long double f){
	char st[1024];
	snprintf(st, sizeof(st), "%.*Lf", VALUE_DECIMAL, f);
	string ans = st;
	return ans;
}

long double switch_s_to_f(string s){
	return strtold(s.c_str(), NULL);
}

Statistics::Statistics(Trade* trade) {
	m_trade = trade;
	Init();
}

Statistics::~Statistics() {
}

bool Statistics::Init() {
	Config config;
	m_server_id = config["server_id"].asInt() || 0;
	m_ticker_last_time = 0;

	{
		Json::Value redis_config = config["redis"];
		const Json::Value sentinel_config = redis_config["sentinels"];
		std::vector<std::pair<std::string, int> > sentinels;
		for (unsigned i = 0; i < sentinel_config.size(); ++i) {
			sentinels.push_back(std::make_pair(sentinel_config[i]["host"].asString(), sentinel_config[i]["port"].asInt()));
			LOG(ERROR) << "Init redis ip: " << sentinel_config[i]["host"].asString();
			LOG(ERROR) << "Init redis port: " << sentinel_config[i]["port"].asInt();
		}
		std::string encode_password = redis_config["password"].asString();
		//std::string password = real_password(encode_password);
		std::string password = encode_password;
		LOG(ERROR) << "Init redis password: " << password;
		

		//m_redis = SentinelRedisConnect(sentinels, redis_config["master_name"].asCString(), password.c_str(), redis_config["database"].asInt());
		for (auto it = sentinels.begin(); it != sentinels.end(); ++it) {
            //m_redis = redisConnect(it->first.c_str(), it->second) ; 
            m_redis = redis_connect(it->first.c_str(), it->second, password.c_str()) ;          
        }

        if (m_redis == NULL) {
            LOG(ERROR) << "m_redis connect faild";
            exit(1);
        }else{
            LOG(ERROR) << "m_redis connect ok";
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
            LOG(ERROR) << "m_stat_redis connect ok";
        }
	}

	LOG(ERROR) << "redis ok";

    return true;
}

bool Statistics::Reposition(){
	m_statistic_json = Json::Value::null;
	m_redis_cmd_list.clear();
	return true;
}

bool Statistics::InitStatistics(string statistics_str){
	Json::CharReaderBuilder rbuilder;
	std::unique_ptr<Json::CharReader> const reader(rbuilder.newCharReader());
	JSONCPP_STRING error;
	bool ret = reader->parse(statistics_str.c_str(), statistics_str.c_str() + statistics_str.size(), &m_statistic_json, &error);
	if (!(ret && error.size() == 0)) {
		LOG(ERROR) << "json error";
		return false;
	}
	m_base_asset = m_statistic_json["order_book"]["base_asset"].asString();
	m_quote_asset = m_statistic_json["order_book"]["quote_asset"].asString();
	
	return true;
}

bool Statistics::UpdateOneKline(string sortset_key, int time_interval){
	Json::CharReaderBuilder rbuilder;
	std::unique_ptr<Json::CharReader> const reader(rbuilder.newCharReader());
	JSONCPP_STRING error;
	redisReply* reply;
	reply = (redisReply*) redisCommand(m_stat_redis, "ZRANGE %s -1 -1", sortset_key.c_str());
	if (reply == NULL){
		LOG(ERROR) << "redis reply null";
		redisFree(m_stat_redis);
		m_stat_redis = NULL;
		return false;
	}
	if (reply->type != REDIS_REPLY_ARRAY){
		LOG(ERROR) << " redis type error:" << reply->type;
		freeReplyObject(reply);
		return false;
	}
	string pre_str;
	Json::Value pre_json = Json::Value::null;
	Json::Value new_json = Json::Value::null;
	if (reply->elements == 0){
		new_json["base_asset"] = m_base_asset;
		new_json["quote_asset"] = m_quote_asset;
		if (time_interval == 7 * 24 * 60 * 60){
			new_json["time"] = ((m_time_trade - 86400 * 4) / (86400 * 7) + 1) * 86400 * 7 - 3 * 86400;
		}else{
			new_json["time"] = (m_time_trade / time_interval) * time_interval;
		}
		new_json["open"] = switch_f_to_s(m_first_price);
		new_json["close"] = switch_f_to_s(m_last_price);
		new_json["high"] = switch_f_to_s(m_high_price);
		new_json["low"] = switch_f_to_s(m_low_price);
		new_json["amount"] = switch_f_to_s(m_amount);
		new_json["quote_amount"] = switch_f_to_s(m_quote_amount);
		new_json["interval"] = time_interval;
		new_json["update_at"] = m_time_trade;
	}else{
		pre_str = reply->element[0]->str;
		bool ret = reader->parse(pre_str.c_str(), pre_str.c_str() + pre_str.size(), &pre_json, &error);
		if (!(ret && error.size() == 0)) {
			LOG(ERROR) << "json error";
			freeReplyObject(reply);
			return false;
		}
		int pre_time = pre_json["time"].asInt();
		if (pre_time + time_interval <= m_time_trade) {
			new_json["time"] = pre_time + (m_time_trade - pre_time) / time_interval * time_interval;
			new_json["base_asset"] = m_base_asset;
			new_json["quote_asset"] = m_quote_asset;
			new_json["open"] = switch_f_to_s(m_first_price);
			new_json["close"] = switch_f_to_s(m_last_price);
			new_json["high"] = switch_f_to_s(m_high_price);
			new_json["low"] = switch_f_to_s(m_low_price);
			new_json["amount"] = switch_f_to_s(m_amount);
			new_json["quote_amount"] = switch_f_to_s(m_quote_amount);
			new_json["interval"] = time_interval;
			new_json["update_at"] = m_time_trade;
		}else{
			pre_json["close"] = switch_f_to_s(m_last_price);
			if (m_high_price > switch_s_to_f(pre_json["high"].asString()))
				pre_json["high"] = switch_f_to_s(m_high_price);
			if (m_low_price < switch_s_to_f(pre_json["low"].asString()))
				pre_json["low"] = switch_f_to_s(m_low_price);
			pre_json["amount"] = switch_f_to_s(switch_s_to_f(pre_json["amount"].asString()) + m_amount);
			pre_json["quote_amount"] = switch_f_to_s(switch_s_to_f(pre_json["quote_amount"].asString()) + m_quote_amount);
			pre_json["update_at"] = m_time_trade;
		}
	}
	freeReplyObject(reply);
	
	Json::StreamWriterBuilder writer;
	writer["indentation"] = "";
	string result;
	char *redis_cmd;
	if (new_json != Json::Value::null){
		result = Json::writeString(writer, new_json);
		if (redisFormatCommand(&redis_cmd, "ZADD %s %d %s", sortset_key.c_str(), new_json["time"].asInt(), result.c_str()) <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		string tmp_str = redis_cmd;
		free(redis_cmd);
		m_redis_cmd_list.push_back(tmp_str);
		m_trade->SendMessage(result, "kline");
	}else{
		if (redisFormatCommand(&redis_cmd, "ZREM %s %s", sortset_key.c_str(), pre_str.c_str()) <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		string tmp_str = redis_cmd;
		free(redis_cmd);
		m_redis_cmd_list.push_back(tmp_str);
		
		result = Json::writeString(writer, pre_json);
		if (redisFormatCommand(&redis_cmd, "ZADD %s %d %s", sortset_key.c_str(), pre_json["time"].asInt(), result.c_str()) <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		tmp_str = redis_cmd;
		free(redis_cmd);
		m_redis_cmd_list.push_back(tmp_str);
		m_trade->SendMessage(result, "kline");
	}
	
	return true;
}

bool Statistics::StatisticsKline(){
	int trade_num = m_statistic_json["trade"]["list"].size();
	if (trade_num <= 0){
		LOG(ERROR) << "no trade";
		return false;
	}
	m_time_trade = m_statistic_json["trade"]["list"][0]["create_at"].asInt();
	m_first_price = switch_s_to_f(m_statistic_json["trade"]["list"][0]["price"].asString());
	m_last_price = switch_s_to_f(m_statistic_json["trade"]["list"][trade_num - 1]["price"].asString());
	m_high_price = m_first_price;
	m_low_price = m_first_price;
	m_amount = 0L;
	m_quote_amount = 0L;
	for (int i = 0; i < trade_num; i++){
		long double tmp_price = switch_s_to_f(m_statistic_json["trade"]["list"][i]["price"].asString());
		if (tmp_price < m_low_price) m_low_price = tmp_price;
		if (tmp_price > m_high_price) m_high_price = tmp_price;
		m_amount = m_amount + switch_s_to_f(m_statistic_json["trade"]["list"][i]["amount"].asString());
		m_quote_amount = m_quote_amount + switch_s_to_f(m_statistic_json["trade"]["list"][i]["quote_amount"].asString());
	}
	
	string tmp_str;
	tmp_str = "kline_1m_" + m_base_asset + '_' + m_quote_asset;
	UpdateOneKline(tmp_str, 60);
	tmp_str = "kline_5m_" + m_base_asset + '_' + m_quote_asset;
	UpdateOneKline(tmp_str, 5 * 60);
	tmp_str = "kline_15m_" + m_base_asset + '_' + m_quote_asset;
	UpdateOneKline(tmp_str, 15 * 60);
	tmp_str = "kline_30m_" + m_base_asset + '_' + m_quote_asset;
	UpdateOneKline(tmp_str, 30 * 60);
	tmp_str = "kline_1h_" + m_base_asset + '_' + m_quote_asset;
	UpdateOneKline(tmp_str, 60 * 60);
	tmp_str = "kline_2h_" + m_base_asset + '_' + m_quote_asset;
	UpdateOneKline(tmp_str, 2 * 60 * 60);
	tmp_str = "kline_4h_" + m_base_asset + '_' + m_quote_asset;
	UpdateOneKline(tmp_str, 4 * 60 * 60);
	tmp_str = "kline_6h_" + m_base_asset + '_' + m_quote_asset;
	UpdateOneKline(tmp_str, 6 * 60 * 60);
	tmp_str = "kline_12h_" + m_base_asset + '_' + m_quote_asset;
	UpdateOneKline(tmp_str, 12 * 60 * 60);
	tmp_str = "kline_1d_" + m_base_asset + '_' + m_quote_asset;
	UpdateOneKline(tmp_str, 24 * 60 * 60);
	tmp_str = "kline_1w_" + m_base_asset + '_' + m_quote_asset;
	UpdateOneKline(tmp_str, 7 * 24 * 60 * 60);
	
	return true;
}

bool Statistics::UpdateOneOrder(string sortset_key, string opposite_key, Json::Value order_json, int order_op){
	long double price_f = switch_s_to_f(order_json["price"].asString());
	string price_str = switch_f_to_s(price_f);
	
	char *redis_cmd;
	redisReply* reply;
	
	if (switch_s_to_f(order_json["amount"].asString()) > EPS){
		if (order_op == ORDER_SIDE_BUY){
			reply = (redisReply*) redisCommand(m_stat_redis, "ZRANGEBYSCORE %s -inf %.9f", opposite_key.c_str(), (double)price_f + EPS);
		}else{
			reply = (redisReply*) redisCommand(m_stat_redis, "ZREVRANGEBYSCORE %s +inf %.9f", opposite_key.c_str(), (double)price_f - EPS);
		}
		if (reply == NULL){
			LOG(ERROR) << "redis reply null";
			redisFree(m_stat_redis);
			m_stat_redis = NULL;
			return false;
		}
		if (reply->type != REDIS_REPLY_ARRAY && reply->type != REDIS_REPLY_NIL){
			LOG(ERROR) << " redis type error:" << reply->type;
			freeReplyObject(reply);
			return false;
		}
		
		if (reply->type == REDIS_REPLY_ARRAY) {
			int len = reply->elements;
			for (int i = 0; i < len; i++){
				string l_sortset_str = reply->element[i]->str;
				if (redisFormatCommand(&redis_cmd, "ZREM %s %s", opposite_key.c_str(), l_sortset_str.c_str()) <= 0){
					LOG(ERROR) << "redis format error";
					freeReplyObject(reply);
					return false;
				}
				string tmp_str = redis_cmd;
				free(redis_cmd);
				m_redis_cmd_list.push_back(tmp_str);
			}
		}
		freeReplyObject(reply);
	}
	
	reply = (redisReply*) redisCommand(m_stat_redis, "ZRANGEBYSCORE %s %.9f %.9f", sortset_key.c_str(), (double)price_f - EPS, (double)price_f + EPS);
	if (reply == NULL){
		LOG(ERROR) << "redis reply null";
		redisFree(m_stat_redis);
		m_stat_redis = NULL;
		return false;
	}
	if (reply->type != REDIS_REPLY_ARRAY){
		LOG(ERROR) << " redis type error:" << reply->type;
		freeReplyObject(reply);
		return false;
	}
	
	for (int i = 0; i < (int)reply->elements; i++){
		string tmp_member = reply->element[i]->str;
		
		if (redisFormatCommand(&redis_cmd, "ZREM %s %s", sortset_key.c_str(), tmp_member.c_str()) <= 0){
			LOG(ERROR) << "redis format error";
			freeReplyObject(reply);
			return false;
		}
		string tmp_str = redis_cmd;
		free(redis_cmd);
		m_redis_cmd_list.push_back(tmp_str);
	}
	freeReplyObject(reply);
	Json::Value new_order = Json::Value::null;
	new_order["price"] = price_str;
	new_order["amount"] = order_json["amount"].asString();
	
	if (new_order["amount"].asString() != switch_f_to_s(0)){
		Json::StreamWriterBuilder writer;
		writer["indentation"] = "";
		string result = Json::writeString(writer, new_order);
		
		if (redisFormatCommand(&redis_cmd, "ZADD %s %s %s", sortset_key.c_str(), price_str.c_str(), result.c_str()) <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		string tmp_str = redis_cmd;
		free(redis_cmd);
		m_redis_cmd_list.push_back(tmp_str);
	}
	return true;
}

bool Statistics::StatisticsOrderbook(){
	if (!m_statistic_json.isMember("order_book")){
		LOG(ERROR) << "no order change";
		return false;
	}
	
	string tmp_buy_key = "order_book_simple_" + m_base_asset + "_" + m_quote_asset + "_buy";
	string tmp_sell_key = "order_book_simple_" + m_base_asset + "_" + m_quote_asset + "_sell";
	for (int i = 0; i < (int)m_statistic_json["order_book"]["buy"].size(); i++){
		UpdateOneOrder(tmp_buy_key, tmp_sell_key, m_statistic_json["order_book"]["buy"][i], ORDER_SIDE_BUY);
	}
	for (int i = 0; i < (int)m_statistic_json["order_book"]["sell"].size(); i++){
		UpdateOneOrder(tmp_sell_key, tmp_buy_key, m_statistic_json["order_book"]["sell"][i], ORDER_SIDE_SELL);
	}
	
	Json::StreamWriterBuilder writer;
	writer["indentation"] = "";
	string result = Json::writeString(writer, m_statistic_json["order_book"]);
	m_trade->SendMessage(result, "order_book");
	
	return true;
}

bool Statistics::SendOneTicker(string pair_key){
	int pos = -1;
	for (int i = 0; i < (int)pair_key.size(); i++){
		if (pair_key[i] == '_'){
			pos = i;
			break;
		}
	}
	if (pos < 0){
		LOG(ERROR) << "pair_key error : " << pair_key;
		return false;
	}
	m_base_asset = pair_key.substr(0, pos);
	m_quote_asset = pair_key.substr(pos + 1);
	
	redisReply* reply;
	reply = (redisReply*) redisCommand(m_stat_redis, "HGET trade_config_%s state", pair_key.c_str());
	if (reply == NULL){
		LOG(ERROR) << "redis reply null";
		redisFree(m_stat_redis);
		m_stat_redis = NULL;
		return false;
	}
	if (reply->type != REDIS_REPLY_STRING){
		LOG(ERROR) << " redis type error:" << reply->type;
		freeReplyObject(reply);
		return false;
	}
	int pair_state = atoi(reply->str);
	if (pair_state != 1){
		LOG(INFO) << " pair_state error:" << pair_state;
		freeReplyObject(reply);
		return false;
	}
	freeReplyObject(reply);
	
	long double open_price = 0L;
	int open_timestamp = 0;
	reply = (redisReply*) redisCommand(m_stat_redis, "HMGET trade_config_%s open_price open_timestamp", pair_key.c_str());
	if (reply == NULL){
		LOG(ERROR) << "redis reply null";
		redisFree(m_stat_redis);
		m_stat_redis = NULL;
		return false;
	}
	if (reply->type != REDIS_REPLY_ARRAY && reply->type != REDIS_REPLY_NIL){
		LOG(ERROR) << " redis type error:" << reply->type;
		freeReplyObject(reply);
		return false;
	}
	if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 2){
		if (reply->element[0]->type == REDIS_REPLY_STRING && reply->element[1]->type == REDIS_REPLY_STRING){
			open_price = strtold(reply->element[0]->str, NULL);
			open_timestamp = atoi(reply->element[1]->str);
		}
	}
	freeReplyObject(reply);
	
	//get all kline_data
	int time_now = time(0);
	string kline_key = "kline_1m_" + pair_key;
	
	reply = (redisReply*) redisCommand(m_stat_redis, "ZRANGEBYSCORE %s %d +inf", kline_key.c_str(), time_now - 86400);
	if (reply == NULL){
		LOG(ERROR) << "redis reply null";
		redisFree(m_stat_redis);
		m_stat_redis = NULL;
		return false;
	}
	if (reply->type != REDIS_REPLY_ARRAY){
		LOG(ERROR) << " redis type error:" << reply->type;
		freeReplyObject(reply);
		return false;
	}
	if (reply->elements <= 0){
		LOG(ERROR) << " redis array null";
		freeReplyObject(reply);
		return false;
	}
	
	//push kline_list 
	vector<Json::Value> kline_list;
	kline_list.clear();
	Json::CharReaderBuilder rbuilder;
	std::unique_ptr<Json::CharReader> const reader(rbuilder.newCharReader());
	JSONCPP_STRING error;
	for (int i = 0; i < (int)reply->elements; i++){
		Json::Value tmp_kline_json;
		string		tmp_kline_str = reply->element[i]->str;
		bool ret = reader->parse(tmp_kline_str.c_str(), tmp_kline_str.c_str() + tmp_kline_str.size(), &tmp_kline_json, &error);
		if (!(ret && error.size() == 0)) {
			freeReplyObject(reply);
			LOG(ERROR) << "json error";
			return false;
		}
		kline_list.push_back(tmp_kline_json);
	}
	freeReplyObject(reply);
	int len = kline_list.size();
	
	int tmp_time = kline_list[len - 1]["update_at"].asInt();
	if ((time_now - tmp_time) % TICKER_INTERVAL_MAX > TICKER_INTERVAL_MIN + 1){
		LOG(ERROR) << "this ticker no trade return";
		return false;
	}
	
	//CAL value
	long double price_pre = 0L;
	if (time_now - open_timestamp < 24 * 60 * 60){
		price_pre = open_price;
	}else{
		price_pre = switch_s_to_f(kline_list[0]["open"].asString());
	}
	long double price_now = switch_s_to_f(kline_list[len - 1]["close"].asString());
	long double price_change_amount = price_now - price_pre;
	long double price_change_percent;
	long double base_amount = 0L;
	long double quote_amount = 0L;
	if (price_pre > EPS) {
		price_change_percent = price_change_amount / price_pre;
	}else{
		price_change_percent = 0L;
	}
	long double price_high = price_pre;
	long double price_low = price_pre;
	for (int i = 0; i < len; i++){
		long double tmp_price_high = switch_s_to_f(kline_list[i]["high"].asString());
		long double tmp_price_low = switch_s_to_f(kline_list[i]["low"].asString());
		if (tmp_price_high > price_high) price_high = tmp_price_high;
		if (tmp_price_low < price_low) price_low = tmp_price_low;
		base_amount += switch_s_to_f(kline_list[i]["amount"].asString());
		quote_amount += switch_s_to_f(kline_list[i]["quote_amount"].asString());
	}
	
	string bid = switch_f_to_s(0L);
	string bid_amount = switch_f_to_s(0L);
	
	reply = (redisReply*) redisCommand(m_stat_redis, "ZRANGE order_book_simple_%s_%s_buy -1 -1", m_base_asset.c_str(), m_quote_asset.c_str());
	if (reply == NULL){
		LOG(ERROR) << "redis reply null";
		redisFree(m_stat_redis);
		m_stat_redis = NULL;
		return false;
	}
	if (reply->type != REDIS_REPLY_ARRAY){
		LOG(ERROR) << " redis type error:" << reply->type;
		freeReplyObject(reply);
		return false;
	}
	if (reply->elements == 1){
		Json::Value tmp_kline_json;
		string tmp_kline_str = reply->element[0]->str;
		freeReplyObject(reply);
		bool ret = reader->parse(tmp_kline_str.c_str(), tmp_kline_str.c_str() + tmp_kline_str.size(), &tmp_kline_json, &error);
		if (!(ret && error.size() == 0)) {
			LOG(ERROR) << "json error";
			return false;
		}
		bid = tmp_kline_json["price"].asString();
		bid_amount = tmp_kline_json["amount"].asString();
	}else{
		freeReplyObject(reply);
	}
	
	string ask = switch_f_to_s(0L);
	string ask_amount = switch_f_to_s(0L);
	
	reply = (redisReply*) redisCommand(m_stat_redis, "ZRANGE order_book_simple_%s_%s_sell 0 0", m_base_asset.c_str(), m_quote_asset.c_str());
	if (reply == NULL){
		LOG(ERROR) << "redis reply null";
		redisFree(m_stat_redis);
		m_stat_redis = NULL;
		return false;
	}
	if (reply->type != REDIS_REPLY_ARRAY){
		LOG(ERROR) << " redis type error:" << reply->type;
		freeReplyObject(reply);
		return false;
	}
	if (reply->elements == 1){
		Json::Value tmp_kline_json;
		string tmp_kline_str = reply->element[0]->str;
		freeReplyObject(reply);
		bool ret = reader->parse(tmp_kline_str.c_str(), tmp_kline_str.c_str() + tmp_kline_str.size(), &tmp_kline_json, &error);
		if (!(ret && error.size() == 0)) {
			LOG(ERROR) << "json error";
			return false;
		}
		ask = tmp_kline_json["price"].asString();
		ask_amount = tmp_kline_json["amount"].asString();
	}else{
		freeReplyObject(reply);
	}
	
	long double RATE_USD_CNY = 6.5L;
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
		RATE_USD_CNY = switch_s_to_f(tmp_str);
	}
	freeReplyObject(reply);
	
	long double price_base_bitCNY = 0L;
	long double price_ETH_bitCNY = 0L;
	long double price_ETH_USDT = 0L;
	long double price_ETH_quote = 0L;
	reply = (redisReply*) redisCommand(m_redis, "HMGET last_trade_price ETH_bitCNY ETH_USDT ETH_%s", m_quote_asset.c_str());
	if (reply == NULL){
		LOG(ERROR) << "redis reply null";
		redisFree(m_redis);
		m_redis = NULL;
		return false;
	}
	if (reply->type != REDIS_REPLY_ARRAY){
		LOG(ERROR) << " redis type error:" << reply->type;
		freeReplyObject(reply);
		return false;
	}
	if (reply->elements == 3){
		if (reply->element[0]->type == REDIS_REPLY_STRING){
			price_ETH_bitCNY = strtold(reply->element[0]->str, NULL);
		}
		if (reply->element[1]->type == REDIS_REPLY_STRING){
			price_ETH_USDT = strtold(reply->element[1]->str, NULL);
		}
		if (reply->element[2]->type == REDIS_REPLY_STRING){
			price_ETH_quote = strtold(reply->element[2]->str, NULL);
		}
	}
	freeReplyObject(reply);
	if (m_quote_asset == "bitCNY"){
		price_base_bitCNY = price_now;
	}else if (m_quote_asset == "ETH"){
		if (price_ETH_USDT > EPS){
			price_base_bitCNY = price_now * price_ETH_USDT * RATE_USD_CNY;
		}else if (price_ETH_bitCNY > EPS){
			price_base_bitCNY = price_now * price_ETH_bitCNY;
		}
	}else if (m_quote_asset == "USC" || m_quote_asset == "USDT"){
		price_base_bitCNY = price_now * RATE_USD_CNY;
	}else if (price_ETH_USDT > EPS && price_ETH_quote > EPS){
		price_base_bitCNY = price_now / price_ETH_quote * price_ETH_USDT * RATE_USD_CNY;
	}else if (price_ETH_bitCNY > EPS && price_ETH_quote > EPS){
		price_base_bitCNY = price_now / price_ETH_quote * price_ETH_bitCNY;
	}
	
	Json::Value ticker_json = Json::Value::null;
	ticker_json["price_now"] = switch_f_to_s(price_now);
	ticker_json["base_asset"] = m_base_asset;
	ticker_json["quote_asset"] = m_quote_asset;
	ticker_json["price_change_amount"] = switch_f_to_s(price_change_amount);
	ticker_json["price_change_percent"] = switch_f_to_s(price_change_percent);
	ticker_json["price_high"] = switch_f_to_s(price_high);
	ticker_json["price_low"] = switch_f_to_s(price_low);
	ticker_json["base_amount"] = switch_f_to_s(base_amount);
	ticker_json["quote_amount"] = switch_f_to_s(quote_amount);
	ticker_json["bid"] = bid;
	ticker_json["bid_amount"] = bid_amount;
	ticker_json["ask"] = ask;
	ticker_json["ask_amount"] = ask_amount;
	ticker_json["price_base_bitCNY"] = switch_f_to_s(price_base_bitCNY);
	
	Json::StreamWriterBuilder writer;
	writer["indentation"] = "";
	string result = Json::writeString(writer, ticker_json);
	m_trade->SendMessage(result, "ticker");
	
	reply = (redisReply*) redisCommand(m_stat_redis, "HMSET ticker_%s_%s price_now %s price_change_amount %s price_change_percent %s price_high %s price_low %s base_amount %s quote_amount %s bid %s bid_amount %s ask %s ask_amount %s price_base_bitCNY %s", m_base_asset.c_str(), m_quote_asset.c_str(), ticker_json["price_now"].asString().c_str(), ticker_json["price_change_amount"].asString().c_str(), ticker_json["price_change_percent"].asString().c_str(), ticker_json["price_high"].asString().c_str(), ticker_json["price_low"].asString().c_str(), ticker_json["base_amount"].asString().c_str(), ticker_json["quote_amount"].asString().c_str(), ticker_json["bid"].asString().c_str(), ticker_json["bid_amount"].asString().c_str(), ticker_json["ask"].asString().c_str(), ticker_json["ask_amount"].asString().c_str(), ticker_json["price_base_bitCNY"].asString().c_str());
	if (reply == NULL) {
		redisFree(m_stat_redis);
		m_stat_redis = NULL;
		return false;
	}
	freeReplyObject(reply);

	reply = (redisReply*) redisCommand(m_stat_redis, "HSET asset_usd_value %s %s", m_base_asset.c_str(), switch_f_to_s(price_base_bitCNY / RATE_USD_CNY).c_str());
	if (reply == NULL) {
		redisFree(m_stat_redis);
		m_stat_redis = NULL;
		return false;
	}
	freeReplyObject(reply);
	
	return true;
}

bool Statistics::SendAllTicker(){
	if (m_server_id != 1) return false;

	if (m_stat_redis == NULL) {
		Config config;
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
			LOG(ERROR) << "m_redis connect faild";
			exit(1);
		}else{
			LOG(ERROR) << "m_redis connect ok";
		}

	}

	int time_now = time(0);
	if (time_now - m_ticker_last_time < TICKER_INTERVAL_MIN) return false;
	m_ticker_last_time = time_now;
	
	redisReply* reply;
	reply = (redisReply*) redisCommand(m_stat_redis, "SMEMBERS trade_pair_set");
	if (reply == NULL){
		LOG(ERROR) << " SMEMBERS trade_pair_set  redis reply null";
		redisFree(m_stat_redis);
		m_stat_redis = NULL;
		return false;
	}
	if (reply->type != REDIS_REPLY_ARRAY){
		LOG(ERROR) << "redis type error:" << reply->type;
		freeReplyObject(reply);
		return false;
	}
	
	for (int i = 0; i < (int)reply->elements; i++){
		string pair_key = reply->element[i]->str;

		if (m_redis == NULL) {
			Config config;
			Json::Value redis_config = config["redis"];
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
            	//m_redis = redisConnect(it->first.c_str(), it->second) ;  m_stat_redis = redis_connect(it->first.c_str(), it->second, password.c_str()) ; 
            	m_redis = redis_connect(it->first.c_str(), it->second, password.c_str()) ;         
        	}

        	if (m_redis == NULL) {
            	LOG(ERROR) << "m_redis connect faild";
            	exit(1);
        	}else{
            	LOG(ERROR) << "m_redis connect ok";
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
            	LOG(ERROR) << "m_stat_redis connect ok";
        	}
		}

		SendOneTicker(pair_key);
	}
	freeReplyObject(reply);
	
	return true;
}

bool Statistics::StatisticsTrade(){
	if (m_statistic_json["trade"]["list"].size() <= 0){
		LOG(ERROR) << " no trade";
		return false;
	}
	
	Json::StreamWriterBuilder writer;
	writer["indentation"] = "";
	string result = Json::writeString(writer, m_statistic_json["trade"]);
	m_trade->SendMessage(result, "trade");
	
	char *redis_cmd;
	for (int i = 0; i < (int)m_statistic_json["trade"]["list"].size(); i++){
		result = Json::writeString(writer, m_statistic_json["trade"]["list"][i]);
		if (redisFormatCommand(&redis_cmd, "LPUSH trade_list_%s_%s %s", m_base_asset.c_str(), m_quote_asset.c_str(), result.c_str()) <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		string tmp_str = redis_cmd;
		free(redis_cmd);
		m_redis_cmd_list.push_back(tmp_str);
	}
	
	if (redisFormatCommand(&redis_cmd, "LTRIM trade_list_%s_%s 0 %d", m_base_asset.c_str(), m_quote_asset.c_str(), TRADE_LIST_MAX_LEN - 1) <= 0){
		LOG(ERROR) << "redis format error";
		return false;
	}
	string tmp_str = redis_cmd;
	free(redis_cmd);
	m_redis_cmd_list.push_back(tmp_str);
	
	return true;
}


bool Statistics::WriteRedis(){
	
	redisAppendCommand(m_stat_redis, "MULTI");
	for (int i = 0; i < (int)m_redis_cmd_list.size(); i++){
		redisAppendFormattedCommand(m_stat_redis, m_redis_cmd_list[i].c_str(), m_redis_cmd_list[i].size());
	}
	redisAppendCommand(m_stat_redis, "EXEC");
	redisReply* reply = NULL;
	for (int i = 0; i < (int)m_redis_cmd_list.size() + 2; i++){
		redisGetReply(m_stat_redis, (void**)&reply);
		if (reply == NULL) {
			redisFree(m_stat_redis);
			m_stat_redis = NULL;
			return false;
		}
		if (reply->type == REDIS_REPLY_ERROR){
			LOG(ERROR) << "redis error:" << reply->str;
		}
		freeReplyObject(reply);
		reply = NULL;
	}
	return true;
}

bool Statistics::Msg(string statistics_str){
	LOG(INFO) << statistics_str;
	
	if (m_redis == NULL) {
		Config config;
		Json::Value redis_config = config["redis"];
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
			LOG(ERROR) << "m_redis connect ok";
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
		//std::string password = real_password(encode_password);
		std::string password = encode_password;

		//m_stat_redis = SentinelRedisConnect(sentinels, redis_config["master_name"].asCString(), password.c_str(), redis_config["database"].asInt());
		for (auto it = sentinels.begin(); it != sentinels.end(); ++it) {
			//m_stat_redis = redisConnect(it->first.c_str(), it->second) ; 
			m_stat_redis = redis_connect(it->first.c_str(), it->second, password.c_str()) ; 		 
		}
		
		if (m_stat_redis == NULL) {
			LOG(ERROR) << "m_redis connect faild";
			exit(1);
		}else{
			LOG(ERROR) << "m_redis connect ok";
		}

	}

	if (!Reposition()) return false;
	
	if (!InitStatistics(statistics_str)) return false;

	StatisticsKline();

	StatisticsOrderbook();

	StatisticsTrade();
	
	Json::StreamWriterBuilder writer;
	writer["indentation"] = "";
	string result = Json::writeString(writer, m_statistic_json["order"]);
	m_trade->SendMessage(result, "order_change");
	
	WriteRedis();
	
	return true;
}
