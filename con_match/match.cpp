#include "match.h"
#include "trade.h"
#include <memory>
#include <string.h>  
#include <iostream>
#include "utils.h"
#include "redis_utils.h"

std::string switch_f_to_s(long double f){
	char st[1024];
	snprintf(st, sizeof(st), "%.*Lf", VALUE_DECIMAL, f);
	std::string ans = st;
	return ans;
}

long double switch_s_to_f(std::string s){
	return strtold(s.c_str(), NULL);
}

long double fabs(long double value){
	if (value > 0){
		return value;
	}else{
		return -value;
	}
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

bool JudgeOneUtcDay(time_t t1, time_t t2){
	struct tm utc_time1 = {0};
    gmtime_r(&t1, &utc_time1);
	int day1 = utc_time1.tm_mday;

	struct tm utc_time2 = {0};
    gmtime_r(&t2, &utc_time2);
	int day2 = utc_time2.tm_mday;

	if (day1 == day2 && t1 - t2 < 86400 && t2 - t1 < 86400){
		return true;
	}else{
		return false;
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
		std::string password = real_password(encode_password);

		m_redis = SentinelRedisConnect(sentinels, redis_config["master_name"].asCString(), password.c_str(), redis_config["database"].asInt());
		if (m_redis == NULL) {
			LOG(ERROR) << "m_redis connect faild";
        	exit(1);
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
		std::string password = real_password(encode_password);

		m_stat_redis = SentinelRedisConnect(sentinels, redis_config["master_name"].asCString(), password.c_str(), redis_config["database"].asInt());
		if (m_stat_redis == NULL) {
			LOG(ERROR) << "m_stat_redis connect faild";
        	exit(1);
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
		std::string password = real_password(encode_password);

		m_index_redis = SentinelRedisConnect(sentinels, redis_config["master_name"].asCString(), password.c_str(), redis_config["database"].asInt());
		if (m_index_redis == NULL) {
			LOG(ERROR) << "m_stat_redis connect faild";
        	exit(1);
		}
	}
	
	LOG(INFO) << "redis ok";

	maker_user_set.clear();
	m_makers.clear();
	{
		const Json::Value makers = config["makers"];
		for (unsigned i = 0; i < makers.size(); i++) {
			int contract_id = makers[i]["contract_id"].asInt();
			int user_id = makers[i]["user_id"].asInt();
			m_makers.insert(std::make_pair(contract_id, user_id));
			maker_user_set.insert(user_id);
		}
	}

	{
		const Json::Value settle_asset = config["settle_asset"];
		m_settle_asset = settle_asset["name"].asString();
		if (m_settle_asset == "USDT") {
			m_rate = 1.0L;
			m_server_currency = "USD";
		} else if (m_settle_asset == "BTC") {
			m_rate = BTC_RATE;
			m_server_currency = "BTC";
		} else if (m_settle_asset == "ETH") {
			m_rate = ETH_RATE;
			m_server_currency = "ETH";
		} else {
			LOG(ERROR) << "no exchange zone config";
			exit(1);
		}
		m_server_lang_cn = m_server_currency + "交易区—";
		m_server_lang_tw = m_server_currency + "交易區—";
		m_server_lang_en = m_server_currency + " Market—";
		m_server_lang_vi = "Khu giao dịch " + m_server_currency;
	}

	redisReply* reply = (redisReply*)redisCommand(m_redis, "SMEMBERS position_set");
	if (reply == NULL) {
		LOG(ERROR) << "redis reply null";
		redisFree(m_redis);
		m_redis = NULL;
		exit(1);
	}
	if (reply->type != REDIS_REPLY_ARRAY) {
		LOG(ERROR) << "redis type error:" << reply->type;
		freeReplyObject(reply);
		exit(1);
	}
	std::vector<std::string> position_user;
	for (unsigned i = 0; i < reply->elements; i++) {
		position_user.push_back(reply->element[i]->str);
	}
	freeReplyObject(reply);

	if (!GetContractLastPrice()) {
		exit(1);
	}

	for (auto it = position_user.begin(); it != position_user.end(); it++) {
		redisAppendCommand(m_redis, "GET position_user_%s", it->c_str());
	}
	Json::StreamWriterBuilder writer;
	writer["indentation"] = "";
	Json::CharReaderBuilder rbuilder;
	std::unique_ptr<Json::CharReader> const reader(rbuilder.newCharReader());
	JSONCPP_STRING error;
	std::vector<std::string> temp_cmd_list;
	for (auto it = position_user.begin(); it != position_user.end(); it++) {
		redisReply* temp_reply = NULL;
		redisGetReply(m_redis, (void**)&temp_reply);
		if (temp_reply == NULL){
			LOG(ERROR) << "redis reply null";
			redisFree(m_redis);
			m_redis = NULL;
			exit(1);
		}
		if (temp_reply->type == REDIS_REPLY_NIL) {
			freeReplyObject(temp_reply);
			continue;
		}
		if (temp_reply->type != REDIS_REPLY_STRING){
			LOG(ERROR) << "redis type error:" << temp_reply->type;
			freeReplyObject(temp_reply);
			exit(1);
		}
		std::string user_position_str = temp_reply->str;
		freeReplyObject(temp_reply);
		Json::Value user_position_json = Json::Value::null;
		bool ret = reader->parse(user_position_str.c_str(), user_position_str.c_str() + user_position_str.size(), &user_position_json, &error);
		if (!(ret && error.size() == 0)) {
			LOG(ERROR) << "json error";
			exit(1);
		}

		if (user_position_json.size() && (*user_position_json.begin()).isMember("buy_amount")) {
			for (auto iter = user_position_json.begin(); iter != user_position_json.end(); iter++) {
				if (m_contract_config.isMember(iter.name())) {
					std::string lever_rate = m_contract_config[iter.name()]["lever_rate"].asString();
					m_positions[*it][iter.name()][lever_rate] = user_position_json[iter.name()];
				} else {
					LOG(ERROR) << "no contract config";
					exit(1);
				}
			}
			char * redis_cmd = NULL;
			std::string result = Json::writeString(writer, m_positions[*it]);
			if (redisFormatCommand(&redis_cmd, "SET position_user_%s %s", it->c_str(), result.c_str()) <= 0){
				LOG(ERROR) << "redis format error";
				exit(1);
			}
			temp_cmd_list.push_back(std::string(redis_cmd));
			free(redis_cmd);
		} else {
			m_positions[*it] = user_position_json;
		}
	}
	if (temp_cmd_list.size()) {
		redisAppendCommand(m_redis, "MULTI");
		for (int i = 0; i < (int)temp_cmd_list.size(); i++){
			redisAppendFormattedCommand(m_redis, temp_cmd_list[i].c_str(), temp_cmd_list[i].size());
		}
		redisAppendCommand(m_redis, "EXEC");
		redisReply* temp_reply = NULL;
		for (int i = 0; i < (int)temp_cmd_list.size() + 2; i++){
			redisGetReply(m_redis, (void**)&temp_reply);
			if (temp_reply == NULL) {
				redisFree(m_redis);
				m_redis = NULL;
				LOG(ERROR) << "redis error";
				exit(1);
			}
			if (temp_reply->type == REDIS_REPLY_ERROR){
				LOG(ERROR) << "redis error:" << temp_reply->str;
				exit(1);
			}
			freeReplyObject(temp_reply);
			temp_reply = NULL;
		}
	}

	reply = (redisReply*)redisCommand(m_redis, "SMEMBERS account_set");
	if (reply == NULL) {
		LOG(ERROR) << "redis reply null";
		redisFree(m_redis);
		m_redis = NULL;
		exit(1);
	}
	if (reply->type != REDIS_REPLY_ARRAY) {
		LOG(ERROR) << "redis type error:" << reply->type;
		freeReplyObject(reply);
		exit(1);
	}
	std::vector<std::string> account_user;
	for (unsigned i = 0; i < reply->elements; i++) {
		account_user.push_back(reply->element[i]->str);
	}
	freeReplyObject(reply);

	for (auto it = account_user.begin(); it != account_user.end(); it++) {
		redisAppendCommand(m_redis, "GET account_user_%s", it->c_str());
	}
	for (auto it = account_user.begin(); it != account_user.end(); it++) {
		redisReply* temp_reply = NULL;
		redisGetReply(m_redis, (void**)&temp_reply);
		if (temp_reply == NULL){
			LOG(ERROR) << "redis reply null";
			redisFree(m_redis);
			m_redis = NULL;
			exit(1);
		}
		if (temp_reply->type == REDIS_REPLY_NIL) {
			freeReplyObject(temp_reply);
			continue;
		}
		if (temp_reply->type != REDIS_REPLY_STRING){
			LOG(ERROR) << "redis type error:" << temp_reply->type;
			freeReplyObject(temp_reply);
			exit(1);
		}
		std::string user_account_str = temp_reply->str;
		freeReplyObject(temp_reply);
		Json::Value user_account_json = Json::Value::null;
		bool ret = reader->parse(user_account_str.c_str(), user_account_str.c_str() + user_account_str.size(), &user_account_json, &error);
		if (!(ret && error.size() == 0)) {
			LOG(ERROR) << "json error";
			exit(1);
		}
		m_accounts[*it] = user_account_json;
	}
	m_pending_msg_list.clear();

	m_usd_cny_value = 6.5L;
	m_usd_cny_timestamp = 0;
	m_match_stop_interval = config["match_stop_inteval"].asInt();
    return true;
}

bool Match::Reposition(){
	m_redis_cmd_list.clear();
	m_trade_list.resize(0);
	m_order_result_list.resize(0);
	m_trade_users_set.clear();
	m_trade_users_list.clear();
	m_trade_users_lever_rate.clear();
	m_user_account = Json::Value::null;
	m_user_position = Json::Value::null;
	m_contract_price = Json::Value::null;
	m_time_now = time(0);
	m_executed_quote_amount = 0L;
	m_executed_settle_amount = 0L;
	m_frozen_margin = 0.0L;
	m_order_user_id = -1;
	m_statistics = Json::Value::null;
	m_trade_msg_array.resize(0);
	m_email_msg_array.resize(0);
	m_spot_price = 0;
	
	return true;
}

bool Match::InitOrder(Json::Value& order_json){
	if (m_msg_type == "order"){
		if (!order_json.isMember("order_id")){
			LOG(ERROR) << "json no order_id";
			return false;
		}
		if (!order_json.isMember("user_id")){
			LOG(ERROR) << "json no user_id";
			return false;
		}
		if (!order_json.isMember("contract_id")){
			LOG(ERROR) << "json no contract_id";
			return false;
		}
		if (!order_json.isMember("order_type")){
			LOG(ERROR) << "json no order_type";
			return false;
		}
		if (!order_json.isMember("is_bbo")) {
			LOG(ERROR) << "json no is_bbo";
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
		m_order_user_id_str = std::to_string(m_order_user_id);
		m_order_contract_id = order_json["contract_id"].asInt();
		m_order_contract_id_str = std::to_string(m_order_contract_id);
		m_order_type = order_json["order_type"].asInt();
		m_order_isbbo = order_json["is_bbo"].asInt();
		m_order_op = order_json["order_op"].asInt();
		m_order_amt = switch_s_to_f(order_json["origin_amount"].asString());
		m_order_amt_str = switch_f_to_s(m_order_amt);
		m_order_ip = order_json["ip"].asString();
		m_order_source = order_json["source"].asInt();

		if (order_json.isMember("take_profit")){
			m_order_profit_limit = switch_f_to_s(switch_s_to_f(order_json["take_profit"].asString()));
		}else{
			m_order_profit_limit = switch_f_to_s(0);
		}
		if (order_json.isMember("stop_loss")){
			m_order_lose_limit = switch_f_to_s(switch_s_to_f(order_json["stop_loss"].asString()));
		}else{
			m_order_lose_limit = switch_f_to_s(0);
		}

		if (order_json.isMember("system_type")){
			m_order_system_type = order_json["system_type"].asInt();
		}else{
			m_order_system_type = ORDER_SYSTEM_TYPE_NORMAL;
		}

		if (order_json.isMember("lever_rate")){
    		m_lever_rate_str = order_json["lever_rate"].asString();
			m_lever_rate = atoi(m_lever_rate_str.c_str());
		}else{
			if (m_contract_config.isMember(m_order_contract_id_str)){
    			m_lever_rate_str = m_contract_config[m_order_contract_id_str]["lever_rate"].asString();
				m_lever_rate = atoi(m_lever_rate_str.c_str());
			}else{
				LOG(ERROR) << "m_order_id:" << m_order_id << " contract_id:" << m_order_contract_id_str << " error";
				return false;
			}
		}
		if (m_lever_rate != 5 && m_lever_rate != 10 && m_lever_rate != 20 && m_lever_rate != 50) {
			LOG(ERROR) << "m_order_id:" << m_order_id << " lever_rate:" << m_lever_rate << " error";
			return false;
		}
		
		if (m_order_type == ORDER_TYPE_LIMIT && m_order_isbbo == 0) {
			if (!order_json.isMember("price")){
				LOG(ERROR) << "json no price";
				return false;
			}
			m_order_price = switch_s_to_f(order_json["price"].asString());
			m_order_price_str = switch_f_to_s(m_order_price);
			if (m_order_price <= EPS){
				LOG(ERROR) << "m_order_id:" << m_order_id << " m_order_price error";
				return false;
			}
		} else if (m_order_type == ORDER_TYPE_MARKET || (m_order_type == ORDER_TYPE_LIMIT && m_order_isbbo == 1)) {
			m_order_price = 0;
			m_order_price_str = switch_f_to_s(m_order_price);
		} else {
			LOG(ERROR) << "m_order_id:" << m_order_id << " m_order_type error";
			return false;
		}
		if (m_order_op != ORDER_SIDE_OPEN_LONG && m_order_op != ORDER_SIDE_OPEN_SHORT && 
			m_order_op != ORDER_SIDE_CLOSE_LONG && m_order_op != ORDER_SIDE_CLOSE_SHORT){
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
		m_order_user_id_str = std::to_string(m_order_user_id);
		if (!GetOrderInfo()){
			LOG(INFO) << "m_order_id:" << m_order_id << " no this order";
			return false;
		}
		if (!m_redis) {
			return false;
		}
	}else if (m_msg_type == "profit_loss"){
		if (!order_json.isMember("user_id")){
			LOG(ERROR) << "json no user_id";
			return false;
		}
		if (!order_json.isMember("contract_id")){
			LOG(ERROR) << "json no contract_id";
			return false;
		}
		if (!order_json.isMember("order_op")){
			LOG(ERROR) << "json no order_op";
			return false;
		}
		if (!order_json.isMember("stop_loss")){
			LOG(ERROR) << "json no stop_loss";
			return false;
		}
		if (!order_json.isMember("take_profit")){
			LOG(ERROR) << "json no take_profit";
			return false;
		}
		m_order_user_id = order_json["user_id"].asInt();
		m_order_user_id_str = std::to_string(m_order_user_id);
		m_order_contract_id = order_json["contract_id"].asInt();
		m_order_contract_id_str = std::to_string(m_order_contract_id);
		m_order_op = order_json["order_op"].asInt();
		m_order_profit_limit = switch_f_to_s(switch_s_to_f(order_json["take_profit"].asString()));
		m_order_lose_limit = switch_f_to_s(switch_s_to_f(order_json["stop_loss"].asString()));

		if (order_json.isMember("lever_rate")){
    		m_lever_rate_str = order_json["lever_rate"].asString();
			m_lever_rate = atoi(m_lever_rate_str.c_str());
		}else{
			if (m_contract_config.isMember(m_order_contract_id_str)){
    			m_lever_rate_str = m_contract_config[m_order_contract_id_str]["lever_rate"].asString();
				m_lever_rate = atoi(m_lever_rate_str.c_str());
			}else{
				LOG(ERROR) << "m_order_id:" << m_order_id << " contract_id:" << m_order_contract_id_str << " error";
				return false;
			}
		}

		if (!m_redis) {
			return false;
		}
		return true;
	}else{
		return false;
	}

	m_statistics["trade"]["contract_id"] = m_order_contract_id_str;
	m_statistics["trade"]["list"].resize(0);
	
	m_statistics["order_book"]["contract_id"] = m_order_contract_id_str;
	m_statistics["order_book"]["buy"].resize(0);
	m_statistics["order_book"]["sell"].resize(0);

	return true;
}


bool Match::PrepareForOrder(){
	//查询合约参数
	LOG(INFO) << "m_order_id:" << m_order_id << " get contract info start";

	Json::CharReaderBuilder rbuilder;
	std::unique_ptr<Json::CharReader> const reader(rbuilder.newCharReader());
	JSONCPP_STRING error;

	redisReply* reply = NULL;
	redisAppendCommand(m_stat_redis, "HMGET contract:config:%d contract_name asset_symbol unit_amount price_decimal max_amt max_hold_amt maker_fee taker_fee stop_amt max_stop_amt state trading_area", m_order_contract_id);
	redisGetReply(m_stat_redis, (void**)&reply);
	if (reply == NULL){
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
		redisFree(m_stat_redis);
		m_stat_redis = NULL;
		return false;
	}
	if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 12){
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis type error:" << reply->type;
		freeReplyObject(reply);
		return false;
	}
	for (int i = 0; i < 12; i++){
		if (reply->element[i]->type != REDIS_REPLY_STRING){
			LOG(ERROR) << "m_order_id:" << m_order_id << " i: " << i << " redis type error: i:" << reply->element[i]->type;
			freeReplyObject(reply);
			return false;
		}
	}
	
	m_contract_name = reply->element[0]->str;
	m_order_base_asset = reply->element[1]->str;
	m_unit_amount = strtold(reply->element[2]->str, NULL);
	m_price_decimal = atoi(reply->element[3]->str);
	m_max_order_amount = strtold(reply->element[4]->str, NULL);
	m_max_hold_amount = strtold(reply->element[5]->str, NULL);
	m_maker_fee = strtold(reply->element[6]->str, NULL);
	m_taker_fee = strtold(reply->element[7]->str, NULL);
	m_stop_amount = strtold(reply->element[8]->str, NULL);
	m_max_stop_amount = strtold(reply->element[9]->str, NULL);
	int pair_state = atoi(reply->element[10]->str);
	std::string settle_asset = reply->element[11]->str;

	freeReplyObject(reply);

	if (settle_asset != m_server_currency) {
		LOG(ERROR) << "m_order_id:" << m_order_id << " settle_asset error:" << settle_asset;
		return false;
	}
	if (pair_state != TRADE_PAIR_STATE_ON){
		LOG(ERROR) << "m_order_id:" << m_order_id << " pair_state error:" << pair_state;
		return false;
	}
	if (m_order_amt + EPS < m_unit_amount){
		LOG(ERROR) << "m_order_id:" << m_order_id << " amount: " << m_order_amt << " min_amount: " << m_unit_amount;
		return false;
	}
	if ((m_order_op == ORDER_SIDE_OPEN_LONG || m_order_op == ORDER_SIDE_OPEN_SHORT) && m_order_amt - EPS > m_max_order_amount && maker_user_set.find(m_order_user_id) == maker_user_set.end()){
		LOG(ERROR) << "m_order_id:" << m_order_id << " amount: " << m_order_amt << " max_amount: " << m_max_order_amount;
		return false;
	}
	if (!CheckDecimal(m_order_amt / m_unit_amount, 0)){
		LOG(ERROR) << "m_order_id:" << m_order_id << " amount: " << m_order_amt << " unit_amount: " << m_unit_amount;
		return false;
	}
	if (!CheckDecimal(m_order_price, m_price_decimal)){
		LOG(ERROR) << "m_order_id:" << m_order_id << " m_order_price: " << m_order_price << " m_price_decimal: " << m_price_decimal;
		return false;
	}
	if (m_lever_rate > 100 || m_lever_rate <= 0) {
		LOG(INFO) << "m_order_id:" << m_order_id << " m_lever_rate: " << m_lever_rate;
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
	LOG(INFO) << "m_maker_fee:" << m_maker_fee;
	LOG(INFO) << "m_taker_fee:" << m_taker_fee;
	
	LOG(INFO) << "m_order_id:" << m_order_id << " get contract info end";

	//查询现货指数价格
	LOG(INFO) << "m_order_id:" << m_order_id << " query spot price start";

	reply = NULL;
	redisAppendCommand(m_index_redis, "ZRANGE kline_%s -1 -1", m_order_base_asset.c_str());
	redisGetReply(m_index_redis, (void**)&reply);
	if (reply == NULL) {
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
		redisFree(m_index_redis);
		m_index_redis = NULL;
		return false;
	}
	if (reply->type != REDIS_REPLY_ARRAY){
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis type error:" << reply->type;
		freeReplyObject(reply);
		return false;
	}
	if (reply->elements != 1){
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply elements error:" << reply->elements;
		freeReplyObject(reply);
		return false;
	}
	std::string kline_str = reply->element[0]->str;
	freeReplyObject(reply);
	Json::Value kline_json = Json::Value::null;
	bool ret = reader->parse(kline_str.c_str(), kline_str.c_str() + kline_str.size(), &kline_json, &error);
	if (!(ret && error.size() == 0)) {
		LOG(ERROR) << "kline_str:" << kline_str << " json error";
		return false;
	}
	m_spot_price = switch_s_to_f(kline_json["close"].asString());
	LOG(INFO) << "m_spot_price:" << m_spot_price;

	if (m_order_type == ORDER_TYPE_LIMIT && m_order_isbbo == 0){
		if (ORDER_PRICE_LIMIT_SWITCH && !DiffPrice(m_order_price, m_spot_price)){
			if ((m_order_op == ORDER_SIDE_OPEN_LONG || m_order_op == ORDER_SIDE_CLOSE_SHORT) && m_order_price > m_spot_price){
				LOG(INFO) << " price diff much m_order_price: " << m_order_price << " m_spot_price: " << m_spot_price;
				return false;
			}
			if ((m_order_op == ORDER_SIDE_OPEN_SHORT || m_order_op == ORDER_SIDE_CLOSE_LONG) && m_order_price < m_spot_price){
				LOG(INFO) << " price diff much m_order_price: " << m_order_price << " m_spot_price: " << m_spot_price;
				return false;
			}
		}
	}

	LOG(INFO) << "m_order_id:" << m_order_id << " query spot price end";

	//查询成交最新价
	LOG(INFO) << "m_order_id:" << m_order_id << " query last price start";

	reply = NULL;
	redisAppendCommand(m_redis, "HGETALL last_trade_price");
	redisGetReply(m_redis, (void**)&reply);
	if (reply == NULL) {
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
	for (unsigned i = 0; i < reply->elements; i = i + 2) {
		char* contract_id = reply->element[i]->str;
		char* price = reply->element[i+1]->str;
		m_contract_price[contract_id] = price;
	}
	freeReplyObject(reply);

	LOG(INFO) << "m_order_id:" << m_order_id << " query last price end";

	//查询挂单用户余额
	LOG(INFO) << "m_order_id:" << m_order_id << " CheckAmount start";

	long double trigger_liquidation = 0L;
	reply = NULL;
	redisAppendCommand(m_stat_redis, "HGET user_trigger_liquidation %d", m_order_user_id);
	redisGetReply(m_stat_redis, (void**)&reply);
	if (reply == NULL) {
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
		redisFree(m_stat_redis);
		m_stat_redis = NULL;
		return false;
	}
	if (reply->type != REDIS_REPLY_NIL && reply->type != REDIS_REPLY_STRING){
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis type error:" << reply->type;
		freeReplyObject(reply);
		return false;
	}
	if (reply->type == REDIS_REPLY_STRING){
		trigger_liquidation = strtold(reply->str, NULL);
	}
	freeReplyObject(reply);

	Json::Value order_user_account_json = Json::Value::null;
	if (!m_accounts.isMember(m_order_user_id_str)){
		LOG(ERROR) << "m_order_id:" << m_order_id << " user_id:" << m_order_user_id << " no account info";
	}
	order_user_account_json = m_accounts[m_order_user_id_str];

	if (!order_user_account_json.isMember(m_settle_asset)){
		LOG(ERROR) << "m_order_id:" << m_order_id << " available not enough";
		return false;
	}
	std::string balance = order_user_account_json[m_settle_asset]["balance"].asString();
	std::string encode_balance = order_user_account_json[m_settle_asset]["encode_balance"].asString();
	std::string margin = order_user_account_json[m_settle_asset]["margin"].asString();
	std::string encode_margin = order_user_account_json[m_settle_asset]["encode_margin"].asString();
	std::string frozen_margin = order_user_account_json[m_settle_asset]["frozen_margin"].asString();
	std::string encode_frozen_margin = order_user_account_json[m_settle_asset]["encode_frozen_margin"].asString();
	std::string profit = order_user_account_json[m_settle_asset]["profit"].asString();
	std::string encode_profit = order_user_account_json[m_settle_asset]["encode_profit"].asString();
	m_available_margin = strtold(balance.c_str(), NULL) + strtold(profit.c_str(), NULL) - strtold(margin.c_str(), NULL) - strtold(frozen_margin.c_str(), NULL);
	if (HmacSha256Encode(m_order_user_id_str + m_settle_asset + balance) != encode_balance) {
		LOG(ERROR) << "m_order_id:" << m_order_id << " encode_balance error";
		return false;
	}
	if (HmacSha256Encode(m_order_user_id_str + m_settle_asset + margin) != encode_margin) {
		LOG(ERROR) << "m_order_id:" << m_order_id << " encode_margin error";
		return false;
	}
	if (HmacSha256Encode(m_order_user_id_str + m_settle_asset + frozen_margin) != encode_frozen_margin) {
		LOG(ERROR) << "m_order_id:" << m_order_id << " encode_frozen_margin error";
		return false;
	}
	if (HmacSha256Encode(m_order_user_id_str + m_settle_asset + profit) != encode_profit) {
		LOG(ERROR) << "m_order_id:" << m_order_id << " encode_profit error";
		return false;
	}

	Json::Value order_user_position = m_positions[m_order_user_id_str];
	for (auto iter = order_user_position.begin(); iter != order_user_position.end(); iter++) {
		Json::Value positions = order_user_position[iter.name()];
		for (auto it = positions.begin(); it != positions.end(); it++) {
			Json::Value position = positions[it.name()];
			long double buy_amount = strtold(position["buy_amount"].asCString(), NULL);
			long double buy_quote_amount_settle = strtold(position["buy_quote_amount_settle"].asCString(), NULL);
			long double sell_amount = strtold(position["sell_amount"].asCString(), NULL);
			long double sell_quote_amount_settle = strtold(position["sell_quote_amount_settle"].asCString(), NULL);
			long double buy_profit = 0;
			long double sell_profit = 0;
			if (buy_amount > EPS) {
				long double price = strtold(m_contract_price[iter.name()].asCString(), NULL);
				buy_profit = buy_amount * price * m_rate - buy_quote_amount_settle; 
			}
			if (sell_amount > EPS) {
				long double price = strtold(m_contract_price[iter.name()].asCString(), NULL);
				sell_profit = sell_quote_amount_settle - sell_amount * price * m_rate; 
			}
			m_available_margin += buy_profit + sell_profit;
		}
	}

	if (m_order_op == ORDER_SIDE_OPEN_LONG || m_order_op == ORDER_SIDE_OPEN_SHORT) {
		if (maker_user_set.find(m_order_user_id) == maker_user_set.end() && order_user_position.isMember(m_order_contract_id_str)) {
			long double buy_frozen = 0;
			long double buy_amount = 0;
			long double sell_frozen = 0;
			long double sell_amount = 0;
			Json::Value::Members keys = order_user_position[m_order_contract_id_str].getMemberNames();
			for (auto iter = keys.begin(); iter != keys.end(); iter++) {
				buy_frozen += strtold(order_user_position[m_order_contract_id_str][*iter]["buy_frozen"].asCString(), NULL);
				buy_amount += strtold(order_user_position[m_order_contract_id_str][*iter]["buy_amount"].asCString(), NULL);
				sell_frozen += strtold(order_user_position[m_order_contract_id_str][*iter]["sell_frozen"].asCString(), NULL);
				sell_amount += strtold(order_user_position[m_order_contract_id_str][*iter]["sell_amount"].asCString(), NULL);
			}
			if (buy_frozen + buy_amount + sell_frozen + sell_amount + m_order_amt - EPS > m_max_hold_amount) {
				LOG(ERROR) << "m_order_id:" << m_order_id << " amount: " << m_order_amt << " max_hold_amount: " << m_max_hold_amount;
				return false;
			}
		}
		if (m_available_margin < EPS) {
			LOG(ERROR) << "m_order_id:" << m_order_id << " available_margin not enough";
			return false;
		}
		long double rights = m_available_margin + strtold(margin.c_str(), NULL) + strtold(frozen_margin.c_str(), NULL);
		if (rights < trigger_liquidation) {
			LOG(ERROR) << "m_order_id:" << m_order_id << " rights: " << rights << " trigger_liquidation: " << trigger_liquidation;
			return false;
		}
	} else {
		long double buy_available = 0;
		long double sell_available = 0;
		if (order_user_position.isMember(m_order_contract_id_str)) {
			buy_available = strtold(order_user_position[m_order_contract_id_str][m_lever_rate_str]["buy_available"].asCString(), NULL);
			sell_available = strtold(order_user_position[m_order_contract_id_str][m_lever_rate_str]["sell_available"].asCString(), NULL);
		}

		if (m_order_op == ORDER_SIDE_CLOSE_LONG) {
			if (buy_available + EPS < m_order_amt) {
				LOG(ERROR) << "m_order_id:" << m_order_id << " buy_available not enough";
				return false;
			}
		} else {
			if (sell_available + EPS < m_order_amt) {
				LOG(ERROR) << "m_order_id:" << m_order_id << " sell_available not enough";
				return false;
			}
		}
	}

	LOG(INFO) << "m_order_id:" << m_order_id << " CheckAmount end";

	//查询交易对币种和KK的价格
	LOG(INFO) << "m_order_id:" << m_order_id << " GetAssetPrice start";

	if (m_time_now - m_usd_cny_timestamp > 300){
		reply = NULL;
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
			std::string tmp_str = reply->str;
			m_usd_cny_value = switch_s_to_f(tmp_str);
		}
		m_usd_cny_timestamp = m_time_now;
		freeReplyObject(reply);

		reply = (redisReply*) redisCommand(m_stat_redis, "HMGET global_match_config order_price_limit_high order_price_limit_low deposit_send_relative_rate deposit_send_absolute_rate deposit_send_warning_rate");
		if (reply == NULL){
			LOG(ERROR) << "redis reply null";
			redisFree(m_stat_redis);
			m_stat_redis = NULL;
			return false;
		}
		if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 5){
			if (reply->element[0]->type == REDIS_REPLY_STRING){
				m_order_price_limit_high = strtold(reply->element[0]->str, NULL);
			}
			if (reply->element[1]->type == REDIS_REPLY_STRING){
				m_order_price_limit_low = strtold(reply->element[1]->str, NULL);
			}
			if (reply->element[2]->type == REDIS_REPLY_STRING){
				m_send_stop_relative_rate = strtold(reply->element[2]->str, NULL);
			}
			if (reply->element[3]->type == REDIS_REPLY_STRING){
				m_send_stop_absolute_rate = strtold(reply->element[3]->str, NULL);
			}
			if (reply->element[4]->type == REDIS_REPLY_STRING){
				m_send_stop_warning_rate = strtold(reply->element[4]->str, NULL);
			}
		}
		freeReplyObject(reply);
	}

	LOG(INFO) << "m_order_id:" << m_order_id << " GetAssetPrice end";

	return true;
}

std::string Match::GetListId(){
	LOG(INFO) << "m_order_id:" << m_order_id << " GetListId start";
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
		return EmptyString;
	}
	if (reply->type != REDIS_REPLY_STRING){
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis type error:" << reply->type;
		freeReplyObject(reply);
		return EmptyString;
	}
	std::string list_id = reply->str;
	freeReplyObject(reply);
	
	LOG(INFO) << "m_order_id:" << m_order_id << " GetListId end";
	return list_id;
}

bool Match::MatchOrder(){
	Json::StreamWriterBuilder writer;
	writer["indentation"] = "";
	Json::CharReaderBuilder rbuilder;
	std::unique_ptr<Json::CharReader> const reader(rbuilder.newCharReader());
	JSONCPP_STRING error;

	LOG(INFO) << "m_order_id:" << m_order_id << " MatchOrder start";
	m_trade_users_set.insert(m_order_user_id);
	std::set<int> users_lever_rate;
	users_lever_rate.insert(m_lever_rate);
	m_trade_users_lever_rate.insert(std::map<int, std::set<int>>::value_type(m_order_user_id, users_lever_rate));
	m_remain_amount = m_order_amt;
	
	int start = 0;
	int step = 10;
	int stop = 0;

	if (m_order_type == ORDER_TYPE_LIMIT && m_order_isbbo != 0) {
		step = 1;
	}

	for (; !stop && m_remain_amount > EPS; start += step) {
		char sortsetkey[128];
		redisReply* reply = NULL;
		if (m_order_op == ORDER_SIDE_OPEN_LONG || m_order_op == ORDER_SIDE_CLOSE_SHORT){
			snprintf(sortsetkey, sizeof(sortsetkey), "order_book_%d_sell", m_order_contract_id);
			reply = (redisReply*) redisCommand(m_redis, "ZRANGE %s %d %d WITHSCORES", sortsetkey, start, start + step - 1);
		} else {
			snprintf(sortsetkey, sizeof(sortsetkey), "order_book_%d_buy", m_order_contract_id);
			reply = (redisReply*) redisCommand(m_redis, "ZREVRANGE %s %d %d WITHSCORES", sortsetkey, start, start + step - 1);
		}
		if (reply == NULL) {
			LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
			redisFree(m_redis);
			m_redis = NULL;
			return false;
		}
		if (reply->type != REDIS_REPLY_ARRAY) {
			LOG(ERROR) << "m_order_id:" << m_order_id << " redis type error:" << reply->type;
			freeReplyObject(reply);
			return false;
		}
		if (m_order_type == ORDER_TYPE_LIMIT && m_order_isbbo != 0 && reply->elements == 0) {
			LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply empty";
			freeReplyObject(reply);
			return false;
		}
		
		for (unsigned i = 0; i < reply->elements && m_remain_amount > EPS; i = i + 2) {
			std::string l_sortset_str = reply->element[i]->str;
			Json::Value l_sortset_json;
			bool ret = reader->parse(l_sortset_str.c_str(), l_sortset_str.c_str() + l_sortset_str.size(), &l_sortset_json, &error);
			if (!(ret && error.size() == 0)) {
				LOG(ERROR) << "m_order_id:" << m_order_id << " json error";
				freeReplyObject(reply);
				return false;
			}

			long double l_price = switch_s_to_f(reply->element[i + 1]->str);
			std::string l_price_str = switch_f_to_s(l_price);

			if ((m_order_type == ORDER_TYPE_LIMIT && m_order_isbbo) || m_order_type == ORDER_TYPE_MARKET){
				if (ORDER_PRICE_LIMIT_SWITCH && !DiffPrice(l_price, m_spot_price)){
					if ((l_price > m_spot_price && (m_order_op == ORDER_SIDE_OPEN_LONG || m_order_op == ORDER_SIDE_CLOSE_SHORT)) || (l_price < m_spot_price && (m_order_op == ORDER_SIDE_OPEN_SHORT || m_order_op == ORDER_SIDE_CLOSE_LONG))){
						LOG(ERROR) << "m_order_id:" << m_order_id << " price diff much m_spot_price:" << m_spot_price << " l_price:" << l_price;
						if (m_order_type == ORDER_TYPE_LIMIT && m_order_isbbo){
							freeReplyObject(reply);
							return false;
						}
						stop = true;
						break;
					}
				}
			}

			if (m_order_type == ORDER_TYPE_LIMIT) {
				if (m_order_isbbo) {
					m_order_price = l_price;
					m_order_price_str = l_price_str;
					stop = true;
				}
				
				if (m_order_op == ORDER_SIDE_OPEN_LONG || m_order_op == ORDER_SIDE_CLOSE_SHORT){
					if (l_price - EPS > m_order_price) {
						stop = true;
						break;
					}
				} else {
					if (l_price + EPS < m_order_price) {
						stop = true;
						break;
					}
				}
			}

			char* redis_cmd = NULL;
			if (redisFormatCommand(&redis_cmd, "ZREM %s %s", sortsetkey, l_sortset_str.c_str()) <= 0){
				LOG(ERROR) << "redis format error";
				freeReplyObject(reply);
				return false;
			}
			m_redis_cmd_list.push_back(redis_cmd);
			free(redis_cmd);

			Json::Value temp_json = Json::Value::null;		//新的redis sortset member
			int j = 0;
			for (; j < (int)l_sortset_json["orders"].size() && m_remain_amount > EPS; j++) {
				long double tmp_order_amount = switch_s_to_f(l_sortset_json["orders"][j]["amount"].asString());
				long double trade_amount;
				if (m_remain_amount <= tmp_order_amount){
					trade_amount = m_remain_amount;
				}else{
					trade_amount = tmp_order_amount;
				}
				
				long double cur_executed_quote_amount = trade_amount * l_price;
				long double ori_executed_quote_amount = switch_s_to_f(l_sortset_json["orders"][j]["executed_quote_amount"].asString());
				m_executed_quote_amount += cur_executed_quote_amount;

				if (m_settle_asset == "USDT" && !l_sortset_json["orders"][j].isMember("executed_settle_amount")) {
					l_sortset_json["orders"][j]["executed_settle_amount"] = l_sortset_json["orders"][j]["executed_quote_amount"];
				}
				long double cur_executed_settle_amount = switch_s_to_f(switch_f_to_s(trade_amount * l_price * m_rate));
				long double ori_executed_settle_amount = switch_s_to_f(l_sortset_json["orders"][j]["executed_settle_amount"].asString());
				m_executed_settle_amount += cur_executed_settle_amount;

				if (m_settle_asset == "USDT" && !l_sortset_json["orders"][j].isMember("frozen_margin")) {
					l_sortset_json["orders"][j]["frozen_margin"] = switch_f_to_s(tmp_order_amount * l_price / m_lever_rate);
				}
				long double frozen_margin = switch_s_to_f(l_sortset_json["orders"][j]["frozen_margin"].asString());
				long double ratio =  switch_s_to_f(switch_f_to_s(trade_amount / tmp_order_amount));
				long double change_frozen_margin = switch_s_to_f(switch_f_to_s(frozen_margin * ratio));
				frozen_margin -= change_frozen_margin;

				int cur_user_id = l_sortset_json["orders"][j]["user_id"].asInt();
				int cur_lever_rate = m_lever_rate;
				if (l_sortset_json["orders"][j].isMember("lever_rate")){
					cur_lever_rate = l_sortset_json["orders"][j]["lever_rate"].asInt();
				}

				if (m_order_op == ORDER_SIDE_OPEN_LONG || m_order_op == ORDER_SIDE_OPEN_SHORT) {
					if (m_available_margin * m_lever_rate + EPS < m_executed_settle_amount) {
						LOG(ERROR) << "m_order_id:" << m_order_id << " available not enough";
						freeReplyObject(reply);
						return false;
					}
				}

				m_trade_users_set.insert(cur_user_id);
				std::map<int, std::set<int>>::iterator it;
				it = m_trade_users_lever_rate.find(cur_user_id);
				if (it == m_trade_users_lever_rate.end()){
					std::set<int> users_lever_rate;
					users_lever_rate.insert(cur_lever_rate);
					m_trade_users_lever_rate.insert(std::map<int, std::set<int>>::value_type(cur_user_id, users_lever_rate));
				}else{
					it->second.insert(cur_lever_rate);
				}
				Json::Value temp_trade = Json::Value::null;
				temp_trade["price"] = l_price_str;
				temp_trade["amount"] = switch_f_to_s(trade_amount);
				temp_trade["lever_rate"] = cur_lever_rate;
				temp_trade["order_op"] = l_sortset_json["orders"][j]["order_op"].asInt();
				if (l_sortset_json["orders"][j].isMember("system_type")){
					temp_trade["system_type"] = l_sortset_json["orders"][j]["system_type"].asInt();
				}else{
					temp_trade["system_type"] = ORDER_SYSTEM_TYPE_NORMAL;
				}
				if (l_sortset_json["orders"][j].isMember("profit_limit")){
					temp_trade["profit_limit"] = l_sortset_json["orders"][j]["profit_limit"].asString();
				}else{
					temp_trade["profit_limit"] = switch_f_to_s(0);
				}
				if (l_sortset_json["orders"][j].isMember("lose_limit")){
					temp_trade["lose_limit"] = l_sortset_json["orders"][j]["lose_limit"].asString();
				}else{
					temp_trade["lose_limit"] = switch_f_to_s(0);
				}
				temp_trade["frozen_margin"] = switch_f_to_s(change_frozen_margin);
				temp_trade["user_id"] = l_sortset_json["orders"][j]["user_id"].asInt();
				temp_trade["order_id"] = l_sortset_json["orders"][j]["order_id"].asString();
				m_cur_trade_id = GetListId();
				if (m_cur_trade_id == EmptyString){
					freeReplyObject(reply);
					return false;
				}
				temp_trade["id"] = m_cur_trade_id;
				m_trade_list.append(temp_trade);

				std::string cur_order_left_amount;
				if (trade_amount + EPS < tmp_order_amount){
					Json::Value temp_order = Json::Value::null;
					temp_order["user_id"] = l_sortset_json["orders"][j]["user_id"].asInt();
					temp_order["order_id"] = l_sortset_json["orders"][j]["order_id"].asString();
					temp_order["order_op"] = l_sortset_json["orders"][j]["order_op"].asInt();
					if (l_sortset_json["orders"][j].isMember("system_type")){
						temp_order["system_type"] = l_sortset_json["orders"][j]["system_type"].asInt();
					}else{
						temp_order["system_type"] = ORDER_SYSTEM_TYPE_NORMAL;
					}
					if (l_sortset_json["orders"][j].isMember("profit_limit")){
						temp_order["profit_limit"] = l_sortset_json["orders"][j]["profit_limit"].asString();
					}else{
						temp_order["profit_limit"] = switch_f_to_s(0);
					}
					if (l_sortset_json["orders"][j].isMember("lose_limit")){
						temp_order["lose_limit"] = l_sortset_json["orders"][j]["lose_limit"].asString();
					}else{
						temp_order["lose_limit"] = switch_f_to_s(0);
					}
					temp_order["origin_amount"] = l_sortset_json["orders"][j]["origin_amount"].asString();
					temp_order["amount"] = switch_f_to_s(tmp_order_amount - trade_amount);
					temp_order["lever_rate"] = cur_lever_rate;
					temp_order["executed_quote_amount"] = switch_f_to_s(cur_executed_quote_amount + ori_executed_quote_amount);
					temp_order["executed_settle_amount"] = switch_f_to_s(cur_executed_settle_amount + ori_executed_settle_amount);
					temp_order["frozen_margin"] = switch_f_to_s(frozen_margin);
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
				tmp_order_result["contract_id"] = m_order_contract_id;
				tmp_order_result["contract_name"] = m_contract_name;
				tmp_order_result["asset_symbol"] = m_order_base_asset;
				tmp_order_result["unit_amount"] = switch_f_to_s(m_unit_amount);
				tmp_order_result["lever_rate"] = cur_lever_rate;
				tmp_order_result["settle_asset"] = m_settle_asset;
				tmp_order_result["id"] = l_sortset_json["orders"][j]["order_id"].asString();
				tmp_order_result["user_id"] = l_sortset_json["orders"][j]["user_id"].asInt();
				tmp_order_result["order_type"] = ORDER_TYPE_LIMIT;
				tmp_order_result["order_op"] = l_sortset_json["orders"][j]["order_op"].asInt();
				if (l_sortset_json["orders"][j].isMember("system_type")){
					tmp_order_result["system_type"] = l_sortset_json["orders"][j]["system_type"].asInt();
				}else{
					tmp_order_result["system_type"] = ORDER_SYSTEM_TYPE_NORMAL;
				}
				if (l_sortset_json["orders"][j].isMember("profit_limit")){
					tmp_order_result["profit_limit"] = l_sortset_json["orders"][j]["profit_limit"].asString();
				}else{
					tmp_order_result["profit_limit"] = switch_f_to_s(0);
				}
				if (l_sortset_json["orders"][j].isMember("lose_limit")){
					tmp_order_result["lose_limit"] = l_sortset_json["orders"][j]["lose_limit"].asString();
				}else{
					tmp_order_result["lose_limit"] = switch_f_to_s(0);
				}
				tmp_order_result["price"] = l_price_str;
				tmp_order_result["amount"] = cur_order_left_amount;
				tmp_order_result["origin_amount"] = l_sortset_json["orders"][j]["origin_amount"].asString();
				tmp_order_result["executed_quote_amount"] = switch_f_to_s(cur_executed_quote_amount + ori_executed_quote_amount);
				tmp_order_result["executed_settle_amount"] = switch_f_to_s(cur_executed_settle_amount + ori_executed_settle_amount);
				tmp_order_result["frozen_margin"] = switch_f_to_s(frozen_margin);
				if (cur_order_left_amount == switch_f_to_s(0)){
					tmp_order_result["status"] = ORDER_STATUS_FILLED;
				}else{
					tmp_order_result["status"] = ORDER_STATUS_PARTIALLY;
				}
				tmp_order_result["update_at"] = m_time_now;
				m_order_result_list.append(tmp_order_result);
				m_statistics["order"].append(tmp_order_result);

				m_remain_amount -= trade_amount;
			}

			for (int k = j; k < (int)l_sortset_json["orders"].size(); k++){
				temp_json["orders"].append(l_sortset_json["orders"][k]);
				if (!temp_json.isMember("total_amount")){
					temp_json["total_amount"] = "0.00000000";
				}
				temp_json["total_amount"] = switch_f_to_s(switch_s_to_f(temp_json["total_amount"].asString()) + switch_s_to_f(l_sortset_json["orders"][k]["amount"].asString()));
			}

			if (temp_json != Json::Value::null) {
				temp_json["price"] = l_price_str;
				std::string result = Json::writeString(writer, temp_json);
				if (redisFormatCommand(&redis_cmd, "ZADD %s %s %s", sortsetkey, l_price_str.c_str(), result.c_str()) <= 0){
					LOG(ERROR) << "redis format error";
					freeReplyObject(reply);
					return false;
				}
				std::string redis_cmd_str = redis_cmd;
				free(redis_cmd);
				m_redis_cmd_list.push_back(redis_cmd_str);
				
				Json::Value temp_order_book = Json::Value::null;
				temp_order_book["amount"]  = temp_json["total_amount"].asString();
				temp_order_book["price"]  = temp_json["price"].asString();
				if (m_order_op == ORDER_SIDE_OPEN_LONG || m_order_op == ORDER_SIDE_CLOSE_SHORT){
					m_statistics["order_book"]["sell"].append(temp_order_book);
				} else {
					m_statistics["order_book"]["buy"].append(temp_order_book);
				}
			} else {
				Json::Value temp_order_book = Json::Value::null;
				temp_order_book["amount"]  = switch_f_to_s(0);
				temp_order_book["price"]  = l_price_str;
				if (m_order_op == ORDER_SIDE_OPEN_LONG || m_order_op == ORDER_SIDE_CLOSE_SHORT){
					m_statistics["order_book"]["sell"].append(temp_order_book);
				} else {
					m_statistics["order_book"]["buy"].append(temp_order_book);
				}
			}
		}
		int len = reply->elements;
		freeReplyObject(reply);

		if (len < step * 2) {
			break;
		}
	}

	if (m_trade_list.size() > 0){
		long double m_order_profit_limit_l = strtold(m_order_profit_limit.c_str(), NULL);
		long double m_order_lose_limit_l = strtold(m_order_lose_limit.c_str(), NULL);
		long double last_price = switch_s_to_f(m_trade_list[m_trade_list.size() - 1]["price"].asString());

		LOG(ERROR) << "m_order_id:" << m_order_id << " m_order_profit_limit_l:" << m_order_profit_limit_l << " m_order_lose_limit_l:" << m_order_lose_limit_l << " m_order_price:" << m_order_price << " last price:" << last_price;
		if (m_order_type == ORDER_TYPE_LIMIT){
			if (m_order_op == ORDER_SIDE_OPEN_LONG){
				if (m_order_profit_limit_l > EPS && m_order_profit_limit_l < m_order_price + EPS){
					LOG(ERROR) << "m_order_id:" << m_order_id << " price limit error";
					return false;
				}
				if (m_order_lose_limit_l > EPS && m_order_lose_limit_l + EPS > m_order_price){
					LOG(ERROR) << "m_order_id:" << m_order_id << " price limit error";
					return false;
				}
				if (m_order_lose_limit_l > EPS && m_order_lose_limit_l + EPS > last_price && last_price > EPS){
					LOG(ERROR) << "m_order_id:" << m_order_id << " price limit error";
					return false;
				}
			}
			if (m_order_op == ORDER_SIDE_OPEN_SHORT){
				if (m_order_profit_limit_l > EPS && m_order_profit_limit_l + EPS > m_order_price){
					LOG(ERROR) << "m_order_id:" << m_order_id << " price limit error";
					return false;
				}
				if (m_order_lose_limit_l > EPS && m_order_lose_limit_l < m_order_price + EPS){
					LOG(ERROR) << "m_order_id:" << m_order_id << " price limit error";
					return false;
				}
				if (m_order_lose_limit_l > EPS && m_order_lose_limit_l < last_price + EPS && last_price > EPS){
					LOG(ERROR) << "m_order_id:" << m_order_id << " price limit error";
					return false;
				}
			}
		}else if (m_order_type == ORDER_TYPE_MARKET){
			if (m_order_op == ORDER_SIDE_OPEN_LONG){
				if (m_order_lose_limit_l > EPS && m_order_lose_limit_l + EPS > last_price && last_price > EPS){
					LOG(ERROR) << "m_order_id:" << m_order_id << " price limit error";
					return false;
				}
				if (m_order_profit_limit_l > EPS && m_order_profit_limit_l < last_price + EPS && last_price > EPS){
					LOG(ERROR) << "m_order_id:" << m_order_id << " price limit error";
					return false;
				}
			}
			if (m_order_op == ORDER_SIDE_OPEN_SHORT){
				if (m_order_lose_limit_l > EPS && m_order_lose_limit_l < last_price + EPS && last_price > EPS){
					LOG(ERROR) << "m_order_id:" << m_order_id << " price limit error";
					return false;
				}
				if (m_order_profit_limit_l > EPS && m_order_profit_limit_l + EPS > last_price && last_price > EPS){
					LOG(ERROR) << "m_order_id:" << m_order_id << " price limit error";
					return false;
				}
			}
		}

	}

	// 止盈止损系统接单
	if (m_order_type == ORDER_TYPE_LIMIT && (m_order_system_type == ORDER_SYSTEM_TYPE_PROFIT || m_order_system_type == ORDER_SYSTEM_TYPE_LOSS)) {
		long double stop_amount = 0.0;
		int updated_at = 0;
		redisReply* reply = NULL;
		redisAppendCommand(m_redis, "HMGET stop_amount_%d stop_amount updated_at", m_order_contract_id);
		redisGetReply(m_redis, (void**)&reply);
		if (reply == NULL) {
			LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
			redisFree(m_redis);
			m_redis = NULL;
			return false;
		}
		if (reply->type != REDIS_REPLY_NIL && reply->type != REDIS_REPLY_ARRAY){
			LOG(ERROR) << "m_order_id:" << m_order_id << " redis type error:" << reply->type;
			freeReplyObject(reply);
			return false;
		}
		if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 2 && 
				reply->element[0]->type == REDIS_REPLY_STRING && reply->element[1]->type == REDIS_REPLY_STRING){
			stop_amount = strtold(reply->element[0]->str, NULL);
			updated_at = atoi(reply->element[1]->str);
			if (updated_at < int(m_time_now / 86400) * 86400) {
				stop_amount = 0.0;
			}
		}
		freeReplyObject(reply);

		stop_amount += m_order_op == ORDER_SIDE_CLOSE_LONG ? m_remain_amount : -m_remain_amount;
		if (m_remain_amount > EPS && m_remain_amount < m_stop_amount + EPS && fabs(stop_amount) < m_max_stop_amount + EPS) {
			auto maker = m_makers.find(m_order_contract_id);
			int user_id = maker->second;
			std::string user_id_str = std::to_string(maker->second);
			if (maker != m_makers.end() && m_accounts.isMember(user_id_str) && m_accounts[user_id_str].isMember(m_settle_asset)) {
				Json::Value order_user_account_json = m_accounts[user_id_str];
				std::string balance = order_user_account_json[m_settle_asset]["balance"].asString();
				std::string margin = order_user_account_json[m_settle_asset]["margin"].asString();
				std::string frozen_margin = order_user_account_json[m_settle_asset]["frozen_margin"].asString();
				std::string profit = order_user_account_json[m_settle_asset]["profit"].asString();
				long double available_margin = strtold(balance.c_str(), NULL) + strtold(profit.c_str(), NULL) - strtold(margin.c_str(), NULL) - strtold(frozen_margin.c_str(), NULL);

				Json::Value order_user_position = m_positions[user_id_str];
				for (auto iter = order_user_position.begin(); iter != order_user_position.end(); iter++) {
					Json::Value positions = order_user_position[iter.name()];
					for (auto it = positions.begin(); it != positions.end(); it++) {
						Json::Value position = positions[it.name()];
						long double buy_amount = strtold(position["buy_amount"].asCString(), NULL);
						long double buy_quote_amount_settle = strtold(position["buy_quote_amount_settle"].asCString(), NULL);
						long double sell_amount = strtold(position["sell_amount"].asCString(), NULL);
						long double sell_quote_amount_settle = strtold(position["sell_quote_amount_settle"].asCString(), NULL);

						long double buy_profit = 0;
						long double sell_profit = 0;
						if (buy_amount > EPS) {
							long double price = strtold(m_contract_price[iter.name()].asCString(), NULL);
							buy_profit = buy_amount * price * m_rate - buy_quote_amount_settle; 
						}
						if (sell_amount > EPS) { 
							long double price = strtold(m_contract_price[iter.name()].asCString(), NULL);
							sell_profit = sell_quote_amount_settle - sell_amount * price * m_rate; 
						}
						available_margin = available_margin + buy_profit + sell_profit;
					}
				}

				long double executed_quote_amount = m_remain_amount * m_order_price;
				long double executed_settle_amount = switch_s_to_f(switch_f_to_s(m_remain_amount * m_order_price * m_rate));

				m_cur_trade_id = GetListId();
				if (m_cur_trade_id == EmptyString){
					return false;
				}
				m_new_order_id = GetListId();
				if (m_new_order_id == EmptyString){
					return false;
				}
				if (available_margin * m_lever_rate + EPS > executed_quote_amount) {
					m_trade_users_set.insert(user_id);
					std::map<int, std::set<int>>::iterator it;
					it = m_trade_users_lever_rate.find(user_id);
					if (it == m_trade_users_lever_rate.end()){
						std::set<int> users_lever_rate;
						users_lever_rate.insert(atoi(m_contract_config[m_order_contract_id_str]["lever_rate"].asCString()));
						m_trade_users_lever_rate.insert(std::map<int, std::set<int>>::value_type(user_id, users_lever_rate));
					}else{
						it->second.insert(atoi(m_contract_config[m_order_contract_id_str]["lever_rate"].asCString()));
					}
					
					m_executed_quote_amount += executed_quote_amount;
					m_executed_settle_amount += executed_settle_amount;

					// 成交记录
					Json::Value temp_trade = Json::Value::null;
					temp_trade["price"] = switch_f_to_s(m_order_price);
					temp_trade["amount"] = switch_f_to_s(m_remain_amount);
					temp_trade["lever_rate"] = atoi(m_contract_config[m_order_contract_id_str]["lever_rate"].asCString());
					temp_trade["order_op"] = m_order_op == ORDER_SIDE_CLOSE_LONG ? ORDER_SIDE_OPEN_LONG : ORDER_SIDE_OPEN_SHORT;
					temp_trade["system_type"] = m_order_system_type;
					temp_trade["profit_limit"] = switch_f_to_s(0);
					temp_trade["lose_limit"] = switch_f_to_s(0);
					temp_trade["frozen_margin"] = switch_f_to_s(m_remain_amount * m_order_price * m_rate / m_lever_rate);
					temp_trade["user_id"] = user_id;
					temp_trade["order_id"] = m_new_order_id;
					temp_trade["id"] = m_cur_trade_id;
					m_trade_list.append(temp_trade);

					// 订单成交
					Json::Value tmp_order_result = Json::Value::null;
					tmp_order_result["type"] = "order";
					tmp_order_result["is_new"] = 1;
					tmp_order_result["contract_id"] = m_order_contract_id;
					tmp_order_result["contract_name"] = m_contract_name;
					tmp_order_result["asset_symbol"] = m_order_base_asset;
					tmp_order_result["unit_amount"] = switch_f_to_s(m_unit_amount);
					tmp_order_result["lever_rate"] = atoi(m_contract_config[m_order_contract_id_str]["lever_rate"].asCString());
					tmp_order_result["settle_asset"] = m_settle_asset;
					tmp_order_result["id"] = m_new_order_id;
					tmp_order_result["user_id"] = user_id;
					tmp_order_result["order_type"] = ORDER_TYPE_LIMIT;
					tmp_order_result["order_isbbo"] = 0;
					tmp_order_result["order_op"] = m_order_op == ORDER_SIDE_CLOSE_LONG ? ORDER_SIDE_OPEN_LONG : ORDER_SIDE_OPEN_SHORT;
					tmp_order_result["system_type"] = m_order_system_type;
					tmp_order_result["profit_limit"] = switch_f_to_s(0);
					tmp_order_result["lose_limit"] = switch_f_to_s(0);
					tmp_order_result["price"] = switch_f_to_s(m_order_price);
					tmp_order_result["amount"] = switch_f_to_s(0);
					tmp_order_result["origin_amount"] = switch_f_to_s(m_remain_amount);
					tmp_order_result["executed_quote_amount"] = switch_f_to_s(executed_quote_amount);
					tmp_order_result["executed_settle_amount"] = switch_f_to_s(executed_settle_amount);
					tmp_order_result["frozen_margin"] = switch_f_to_s(0);
					tmp_order_result["status"] = ORDER_STATUS_FILLED;
					tmp_order_result["update_at"] = m_time_now;
					tmp_order_result["update_at"] = m_time_now;
					tmp_order_result["ip"] = m_order_ip;
					tmp_order_result["source"] = m_order_source;
					m_order_result_list.append(tmp_order_result);
					m_statistics["order"].append(tmp_order_result);
					
					// 完全成交
					std::string stop_amount_str = switch_f_to_s(stop_amount);
					m_remain_amount = 0;
					char* redis_cmd = NULL;
					if (redisFormatCommand(&redis_cmd, "HMSET stop_amount_%d stop_amount %s updated_at %d", m_order_contract_id, stop_amount_str.c_str(), m_time_now) <= 0){
						LOG(ERROR) << "redis format error";
						freeReplyObject(reply);
						return false;
					}
					m_redis_cmd_list.push_back(redis_cmd);
					free(redis_cmd);
				} else {
					LOG(INFO) << "m_order_id:" << m_order_id << " available_margin:" << available_margin << " m_lever_rate:" << m_lever_rate << " executed_quote_amount:" << executed_quote_amount;
				}
			}
		} else {
			LOG(INFO) << "m_order_id:" << m_order_id << " m_remain_amount:" << m_remain_amount << " m_stop_amount:" << m_stop_amount << " stop_amount:" << stop_amount << " m_max_stop_amount:" << m_max_stop_amount;
		}
	}

	Json::Value tmp_order_result = Json::Value::null;
	tmp_order_result["type"] = "order";
	tmp_order_result["is_new"] = 1;
	tmp_order_result["contract_id"] = m_order_contract_id;
	tmp_order_result["contract_name"] = m_contract_name;
	tmp_order_result["asset_symbol"] = m_order_base_asset;
	tmp_order_result["unit_amount"] = switch_f_to_s(m_unit_amount);
	tmp_order_result["lever_rate"] = m_lever_rate;
	tmp_order_result["settle_asset"] = m_settle_asset;
	tmp_order_result["id"] = m_order_id;
	tmp_order_result["user_id"] = m_order_user_id;
	tmp_order_result["order_type"] = m_order_type;
	tmp_order_result["order_isbbo"] = m_order_isbbo;
	tmp_order_result["order_op"] = m_order_op;
	tmp_order_result["system_type"] = m_order_system_type;
	tmp_order_result["profit_limit"] = m_order_profit_limit;
	tmp_order_result["lose_limit"] = m_order_lose_limit;
	tmp_order_result["amount"] = switch_f_to_s(m_remain_amount);
	tmp_order_result["origin_amount"] = m_order_amt_str;
	tmp_order_result["executed_quote_amount"] = switch_f_to_s(m_executed_quote_amount);
	tmp_order_result["executed_settle_amount"] = switch_f_to_s(m_executed_settle_amount);
	tmp_order_result["frozen_margin"] = switch_f_to_s(m_remain_amount * m_order_price * m_rate / m_lever_rate);
	m_frozen_margin = switch_s_to_f(tmp_order_result["frozen_margin"].asString());
	tmp_order_result["create_at"] = m_time_now;
	tmp_order_result["update_at"] = m_time_now;
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
		} else if (switch_f_to_s(m_remain_amount) == m_order_amt_str){
			tmp_order_result["status"] = ORDER_STATUS_CANCELED;
		} else{
			tmp_order_result["status"] = ORDER_STATUS_PARTIALLY_CANCELED;
		}
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
	
	for (auto it = m_trade_users_set.begin(); it != m_trade_users_set.end(); it++){
		int trade_user = *it;
		m_trade_users_list.push_back(trade_user);
	}
	
	for (int i = 0; i < (int)m_trade_users_list.size(); i++){
		if (!m_accounts.isMember(std::to_string(m_trade_users_list[i]))){
			LOG(ERROR) << "account is null user_id:" << m_trade_users_list[i];
			return false;
		}
		Json::Value user_account_json = m_accounts[std::to_string(m_trade_users_list[i])];
		if (!user_account_json.isMember(m_settle_asset)){
			user_account_json[m_settle_asset]["balance"] = "0.00000000";
			user_account_json[m_settle_asset]["margin"] = "0.00000000";
			user_account_json[m_settle_asset]["frozen_margin"] = "0.00000000";
			user_account_json[m_settle_asset]["profit"] = "0.00000000";
			user_account_json[m_settle_asset]["encode_balance"] = HmacSha256Encode(std::to_string(m_trade_users_list[i]) + m_settle_asset + "0.00000000");
			user_account_json[m_settle_asset]["encode_margin"] = HmacSha256Encode(std::to_string(m_trade_users_list[i]) + m_settle_asset + "0.00000000");
			user_account_json[m_settle_asset]["encode_frozen_margin"] = HmacSha256Encode(std::to_string(m_trade_users_list[i]) + m_settle_asset + "0.00000000");
			user_account_json[m_settle_asset]["encode_profit"] = HmacSha256Encode(std::to_string(m_trade_users_list[i]) + m_settle_asset + "0.00000000");
		}
		m_user_account[std::to_string(m_trade_users_list[i])]["funds"] = user_account_json;

		Json::Value user_position_json = m_positions[std::to_string(m_trade_users_list[i])];
		std::map<int, std::set<int>>::iterator it;
		it = m_trade_users_lever_rate.find(m_trade_users_list[i]);
		if (it != m_trade_users_lever_rate.end()){
			std::set<int>::iterator iter;
			for (iter = it->second.begin(); iter != it->second.end(); iter++){
				int cur_lever_rate = *iter;
				std::string cur_lever_rate_str = std::to_string(cur_lever_rate);
				if (!user_position_json[m_order_contract_id_str].isMember(cur_lever_rate_str)) {
					user_position_json[m_order_contract_id_str][cur_lever_rate_str]["buy_frozen"] = "0.00000000";
					user_position_json[m_order_contract_id_str][cur_lever_rate_str]["buy_margin"] = "0.00000000";
					user_position_json[m_order_contract_id_str][cur_lever_rate_str]["buy_amount"] = "0.00000000";
					user_position_json[m_order_contract_id_str][cur_lever_rate_str]["buy_available"] = "0.00000000";
					user_position_json[m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount"] = "0.00000000";
					user_position_json[m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount_settle"] = "0.00000000";
					user_position_json[m_order_contract_id_str][cur_lever_rate_str]["buy_profit"] = "0.00000000";
					user_position_json[m_order_contract_id_str][cur_lever_rate_str]["buy_profit_limit"] = "0.00000000";
					user_position_json[m_order_contract_id_str][cur_lever_rate_str]["buy_lose_limit"] = "0.00000000";
					user_position_json[m_order_contract_id_str][cur_lever_rate_str]["sell_frozen"] = "0.00000000";
					user_position_json[m_order_contract_id_str][cur_lever_rate_str]["sell_margin"] = "0.00000000";
					user_position_json[m_order_contract_id_str][cur_lever_rate_str]["sell_amount"] = "0.00000000";
					user_position_json[m_order_contract_id_str][cur_lever_rate_str]["sell_available"] = "0.00000000";
					user_position_json[m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount"] = "0.00000000";
					user_position_json[m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount_settle"] = "0.00000000";
					user_position_json[m_order_contract_id_str][cur_lever_rate_str]["sell_profit"] = "0.00000000";
					user_position_json[m_order_contract_id_str][cur_lever_rate_str]["sell_profit_limit"] = "0.00000000";
					user_position_json[m_order_contract_id_str][cur_lever_rate_str]["sell_lose_limit"] = "0.00000000";
				}
			}
		}
		
		m_user_position[std::to_string(m_trade_users_list[i])] = user_position_json;
	}
	Json::Value default_fee_setting = Json::Value::null;
	default_fee_setting["taker"] = 1;
	default_fee_setting["maker"] = 1;
	for (int i = 0; i < (int)m_trade_users_list.size(); i++){
		int len;
		len = redisFormatCommand(&redis_cmd, "HMGET user_trade_fee_setting %d %d_%d", m_trade_users_list[i], m_order_contract_id, m_trade_users_list[i]);
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
	bool flag = true;
	for (unsigned i = 0; i < m_trade_users_list.size(); i++){
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
			m_user_account[std::to_string(m_trade_users_list[i])]["fee_setting"] = default_fee_setting;
			freeReplyObject(temp_reply1);
		}else{
			std::string user_fee_setting_str;
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

			if (tmp_maker_fee < -1 || tmp_maker_fee > 1 || time_now < time_start || time_now > time_end){
				user_fee_setting_json["maker"] = 1;
			}
			if (tmp_taker_fee < -1 || tmp_taker_fee > 1 || time_now < time_start || time_now > time_end){
				user_fee_setting_json["taker"] = 1;
			}
			m_user_account[std::to_string(m_trade_users_list[i])]["fee_setting"] = user_fee_setting_json;
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
		m_user_account[std::to_string(m_trade_users_list[i])]["KK_switch"] = 0;
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
	
	if (m_order_type == ORDER_TYPE_LIMIT && m_remain_amount > EPS) {
		if (m_order_op == ORDER_SIDE_OPEN_LONG || m_order_op == ORDER_SIDE_OPEN_SHORT) {
			Json::Value tmp_obj = Json::Value::null;
			tmp_obj["type"] = "account";
			tmp_obj["id"] = m_order_id;
			tmp_obj["user_id"] = m_order_user_id;
			tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_ORDER_FROZEN;
			tmp_obj["remark"] = "下单";
			tmp_obj["remark_tw"] = "下單";
			tmp_obj["remark_en"] = "Order";
			tmp_obj["remark_vi"] = "Đặt lệnh";
			tmp_obj["update_at"] = m_time_now;
			tmp_obj["asset"] = m_settle_asset;
			tmp_obj["contract"] = m_contract_name;

			tmp_obj["change_balance"] = switch_f_to_s(0.0);
			tmp_obj["new_balance"] = m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["balance"].asString();
			tmp_obj["change_margin"] = switch_f_to_s(0.0);
			tmp_obj["new_margin"] = m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["margin"].asString();
			tmp_obj["change_frozen_margin"] = switch_f_to_s(m_frozen_margin);
			tmp_obj["new_frozen_margin"] = switch_f_to_s(switch_s_to_f(m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["frozen_margin"].asString()) + switch_s_to_f(tmp_obj["change_frozen_margin"].asString()));
			tmp_obj["change_profit"] = switch_f_to_s(0.0);
			tmp_obj["new_profit"] = m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["profit"].asString();

			m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["frozen_margin"] = tmp_obj["new_frozen_margin"];
			m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["encode_frozen_margin"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_settle_asset + tmp_obj["new_frozen_margin"].asString());
			
			tmp_obj["new_encode_balance"] = m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["encode_balance"];
			tmp_obj["new_encode_margin"] = m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["encode_margin"];
			tmp_obj["new_encode_frozen_margin"] = m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["encode_frozen_margin"];
			tmp_obj["new_encode_profit"] = m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["encode_profit"];
			m_order_result_list.append(tmp_obj);
			
			if (m_order_op == ORDER_SIDE_OPEN_LONG) {
				m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_frozen"] = switch_f_to_s(switch_s_to_f(m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_frozen"].asString()) + m_remain_amount);
			} else {
				m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_frozen"] = switch_f_to_s(switch_s_to_f(m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_frozen"].asString()) + m_remain_amount);
			}
		} else {
			if (m_order_op == ORDER_SIDE_CLOSE_LONG) {
				m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_available"] = switch_f_to_s(switch_s_to_f(m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_available"].asString()) - m_remain_amount);
			} else {
				m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_available"] = switch_f_to_s(switch_s_to_f(m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_available"].asString()) - m_remain_amount);
			}
		}
	}

	LOG(INFO) << "m_order_id:" << m_order_id << " Frozen end";
	return true;
}

bool Match::SettleTrade(){
	LOG(INFO) << "m_order_id:" << m_order_id << " SettleTrade start";
	
	// 重置止盈止损
	if (m_order_system_type == ORDER_SYSTEM_TYPE_PROFIT) {
		if (m_order_op == ORDER_SIDE_CLOSE_LONG) {
			m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_profit_limit"] = switch_f_to_s(0);
		} else {
			m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_profit_limit"] = switch_f_to_s(0);
		}
	} else if (m_order_system_type == ORDER_SYSTEM_TYPE_LOSS) {
		if (m_order_op == ORDER_SIDE_CLOSE_LONG) {
			m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_lose_limit"] = switch_f_to_s(0);
		} else {
			m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_lose_limit"] = switch_f_to_s(0);
		}
	}

	for (unsigned i = 0; i < m_trade_list.size(); i++){
		Json::Value tmp_obj = Json::Value::null;
		std::string order_user_id = m_order_user_id_str;
		int order_op = m_order_op;
		int cur_lever_rate = m_lever_rate;
		std::string cur_lever_rate_str = m_lever_rate_str;
		std::string order_id = m_order_id;
		std::string close_profit;
		tmp_obj["type"] = "account";
		tmp_obj["id"] = m_trade_list[i]["id"].asString();
		tmp_obj["update_at"] = m_time_now;
		tmp_obj["user_id"] = m_order_user_id;
		tmp_obj["asset"] = m_settle_asset;
		tmp_obj["contract"] = m_contract_name;
		if (order_op == ORDER_SIDE_OPEN_LONG || order_op == ORDER_SIDE_OPEN_SHORT) {

			// 开仓
			tmp_obj["change_balance"] = switch_f_to_s(0);
			tmp_obj["new_balance"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][m_settle_asset]["balance"].asString()) + switch_s_to_f(tmp_obj["change_balance"].asString()));
			tmp_obj["change_margin"] = switch_f_to_s(switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_trade_list[i]["price"].asString()) * m_rate / cur_lever_rate);
			tmp_obj["new_margin"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][m_settle_asset]["margin"].asString()) + switch_s_to_f(tmp_obj["change_margin"].asString()));
			tmp_obj["change_frozen_margin"] = switch_f_to_s(0);
			tmp_obj["new_frozen_margin"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][m_settle_asset]["frozen_margin"].asString()) + switch_s_to_f(tmp_obj["change_frozen_margin"].asString()));
			tmp_obj["change_profit"] = switch_f_to_s(0);
			tmp_obj["new_profit"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][m_settle_asset]["profit"].asString()) + switch_s_to_f(tmp_obj["change_profit"].asString()));
			
			m_user_account[order_user_id]["funds"][m_settle_asset]["margin"] = tmp_obj["new_margin"];
			m_user_account[order_user_id]["funds"][m_settle_asset]["encode_margin"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_settle_asset + tmp_obj["new_margin"].asString());

			tmp_obj["new_encode_balance"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_balance"];
			tmp_obj["new_encode_margin"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_margin"];
			tmp_obj["new_encode_frozen_margin"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_frozen_margin"];
			tmp_obj["new_encode_profit"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_profit"];

			if (order_op == ORDER_SIDE_OPEN_LONG) {
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_margin"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_margin"].asString()) + switch_s_to_f(tmp_obj["change_margin"].asString()));
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_amount"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_amount"].asString()) + switch_s_to_f(m_trade_list[i]["amount"].asString()));
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_available"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_available"].asString()) + switch_s_to_f(m_trade_list[i]["amount"].asString()));
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount"].asString()) + switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_trade_list[i]["price"].asString()));
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount_settle"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount_settle"].asString()) + switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_trade_list[i]["price"].asString()) * m_rate);
			} else {
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_margin"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_margin"].asString()) + switch_s_to_f(tmp_obj["change_margin"].asString()));
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_amount"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_amount"].asString()) + switch_s_to_f(m_trade_list[i]["amount"].asString()));
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_available"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_available"].asString()) + switch_s_to_f(m_trade_list[i]["amount"].asString()));
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount"].asString()) + switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_trade_list[i]["price"].asString()));
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount_settle"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount_settle"].asString()) + switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_trade_list[i]["price"].asString()) * m_rate);
			}
		} else {
			// 平仓
			if (order_op == ORDER_SIDE_CLOSE_LONG) {
				long double open_margin = switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_margin"].asString());
				long double open_amount = switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_amount"].asString());
				long double open_quote_amount = switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount"].asString());
				long double open_quote_amount_settle = switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount_settle"].asString());
				long double open_buy_available = switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_available"].asString());

				long double close_amount = switch_s_to_f(m_trade_list[i]["amount"].asString());
				long double close_price = switch_s_to_f(m_trade_list[i]["price"].asString());

				long double ratio =  switch_s_to_f(switch_f_to_s(close_amount / open_amount));
				long double change_margin = -switch_s_to_f(switch_f_to_s(open_margin * ratio));
				long double change_quote_amount = -switch_s_to_f(switch_f_to_s(open_quote_amount * ratio));
				long double change_quote_amount_settle = -switch_s_to_f(switch_f_to_s(open_quote_amount_settle * ratio));
				long double change_profit = switch_s_to_f(switch_f_to_s(close_price * close_amount * m_rate)) + change_quote_amount_settle;
				long double buy_margin = open_margin + change_margin;
				long double buy_amount = open_amount - close_amount;
				long double buy_available = open_buy_available - close_amount;
				long double buy_quote_amount = open_quote_amount + change_quote_amount;
				long double buy_quote_amount_settle = open_quote_amount_settle + change_quote_amount_settle;

				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_margin"] = switch_f_to_s(buy_margin);
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_amount"] = switch_f_to_s(buy_amount);
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_available"] = switch_f_to_s(buy_available);
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount"] = switch_f_to_s(buy_quote_amount);
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount_settle"] = switch_f_to_s(buy_quote_amount_settle);
				tmp_obj["change_margin"] = switch_f_to_s(change_margin);
				tmp_obj["change_balance"] = switch_f_to_s(change_profit);
			} else {
				long double open_margin = switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_margin"].asString());
				long double open_amount = switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_amount"].asString());
				long double open_quote_amount = switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount"].asString());
				long double open_quote_amount_settle = switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount_settle"].asString());
				long double open_sell_available = switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_available"].asString());
        
				long double close_amount = switch_s_to_f(m_trade_list[i]["amount"].asString());
				long double close_price = switch_s_to_f(m_trade_list[i]["price"].asString());

				long double ratio = switch_s_to_f(switch_f_to_s(close_amount / open_amount));
				long double change_margin = -switch_s_to_f(switch_f_to_s(open_margin * ratio));
				long double change_quote_amount = -switch_s_to_f(switch_f_to_s(open_quote_amount * ratio));
				long double change_quote_amount_settle = -switch_s_to_f(switch_f_to_s(open_quote_amount_settle * ratio));
				long double change_profit = -change_quote_amount_settle - switch_s_to_f(switch_f_to_s(close_price * close_amount * m_rate));
				long double sell_margin = open_margin + change_margin;
				long double sell_amount = open_amount - close_amount;
				long double sell_available = open_sell_available - close_amount;
				long double sell_quote_amount = open_quote_amount + change_quote_amount;
				long double sell_quote_amount_settle = open_quote_amount_settle + change_quote_amount_settle;

				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_margin"] = switch_f_to_s(sell_margin);
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_amount"] = switch_f_to_s(sell_amount);
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_available"] = switch_f_to_s(sell_available);
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount"] = switch_f_to_s(sell_quote_amount);
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount_settle"] = switch_f_to_s(sell_quote_amount_settle);
				tmp_obj["change_margin"] = switch_f_to_s(change_margin);
				tmp_obj["change_balance"] = switch_f_to_s(change_profit);
			}

			tmp_obj["change_profit"] = switch_f_to_s(0);
			tmp_obj["change_frozen_margin"] = switch_f_to_s(0);
			tmp_obj["new_balance"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][m_settle_asset]["balance"].asString()) + switch_s_to_f(tmp_obj["change_balance"].asString()));
			tmp_obj["new_margin"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][m_settle_asset]["margin"].asString()) + switch_s_to_f(tmp_obj["change_margin"].asString()));
			tmp_obj["new_frozen_margin"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][m_settle_asset]["frozen_margin"].asString()) + switch_s_to_f(tmp_obj["change_frozen_margin"].asString()));
			tmp_obj["new_profit"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][m_settle_asset]["profit"].asString()) + switch_s_to_f(tmp_obj["change_profit"].asString()));
			
			m_user_account[order_user_id]["funds"][m_settle_asset]["margin"] = tmp_obj["new_margin"];
			m_user_account[order_user_id]["funds"][m_settle_asset]["encode_margin"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_settle_asset + tmp_obj["new_margin"].asString());
			m_user_account[order_user_id]["funds"][m_settle_asset]["balance"] = tmp_obj["new_balance"];
			m_user_account[order_user_id]["funds"][m_settle_asset]["encode_balance"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_settle_asset + tmp_obj["new_balance"].asString());
			
			tmp_obj["new_encode_balance"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_balance"];
			tmp_obj["new_encode_margin"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_margin"];
			tmp_obj["new_encode_frozen_margin"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_frozen_margin"];
			tmp_obj["new_encode_profit"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_profit"];
		}
		close_profit = tmp_obj["change_balance"].asString();
		if (order_op == ORDER_SIDE_OPEN_LONG) {
			tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_OPEN_LONG;
			tmp_obj["remark"] = "开多";
			tmp_obj["remark_tw"] = "開多";
			tmp_obj["remark_en"] = "Open Long";
			tmp_obj["remark_vi"] = "Mở Long";
		} else if (order_op == ORDER_SIDE_OPEN_SHORT) {
			tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_OPEN_SHORT;
			tmp_obj["remark"] = "开空";
			tmp_obj["remark_tw"] = "開空";
			tmp_obj["remark_en"] = "Sell Short";
			tmp_obj["remark_vi"] = "Mở Short";
		} else if (order_op == ORDER_SIDE_CLOSE_LONG) {
			tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_CLOSE_LONG;
			tmp_obj["remark"] = "平多";
			tmp_obj["remark_tw"] = "平多";
			tmp_obj["remark_en"] = "Liquidate Long";
			tmp_obj["remark_vi"] = "Đóng Long";
		} else {
			tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_CLOSE_SHORT;
			tmp_obj["remark"] = "平空";
			tmp_obj["remark_tw"] = "平空";
			tmp_obj["remark_en"] = "Liquidate Short";
			tmp_obj["remark_vi"] = "Đóng Short";
		}
		m_order_result_list.append(tmp_obj);

		// 计算手续费
		std::string fee_coin = m_settle_asset;
		long double fee_amount = m_taker_fee * switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_trade_list[i]["price"].asString()) * m_rate * switch_s_to_f(m_user_account[order_user_id]["fee_setting"]["taker"].asString());
		fee_amount = switch_s_to_f(switch_f_to_s(fee_amount));
		if (switch_f_to_s(fee_amount) != switch_f_to_s(0)){
			tmp_obj = Json::Value::null;
			tmp_obj["type"] = "account";
			tmp_obj["id"] = m_trade_list[i]["id"].asString();
			tmp_obj["user_id"] = atoi(order_user_id.c_str());
			tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_FEE;
			tmp_obj["remark"] = "交易手续费";
			tmp_obj["remark_tw"] = "交易手續費";
			tmp_obj["remark_en"] = "Transaction Fee";
			tmp_obj["remark_vi"] = "Phí thủ tục GD";
			tmp_obj["update_at"] = m_time_now;
			tmp_obj["asset"] = fee_coin;
			tmp_obj["contract"] = m_contract_name;
			tmp_obj["change_balance"] = switch_f_to_s(-fee_amount);
			tmp_obj["new_balance"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][fee_coin]["balance"].asString()) - fee_amount);
			tmp_obj["change_margin"] = switch_f_to_s(0);
			tmp_obj["new_margin"] = m_user_account[order_user_id]["funds"][fee_coin]["margin"].asString();
			tmp_obj["change_frozen_margin"] = switch_f_to_s(0);
			tmp_obj["new_frozen_margin"] = m_user_account[order_user_id]["funds"][fee_coin]["frozen_margin"];
			tmp_obj["change_profit"] = switch_f_to_s(0);
			tmp_obj["new_profit"] = m_user_account[order_user_id]["funds"][fee_coin]["profit"];
			
			m_user_account[order_user_id]["funds"][fee_coin]["balance"] = tmp_obj["new_balance"];
			m_user_account[order_user_id]["funds"][fee_coin]["encode_balance"] = HmacSha256Encode(tmp_obj["user_id"].asString() + fee_coin + tmp_obj["new_balance"].asString());
			m_user_account[order_user_id]["funds"][fee_coin]["profit"] = tmp_obj["new_profit"];
			m_user_account[order_user_id]["funds"][fee_coin]["encode_profit"] = HmacSha256Encode(tmp_obj["user_id"].asString() + fee_coin + tmp_obj["new_profit"].asString());
			
			tmp_obj["new_encode_balance"] = m_user_account[order_user_id]["funds"][fee_coin]["encode_balance"];
			tmp_obj["new_encode_margin"] = m_user_account[order_user_id]["funds"][fee_coin]["encode_margin"];
			tmp_obj["new_encode_frozen_margin"] = m_user_account[order_user_id]["funds"][fee_coin]["encode_frozen_margin"];
			tmp_obj["new_encode_profit"] = m_user_account[order_user_id]["funds"][fee_coin]["encode_profit"];

			m_order_result_list.append(tmp_obj);
		}
		// 成交记录
		Json::Value tmp_trade_obj =  Json::Value::null;
		tmp_trade_obj["type"] = "trade";
		tmp_trade_obj["contract_id"] = m_order_contract_id;
		tmp_trade_obj["contract_name"] = m_contract_name;
		tmp_trade_obj["asset_symbol"] = m_order_base_asset;
		tmp_trade_obj["unit_amount"] = switch_f_to_s(m_unit_amount);
		tmp_trade_obj["lever_rate"] = cur_lever_rate;
		tmp_trade_obj["settle_asset"] = m_settle_asset;
		tmp_trade_obj["id"] = m_trade_list[i]["id"].asString();
		tmp_trade_obj["order_id"] = order_id;
		tmp_trade_obj["user_id"] = atoi(order_user_id.c_str());
		tmp_trade_obj["amount"] = m_trade_list[i]["amount"].asString();
		tmp_trade_obj["quote_amount"] = switch_f_to_s(switch_s_to_f(m_trade_list[i]["price"].asString()) * switch_s_to_f(m_trade_list[i]["amount"].asString()));
		tmp_trade_obj["settle_amount"] = switch_f_to_s(switch_s_to_f(m_trade_list[i]["price"].asString()) * switch_s_to_f(m_trade_list[i]["amount"].asString()) * m_rate);
		tmp_trade_obj["price"] = m_trade_list[i]["price"].asString();
		tmp_trade_obj["profit"] = close_profit;
		tmp_trade_obj["is_maker"] = 0;
		tmp_trade_obj["create_at"] = m_time_now;
		tmp_trade_obj["fee_asset"] = fee_coin;
		tmp_trade_obj["fee_amount"] = switch_f_to_s(fee_amount);
		tmp_trade_obj["order_op"] = order_op;
		m_order_result_list.append(tmp_trade_obj);

		tmp_obj = Json::Value::null;
		order_user_id = std::to_string(m_trade_list[i]["user_id"].asInt());
		order_op = m_trade_list[i]["order_op"].asInt();
		cur_lever_rate = m_trade_list[i]["lever_rate"].asInt();
		cur_lever_rate_str = std::to_string(cur_lever_rate);
		order_id =  m_trade_list[i]["order_id"].asString();
		tmp_obj["type"] = "account";
		tmp_obj["id"] = m_trade_list[i]["id"].asString();
		tmp_obj["update_at"] = m_time_now;
		tmp_obj["user_id"] = m_trade_list[i]["user_id"].asInt();
		tmp_obj["asset"] = m_settle_asset;
		tmp_obj["contract"] = m_contract_name;

		if ((m_trade_list[i]["system_type"] == ORDER_SYSTEM_TYPE_PROFIT || m_trade_list[i]["system_type"] == ORDER_SYSTEM_TYPE_LOSS) 
				&& maker_user_set.find(m_trade_list[i]["user_id"].asInt()) != maker_user_set.end()) {
			// 止盈止损系统接单，冻结保证金
			if (order_op == ORDER_SIDE_OPEN_LONG || order_op == ORDER_SIDE_OPEN_SHORT) {
				Json::Value tmp_obj = Json::Value::null;
				tmp_obj["type"] = "account";
				tmp_obj["id"] = m_trade_list[i]["order_id"].asString();
				tmp_obj["user_id"] = m_trade_list[i]["user_id"].asInt();
				tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_ORDER_FROZEN;
				tmp_obj["remark"] = "下单";
				tmp_obj["remark_tw"] = "下單";
				tmp_obj["remark_en"] = "Order";
				tmp_obj["remark_vi"] = "Đặt lệnh";
				tmp_obj["update_at"] = m_time_now;
				tmp_obj["asset"] = m_settle_asset;
				tmp_obj["contract"] = m_contract_name;

				tmp_obj["change_balance"] = switch_f_to_s(0.0);
				tmp_obj["new_balance"] = m_user_account[order_user_id]["funds"][m_settle_asset]["balance"].asString();
				tmp_obj["change_margin"] = switch_f_to_s(0.0);
				tmp_obj["new_margin"] = m_user_account[order_user_id]["funds"][m_settle_asset]["margin"].asString();
				tmp_obj["change_frozen_margin"] = m_trade_list[i]["frozen_margin"];
				tmp_obj["new_frozen_margin"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][m_settle_asset]["frozen_margin"].asString()) + switch_s_to_f(tmp_obj["change_frozen_margin"].asString()));
				tmp_obj["change_profit"] = switch_f_to_s(0.0);
				tmp_obj["new_profit"] = m_user_account[order_user_id]["funds"][m_settle_asset]["profit"].asString();

				m_user_account[order_user_id]["funds"][m_settle_asset]["frozen_margin"] = tmp_obj["new_frozen_margin"];
				m_user_account[order_user_id]["funds"][m_settle_asset]["encode_frozen_margin"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_settle_asset + tmp_obj["new_frozen_margin"].asString());
				
				tmp_obj["new_encode_balance"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_balance"];
				tmp_obj["new_encode_margin"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_margin"];
				tmp_obj["new_encode_frozen_margin"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_frozen_margin"];
				tmp_obj["new_encode_profit"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_profit"];
				m_order_result_list.append(tmp_obj);
				
				if (order_op == ORDER_SIDE_OPEN_LONG) {
					m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_frozen"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_frozen"].asString()) + switch_s_to_f(m_trade_list[i]["amount"].asString()));
				} else {
					m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_frozen"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_frozen"].asString()) + switch_s_to_f(m_trade_list[i]["amount"].asString()));
				}
			}
		}

		if (order_op == ORDER_SIDE_OPEN_LONG || order_op == ORDER_SIDE_OPEN_SHORT) {
			// 开仓
			tmp_obj["change_balance"] = switch_f_to_s(0);
			tmp_obj["new_balance"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][m_settle_asset]["balance"].asString()) + switch_s_to_f(tmp_obj["change_balance"].asString()));
			tmp_obj["change_margin"] = switch_f_to_s(switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_trade_list[i]["price"].asString()) * m_rate / cur_lever_rate);
			tmp_obj["new_margin"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][m_settle_asset]["margin"].asString()) + switch_s_to_f(tmp_obj["change_margin"].asString()));
			tmp_obj["change_frozen_margin"] = switch_f_to_s(-switch_s_to_f(m_trade_list[i]["frozen_margin"].asString()));
			tmp_obj["new_frozen_margin"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][m_settle_asset]["frozen_margin"].asString()) + switch_s_to_f(tmp_obj["change_frozen_margin"].asString()));
			tmp_obj["change_profit"] = switch_f_to_s(0);
			tmp_obj["new_profit"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][m_settle_asset]["profit"].asString()) + switch_s_to_f(tmp_obj["change_profit"].asString()));
			
			m_user_account[order_user_id]["funds"][m_settle_asset]["margin"] = tmp_obj["new_margin"];
			m_user_account[order_user_id]["funds"][m_settle_asset]["encode_margin"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_settle_asset + tmp_obj["new_margin"].asString());
			m_user_account[order_user_id]["funds"][m_settle_asset]["frozen_margin"] = tmp_obj["new_frozen_margin"];
			m_user_account[order_user_id]["funds"][m_settle_asset]["encode_frozen_margin"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_settle_asset + tmp_obj["new_frozen_margin"].asString());
			
			tmp_obj["new_encode_balance"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_balance"];
			tmp_obj["new_encode_margin"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_margin"];
			tmp_obj["new_encode_frozen_margin"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_frozen_margin"];
			tmp_obj["new_encode_profit"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_profit"];

			if (order_op == ORDER_SIDE_OPEN_LONG) {
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_frozen"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_frozen"].asString()) - switch_s_to_f(m_trade_list[i]["amount"].asString()));
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_margin"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_margin"].asString()) + switch_s_to_f(tmp_obj["change_margin"].asString()));
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_amount"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_amount"].asString()) + switch_s_to_f(m_trade_list[i]["amount"].asString()));
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_available"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_available"].asString()) + switch_s_to_f(m_trade_list[i]["amount"].asString()));
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount"].asString()) + switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_trade_list[i]["price"].asString()));
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount_settle"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount_settle"].asString()) + switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_trade_list[i]["price"].asString()) * m_rate);
			} else {
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_frozen"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_frozen"].asString()) - switch_s_to_f(m_trade_list[i]["amount"].asString()));
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_margin"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_margin"].asString()) + switch_s_to_f(tmp_obj["change_margin"].asString()));
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_amount"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_amount"].asString()) + switch_s_to_f(m_trade_list[i]["amount"].asString()));
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_available"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_available"].asString()) + switch_s_to_f(m_trade_list[i]["amount"].asString()));
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount"].asString()) + switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_trade_list[i]["price"].asString()));
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount_settle"] = switch_f_to_s(switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount_settle"].asString()) + switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_trade_list[i]["price"].asString()) * m_rate);
			}
		} else {
			// 平仓
			if (order_op == ORDER_SIDE_CLOSE_LONG) {
				long double open_margin = switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_margin"].asString());
				long double open_amount = switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_amount"].asString());
				long double open_quote_amount = switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount"].asString());
				long double open_quote_amount_settle = switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount_settle"].asString());
				
				long double close_amount = switch_s_to_f(m_trade_list[i]["amount"].asString());
				long double close_price = switch_s_to_f(m_trade_list[i]["price"].asString());

				long double ratio =  switch_s_to_f(switch_f_to_s(close_amount / open_amount));
				long double change_margin = -switch_s_to_f(switch_f_to_s(open_margin * ratio));
				long double change_quote_amount = -switch_s_to_f(switch_f_to_s(open_quote_amount * ratio));
				long double change_quote_amount_settle = -switch_s_to_f(switch_f_to_s(open_quote_amount_settle * ratio));
				long double change_profit = switch_s_to_f(switch_f_to_s(close_price * close_amount * m_rate)) + change_quote_amount_settle;
				long double buy_margin = open_margin + change_margin;
				long double buy_amount = open_amount - close_amount;
				long double buy_quote_amount = open_quote_amount + change_quote_amount;
				long double buy_quote_amount_settle = open_quote_amount_settle + change_quote_amount_settle;

				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_margin"] = switch_f_to_s(buy_margin);
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_amount"] = switch_f_to_s(buy_amount);
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount"] = switch_f_to_s(buy_quote_amount);
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount_settle"] = switch_f_to_s(buy_quote_amount_settle);
				tmp_obj["change_margin"] = switch_f_to_s(change_margin);
				tmp_obj["change_balance"] = switch_f_to_s(change_profit);
			} else {
				long double open_margin = switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_margin"].asString());
				long double open_amount = switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_amount"].asString());
				long double open_quote_amount = switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount"].asString());
				long double open_quote_amount_settle = switch_s_to_f(m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount_settle"].asString());
				
				long double close_amount = switch_s_to_f(m_trade_list[i]["amount"].asString());
				long double close_price = switch_s_to_f(m_trade_list[i]["price"].asString());

				long double ratio = switch_s_to_f(switch_f_to_s(close_amount / open_amount));
				long double change_margin = -switch_s_to_f(switch_f_to_s(open_margin * ratio));
				long double change_quote_amount = -switch_s_to_f(switch_f_to_s(open_quote_amount * ratio));
				long double change_quote_amount_settle = -switch_s_to_f(switch_f_to_s(open_quote_amount_settle * ratio));
				long double change_profit = -change_quote_amount_settle - switch_s_to_f(switch_f_to_s(close_price * close_amount * m_rate));
				long double sell_margin = open_margin + change_margin;
				long double sell_amount = open_amount - close_amount;
				long double sell_quote_amount = open_quote_amount + change_quote_amount;
				long double sell_quote_amount_settle = open_quote_amount_settle + change_quote_amount_settle;
				
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_margin"] = switch_f_to_s(sell_margin);
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_amount"] = switch_f_to_s(sell_amount);
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount"] = switch_f_to_s(sell_quote_amount);
				m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount_settle"] = switch_f_to_s(sell_quote_amount_settle);
				tmp_obj["change_margin"] = switch_f_to_s(change_margin);
				tmp_obj["change_balance"] = switch_f_to_s(change_profit);
			}

			tmp_obj["change_profit"] = switch_f_to_s(0);
			tmp_obj["change_frozen_margin"] = switch_f_to_s(0);
			tmp_obj["new_balance"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][m_settle_asset]["balance"].asString()) + switch_s_to_f(tmp_obj["change_balance"].asString()));
			tmp_obj["new_margin"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][m_settle_asset]["margin"].asString()) + switch_s_to_f(tmp_obj["change_margin"].asString()));
			tmp_obj["new_frozen_margin"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][m_settle_asset]["frozen_margin"].asString()) + switch_s_to_f(tmp_obj["change_frozen_margin"].asString()));
			tmp_obj["new_profit"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][m_settle_asset]["profit"].asString()) + switch_s_to_f(tmp_obj["change_profit"].asString()));
			
			m_user_account[order_user_id]["funds"][m_settle_asset]["margin"] = tmp_obj["new_margin"];
			m_user_account[order_user_id]["funds"][m_settle_asset]["encode_margin"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_settle_asset + tmp_obj["new_margin"].asString());
			m_user_account[order_user_id]["funds"][m_settle_asset]["balance"] = tmp_obj["new_balance"];
			m_user_account[order_user_id]["funds"][m_settle_asset]["encode_balance"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_settle_asset + tmp_obj["new_balance"].asString());
			
			tmp_obj["new_encode_balance"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_balance"];
			tmp_obj["new_encode_margin"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_margin"];
			tmp_obj["new_encode_frozen_margin"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_frozen_margin"];
			tmp_obj["new_encode_profit"] = m_user_account[order_user_id]["funds"][m_settle_asset]["encode_profit"];
		}
		close_profit = tmp_obj["change_balance"].asString();
		if (order_op == ORDER_SIDE_OPEN_LONG) {
			tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_OPEN_LONG;
			tmp_obj["remark"] = "开多";
			tmp_obj["remark_tw"] = "開多";
			tmp_obj["remark_en"] = "Open Long";
			tmp_obj["remark_vi"] = "Mở Long";
		} else if (order_op == ORDER_SIDE_OPEN_SHORT) {
			tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_OPEN_SHORT;
			tmp_obj["remark"] = "开空";
			tmp_obj["remark_tw"] = "開空";
			tmp_obj["remark_en"] = "Sell Short";
			tmp_obj["remark_vi"] = "Mở Short";
		} else if (order_op == ORDER_SIDE_CLOSE_LONG) {
			tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_CLOSE_LONG;
			tmp_obj["remark"] = "平多";
			tmp_obj["remark_tw"] = "平多";
			tmp_obj["remark_en"] = "Liquidate Long";
			tmp_obj["remark_vi"] = "Đóng Long";
		} else {
			tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_CLOSE_SHORT;
			tmp_obj["remark"] = "平空";
			tmp_obj["remark_tw"] = "平空";
			tmp_obj["remark_en"] = "Liquidate Short";
			tmp_obj["remark_vi"] = "Đóng Short";
		}
		m_order_result_list.append(tmp_obj);
		// 计算手续费
		fee_coin = m_settle_asset;
		fee_amount = m_maker_fee * switch_s_to_f(m_trade_list[i]["amount"].asString()) * switch_s_to_f(m_trade_list[i]["price"].asString()) * m_rate * switch_s_to_f(m_user_account[order_user_id]["fee_setting"]["maker"].asString());
		fee_amount = switch_s_to_f(switch_f_to_s(fee_amount));
		if (switch_f_to_s(fee_amount) != switch_f_to_s(0)){
			tmp_obj = Json::Value::null;
			tmp_obj["type"] = "account";
			tmp_obj["id"] = m_trade_list[i]["id"].asString();
			tmp_obj["user_id"] = atoi(order_user_id.c_str());
			tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_FEE;
			tmp_obj["remark"] = "交易手续费";
			tmp_obj["remark_tw"] = "交易手續費";
			tmp_obj["remark_en"] = "Transaction Fee";
			tmp_obj["remark_vi"] = "Phí thủ tục GD";
			tmp_obj["update_at"] = m_time_now;
			tmp_obj["asset"] = fee_coin;
			tmp_obj["contract"] = m_contract_name;
			tmp_obj["change_balance"] = switch_f_to_s(-fee_amount);
			tmp_obj["new_balance"] = switch_f_to_s(switch_s_to_f(m_user_account[order_user_id]["funds"][fee_coin]["balance"].asString()) - fee_amount);;
			tmp_obj["change_margin"] = switch_f_to_s(0);
			tmp_obj["new_margin"] = m_user_account[order_user_id]["funds"][fee_coin]["margin"].asString();
			tmp_obj["change_frozen_margin"] = switch_f_to_s(0);
			tmp_obj["new_frozen_margin"] = m_user_account[order_user_id]["funds"][fee_coin]["frozen_margin"];
			tmp_obj["change_profit"] = switch_f_to_s(0);
			tmp_obj["new_profit"] = m_user_account[order_user_id]["funds"][fee_coin]["profit"];
			
			m_user_account[order_user_id]["funds"][fee_coin]["balance"] = tmp_obj["new_balance"];
			m_user_account[order_user_id]["funds"][fee_coin]["encode_balance"] = HmacSha256Encode(tmp_obj["user_id"].asString() + fee_coin + tmp_obj["new_balance"].asString());
			m_user_account[order_user_id]["funds"][fee_coin]["profit"] = tmp_obj["new_profit"];
			m_user_account[order_user_id]["funds"][fee_coin]["encode_profit"] = HmacSha256Encode(tmp_obj["user_id"].asString() + fee_coin + tmp_obj["new_profit"].asString());
			
			tmp_obj["new_encode_balance"] = m_user_account[order_user_id]["funds"][fee_coin]["encode_balance"];
			tmp_obj["new_encode_margin"] = m_user_account[order_user_id]["funds"][fee_coin]["encode_margin"];
			tmp_obj["new_encode_frozen_margin"] = m_user_account[order_user_id]["funds"][fee_coin]["encode_frozen_margin"];
			tmp_obj["new_encode_profit"] = m_user_account[order_user_id]["funds"][fee_coin]["encode_profit"];

			m_order_result_list.append(tmp_obj);
		}
		// 成交记录
		tmp_trade_obj =  Json::Value::null;
		tmp_trade_obj["type"] = "trade";
		tmp_trade_obj["contract_id"] = m_order_contract_id;
		tmp_trade_obj["contract_name"] = m_contract_name;
		tmp_trade_obj["asset_symbol"] = m_order_base_asset;
		tmp_trade_obj["unit_amount"] = switch_f_to_s(m_unit_amount);
		tmp_trade_obj["lever_rate"] = cur_lever_rate;
		tmp_trade_obj["settle_asset"] = m_settle_asset;
		tmp_trade_obj["id"] = m_trade_list[i]["id"].asString();
		tmp_trade_obj["order_id"] = order_id;
		tmp_trade_obj["user_id"] = atoi(order_user_id.c_str());
		tmp_trade_obj["amount"] = m_trade_list[i]["amount"].asString();
		tmp_trade_obj["quote_amount"] = switch_f_to_s(switch_s_to_f(m_trade_list[i]["price"].asString()) * switch_s_to_f(m_trade_list[i]["amount"].asString()));
		tmp_trade_obj["settle_amount"] = switch_f_to_s(switch_s_to_f(m_trade_list[i]["price"].asString()) * switch_s_to_f(m_trade_list[i]["amount"].asString()) * m_rate);
		tmp_trade_obj["price"] = m_trade_list[i]["price"].asString();
		tmp_trade_obj["profit"] = close_profit;
		tmp_trade_obj["is_maker"] = 1;
		tmp_trade_obj["create_at"] = m_time_now;
		tmp_trade_obj["fee_asset"] = fee_coin;
		tmp_trade_obj["fee_amount"] = switch_f_to_s(fee_amount);
		tmp_trade_obj["order_op"] = order_op;
		m_order_result_list.append(tmp_trade_obj);
		
		tmp_trade_obj =  Json::Value::null;
		tmp_trade_obj["id"] = m_trade_list[i]["id"].asString();
		tmp_trade_obj["price"] = m_trade_list[i]["price"].asString();
		tmp_trade_obj["amount"] = m_trade_list[i]["amount"].asString();
		tmp_trade_obj["quote_amount"] = switch_f_to_s(switch_s_to_f(m_trade_list[i]["price"].asString()) * switch_s_to_f(m_trade_list[i]["amount"].asString()));
		tmp_trade_obj["create_at"] = m_time_now;
		tmp_trade_obj["price_base_bitCNY"] = switch_f_to_s(switch_s_to_f(m_trade_list[i]["price"].asString()) * m_usd_cny_value);
		if (m_order_op == ORDER_SIDE_OPEN_LONG || m_order_op == ORDER_SIDE_CLOSE_SHORT) {
			tmp_trade_obj["maker_is_buyer"] = 0;
		}else{
			tmp_trade_obj["maker_is_buyer"] = 1;
		}
		if ((m_trade_list[i]["system_type"] == ORDER_SYSTEM_TYPE_PROFIT || m_trade_list[i]["system_type"] == ORDER_SYSTEM_TYPE_LOSS) 
				&& maker_user_set.find(m_trade_list[i]["user_id"].asInt()) != maker_user_set.end()) {
			// 止盈止损系统接单，不显示成交记录
		} else {
			m_statistics["trade"]["list"].append(tmp_trade_obj);
		}
		std::string profit_limit = m_trade_list[i]["profit_limit"].asString();
		std::string lose_limit = m_trade_list[i]["lose_limit"].asString();
		if (order_op == ORDER_SIDE_OPEN_LONG){
			m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_profit_limit"] = profit_limit;
			m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_lose_limit"] = lose_limit;
		}else if (order_op == ORDER_SIDE_OPEN_SHORT){
			m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_profit_limit"] = profit_limit;
			m_user_position[order_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_lose_limit"] = lose_limit;
		}

		int system_type = m_trade_list[i]["system_type"].asInt();
		int user_id = m_trade_list[i]["user_id"].asInt();
		if (system_type != ORDER_SYSTEM_TYPE_STOP){
			Json::Value tmp_msg_json = Json::Value::null;
			tmp_msg_json["type"] = "trade_msg";
			tmp_msg_json["user_id"] = user_id;
			tmp_msg_json["msg_type"] = MESSAGE_TYPE_TRADE;
			char tmp_st[10240];
			if (system_type == ORDER_SYSTEM_TYPE_PROFIT){
				snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】止盈挂单已成交，请关注行情波动，控制交易风险！", m_server_lang_cn.c_str(), m_contract_name.c_str());
				tmp_msg_json["title"] = tmp_st;
				snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】止盈掛單已成交，請關注行情波動，控制交易風險！", m_server_lang_tw.c_str(), m_contract_name.c_str());
				tmp_msg_json["title_tw"] = tmp_st;
				snprintf(tmp_st, sizeof(tmp_st), "Your Limit Order for your【%s%s】position has been filled. Please be aware of market fluctuations and control your risks!", m_server_lang_en.c_str(), m_contract_name.c_str());
				tmp_msg_json["title_en"] = tmp_st;
				snprintf(tmp_st, sizeof(tmp_st), "Lệnh limit cắt lãi 【%s%s】 của bạn đã giao dịch thành công, xin mời bạn quan sát tình hình dao động, kiểm soát rủi ro giao dịch!", m_server_lang_vi.c_str(), m_contract_name.c_str());
				tmp_msg_json["title_vi"] = tmp_st;
			}else if (system_type == ORDER_SYSTEM_TYPE_LOSS){
				snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】止损挂单已成交，请关注行情波动，控制交易风险！", m_server_lang_cn.c_str(), m_contract_name.c_str());
				tmp_msg_json["title"] = tmp_st;
				snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】止損掛單已成交，請關注行情波動，控制交易風險！", m_server_lang_tw.c_str(), m_contract_name.c_str());
				tmp_msg_json["title_tw"] = tmp_st;
				snprintf(tmp_st, sizeof(tmp_st), "Your Stop Loss for your【%s%s】position has been filled. Please be aware of market fluctuations and control your risks!", m_server_lang_en.c_str(), m_contract_name.c_str());
				tmp_msg_json["title_en"] = tmp_st;
				snprintf(tmp_st, sizeof(tmp_st), "Lệnh limit cắt lỗ 【%s%s】 của bạn đã giao dịch thành công, xin mời bạn quan sát tình hình dao động, kiểm soát rủi ro giao dịch!", m_server_lang_vi.c_str(), m_contract_name.c_str());
				tmp_msg_json["title_vi"] = tmp_st;
			}else if (order_op == ORDER_SIDE_OPEN_LONG){
				snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】买涨委托单已成交，请关注行情波动，控制交易风险！", m_server_lang_cn.c_str(), m_contract_name.c_str());
				tmp_msg_json["title"] = tmp_st;
				snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】買漲委託單已成交，請關注行情波動，控制交易風險！", m_server_lang_tw.c_str(), m_contract_name.c_str());
				tmp_msg_json["title_tw"] = tmp_st;
				snprintf(tmp_st, sizeof(tmp_st), "Your 【%s%s】 open long order has been filled. Please be aware of market fluctuations and control your risks!", m_server_lang_en.c_str(), m_contract_name.c_str());
				tmp_msg_json["title_en"] = tmp_st;
				snprintf(tmp_st, sizeof(tmp_st), "Lệnh đặt mua Tăng【%s%s】của bạn đã giao dịch thành công, xin mời bạn quan sát tình hình dao động, kiểm soát rủi ro giao dịch!", m_server_lang_vi.c_str(), m_contract_name.c_str());
				tmp_msg_json["title_vi"] = tmp_st;
			}else if (order_op == ORDER_SIDE_OPEN_SHORT){
				snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】买跌委托单已成交，请关注行情波动，控制交易风险！", m_server_lang_cn.c_str(), m_contract_name.c_str());
				tmp_msg_json["title"] = tmp_st;
				snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】買跌委託單已成交，請關注行情波動，控制交易風險！", m_server_lang_tw.c_str(), m_contract_name.c_str());
				tmp_msg_json["title_tw"] = tmp_st;
				snprintf(tmp_st, sizeof(tmp_st), "Your 【%s%s】 open short order has been filled. Please be aware of market fluctuations and control your risks!", m_server_lang_en.c_str(), m_contract_name.c_str());
				tmp_msg_json["title_en"] = tmp_st;
				snprintf(tmp_st, sizeof(tmp_st), "Lệnh đặt mua Giảm【%s%s】của bạn đã giao dịch thành công, xin mời bạn quan sát tình hình dao động, kiểm soát rủi ro giao dịch!", m_server_lang_vi.c_str(), m_contract_name.c_str());
				tmp_msg_json["title_vi"] = tmp_st;
			}else if (order_op == ORDER_SIDE_CLOSE_LONG){
				snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】多单的平仓委托单已成交，请关注行情波动，控制交易风险！", m_server_lang_cn.c_str(), m_contract_name.c_str());
				tmp_msg_json["title"] = tmp_st;
				snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】多單的平倉委託單已成交，請關注行情波動，控制交易風險！", m_server_lang_tw.c_str(), m_contract_name.c_str());
				tmp_msg_json["title_tw"] = tmp_st;
				snprintf(tmp_st, sizeof(tmp_st), "Your 【%s%s】 sell long order has been filled. Please be aware of market fluctuations and control your risks!", m_server_lang_en.c_str(), m_contract_name.c_str());
				tmp_msg_json["title_en"] = tmp_st;
				snprintf(tmp_st, sizeof(tmp_st), "Lệnh đặt bán Tăng đóng vị thế【%s%s】của bạn đã giao dịch thành công, xin mời bạn quan sát tình hình dao động, kiểm soát rủi ro giao dịch!", m_server_lang_vi.c_str(), m_contract_name.c_str());
				tmp_msg_json["title_vi"] = tmp_st;
			}else{
				snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】空单的平仓委托单已成交，请关注行情波动，控制交易风险！", m_server_lang_cn.c_str(), m_contract_name.c_str());
				tmp_msg_json["title"] = tmp_st;
				snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】空單的平倉委託單已成交，請關注行情波動，控制交易風險！", m_server_lang_tw.c_str(), m_contract_name.c_str());
				tmp_msg_json["title_tw"] = tmp_st;
				snprintf(tmp_st, sizeof(tmp_st), "Your 【%s%s】 sell short order has been filled. Please be aware of market fluctuations and control your risks!", m_server_lang_en.c_str(), m_contract_name.c_str());
				tmp_msg_json["title_en"] = tmp_st;
				snprintf(tmp_st, sizeof(tmp_st), "Lệnh đặt bán Giảm đóng vị thế【%s%s】của bạn đã giao dịch thành công, xin mời bạn quan sát tình hình dao động, kiểm soát rủi ro giao dịch!", m_server_lang_vi.c_str(), m_contract_name.c_str());
				tmp_msg_json["title_vi"] = tmp_st;
			}
			tmp_msg_json["currency"] = m_server_currency;
			m_order_result_list.append(tmp_msg_json);
			
			Json::Value trade_msg_single = Json::Value::null;
			trade_msg_single["user_id"] = std::to_string(user_id);
			trade_msg_single["message"] = Json::Value::null;
			trade_msg_single["message"]["type"] = MESSAGE_TYPE_TRADE;
			trade_msg_single["message"]["show"] = 0;
			trade_msg_single["message"]["title"] = tmp_msg_json["title"].asString();
			trade_msg_single["message"]["title_tw"] = tmp_msg_json["title_tw"].asString();
			trade_msg_single["message"]["title_en"] = tmp_msg_json["title_en"].asString();
			trade_msg_single["message"]["title_vi"] = tmp_msg_json["title_vi"].asString();
			trade_msg_single["message"]["currency"] = m_server_currency;
			trade_msg_single["message"]["created_at"] = m_time_now;
			m_trade_msg_array.append(trade_msg_single);

			if (system_type == ORDER_SYSTEM_TYPE_PROFIT || system_type == ORDER_SYSTEM_TYPE_LOSS){
				Json::Value email_msg_single = Json::Value::null;
				email_msg_single["user_id"] = user_id;
				if (system_type == ORDER_SYSTEM_TYPE_PROFIT){
					email_msg_single["title_cn"] = "合约交易止盈提示";
					email_msg_single["title_tw"] = "合約交易止盈提示";
					email_msg_single["title_en"] = "Perpetual Contracts Limit Order prompt";
					email_msg_single["title_vi"] = "Nhắc nhở cắt lãi Hợp Đồng giao dịch";
					snprintf(tmp_st, sizeof(tmp_st), "尊敬的用户，您好！您的【%s%s】止盈挂单已成交，请登录您的合约交易账户查看成交结果！", m_server_lang_cn.c_str(), m_contract_name.c_str());
					email_msg_single["content_cn"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "尊敬的用戶，您好！您的【%s%s】止盈掛單已成交，請登入您的延期交易賬戶檢視成交結果！", m_server_lang_tw.c_str(), m_contract_name.c_str());
					email_msg_single["content_tw"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "Dear User, your Limit Order for your【%s%s】position has been filled. Please login to your Perpetual Contracts account to see results!", m_server_lang_en.c_str(), m_contract_name.c_str());
					email_msg_single["content_en"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "Kính gửi người dùng, lệnh limit cắt lãi 【%s%s】 của bạn đã giao dịch thành công, xin mời bạn đăng nhập tài khoản Hợp Đồng giao dịch để kiểm tra kết quả giao dịch đã thành công!", m_server_lang_vi.c_str(), m_contract_name.c_str());
					email_msg_single["content_vi"] = tmp_st;
				}else{
					email_msg_single["title_cn"] = "合约交易止损提示";
					email_msg_single["title_tw"] = "合約交易止損提示";
					email_msg_single["title_en"] = "Perpetual Contracts Stop Loss prompt";
					email_msg_single["title_vi"] = "Nhắc nhở cắt lỗ Hợp Đồng giao dịch";
					snprintf(tmp_st, sizeof(tmp_st), "尊敬的用户，您好！您的【%s%s】止损挂单已成交，请登录您的合约交易账户查看成交结果！", m_server_lang_cn.c_str(), m_contract_name.c_str());
					email_msg_single["content_cn"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "尊敬的用戶，您好！您的【%s%s】止損掛單已成交，請登入您的延期交易賬戶檢視成交結果！", m_server_lang_tw.c_str(), m_contract_name.c_str());
					email_msg_single["content_tw"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "Dear User, your Stop Loss for your【%s%s】position has been filled. Please login to your Perpetual Contracts account to see results!", m_server_lang_en.c_str(), m_contract_name.c_str());
					email_msg_single["content_en"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "Kính gửi người dùng, lệnh limit cắt lỗ 【%s%s】 của bạn đã giao dịch thành công, xin mời bạn đăng nhập tài khoản Hợp Đồng giao dịch để kiểm tra kết quả giao dịch đã thành công!", m_server_lang_vi.c_str(), m_contract_name.c_str());
					email_msg_single["content_vi"] = tmp_st;
				}
				m_email_msg_array.append(email_msg_single);
			}
		}

		if ((m_trade_list[i]["system_type"] == ORDER_SYSTEM_TYPE_PROFIT || m_trade_list[i]["system_type"] == ORDER_SYSTEM_TYPE_LOSS) 
				&& maker_user_set.find(m_trade_list[i]["user_id"].asInt()) != maker_user_set.end()) {
			// 止盈止损系统接单，不显示成交价格
			Json::Value value;
			m_trade_list.removeIndex(i, &value);
			i--;
		}

		if (i == m_trade_list.size() - 1) {
			if (m_order_op == ORDER_SIDE_OPEN_LONG){
				m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_profit_limit"] = m_order_profit_limit;
				m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_lose_limit"] = m_order_lose_limit;
			}else if (m_order_op == ORDER_SIDE_OPEN_SHORT){
				m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_profit_limit"] = m_order_profit_limit;
				m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_lose_limit"] = m_order_lose_limit;
			}

			if (m_order_system_type != ORDER_SYSTEM_TYPE_STOP){
				Json::Value tmp_msg_json = Json::Value::null;
				tmp_msg_json["type"] = "trade_msg";
				tmp_msg_json["user_id"] = m_order_user_id;
				tmp_msg_json["msg_type"] = MESSAGE_TYPE_TRADE;
				char tmp_st[10240];
				if (m_order_system_type == ORDER_SYSTEM_TYPE_PROFIT){
					snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】止盈挂单已成交，请关注行情波动，控制交易风险！", m_server_lang_cn.c_str(), m_contract_name.c_str());
					tmp_msg_json["title"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】止盈掛單已成交，請關注行情波動，控制交易風險！", m_server_lang_tw.c_str(), m_contract_name.c_str());
					tmp_msg_json["title_tw"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "Your Limit Order for your【%s%s】position has been filled. Please be aware of market fluctuations and control your risks!", m_server_lang_en.c_str(), m_contract_name.c_str());
					tmp_msg_json["title_en"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "Lệnh limit cắt lãi 【%s%s】 của bạn đã giao dịch thành công, xin mời bạn quan sát tình hình dao động, kiểm soát rủi ro giao dịch!", m_server_lang_vi.c_str(), m_contract_name.c_str());
					tmp_msg_json["title_vi"] = tmp_st;
				}else if (m_order_system_type == ORDER_SYSTEM_TYPE_LOSS){
					snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】止损挂单已成交，请关注行情波动，控制交易风险！", m_server_lang_cn.c_str(), m_contract_name.c_str());
					tmp_msg_json["title"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】止損掛單已成交，請關注行情波動，控制交易風險！", m_server_lang_tw.c_str(), m_contract_name.c_str());
					tmp_msg_json["title_tw"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "Your Stop Loss for your【%s%s】position has been filled. Please be aware of market fluctuations and control your risks!", m_server_lang_en.c_str(), m_contract_name.c_str());
					tmp_msg_json["title_en"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "Lệnh limit cắt lỗ 【%s%s】 của bạn đã giao dịch thành công, xin mời bạn quan sát tình hình dao động, kiểm soát rủi ro giao dịch!", m_server_lang_vi.c_str(), m_contract_name.c_str());
					tmp_msg_json["title_vi"] = tmp_st;
				}else if (m_order_op == ORDER_SIDE_OPEN_LONG){
					snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】买涨委托单已成交，请关注行情波动，控制交易风险！", m_server_lang_cn.c_str(), m_contract_name.c_str());
					tmp_msg_json["title"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】買漲委託單已成交，請關注行情波動，控制交易風險！", m_server_lang_tw.c_str(), m_contract_name.c_str());
					tmp_msg_json["title_tw"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "Your 【%s%s】 open long order has been filled. Please be aware of market fluctuations and control your risks!", m_server_lang_en.c_str(), m_contract_name.c_str());
					tmp_msg_json["title_en"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "Lệnh đặt mua Tăng【%s%s】của bạn đã giao dịch thành công, xin mời bạn quan sát tình hình dao động, kiểm soát rủi ro giao dịch!", m_server_lang_vi.c_str(), m_contract_name.c_str());
					tmp_msg_json["title_vi"] = tmp_st;
				}else if (m_order_op == ORDER_SIDE_OPEN_SHORT){
					snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】买跌委托单已成交，请关注行情波动，控制交易风险！", m_server_lang_cn.c_str(), m_contract_name.c_str());
					tmp_msg_json["title"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】買跌委託單已成交，請關注行情波動，控制交易風險！", m_server_lang_tw.c_str(), m_contract_name.c_str());
					tmp_msg_json["title_tw"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "Your 【%s%s】 open short order has been filled. Please be aware of market fluctuations and control your risks!", m_server_lang_en.c_str(), m_contract_name.c_str());
					tmp_msg_json["title_en"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "Lệnh đặt mua Giảm【%s%s】của bạn đã giao dịch thành công, xin mời bạn quan sát tình hình dao động, kiểm soát rủi ro giao dịch!", m_server_lang_vi.c_str(), m_contract_name.c_str());
					tmp_msg_json["title_vi"] = tmp_st;
				}else if (m_order_op == ORDER_SIDE_CLOSE_LONG){
					snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】多单的平仓委托单已成交，请关注行情波动，控制交易风险！", m_server_lang_cn.c_str(), m_contract_name.c_str());
					tmp_msg_json["title"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】多單的平倉委託單已成交，請關注行情波動，控制交易風險！", m_server_lang_tw.c_str(), m_contract_name.c_str());
					tmp_msg_json["title_tw"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "Your 【%s%s】 sell long order has been filled. Please be aware of market fluctuations and control your risks!", m_server_lang_en.c_str(), m_contract_name.c_str());
					tmp_msg_json["title_en"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "Lệnh đặt bán Tăng đóng vị thế【%s%s】của bạn đã giao dịch thành công, xin mời bạn quan sát tình hình dao động, kiểm soát rủi ro giao dịch!", m_server_lang_vi.c_str(), m_contract_name.c_str());
					tmp_msg_json["title_vi"] = tmp_st;
				}else{
					snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】空单的平仓委托单已成交，请关注行情波动，控制交易风险！", m_server_lang_cn.c_str(), m_contract_name.c_str());
					tmp_msg_json["title"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】空單的平倉委託單已成交，請關注行情波動，控制交易風險！", m_server_lang_tw.c_str(), m_contract_name.c_str());
					tmp_msg_json["title_tw"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "Your 【%s%s】 sell short order has been filled. Please be aware of market fluctuations and control your risks!", m_server_lang_en.c_str(), m_contract_name.c_str());
					tmp_msg_json["title_en"] = tmp_st;
					snprintf(tmp_st, sizeof(tmp_st), "Lệnh đặt bán Giảm đóng vị thế【%s%s】của bạn đã giao dịch thành công, xin mời bạn quan sát tình hình dao động, kiểm soát rủi ro giao dịch!", m_server_lang_vi.c_str(), m_contract_name.c_str());
					tmp_msg_json["title_vi"] = tmp_st;
				}
				tmp_msg_json["currency"] = m_server_currency;
				m_order_result_list.append(tmp_msg_json);
				
				Json::Value trade_msg_single = Json::Value::null;
				trade_msg_single["user_id"] = m_order_user_id_str;
				trade_msg_single["message"] = Json::Value::null;
				trade_msg_single["message"]["type"] = MESSAGE_TYPE_TRADE;
				trade_msg_single["message"]["show"] = 0;
				trade_msg_single["message"]["title"] = tmp_msg_json["title"].asString();
				trade_msg_single["message"]["title_tw"] = tmp_msg_json["title_tw"].asString();
				trade_msg_single["message"]["title_en"] = tmp_msg_json["title_en"].asString();
				trade_msg_single["message"]["title_vi"] = tmp_msg_json["title_vi"].asString();
				trade_msg_single["message"]["currency"] = m_server_currency;
				trade_msg_single["message"]["created_at"] = m_time_now;
				m_trade_msg_array.append(trade_msg_single);

				if (m_order_system_type == ORDER_SYSTEM_TYPE_PROFIT || m_order_system_type == ORDER_SYSTEM_TYPE_LOSS){
					Json::Value email_msg_single = Json::Value::null;
					email_msg_single["user_id"] = m_order_user_id;
					if (m_order_system_type == ORDER_SYSTEM_TYPE_PROFIT){
						email_msg_single["title_cn"] = "合约交易止盈提示";
						email_msg_single["title_tw"] = "合約交易止盈提示";
						email_msg_single["title_en"] = "Perpetual Contracts Limit Order prompt";
						email_msg_single["title_vi"] = "Nhắc nhở cắt lãi Hợp Đồng giao dịch";
						snprintf(tmp_st, sizeof(tmp_st), "尊敬的用户，您好！您的【%s%s】止盈挂单已成交，请登录您的合约交易账户查看成交结果！", m_server_lang_cn.c_str(), m_contract_name.c_str());
						email_msg_single["content_cn"] = tmp_st;
						snprintf(tmp_st, sizeof(tmp_st), "尊敬的用戶，您好！您的【%s%s】止盈掛單已成交，請登入您的延期交易賬戶檢視成交結果！", m_server_lang_tw.c_str(), m_contract_name.c_str());
						email_msg_single["content_tw"] = tmp_st;
						snprintf(tmp_st, sizeof(tmp_st), "Dear User, your Limit Order for your【%s%s】position has been filled. Please login to your Perpetual Contracts account to see results!", m_server_lang_en.c_str(), m_contract_name.c_str());
						email_msg_single["content_en"] = tmp_st;
						snprintf(tmp_st, sizeof(tmp_st), "Kính gửi người dùng, lệnh limit cắt lãi 【%s%s】 của bạn đã giao dịch thành công, xin mời bạn đăng nhập tài khoản Hợp Đồng giao dịch để kiểm tra kết quả giao dịch đã thành công!", m_server_lang_vi.c_str(), m_contract_name.c_str());
						email_msg_single["content_vi"] = tmp_st;
					}else{
						email_msg_single["title_cn"] = "合约交易止损提示";
						email_msg_single["title_tw"] = "合約交易止損提示";
						email_msg_single["title_en"] = "Perpetual Contracts Stop Loss prompt";
						email_msg_single["title_vi"] = "Nhắc nhở cắt lỗ Hợp Đồng giao dịch";
						snprintf(tmp_st, sizeof(tmp_st), "尊敬的用户，您好！您的【%s%s】止损挂单已成交，请登录您的合约交易账户查看成交结果！", m_server_lang_cn.c_str(), m_contract_name.c_str());
						email_msg_single["content_cn"] = tmp_st;
						snprintf(tmp_st, sizeof(tmp_st), "尊敬的用戶，您好！您的【%s%s】止損掛單已成交，請登入您的延期交易賬戶檢視成交結果！", m_server_lang_tw.c_str(), m_contract_name.c_str());
						email_msg_single["content_tw"] = tmp_st;
						snprintf(tmp_st, sizeof(tmp_st), "Dear User, your Stop Loss for your【%s%s】position has been filled. Please login to your Perpetual Contracts account to see results!", m_server_lang_en.c_str(), m_contract_name.c_str());
						email_msg_single["content_en"] = tmp_st;
						snprintf(tmp_st, sizeof(tmp_st), "Kính gửi người dùng, lệnh limit cắt lỗ 【%s%s】 của bạn đã giao dịch thành công, xin mời bạn đăng nhập tài khoản Hợp Đồng giao dịch để kiểm tra kết quả giao dịch đã thành công!", m_server_lang_vi.c_str(), m_contract_name.c_str());
						email_msg_single["content_vi"] = tmp_st;
					}
					m_email_msg_array.append(email_msg_single);
				}
			}
		}
	}

	if (m_trade_list.size() > 0){
		char *redis_cmd;
		if (redisFormatCommand(&redis_cmd, "HSET last_trade_price %s %s", m_order_contract_id_str.c_str(), m_trade_list[m_trade_list.size() - 1]["price"].asString().c_str()) <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		std::string redis_cmd_str = redis_cmd;
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
		if (m_order_op == ORDER_SIDE_OPEN_LONG || m_order_op == ORDER_SIDE_CLOSE_SHORT){
			snprintf(sortsetkey, sizeof(sortsetkey), "order_book_%d_buy", m_order_contract_id);
		}else{
			snprintf(sortsetkey, sizeof(sortsetkey), "order_book_%d_sell", m_order_contract_id);
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
			temp_order["order_op"] = m_order_op;
			temp_order["system_type"] = m_order_system_type;
			temp_order["profit_limit"] = m_order_profit_limit;
			temp_order["lose_limit"] = m_order_lose_limit;
			temp_order["amount"] = switch_f_to_s(m_remain_amount);
			temp_order["lever_rate"] = m_lever_rate;
			temp_order["origin_amount"] = m_order_amt_str;
			temp_order["executed_quote_amount"] = switch_f_to_s(m_executed_quote_amount);
			temp_order["executed_settle_amount"] = switch_f_to_s(m_executed_settle_amount);
			temp_order["frozen_margin"] = switch_f_to_s(m_frozen_margin);
			temp_json["orders"].append(temp_order);
			temp_json["total_amount"] = switch_f_to_s(m_remain_amount);
			
			std::string result = Json::writeString(writer, temp_json);
			if (redisFormatCommand(&redis_cmd, "ZADD %s %s %s", sortsetkey, m_order_price_str.c_str(), result.c_str()) <= 0){
				LOG(ERROR) << "redis format error";
				return false;
			}
			std::string redis_cmd_str = redis_cmd;
			free(redis_cmd);
			m_redis_cmd_list.push_back(redis_cmd_str);
			
			Json::Value temp_order_book = Json::Value::null;
			temp_order_book["amount"]  = temp_json["total_amount"].asString();
			temp_order_book["price"]  = m_order_price_str;
			if (m_order_op == ORDER_SIDE_OPEN_LONG || m_order_op == ORDER_SIDE_CLOSE_SHORT){
				m_statistics["order_book"]["buy"].append(temp_order_book);
			}else{
				m_statistics["order_book"]["sell"].append(temp_order_book);
			}
		}else if (reply->elements == 1){
			std::string temp_str = reply->element[0]->str;
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
			temp_order["order_op"] = m_order_op;
			temp_order["system_type"] = m_order_system_type;
			temp_order["profit_limit"] = m_order_profit_limit;
			temp_order["lose_limit"] = m_order_lose_limit;
			temp_order["amount"] = switch_f_to_s(m_remain_amount);
			temp_order["lever_rate"] = m_lever_rate;
			temp_order["origin_amount"] = m_order_amt_str;
			temp_order["executed_quote_amount"] = switch_f_to_s(m_executed_quote_amount);
			temp_order["executed_settle_amount"] = switch_f_to_s(m_executed_settle_amount);
			temp_order["frozen_margin"] = switch_f_to_s(m_frozen_margin);
			temp_json["orders"].append(temp_order);
			temp_json["total_amount"] = switch_f_to_s(switch_s_to_f(temp_json["total_amount"].asString()) + m_remain_amount);
			
			std::string result = Json::writeString(writer, temp_json);
			if (redisFormatCommand(&redis_cmd, "ZREM %s %s", sortsetkey, temp_str.c_str()) <= 0){
				LOG(ERROR) << "redis format error";
				return false;
			}
			std::string redis_cmd_str = redis_cmd;
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
			if (m_order_op == ORDER_SIDE_OPEN_LONG || m_order_op == ORDER_SIDE_CLOSE_SHORT){
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

	for (int i = 0; i < (int)m_statistics["order"].size(); i++){
		int user_id = m_statistics["order"][i]["user_id"].asInt();
		std::string order_id = m_statistics["order"][i]["id"].asString();
		long double amount = switch_s_to_f(m_statistics["order"][i]["amount"].asString());
		int order_status = m_statistics["order"][i]["status"].asInt();
		if (!m_user_account[std::to_string(user_id)]["funds"].isMember("orders")){
			m_user_account[std::to_string(user_id)]["funds"]["orders"].resize(0);
		}
		int pos = -1;
		for (int j = 0; j < (int)m_user_account[std::to_string(user_id)]["funds"]["orders"].size(); j++){
			std::string tmp_order_id = m_user_account[std::to_string(user_id)]["funds"]["orders"][j].asString();
			if (order_id == tmp_order_id){
				pos = j;
				break;
			}
		}
		if (amount < EPS || order_status == ORDER_STATUS_CANCELED || order_status == ORDER_STATUS_PARTIALLY_CANCELED){
			if (pos >= 0){
				Json::Value remove_json;
				m_user_account[std::to_string(user_id)]["funds"]["orders"].removeIndex(pos, &remove_json);
			}
		}else{
			if (pos < 0){
				m_user_account[std::to_string(user_id)]["funds"]["orders"].append(order_id);
			}
		}
	}

	char *redis_cmd;
	
	for (int i = 0; i < (int)m_trade_users_list.size(); i++){
		std::string result = Json::writeString(writer, m_user_account[std::to_string(m_trade_users_list[i])]["funds"]);
		if (redisFormatCommand(&redis_cmd, "SET account_user_%d %s", m_trade_users_list[i], result.c_str()) <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		std::string redis_cmd_str = redis_cmd;
		free(redis_cmd);
		m_redis_cmd_list.push_back(redis_cmd_str);
	}

	{
		std::string result = Json::writeString(writer, m_user_account[m_order_user_id_str]["funds"]);
		if (redisFormatCommand(&redis_cmd, "SET account_user_%d %s", m_order_user_id, result.c_str()) <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		std::string redis_cmd_str = redis_cmd;
		free(redis_cmd);
		m_redis_cmd_list.push_back(redis_cmd_str);
	}

	for (unsigned i = 0; i < m_trade_users_list.size(); i++) {
		std::string trade_user_id = std::to_string(m_trade_users_list[i]);

		std::map<int, std::set<int>>::iterator it;
		it = m_trade_users_lever_rate.find(m_trade_users_list[i]);
		if (it != m_trade_users_lever_rate.end()){
			std::set<int>::iterator iter;
			for (iter = it->second.begin(); iter != it->second.end(); iter++){
				int cur_lever_rate = *iter;
				std::string cur_lever_rate_str = std::to_string(cur_lever_rate);

				Json::Value tmp_position_obj = Json::Value::null;
				tmp_position_obj["type"] = "position";
				tmp_position_obj["user_id"] = m_trade_users_list[i];
				tmp_position_obj["contract_id"] = m_order_contract_id;
				tmp_position_obj["contract_name"] = m_contract_name;
				tmp_position_obj["asset_symbol"] = m_order_base_asset;
				tmp_position_obj["unit_amount"] = switch_f_to_s(m_unit_amount);
				tmp_position_obj["lever_rate"] = cur_lever_rate;
				tmp_position_obj["settle_asset"] = m_settle_asset;
				tmp_position_obj["buy_frozen"] = m_user_position[trade_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_frozen"].asString();
				tmp_position_obj["buy_margin"] = m_user_position[trade_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_margin"].asString();
				tmp_position_obj["buy_amount"] = m_user_position[trade_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_amount"].asString();
				tmp_position_obj["buy_available"] = m_user_position[trade_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_available"].asString();
				tmp_position_obj["buy_quote_amount"] = m_user_position[trade_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount"].asString();
				tmp_position_obj["buy_quote_amount_settle"] = m_user_position[trade_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_quote_amount_settle"].asString();
				tmp_position_obj["buy_take_profit"] = m_user_position[trade_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_profit_limit"].asString();
				tmp_position_obj["buy_stop_loss"] = m_user_position[trade_user_id][m_order_contract_id_str][cur_lever_rate_str]["buy_lose_limit"].asString();
				tmp_position_obj["sell_frozen"] = m_user_position[trade_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_frozen"].asString();
				tmp_position_obj["sell_margin"] = m_user_position[trade_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_margin"].asString();
				tmp_position_obj["sell_amount"] = m_user_position[trade_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_amount"].asString();
				tmp_position_obj["sell_available"] = m_user_position[trade_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_available"].asString();
				tmp_position_obj["sell_quote_amount"] = m_user_position[trade_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount"].asString();
				tmp_position_obj["sell_quote_amount_settle"] = m_user_position[trade_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_quote_amount_settle"].asString();
				tmp_position_obj["sell_take_profit"] = m_user_position[trade_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_profit_limit"].asString();
				tmp_position_obj["sell_stop_loss"] = m_user_position[trade_user_id][m_order_contract_id_str][cur_lever_rate_str]["sell_lose_limit"].asString();
				tmp_position_obj["create_at"] = m_time_now;
				m_order_result_list.append(tmp_position_obj);
				m_statistics["position"].append(tmp_position_obj);
			}
		}

		Json::Value::Members keys = m_user_position[trade_user_id].getMemberNames();
		for (auto iter = keys.begin(); iter != keys.end(); iter++) {
			Json::Value::Members ks = m_user_position[trade_user_id][*iter].getMemberNames();
			for (auto it = ks.begin(); it != ks.end(); it++) {
				if (switch_s_to_f(m_user_position[trade_user_id][*iter][*it]["buy_frozen"].asString()) < EPS &&
					switch_s_to_f(m_user_position[trade_user_id][*iter][*it]["buy_amount"].asString()) < EPS &&
					switch_s_to_f(m_user_position[trade_user_id][*iter][*it]["sell_frozen"].asString()) < EPS &&
					switch_s_to_f(m_user_position[trade_user_id][*iter][*it]["sell_amount"].asString()) < EPS &&
					fabs(switch_s_to_f(m_user_position[trade_user_id][*iter][*it]["buy_profit"].asString())) < EPS &&
					fabs(switch_s_to_f(m_user_position[trade_user_id][*iter][*it]["sell_profit"].asString())) < EPS) {
					m_user_position[trade_user_id][*iter].removeMember(*it);
				}
			}
			if (m_user_position[trade_user_id][*iter].size() == 0) {
				m_user_position[trade_user_id].removeMember(*iter);
			}
		}
		keys = m_user_position[trade_user_id].getMemberNames();
		if (keys.size() > 0) {
			std::string result = Json::writeString(writer, m_user_position[trade_user_id]);
			if (redisFormatCommand(&redis_cmd, "SET position_user_%d %s", m_trade_users_list[i], result.c_str()) <= 0){
				LOG(ERROR) << "redis format error";
				return false;
			}
			std::string redis_cmd_str = redis_cmd;
			free(redis_cmd);
			m_redis_cmd_list.push_back(redis_cmd_str);
		} else {
			if (redisFormatCommand(&redis_cmd, "DEL position_user_%d", m_trade_users_list[i]) <= 0){
				LOG(ERROR) << "redis format error";
				return false;
			}
			std::string redis_cmd_str = redis_cmd;
			free(redis_cmd);
			m_redis_cmd_list.push_back(redis_cmd_str);
		}
		if (redisFormatCommand(&redis_cmd, "SADD position_set %d", m_trade_users_list[i]) <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		std::string redis_cmd_str = redis_cmd;
		free(redis_cmd);
		m_redis_cmd_list.push_back(redis_cmd_str);
	}
	
	std::string result = Json::writeString(writer, m_order_result_list);
	std::string redis_cmd_str;
	if (redisFormatCommand(&redis_cmd, "LPUSH order_result_list %s", result.c_str()) <= 0){
		LOG(ERROR) << "redis format error";
		return false;
	}
	redis_cmd_str = redis_cmd;
	free(redis_cmd);
	m_redis_cmd_list.push_back(redis_cmd_str);
	
	for (int i = 0; i < (int)m_order_result_list.size(); i++){
		if (m_order_result_list[i]["type"].asString() == "order"){
			if (m_order_result_list[i]["amount"].asString() == switch_f_to_s(0) || m_order_result_list[i]["status"].asInt() == ORDER_STATUS_CANCELED || m_order_result_list[i]["status"].asInt() == ORDER_STATUS_PARTIALLY_CANCELED){
				if (redisFormatCommand(&redis_cmd, "DEL order_detail_%s", m_order_result_list[i]["id"].asString().c_str()) <= 0){
					LOG(ERROR) << "redis format error";
					return false;
				}
			}else{
				if (redisFormatCommand(&redis_cmd, "HMSET order_detail_%s contract_id %d contract_name %s asset_symbol %s unit_amount %s lever_rate %d settle_asset %s id %s user_id %d order_type %d is_bbo %d order_op %d price %s amount %s origin_amount %s executed_quote_amount %s frozen_margin %s", 
									   m_order_result_list[i]["id"].asCString(), m_order_result_list[i]["contract_id"].asInt(), m_order_result_list[i]["contract_name"].asCString(), m_order_result_list[i]["asset_symbol"].asCString(), 
									   m_order_result_list[i]["unit_amount"].asCString(), m_order_result_list[i]["lever_rate"].asInt(), m_order_result_list[i]["settle_asset"].asCString(), m_order_result_list[i]["id"].asCString(), m_order_result_list[i]["user_id"].asInt(), 
									   m_order_result_list[i]["order_type"].asInt(), m_order_result_list[i]["is_bbo"].asInt(), m_order_result_list[i]["order_op"].asInt(), m_order_result_list[i]["price"].asCString(), 
									   m_order_result_list[i]["amount"].asCString(), m_order_result_list[i]["origin_amount"].asCString(), m_order_result_list[i]["executed_quote_amount"].asCString(), m_order_result_list[i]["frozen_margin"].asCString()) <= 0){
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
	m_trade->SendStatisticsMessage(result);

	if (m_trade_msg_array.size() > 0){
		result = Json::writeString(writer, m_trade_msg_array);
		LOG(INFO) << " trade msg: " << result;
		m_trade->SendTradeMessage(result);
	}

	if (m_email_msg_array.size() > 0){
		result = Json::writeString(writer, m_email_msg_array);
		LOG(INFO) << " email msg: " << result;
		m_trade->SendEmailMessage(result);
	}
	
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

	Json::Value::Members keys = m_user_position.getMemberNames();
	for (auto iter = keys.begin(); iter != keys.end(); iter++) {
		if (m_user_position[*iter].size() > 0) {
			m_positions[*iter] = m_user_position[*iter];
		} else {
			m_positions.removeMember(*iter);
		}
	}

	keys = m_user_account.getMemberNames();
	for (auto iter = keys.begin(); iter != keys.end(); iter++) {
		m_accounts[*iter] = m_user_account[*iter]["funds"];
	}
	
	LOG(INFO) << "m_order_id:" << m_order_id << " WriteRedis end";
	return true;
}

bool Match::GetOrderInfo(){
	redisReply* reply = NULL;
	
	reply = (redisReply*) redisCommand(m_redis, "HMGET order_detail_%s user_id contract_id contract_name asset_symbol unit_amount lever_rate order_type is_bbo order_op price amount origin_amount frozen_margin", m_order_id.c_str());
	if (reply == NULL){
		LOG(ERROR) << "m_order_id:" << m_order_id << " redis reply null";
		redisFree(m_redis);
		m_redis = NULL;
		return false;
	}
	if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 13){
		for (unsigned i = 0; i < reply->elements; i++){
			if (reply->element[i]->type == REDIS_REPLY_NIL){
				if (m_settle_asset != "USDT" || i != 12) {
					freeReplyObject(reply);
					return false;
				}
			}
		}
		int order_user_id = atoi(reply->element[0]->str);
		if (order_user_id != m_order_user_id){
			LOG(ERROR) << "m_order_id:" << m_order_id << " user_id: " << order_user_id << " cancel user_id: " << m_order_user_id;
			freeReplyObject(reply);
			return false;
		}
		m_order_contract_id = atoi(reply->element[1]->str);
		m_order_contract_id_str = reply->element[1]->str;
		m_contract_name = reply->element[2]->str;
		m_order_base_asset = reply->element[3]->str;
		m_unit_amount = strtold(reply->element[4]->str, NULL);
		m_lever_rate = atoi(reply->element[5]->str);
		m_lever_rate_str = std::to_string(m_lever_rate);
		m_order_type = atoi(reply->element[6]->str);
		m_order_isbbo = atoi(reply->element[7]->str);
		m_order_op = atoi(reply->element[8]->str);
		m_order_price_str = reply->element[9]->str;
		m_order_price = strtold(m_order_price_str.c_str(), NULL);
		m_remain_amount = strtold(reply->element[10]->str, NULL);
		m_order_amt_str = reply->element[11]->str;
		m_order_amt = strtold(m_order_amt_str.c_str(), NULL);
		if (m_settle_asset != "USDT" || reply->element[12]->type != REDIS_REPLY_NIL) {
			m_frozen_margin = strtold(reply->element[12]->str, NULL);
		} else {
			m_frozen_margin = switch_s_to_f(switch_f_to_s(m_remain_amount * m_order_price * m_rate / m_lever_rate));
		}
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
	if (m_order_op == ORDER_SIDE_OPEN_LONG || m_order_op == ORDER_SIDE_CLOSE_SHORT){
		snprintf(sortsetkey, sizeof(sortsetkey), "order_book_%d_buy", m_order_contract_id);
	}else{
		snprintf(sortsetkey, sizeof(sortsetkey), "order_book_%d_sell", m_order_contract_id);
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
		std::string temp_str = reply->element[0]->str;
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
				m_executed_settle_amount = switch_s_to_f(temp_json["orders"][i]["executed_settle_amount"].asString());
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
		std::string redis_cmd_str = redis_cmd;
		free(redis_cmd);
		m_redis_cmd_list.push_back(redis_cmd_str);
		
		if (temp_json["orders"].size() > 0){
			std::string result = Json::writeString(writer, temp_json);
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
		if (m_order_op == ORDER_SIDE_OPEN_LONG || m_order_op == ORDER_SIDE_CLOSE_SHORT){
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
	tmp_obj["contract_id"] = m_order_contract_id;
	tmp_obj["contract_name"] = m_contract_name;
	tmp_obj["asset_symbol"] = m_order_base_asset;
	tmp_obj["unit_amount"] = switch_f_to_s(m_unit_amount);
	tmp_obj["lever_rate"] = m_lever_rate;
	tmp_obj["id"] = m_order_id;
	tmp_obj["user_id"] = m_order_user_id;
	tmp_obj["order_type"] = m_order_type;
	tmp_obj["order_op"] = m_order_op;
	tmp_obj["price"] = m_order_price_str;
	tmp_obj["amount"] = switch_f_to_s(m_remain_amount);
	tmp_obj["origin_amount"] = m_order_amt_str;
	tmp_obj["executed_quote_amount"] = switch_f_to_s(m_executed_quote_amount);
	tmp_obj["executed_settle_amount"] = switch_f_to_s(m_executed_settle_amount);
	if (switch_f_to_s(m_remain_amount) == m_order_amt_str){
		tmp_obj["status"] = ORDER_STATUS_CANCELED;
	}else{
		tmp_obj["status"] = ORDER_STATUS_PARTIALLY_CANCELED;
	}
	tmp_obj["update_at"] = m_time_now;
	
	m_order_result_list.append(tmp_obj);
	m_statistics["order"].append(tmp_obj);
		
	if (m_order_op == ORDER_SIDE_OPEN_LONG || m_order_op == ORDER_SIDE_OPEN_SHORT){
		tmp_obj = Json::Value::null;
		tmp_obj["type"] = "account";
		tmp_obj["id"] = m_order_id;
		tmp_obj["user_id"] = m_order_user_id;
		tmp_obj["update_at"] = m_time_now;
		tmp_obj["asset"] = m_settle_asset;
		tmp_obj["contract"] = m_contract_name;
		tmp_obj["change_balance"] = switch_f_to_s(0);
		tmp_obj["new_balance"] = switch_f_to_s(switch_s_to_f(m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["balance"].asString()) + switch_s_to_f(tmp_obj["change_balance"].asString()));
		tmp_obj["change_margin"] = switch_f_to_s(0);
		tmp_obj["new_margin"] = switch_f_to_s(switch_s_to_f(m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["margin"].asString()) +  switch_s_to_f(tmp_obj["change_margin"].asString()));
		tmp_obj["change_frozen_margin"] = switch_f_to_s(-m_frozen_margin);
		tmp_obj["new_frozen_margin"] = switch_f_to_s(switch_s_to_f(m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["frozen_margin"].asString()) + switch_s_to_f(tmp_obj["change_frozen_margin"].asString()));
		tmp_obj["change_profit"] = switch_f_to_s(0);
		tmp_obj["new_profit"] = switch_f_to_s(switch_s_to_f(m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["profit"].asString()) + switch_s_to_f(tmp_obj["change_profit"].asString()));
		tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_CANCEL_ORDER;
		tmp_obj["remark"] = "取消订单";
		tmp_obj["remark_tw"] = "取消訂單";
		tmp_obj["remark_en"] = "Cancel";
		tmp_obj["remark_vi"] = "Rút lệnh";

		m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["frozen_margin"] = tmp_obj["new_frozen_margin"].asString();
		m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["encode_frozen_margin"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_settle_asset + tmp_obj["new_frozen_margin"].asString());
	
		tmp_obj["new_encode_balance"] = m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["encode_balance"];
		tmp_obj["new_encode_margin"] = m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["encode_margin"];
		tmp_obj["new_encode_frozen_margin"] = m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["encode_frozen_margin"];
		tmp_obj["new_encode_profit"] = m_user_account[m_order_user_id_str]["funds"][m_settle_asset]["encode_profit"];

		m_order_result_list.append(tmp_obj);

		if (m_order_op == ORDER_SIDE_OPEN_LONG) {
			m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_frozen"] = switch_f_to_s(switch_s_to_f(m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_frozen"].asString()) - m_remain_amount);
		} else {
			m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_frozen"] = switch_f_to_s(switch_s_to_f(m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_frozen"].asString()) - m_remain_amount);
		}
	}else {
		if (m_order_op == ORDER_SIDE_CLOSE_LONG) {
			m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_available"] = switch_f_to_s(switch_s_to_f(m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_available"].asString()) + m_remain_amount);
		} else {
			m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_available"] = switch_f_to_s(switch_s_to_f(m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_available"].asString()) + m_remain_amount);
		}
	}
	
	LOG(INFO) << "m_order_id:" << m_order_id << " DeleteOrder end";
	return true;
}

bool Match::GetContractLastPrice(){
	LOG(INFO) << "m_order_id:" << m_order_id << " GetContractLastPrice start";
	if (m_pending_msg_list.size() > 0){
		return false;
	}
	
	m_time_now = time(0);
	if (m_time_now - m_contract_config_timestamp > 60){
		m_contract_config_timestamp = m_time_now;

		m_contract_id_arr.clear();
		redisReply* reply = (redisReply*)redisCommand(m_stat_redis, "smembers contract:union");
		if (reply == NULL) {
			LOG(ERROR) << "redis reply null";
			redisFree(m_stat_redis);
			m_stat_redis = NULL;
			exit(1);
		}
		if (reply->type != REDIS_REPLY_ARRAY) {
			LOG(ERROR) << "redis type error:" << reply->type;
			freeReplyObject(reply);
			exit(1);
		}
		for (unsigned i = 0; i < reply->elements; i++) {
			m_contract_id_arr.push_back(atoi(reply->element[i]->str));
		}
		freeReplyObject(reply);

		m_contract_config = Json::Value::null;
		m_warning_rate_1 = 1.0;
		m_warning_rate_2 = 0.7;
		m_stop_rate = 0.5;
		for (int i = 0; i < (int)m_contract_id_arr.size(); i++){
			int contract_id = m_contract_id_arr[i];
			redisAppendCommand(m_stat_redis, "HGETALL contract:config:%d", contract_id);
		}
		for (int i = 0; i < (int)m_contract_id_arr.size(); i++){
			int contract_id = m_contract_id_arr[i];
			std::string contract_id_str = std::to_string(contract_id);
			redisReply* temp_reply = NULL;
			redisGetReply(m_stat_redis, (void**)&temp_reply);
			if (temp_reply == NULL){
				LOG(ERROR) << "redis reply null";
				redisFree(m_stat_redis);
				m_stat_redis = NULL;
				exit(1);
			}
			if (temp_reply->type != REDIS_REPLY_ARRAY){
				LOG(ERROR) << "redis type error:" << temp_reply->type;
				freeReplyObject(temp_reply);
				exit(1);
			}
			int len = temp_reply->elements;
			m_contract_config[contract_id_str] = Json::Value::null;
			for (int j = 0; j < len; j = j + 2){
				std::string tmp_field = temp_reply->element[j]->str;
				std::string tmp_value = temp_reply->element[j + 1]->str;
				m_contract_config[contract_id_str][tmp_field] = tmp_value;
			}
			freeReplyObject(temp_reply);
			if (m_contract_config[contract_id_str].isMember("state") && m_contract_config[contract_id_str]["state"].asString() == "1"){
				m_warning_rate_1 = switch_s_to_f(m_contract_config[contract_id_str]["warning_rate"].asString());
				m_warning_rate_2 = switch_s_to_f(m_contract_config[contract_id_str]["second_warning_rate"].asString());
				m_stop_rate = switch_s_to_f(m_contract_config[contract_id_str]["stop_rate"].asString());
			}
		}

		char tmp_st[10240];
		snprintf(tmp_st, sizeof(tmp_st), "%.0Lf", m_warning_rate_1 * 100);
		m_warning_percent_1 = tmp_st;
		snprintf(tmp_st, sizeof(tmp_st), "%.0Lf", m_warning_rate_2 * 100);
		m_warning_percent_2 = tmp_st;
		snprintf(tmp_st, sizeof(tmp_st), "%.0Lf", m_stop_rate * 100);
		m_stop_percent = tmp_st;

		redisReply* temp_reply = (redisReply*)redisCommand(m_redis, "HGETALL last_trade_price");
		if (temp_reply == NULL) {
			LOG(ERROR) << "redis reply null";
			redisFree(m_redis);
			m_redis = NULL;
			return false;
		}
		if (temp_reply->type != REDIS_REPLY_ARRAY){
			LOG(ERROR) << "redis type error:" << temp_reply->type;
			freeReplyObject(temp_reply);
			return false;
		}
		for (unsigned i = 0; i < temp_reply->elements; i = i + 2) {
			char* contract_id = temp_reply->element[i]->str;
			char* price = temp_reply->element[i+1]->str;
			m_contract_price[contract_id] = price;
		}
		freeReplyObject(temp_reply);
	}
	
	for (int i = 0; i < (int)m_contract_id_arr.size(); i++){
		int contract_id = m_contract_id_arr[i];
		if (m_contract_price.isMember(std::to_string(contract_id))){
			m_contract_config[std::to_string(contract_id)]["price"] = m_contract_price[std::to_string(contract_id)].asString();
		}else{
			m_contract_config[std::to_string(contract_id)]["price"] = switch_f_to_s(0);
		}
	}

	Json::StreamWriterBuilder writer;
	writer["indentation"] = "";
	//string contract_config_str = Json::writeString(writer, m_contract_config);
	//LOG(INFO) << "m_order_id:" << m_order_id << " m_contract_config: " << contract_config_str;

	LOG(INFO) << "m_order_id:" << m_order_id << " GetContractLastPrice end";
	return true;
}

bool Match::SettleFundFee(){
	LOG(INFO) << "m_order_id:" << m_order_id << " SettleFundFee start";

	int time_now = time(0);
	if (!((time_now % 86400 >= 0 && time_now % 86400 < 30) || (time_now % 86400 >= 28800 && time_now % 86400 < 28830) || (time_now % 86400 >= 57600 && time_now % 86400 < 57630))){
		return true;
	}

	Json::Value fund_fee_result_list;
	std::vector<std::string> redis_cmd_list;
	std::set<int> fund_fee_user_set;
	fund_fee_user_set.clear();

	redisReply* reply = (redisReply*)redisCommand(m_redis, "HGETALL fund_fee_rate_json");
	if (reply == NULL) {
		LOG(ERROR) << "redis reply null";
		redisFree(m_redis);
		m_redis = NULL;
		exit(1);
	}
	if (reply->type != REDIS_REPLY_ARRAY){
		LOG(ERROR) << "redis type error:" << reply->type;
		freeReplyObject(reply);
		exit(1);
	}
	int len = reply->elements;
	for (int j = 0; j < len; j = j + 2){
		std::string tmp_field = reply->element[j]->str;
		std::string tmp_value = reply->element[j + 1]->str;
		m_contract_config[tmp_field]["fund_fee_rate_json"] = tmp_value;
	}
	freeReplyObject(reply);

	Json::Value::Members position_user_keys = m_positions.getMemberNames();

	Json::CharReaderBuilder rbuilder;
	std::unique_ptr<Json::CharReader> const reader(rbuilder.newCharReader());
	JSONCPP_STRING error;
	Json::StreamWriterBuilder writer;
	writer["indentation"] = "";

	for (auto iter1 = position_user_keys.begin(); iter1 != position_user_keys.end(); iter1++){
		std::string user_id_str = *iter1;
		int user_id = atoi(user_id_str.c_str());
		//if (user_id != 131659 && user_id != 131657 && user_id != 131658 && user_id != 131661 && user_id != 131653 && user_id != 131656 && user_id != 75127 && user_id != 116805 && user_id != 75408 && user_id != 131664 && user_id != 68676 && user_id != 116811 && user_id != 74835 && user_id != 99208 && user_id != 131662){
		//	continue;
		//}

		Json::Value user_account_json = Json::Value::null;
		if (m_accounts.isMember(user_id_str)){
			user_account_json = m_accounts[user_id_str];
		}
		if (!user_account_json.isMember(m_settle_asset)){
			user_account_json[m_settle_asset]["balance"] = "0.00000000";
			user_account_json[m_settle_asset]["margin"] = "0.00000000";
			user_account_json[m_settle_asset]["frozen_margin"] = "0.00000000";
			user_account_json[m_settle_asset]["profit"] = "0.00000000";
			user_account_json[m_settle_asset]["encode_balance"] = HmacSha256Encode(user_id_str + m_settle_asset + "0.00000000");
			user_account_json[m_settle_asset]["encode_margin"] = HmacSha256Encode(user_id_str + m_settle_asset + "0.00000000");
			user_account_json[m_settle_asset]["encode_frozen_margin"] = HmacSha256Encode(user_id_str + m_settle_asset + "0.00000000");
			user_account_json[m_settle_asset]["encode_profit"] = HmacSha256Encode(user_id_str + m_settle_asset + "0.00000000");
		}
		m_user_account[user_id_str]["funds"] = user_account_json;

		Json::Value::Members position_contract_keys = m_positions[user_id_str].getMemberNames();
		for (auto iter2 = position_contract_keys.begin(); iter2 != position_contract_keys.end(); iter2++){
			std::string contract_id_str = *iter2;
			int contract_id = atoi(contract_id_str.c_str());
			long double contract_price = strtold(m_contract_config[contract_id_str]["price"].asCString(), NULL);
			std::string contract_name = m_contract_config[contract_id_str]["contract_name"].asString();

			if (!(m_contract_config[contract_id_str].isMember("state") && m_contract_config[contract_id_str]["state"].asString() == "1" && m_contract_config[contract_id_str].isMember("fund_fee_rate_json"))){
				continue;
			}

			Json::Value fund_fee_rate_json = Json::Value::null;
			std::string fund_fee_rate_str = m_contract_config[contract_id_str]["fund_fee_rate_json"].asString();
			bool ret = reader->parse(fund_fee_rate_str.c_str(), fund_fee_rate_str.c_str() + fund_fee_rate_str.size(), &fund_fee_rate_json, &error);
			if (!(ret && error.size() == 0)) {
				LOG(ERROR) << "fund_fee_rate:" << fund_fee_rate_str << " json error";
				return false;
			}

			long double fund_fee_rate = switch_s_to_f(fund_fee_rate_json["rate"].asString());
			int last_settle_timestamp = fund_fee_rate_json["timestamp"].asInt();
			int fund_rate_status = fund_fee_rate_json["status"].asInt();
			
			if (time_now - last_settle_timestamp < 28800 || fund_rate_status != 0){
				continue;
			}

			Json::Value::Members position_lever_keys = m_positions[user_id_str][contract_id_str].getMemberNames();
			for (auto iter3 = position_lever_keys.begin(); iter3 != position_lever_keys.end(); iter3++) {
				std::string lever_rate = *iter3;
				long double buy_amount = strtold(m_positions[user_id_str][contract_id_str][lever_rate]["buy_amount"].asCString(), NULL);
				long double sell_amount = strtold(m_positions[user_id_str][contract_id_str][lever_rate]["sell_amount"].asCString(), NULL);

				if (buy_amount > EPS){
					long double change_balance = -buy_amount * contract_price * m_rate * fund_fee_rate;

					Json::Value tmp_obj = Json::Value::null;
					tmp_obj["type"] = "account";
					tmp_obj["id"] = GetListId();
					if (tmp_obj["id"].asString() == EmptyString){
						return false;
					}
					tmp_obj["user_id"] = user_id;
					tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_FUND_FEE;
					tmp_obj["remark"] = "资金费用";
					tmp_obj["remark_tw"] = "資金費用";
					tmp_obj["remark_en"] = "Funding Fee";
					tmp_obj["remark_vi"] = "Chi phí tài trợ";
					tmp_obj["update_at"] = m_time_now;
					tmp_obj["asset"] = m_settle_asset;
					tmp_obj["contract"] = contract_name;
					tmp_obj["create_at"] = m_time_now;
					tmp_obj["change_balance"] = switch_f_to_s(change_balance);
					tmp_obj["new_balance"] = switch_f_to_s(switch_s_to_f(m_user_account[user_id_str]["funds"][m_settle_asset]["balance"].asString()) + change_balance);
					tmp_obj["change_margin"] = switch_f_to_s(0);
					tmp_obj["new_margin"] = m_user_account[user_id_str]["funds"][m_settle_asset]["margin"].asString();
					tmp_obj["change_frozen_margin"] = switch_f_to_s(0);
					tmp_obj["new_frozen_margin"] = m_user_account[user_id_str]["funds"][m_settle_asset]["frozen_margin"].asString();
					tmp_obj["change_profit"] = switch_f_to_s(0);
					tmp_obj["new_profit"] = m_user_account[user_id_str]["funds"][m_settle_asset]["profit"].asString();

					m_user_account[user_id_str]["funds"][m_settle_asset]["balance"] = tmp_obj["new_balance"];
					m_user_account[user_id_str]["funds"][m_settle_asset]["encode_balance"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_settle_asset + tmp_obj["new_balance"].asString());

					tmp_obj["new_encode_balance"] = m_user_account[user_id_str]["funds"][m_settle_asset]["encode_balance"];
					tmp_obj["new_encode_margin"] = m_user_account[user_id_str]["funds"][m_settle_asset]["encode_margin"];
					tmp_obj["new_encode_frozen_margin"] = m_user_account[user_id_str]["funds"][m_settle_asset]["encode_frozen_margin"];
					tmp_obj["new_encode_profit"] = m_user_account[user_id_str]["funds"][m_settle_asset]["encode_profit"];

					tmp_obj["trading_area"] = m_server_currency;
					tmp_obj["contract_id"] = contract_id;
					tmp_obj["contract_name"] = contract_name;
					tmp_obj["asset_symbol"] = m_contract_config[contract_id_str]["asset_symbol"].asString();
					tmp_obj["unit_amount"] = m_contract_config[contract_id_str]["unit_amount"].asString();
					tmp_obj["lever_rate"] = atoi(lever_rate.c_str());
					tmp_obj["position_type"] = 1;
					tmp_obj["amount"] = switch_f_to_s(buy_amount);
					tmp_obj["position_value"] = switch_f_to_s(buy_amount * contract_price * m_rate);
					tmp_obj["fund_rate"] = switch_f_to_s(fund_fee_rate);
					tmp_obj["fund_fee"] = switch_f_to_s(change_balance);

					fund_fee_user_set.insert(user_id);
					fund_fee_result_list.append(tmp_obj);
				}
				if (sell_amount > EPS){
					long double change_balance = sell_amount * contract_price * m_rate * fund_fee_rate;

					Json::Value tmp_obj = Json::Value::null;
					tmp_obj["type"] = "account";
					tmp_obj["id"] = GetListId();
					if (tmp_obj["id"].asString() == EmptyString){
						return false;
					}
					tmp_obj["user_id"] = user_id;
					tmp_obj["jnl_type"] = USER_ACCOUNT_JNL_FUND_FEE;
					tmp_obj["remark"] = "资金费用";
					tmp_obj["remark_tw"] = "資金費用";
					tmp_obj["remark_en"] = "Funding Fee";
					tmp_obj["remark_vi"] = "Chi phí tài trợ";
					tmp_obj["update_at"] = m_time_now;
					tmp_obj["asset"] = m_settle_asset;
					tmp_obj["contract"] = contract_name;
					tmp_obj["create_at"] = m_time_now;
					tmp_obj["change_balance"] = switch_f_to_s(change_balance);
					tmp_obj["new_balance"] = switch_f_to_s(switch_s_to_f(m_user_account[user_id_str]["funds"][m_settle_asset]["balance"].asString()) + change_balance);
					tmp_obj["change_margin"] = switch_f_to_s(0);
					tmp_obj["new_margin"] = m_user_account[user_id_str]["funds"][m_settle_asset]["margin"].asString();
					tmp_obj["change_frozen_margin"] = switch_f_to_s(0);
					tmp_obj["new_frozen_margin"] = m_user_account[user_id_str]["funds"][m_settle_asset]["frozen_margin"].asString();
					tmp_obj["change_profit"] = switch_f_to_s(0);
					tmp_obj["new_profit"] = m_user_account[user_id_str]["funds"][m_settle_asset]["profit"].asString();

					m_user_account[user_id_str]["funds"][m_settle_asset]["balance"] = tmp_obj["new_balance"];
					m_user_account[user_id_str]["funds"][m_settle_asset]["encode_balance"] = HmacSha256Encode(tmp_obj["user_id"].asString() + m_settle_asset + tmp_obj["new_balance"].asString());

					tmp_obj["new_encode_balance"] = m_user_account[user_id_str]["funds"][m_settle_asset]["encode_balance"];
					tmp_obj["new_encode_margin"] = m_user_account[user_id_str]["funds"][m_settle_asset]["encode_margin"];
					tmp_obj["new_encode_frozen_margin"] = m_user_account[user_id_str]["funds"][m_settle_asset]["encode_frozen_margin"];
					tmp_obj["new_encode_profit"] = m_user_account[user_id_str]["funds"][m_settle_asset]["encode_profit"];

					tmp_obj["trading_area"] = m_server_currency;
					tmp_obj["contract_id"] = contract_id;
					tmp_obj["contract_name"] = contract_name;
					tmp_obj["asset_symbol"] = m_contract_config[contract_id_str]["asset_symbol"].asString();
					tmp_obj["unit_amount"] = m_contract_config[contract_id_str]["unit_amount"].asString();
					tmp_obj["lever_rate"] = atoi(lever_rate.c_str());
					tmp_obj["position_type"] = 2;
					tmp_obj["amount"] = switch_f_to_s(sell_amount);
					tmp_obj["position_value"] = switch_f_to_s(sell_amount * contract_price * m_rate);
					tmp_obj["fund_rate"] = switch_f_to_s(fund_fee_rate);
					tmp_obj["fund_fee"] = switch_f_to_s(change_balance);

					fund_fee_user_set.insert(user_id);
					fund_fee_result_list.append(tmp_obj);
				}
			}
		}
	}
	
	Json::Value::Members contract_id_keys = m_contract_config.getMemberNames();
	for (auto iter = contract_id_keys.begin(); iter != contract_id_keys.end(); iter++){
		std::string contract_id_str = *iter;
		int contract_id = atoi(contract_id_str.c_str());

		if (!(m_contract_config[contract_id_str].isMember("state") && m_contract_config[contract_id_str]["state"].asString() == "1" && m_contract_config[contract_id_str].isMember("fund_fee_rate_json"))){
			continue;
		}

		Json::Value fund_fee_rate_json = Json::Value::null;
		std::string fund_fee_rate = m_contract_config[contract_id_str]["fund_fee_rate_json"].asString();
		bool ret = reader->parse(fund_fee_rate.c_str(), fund_fee_rate.c_str() + fund_fee_rate.size(), &fund_fee_rate_json, &error);
		if (!(ret && error.size() == 0)) {
			LOG(ERROR) << "fund_fee_rate:" << fund_fee_rate << " json error";
			return false;
		}

		fund_fee_rate_json["status"] = 1;
		fund_fee_rate = Json::writeString(writer, fund_fee_rate_json);
		m_contract_config[contract_id_str]["fund_fee_rate_json"] = fund_fee_rate;

		char *redis_cmd;
		if (redisFormatCommand(&redis_cmd, "HSET fund_fee_rate_json %d %s", contract_id, fund_fee_rate.c_str()) <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		std::string redis_cmd_str = redis_cmd;
		free(redis_cmd);
		redis_cmd_list.push_back(redis_cmd_str);
	}

	char *redis_cmd;
	for (auto it = fund_fee_user_set.begin(); it != fund_fee_user_set.end(); it++){
		int user_id = *it;
		std::string result = Json::writeString(writer, m_user_account[std::to_string(user_id)]["funds"]);
		if (redisFormatCommand(&redis_cmd, "SET account_user_%d %s", user_id, result.c_str()) <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		std::string redis_cmd_str = redis_cmd;
		free(redis_cmd);
		redis_cmd_list.push_back(redis_cmd_str);
	}

	std::string result = Json::writeString(writer, fund_fee_result_list);
	std::string redis_cmd_str;
	if (redisFormatCommand(&redis_cmd, "LPUSH order_result_list %s", result.c_str()) <= 0){
		LOG(ERROR) << "redis format error";
		return false;
	}
	redis_cmd_str = redis_cmd;
	free(redis_cmd);
	redis_cmd_list.push_back(redis_cmd_str);

	redisAppendCommand(m_redis, "MULTI");
	for (int i = 0; i < (int)redis_cmd_list.size(); i++){
		redisAppendFormattedCommand(m_redis, redis_cmd_list[i].c_str(), redis_cmd_list[i].size());
	}
	redisAppendCommand(m_redis, "EXEC");
	redisReply* temp_reply = NULL;
	for (int i = 0; i < (int)redis_cmd_list.size() + 2; i++){
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

	Json::Value::Members keys = m_user_account.getMemberNames();
	for (auto iter = keys.begin(); iter != keys.end(); iter++) {
		m_accounts[*iter] = m_user_account[*iter]["funds"];
	}

	LOG(INFO) << "m_order_id:" << m_order_id << " SettleFundFee end";
	return true;
}

bool Match::SettleProfitAndLossLimit(){
	LOG(INFO) << "m_order_id:" << m_order_id << " SettleProfitAndLossLimit start";

	if (m_statistics["trade"]["list"].size() == 0 && m_order_system_type == ORDER_SYSTEM_TYPE_NORMAL){
		LOG(INFO) << "m_order_id:" << m_order_id << " normal order no trade";
		return true;
	}

	Json::StreamWriterBuilder writer;
	writer["indentation"] = "";
	
	Json::Value::Members position_user_keys = m_positions.getMemberNames();

	Json::Value user_trigger_liquidation = Json::Value::null;
	redisReply* reply = NULL;
	reply = (redisReply*)redisCommand(m_stat_redis, "HGETALL user_trigger_liquidation");
	if (reply == NULL) {
		LOG(ERROR) << "redis reply null";
		redisFree(m_stat_redis);
		m_stat_redis = NULL;
		exit(1);
	}
	if (reply->type != REDIS_REPLY_ARRAY) {
		LOG(ERROR) << "redis type error:" << reply->type;
		freeReplyObject(reply);
		exit(1);
	}
	for (int i = 0; i < (int)reply->elements; i = i + 2){
		std::string user_id_str = reply->element[i]->str;
		std::string trigger_liquidation = reply->element[i + 1]->str;
		user_trigger_liquidation[user_id_str] = trigger_liquidation;
	}

	std::vector<std::string> position_user;
	for (unsigned i = 0; i < reply->elements; i++) {
		position_user.push_back(reply->element[i]->str);
	}
	freeReplyObject(reply);

	
	int stop_user_id = 0;
	int stop_type = 0;
	int stop_msg_state = 0;		//1为需要发消息
	int profit_loss_user_id = 0;
	std::string profit_loss_contract_name = "";
	std::string profit_loss_side = "";
	std::string profit_loss_side_tw = "";
	std::string profit_loss_side_en = "";
	std::string profit_loss_side_vi = "";
	Json::Value profit_loss_order_json = Json::Value::null;
	std::vector<std::string> profit_loss_cancel_list;

	long double cur_contract_total_position = 0;
	m_statistics = Json::Value::null;
	m_statistics["order_book"]["contract_id"] = m_order_contract_id_str;
	m_statistics["msg_type"] = "position";

	std::set<int> warning_state_change_user_set;
	warning_state_change_user_set.clear();
	std::vector<int> warning_msg_user_arr;
	warning_msg_user_arr.clear();
	std::vector<int> send_warning_msg_user_arr;
	send_warning_msg_user_arr.clear();
	m_trade_msg_array.resize(0);
	m_email_msg_array.resize(0);

	for (auto iter1 = position_user_keys.begin(); iter1 != position_user_keys.end(); iter1++){
		std::string user_id_str = *iter1;
		int user_id = atoi(user_id_str.c_str());

		if (maker_user_set.find(user_id) != maker_user_set.end()) continue;

		long double balance = switch_s_to_f(m_accounts[user_id_str][m_settle_asset]["balance"].asString());
		long double margin = switch_s_to_f(m_accounts[user_id_str][m_settle_asset]["margin"].asString());
		long double frozen_margin = switch_s_to_f(m_accounts[user_id_str][m_settle_asset]["frozen_margin"].asString());
		long double profit = switch_s_to_f(m_accounts[user_id_str][m_settle_asset]["profit"].asString());

		if (margin < EPS && frozen_margin < EPS){
			continue;
		}
		
		Json::Value::Members position_contract_keys = m_positions[user_id_str].getMemberNames();
		for (auto iter2 = position_contract_keys.begin(); iter2 != position_contract_keys.end(); iter2++){
			std::string contract_id_str = *iter2;
			int contract_id = atoi(contract_id_str.c_str());
			long double contract_price = strtold(m_contract_config[contract_id_str]["price"].asCString(), NULL);
			std::string contract_name = m_contract_config[contract_id_str]["contract_name"].asString();
	
			Json::Value::Members position_lever_keys = m_positions[user_id_str][contract_id_str].getMemberNames();
			for (auto iter3 = position_lever_keys.begin(); iter3 != position_lever_keys.end(); iter3++) {
				std::string lever_rate = *iter3;

				long double buy_amount = strtold(m_positions[user_id_str][contract_id_str][lever_rate]["buy_amount"].asCString(), NULL);
				long double buy_available = strtold(m_positions[user_id_str][contract_id_str][lever_rate]["buy_available"].asCString(), NULL);
				long double buy_quote_amount_settle = strtold(m_positions[user_id_str][contract_id_str][lever_rate]["buy_quote_amount_settle"].asCString(), NULL);
				long double buy_profit_limit = 0L;
				if (m_positions[user_id_str][contract_id_str][lever_rate].isMember("buy_profit_limit")){
					buy_profit_limit = strtold(m_positions[user_id_str][contract_id_str][lever_rate]["buy_profit_limit"].asCString(), NULL);
				}
				long double buy_lose_limit = 0L;
				if (m_positions[user_id_str][contract_id_str][lever_rate].isMember("buy_lose_limit")){
					buy_lose_limit = strtold(m_positions[user_id_str][contract_id_str][lever_rate]["buy_lose_limit"].asCString(), NULL);
				}
				long double sell_amount = strtold(m_positions[user_id_str][contract_id_str][lever_rate]["sell_amount"].asCString(), NULL);
				long double sell_available = strtold(m_positions[user_id_str][contract_id_str][lever_rate]["sell_available"].asCString(), NULL);
				long double sell_quote_amount_settle = strtold(m_positions[user_id_str][contract_id_str][lever_rate]["sell_quote_amount_settle"].asCString(), NULL);
				long double sell_profit_limit = 0L;

				if (contract_id_str == m_order_contract_id_str){
					cur_contract_total_position += buy_amount;
				}

				if (m_positions[user_id_str][contract_id_str][lever_rate].isMember("sell_profit_limit")){
					sell_profit_limit = strtold(m_positions[user_id_str][contract_id_str][lever_rate]["sell_profit_limit"].asCString(), NULL);
				}
				long double sell_lose_limit = 0L;
				if (m_positions[user_id_str][contract_id_str][lever_rate].isMember("sell_lose_limit")){
					sell_lose_limit = strtold(m_positions[user_id_str][contract_id_str][lever_rate]["sell_lose_limit"].asCString(), NULL);
				}

				profit += buy_amount * contract_price * m_rate - buy_quote_amount_settle + sell_quote_amount_settle - sell_amount * contract_price * m_rate;
				if (buy_profit_limit > EPS && contract_price + EPS > buy_profit_limit && buy_amount > EPS && profit_loss_user_id == 0){
					profit_loss_cancel_list.clear();
					if (buy_available + EPS < buy_amount) {
						//TODO 取消订单
						for (int i = 0; i < (int)m_accounts[user_id_str]["orders"].size(); i++){
							std::string tmp_order_id = m_accounts[user_id_str]["orders"][i].asString();
							redisAppendCommand(m_redis, "HMGET order_detail_%s user_id contract_id lever_rate order_op", tmp_order_id.c_str());
						}
						for (int i = 0; i < (int)m_accounts[user_id_str]["orders"].size(); i++){
							std::string tmp_order_id = m_accounts[user_id_str]["orders"][i].asString();
							redisReply* reply = NULL;
							redisGetReply(m_redis, (void**)&reply);
							if (reply == NULL){
								LOG(ERROR) << "tmp_order_id:" << tmp_order_id << " redis reply null";
								redisFree(m_redis);
								m_redis = NULL;
								return false;
							}
							if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 4){
								int tmp_user_id = atoi(reply->element[0]->str);
								if (tmp_user_id != user_id){
									LOG(ERROR) << "tmp_order_id:" << tmp_order_id << " tmp_user_id: " << tmp_user_id << " cancel user_id: " << user_id;
									freeReplyObject(reply);
									return false;
								}
								int tmp_contract_id = atoi(reply->element[1]->str);
								std::string tmp_lever_rate = reply->element[2]->str;
								int tmp_order_op = atoi(reply->element[3]->str);

								if (tmp_contract_id == contract_id && tmp_lever_rate == lever_rate && tmp_order_op == ORDER_SIDE_CLOSE_LONG) {
									Json::Value tmp_msg_json = Json::Value::null;
									tmp_msg_json["msg_type"] = "cancel";
									tmp_msg_json["order_id"] = tmp_order_id;
									tmp_msg_json["user_id"] = tmp_user_id;
									std::string tmp_msg_str = Json::writeString(writer, tmp_msg_json);
									profit_loss_cancel_list.push_back(tmp_msg_str);
								}
							} else {
								freeReplyObject(reply);
								return false;
							}
						}
					}
					
					profit_loss_user_id = user_id;
					profit_loss_contract_name = contract_name;
					profit_loss_side = "止盈";
					profit_loss_side_tw = "止盈";
					profit_loss_side_en = "Limit Order";
					profit_loss_side_vi = "lãi";

					m_new_order_id = GetListId();
					if (m_new_order_id == EmptyString){
						return false;
					}
					profit_loss_order_json["msg_type"] = "order";
					profit_loss_order_json["order_id"] = m_new_order_id;
					profit_loss_order_json["user_id"] = user_id;
					profit_loss_order_json["contract_id"] = contract_id;
					profit_loss_order_json["lever_rate"] = lever_rate;
					profit_loss_order_json["order_type"] = ORDER_TYPE_LIMIT;
					profit_loss_order_json["is_bbo"] = 0;
					profit_loss_order_json["order_op"] = ORDER_SIDE_CLOSE_LONG;
					profit_loss_order_json["system_type"] = ORDER_SYSTEM_TYPE_PROFIT;
					profit_loss_order_json["origin_amount"] = switch_f_to_s(buy_amount);
					profit_loss_order_json["take_profit"] = switch_f_to_s(0);
					profit_loss_order_json["stop_loss"] = switch_f_to_s(0);
					profit_loss_order_json["ip"] = "";
					profit_loss_order_json["source"] = 0;
					profit_loss_order_json["price"] = switch_f_to_s(buy_profit_limit);
				}
				if (buy_lose_limit > EPS && contract_price < buy_lose_limit + EPS && buy_amount > EPS && profit_loss_user_id == 0){
					profit_loss_cancel_list.clear();
					if (buy_available + EPS < buy_amount) {
						//TODO 取消订单
						for (int i = 0; i < (int)m_accounts[user_id_str]["orders"].size(); i++){
							std::string tmp_order_id = m_accounts[user_id_str]["orders"][i].asString();
							redisAppendCommand(m_redis, "HMGET order_detail_%s user_id contract_id lever_rate order_op", tmp_order_id.c_str());
						}
						for (int i = 0; i < (int)m_accounts[user_id_str]["orders"].size(); i++){
							std::string tmp_order_id = m_accounts[user_id_str]["orders"][i].asString();
							redisReply* reply = NULL;
							redisGetReply(m_redis, (void**)&reply);
							if (reply == NULL){
								LOG(ERROR) << "tmp_order_id:" << tmp_order_id << " redis reply null";
								redisFree(m_redis);
								m_redis = NULL;
								return false;
							}
							if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 4){
								int tmp_user_id = atoi(reply->element[0]->str);
								if (tmp_user_id != user_id){
									LOG(ERROR) << "tmp_order_id:" << tmp_order_id << " tmp_user_id: " << tmp_user_id << " cancel user_id: " << user_id;
									freeReplyObject(reply);
									return false;
								}
								int tmp_contract_id = atoi(reply->element[1]->str);
								std::string tmp_lever_rate = reply->element[2]->str;
								int tmp_order_op = atoi(reply->element[3]->str);

								if (tmp_contract_id == contract_id && tmp_lever_rate == lever_rate && tmp_order_op == ORDER_SIDE_CLOSE_LONG) {
									Json::Value tmp_msg_json = Json::Value::null;
									tmp_msg_json["msg_type"] = "cancel";
									tmp_msg_json["order_id"] = tmp_order_id;
									tmp_msg_json["user_id"] = tmp_user_id;
									std::string tmp_msg_str = Json::writeString(writer, tmp_msg_json);
									profit_loss_cancel_list.push_back(tmp_msg_str);
								}
							} else {
								freeReplyObject(reply);
								return false;
							}
						}
					}

					profit_loss_user_id = user_id;
					profit_loss_contract_name = contract_name;
					profit_loss_side = "止损";
					profit_loss_side_tw = "止損";
					profit_loss_side_en = "Stop Loss";
					profit_loss_side_vi = "lỗ";

					m_new_order_id = GetListId();
					if (m_new_order_id == EmptyString){
						return false;
					}
					profit_loss_order_json["msg_type"] = "order";
					profit_loss_order_json["order_id"] = m_new_order_id;
					profit_loss_order_json["user_id"] = user_id;
					profit_loss_order_json["contract_id"] = contract_id;
					profit_loss_order_json["lever_rate"] = lever_rate;
					profit_loss_order_json["order_type"] = ORDER_TYPE_LIMIT;
					profit_loss_order_json["is_bbo"] = 0;
					profit_loss_order_json["order_op"] = ORDER_SIDE_CLOSE_LONG;
					profit_loss_order_json["system_type"] = ORDER_SYSTEM_TYPE_LOSS;
					profit_loss_order_json["origin_amount"] = switch_f_to_s(buy_amount);
					profit_loss_order_json["take_profit"] = switch_f_to_s(0);
					profit_loss_order_json["stop_loss"] = switch_f_to_s(0);
					profit_loss_order_json["ip"] = "";
					profit_loss_order_json["source"] = 0;
					profit_loss_order_json["price"] = switch_f_to_s(buy_lose_limit);
				}
				if (sell_profit_limit > EPS && contract_price < sell_profit_limit + EPS && sell_amount > EPS && profit_loss_user_id == 0){
					profit_loss_cancel_list.clear();
					if (sell_available + EPS < sell_amount) {
						//TODO 取消订单
						for (int i = 0; i < (int)m_accounts[user_id_str]["orders"].size(); i++){
							std::string tmp_order_id = m_accounts[user_id_str]["orders"][i].asString();
							redisAppendCommand(m_redis, "HMGET order_detail_%s user_id contract_id lever_rate order_op", tmp_order_id.c_str());
						}
						for (int i = 0; i < (int)m_accounts[user_id_str]["orders"].size(); i++){
							std::string tmp_order_id = m_accounts[user_id_str]["orders"][i].asString();
							redisReply* reply = NULL;
							redisGetReply(m_redis, (void**)&reply);
							if (reply == NULL){
								LOG(ERROR) << "tmp_order_id:" << tmp_order_id << " redis reply null";
								redisFree(m_redis);
								m_redis = NULL;
								return false;
							}
							if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 4){
								int tmp_user_id = atoi(reply->element[0]->str);
								if (tmp_user_id != user_id){
									LOG(ERROR) << "tmp_order_id:" << tmp_order_id << " tmp_user_id: " << tmp_user_id << " cancel user_id: " << user_id;
									freeReplyObject(reply);
									return false;
								}
								int tmp_contract_id = atoi(reply->element[1]->str);
								std::string tmp_lever_rate = reply->element[2]->str;
								int tmp_order_op = atoi(reply->element[3]->str);

								if (tmp_contract_id == contract_id && tmp_lever_rate == lever_rate && tmp_order_op == ORDER_SIDE_CLOSE_SHORT) {
									Json::Value tmp_msg_json = Json::Value::null;
									tmp_msg_json["msg_type"] = "cancel";
									tmp_msg_json["order_id"] = tmp_order_id;
									tmp_msg_json["user_id"] = tmp_user_id;
									std::string tmp_msg_str = Json::writeString(writer, tmp_msg_json);
									profit_loss_cancel_list.push_back(tmp_msg_str);
								}
							} else {
								freeReplyObject(reply);
								return false;
							}
						}
					}

					profit_loss_user_id = user_id;
					profit_loss_contract_name = contract_name;
					profit_loss_side = "止盈";
					profit_loss_side_tw = "止盈";
					profit_loss_side_en = "Limit Order";
					profit_loss_side_vi = "lãi";

					m_new_order_id = GetListId();
					if (m_new_order_id == EmptyString){
						return false;
					}
					profit_loss_order_json["msg_type"] = "order";
					profit_loss_order_json["order_id"] = m_new_order_id;
					profit_loss_order_json["user_id"] = user_id;
					profit_loss_order_json["contract_id"] = contract_id;
					profit_loss_order_json["lever_rate"] = lever_rate;
					profit_loss_order_json["order_type"] = ORDER_TYPE_LIMIT;
					profit_loss_order_json["is_bbo"] = 0;
					profit_loss_order_json["order_op"] = ORDER_SIDE_CLOSE_SHORT;
					profit_loss_order_json["system_type"] = ORDER_SYSTEM_TYPE_PROFIT;
					profit_loss_order_json["origin_amount"] = switch_f_to_s(sell_amount);
					profit_loss_order_json["take_profit"] = switch_f_to_s(0);
					profit_loss_order_json["stop_loss"] = switch_f_to_s(0);
					profit_loss_order_json["ip"] = "";
					profit_loss_order_json["source"] = 0;
					profit_loss_order_json["price"] = switch_f_to_s(sell_profit_limit);
				}
				if (sell_lose_limit > EPS && contract_price + EPS > sell_lose_limit && sell_amount > EPS && profit_loss_user_id == 0){
					profit_loss_cancel_list.clear();
					if (sell_available + EPS < sell_amount) {
						//TODO 取消订单
						for (int i = 0; i < (int)m_accounts[user_id_str]["orders"].size(); i++){
							std::string tmp_order_id = m_accounts[user_id_str]["orders"][i].asString();
							redisAppendCommand(m_redis, "HMGET order_detail_%s user_id contract_id lever_rate order_op", tmp_order_id.c_str());
						}
						for (int i = 0; i < (int)m_accounts[user_id_str]["orders"].size(); i++){
							std::string tmp_order_id = m_accounts[user_id_str]["orders"][i].asString();
							redisReply* reply = NULL;
							redisGetReply(m_redis, (void**)&reply);
							if (reply == NULL){
								LOG(ERROR) << "tmp_order_id:" << tmp_order_id << " redis reply null";
								redisFree(m_redis);
								m_redis = NULL;
								return false;
							}
							if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 4){
								int tmp_user_id = atoi(reply->element[0]->str);
								if (tmp_user_id != user_id){
									LOG(ERROR) << "tmp_order_id:" << tmp_order_id << " tmp_user_id: " << tmp_user_id << " cancel user_id: " << user_id;
									freeReplyObject(reply);
									return false;
								}
								int tmp_contract_id = atoi(reply->element[1]->str);
								std::string tmp_lever_rate = reply->element[2]->str;
								int tmp_order_op = atoi(reply->element[3]->str);

								if (tmp_contract_id == contract_id && tmp_lever_rate == lever_rate && tmp_order_op == ORDER_SIDE_CLOSE_SHORT) {
									Json::Value tmp_msg_json = Json::Value::null;
									tmp_msg_json["msg_type"] = "cancel";
									tmp_msg_json["order_id"] = tmp_order_id;
									tmp_msg_json["user_id"] = tmp_user_id;
									std::string tmp_msg_str = Json::writeString(writer, tmp_msg_json);
									profit_loss_cancel_list.push_back(tmp_msg_str);
								}
							} else {
								freeReplyObject(reply);
								return false;
							}
						}
					}

					profit_loss_user_id = user_id;
					profit_loss_contract_name = contract_name;
					profit_loss_side = "止损";
					profit_loss_side_tw = "止損";
					profit_loss_side_en = "Stop Loss";
					profit_loss_side_vi = "lỗ";

					m_new_order_id = GetListId();
					if (m_new_order_id == EmptyString){
						return false;
					}
					profit_loss_order_json["msg_type"] = "order";
					profit_loss_order_json["order_id"] = m_new_order_id;
					profit_loss_order_json["user_id"] = user_id;
					profit_loss_order_json["contract_id"] = contract_id;
					profit_loss_order_json["lever_rate"] = lever_rate;
					profit_loss_order_json["order_type"] = ORDER_TYPE_LIMIT;
					profit_loss_order_json["is_bbo"] = 0;
					profit_loss_order_json["order_op"] = ORDER_SIDE_CLOSE_SHORT;
					profit_loss_order_json["system_type"] = ORDER_SYSTEM_TYPE_LOSS;
					profit_loss_order_json["origin_amount"] = switch_f_to_s(sell_amount);
					profit_loss_order_json["take_profit"] = switch_f_to_s(0);
					profit_loss_order_json["stop_loss"] = switch_f_to_s(0);
					profit_loss_order_json["ip"] = "";
					profit_loss_order_json["source"] = 0;
					profit_loss_order_json["price"] = switch_f_to_s(sell_lose_limit);
				}
			}
		}
		long double risk_rate = (balance + profit) / (margin + frozen_margin);
		if (risk_rate < m_stop_rate + EPS){
			if (stop_user_id == 0){
				if (!m_accounts[user_id_str].isMember("stop_timestamp")){
					m_accounts[user_id_str]["stop_timestamp"] = 0;
				}
				int stop_timestamp = m_accounts[user_id_str]["stop_timestamp"].asInt();
				if (m_time_now > stop_timestamp + m_match_stop_interval){
					stop_user_id = user_id;
					stop_type = 1;
					stop_msg_state = 1;
				}
			}
		}else if (user_trigger_liquidation.isMember(user_id_str) && balance + profit < switch_s_to_f(user_trigger_liquidation[user_id_str].asString()) * m_send_stop_absolute_rate){
		//|| balance + profit - switch_s_to_f(user_trigger_liquidation[user_id_str].asString()) < m_send_stop_relative_rate * (margin + frozen_margin)
			if (stop_user_id == 0){
				if (!m_accounts[user_id_str].isMember("stop_timestamp")){
					m_accounts[user_id_str]["stop_timestamp"] = 0;
				}
				int stop_timestamp = m_accounts[user_id_str]["stop_timestamp"].asInt();
				if (m_time_now > stop_timestamp + m_match_stop_interval){
					stop_user_id = user_id;
					stop_type = 2;
					if (!JudgeOneUtcDay(stop_timestamp, m_time_now)){
						stop_msg_state = 1;
					}
				}
			}
		}else{
			if (!m_accounts[user_id_str].isMember("warning_state")){
				m_accounts[user_id_str]["warning_state"] = 0;
			}
			int old_warning_state = m_accounts[user_id_str]["warning_state"].asInt();
			int new_warning_state;
			if (risk_rate < m_warning_rate_1 + EPS){
				new_warning_state = 1;
			}else{
				new_warning_state = 0;
			}
			if (old_warning_state != new_warning_state){
				warning_state_change_user_set.insert(user_id);
				m_accounts[user_id_str]["warning_state"] = new_warning_state;
				if (new_warning_state > old_warning_state){
					warning_msg_user_arr.push_back(user_id);
				}
			}

			if (user_trigger_liquidation.isMember(user_id_str)){
				long double send_amount = switch_s_to_f(user_trigger_liquidation[user_id_str].asString());
				if (!m_accounts[user_id_str].isMember("send_warning_timestamp")){
					m_accounts[user_id_str]["send_warning_timestamp"] = 0;
				}
				int send_warning_timestamp = m_accounts[user_id_str]["send_warning_timestamp"].asInt();
				long double send_risk_rate = (balance + profit - send_amount) / (margin + frozen_margin);
				if (send_risk_rate < m_send_stop_warning_rate && !JudgeOneUtcDay(send_warning_timestamp, m_time_now)){
					warning_state_change_user_set.insert(user_id);
					m_accounts[user_id_str]["send_warning_timestamp"] = m_time_now;
					send_warning_msg_user_arr.push_back(user_id);
				}
			}
		}
	}

	{
		m_statistics["total_position"] = switch_f_to_s(cur_contract_total_position);
		std::string result = Json::writeString(writer, m_statistics);
		m_trade->SendStatisticsMessage(result);
	}

	char *redis_cmd;
	m_redis_cmd_list.clear();
	m_order_result_list.resize(0);
	for (std::set<int>::iterator iter = warning_state_change_user_set.begin(); iter != warning_state_change_user_set.end(); iter++){
		int user_id = *iter;
		std::string result = Json::writeString(writer, m_accounts[std::to_string(user_id)]);
		if (redisFormatCommand(&redis_cmd, "SET account_user_%d %s", user_id, result.c_str()) <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		std::string redis_cmd_str = redis_cmd;
		free(redis_cmd);
		m_redis_cmd_list.push_back(redis_cmd_str);
	}
	for (int i = 0; i < (int)warning_msg_user_arr.size(); i++){
		int user_id = warning_msg_user_arr[i];

		int warning_state = m_accounts[std::to_string(user_id)]["warning_state"].asInt();
		Json::Value tmp_warning_json = Json::Value::null;
		tmp_warning_json["type"] = "trade_msg";
		tmp_warning_json["user_id"] = user_id;
		tmp_warning_json["msg_type"] = MESSAGE_TYPE_TRADE;
		if (warning_state == 1){
			tmp_warning_json["title"] = "您的【" + m_server_currency + "交易区】账户风险率已达到" + m_warning_percent_1 + "%，请及时追加保证金或者降低仓位以控制风险！";
			tmp_warning_json["title_tw"] = "您的【" + m_server_currency + "交易區】賬戶風險率已達到" + m_warning_percent_1 + "%，請及時追加保證金或者降低倉位以控制風險！";
			tmp_warning_json["title_en"] = "Your【" + m_server_currency + " market】account risk rating has reached " + m_warning_percent_1 + "%, please add margin or reduce your positions to control your risk!";
			tmp_warning_json["title_vi"] = "Tỷ lệ rủi ro tài khoản【Khu giao dịch " + m_server_currency + "】của bạn đã đạt mức " + m_warning_percent_1 + "%, vui lòng nạp thêm ký quỹ hoặc giảm vị thế để kiểm soát rủi ro!";
		}else{
			tmp_warning_json["title"] = "您的【" + m_server_currency + "交易区】账户风险率已达到" + m_warning_percent_2 + "%，请及时追加保证金或者降低仓位以控制风险！";
			tmp_warning_json["title_tw"] = "您的【" + m_server_currency + "交易區】賬戶風險率已達到" + m_warning_percent_2 + "%，請及時追加保證金或者降低倉位以控制風險！";
			tmp_warning_json["title_en"] = "Your【" + m_server_currency + " market】account risk rating has reached " + m_warning_percent_2 + "%, please add margin or reduce your positions to control your risk!";
			tmp_warning_json["title_vi"] = "Tỷ lệ rủi ro tài khoản【Khu giao dịch " + m_server_currency + "】của bạn đã đạt mức " + m_warning_percent_2 + "%, vui lòng nạp thêm ký quỹ hoặc giảm vị thế để kiểm soát rủi ro!";
		}
		tmp_warning_json["currency"] = m_server_currency;
		m_order_result_list.append(tmp_warning_json);

		Json::Value trade_msg_single = Json::Value::null;
		trade_msg_single["user_id"] = std::to_string(user_id);
		trade_msg_single["message"] = Json::Value::null;
		trade_msg_single["message"]["type"] = MESSAGE_TYPE_TRADE;
		trade_msg_single["message"]["show"] = 1;
		trade_msg_single["message"]["title"] = tmp_warning_json["title"].asString();
		trade_msg_single["message"]["title_tw"] = tmp_warning_json["title_tw"].asString();
		trade_msg_single["message"]["title_en"] = tmp_warning_json["title_en"].asString();
		trade_msg_single["message"]["title_vi"] = tmp_warning_json["title_vi"].asString();
		trade_msg_single["message"]["currency"] = m_server_currency;
		trade_msg_single["message"]["created_at"] = m_time_now;
		m_trade_msg_array.append(trade_msg_single);

		Json::Value email_msg_single = Json::Value::null;
		email_msg_single["user_id"] = user_id;
		email_msg_single["title_cn"] = "合约交易风险预警";
		email_msg_single["title_tw"] = "合約交易風險預警";
		email_msg_single["title_en"] = "Perpetual Contracts Risk warning";
		email_msg_single["title_vi"] = "Dự cảnh báo rủi do Hợp Đồng giao dịch";
		if (warning_state == 1){
			email_msg_single["content_cn"] = "尊敬的用户，您好！您的【" + m_server_currency + "交易区】账户风险率已达到" + m_warning_percent_1 + "%，请及时追加保证金或者降低仓位以控制风险！";
			email_msg_single["content_tw"] = "尊敬的用戶，您好！您的【" + m_server_currency + "交易區】賬戶風險率已達到" + m_warning_percent_1 + "%，請及時追加保證金或者降低倉位以控制風險！";
			email_msg_single["content_en"] = "Dear User, your【" + m_server_currency + " market】account risk rating has reached " + m_warning_percent_1 + "%, please add margin or reduce your positions to control your risk!";
			email_msg_single["content_vi"] = "Kính gửi người dùng, tỷ lệ rủi ro tài khoản【Khu giao dịch " + m_server_currency + "】của bạn đã đạt mức " + m_warning_percent_1 + "%, vui lòng nạp thêm ký quỹ hoặc giảm vị thế để kiểm soát rủi ro!";
		}else{
			email_msg_single["content_cn"] = "尊敬的用户，您好！您的【" + m_server_currency + "交易区】账户风险率已达到" + m_warning_percent_2 + "%，请及时追加保证金或者降低仓位以控制风险！";
			email_msg_single["content_tw"] = "尊敬的用戶，您好！您的【" + m_server_currency + "交易區】賬戶風險率已達到" + m_warning_percent_2 + "%，請及時追加保證金或者降低倉位以控制風險！";
			email_msg_single["content_en"] = "Dear User, your【" + m_server_currency + " market】account risk rating has reached " + m_warning_percent_2 + "%, please add margin or reduce your positions to control your risk!";
			email_msg_single["content_vi"] = "Kính gửi người dùng, tỷ lệ rủi ro tài khoản【Khu giao dịch " + m_server_currency + "】của bạn đã đạt mức " + m_warning_percent_2 + "%, vui lòng nạp thêm ký quỹ hoặc giảm vị thế để kiểm soát rủi ro!";
		}
		m_email_msg_array.append(email_msg_single);
	}

	for (int i = 0; i < (int)send_warning_msg_user_arr.size(); i++){
		int user_id = send_warning_msg_user_arr[i];

		Json::Value tmp_warning_json = Json::Value::null;
		tmp_warning_json["type"] = "trade_msg";
		tmp_warning_json["user_id"] = user_id;
		tmp_warning_json["msg_type"] = MESSAGE_TYPE_TRADE;
		tmp_warning_json["title"] = "【强平风险提醒】充送活动用户您好，您的" + m_server_currency + "交易区账户即将达到充送用户强平线，请您及时入金或关注行情波动，控制交易风险！";
		tmp_warning_json["title_tw"] = "【強平風險提醒】充送活動用戶您好，您的" + m_server_currency + "交易區賬戶即將達到充送用戶強平線，請您及時入金或關註行情波動，控制交易風險！";
		tmp_warning_json["title_en"] = "【Forced liquidation risk reminder】Dear 100% deposit match customer, your " + m_server_currency + " trading market account has reached forced liquidation level, please add margin and control your risks!";
		tmp_warning_json["title_vi"] = "【Cảnh báo rủi ro cưỡng chế đóng vị thế】Xin chào khách hàng tham gia hoạt động nạp bao nhiêu tặng bấy nhiêu, tài khoản khu giao dịch " + m_server_currency + " của bạn đạt đến mức độ cưỡng chế đóng vị thế của khách hàng nạp bao nhiêu tặng bấy nhiêu, vui lòng thanh toán kịp thời hoặc chú ý đến biến động thị trường và kiểm soát rủi ro giao dịch!";
		tmp_warning_json["currency"] = m_server_currency;
		m_order_result_list.append(tmp_warning_json);

		Json::Value trade_msg_single = Json::Value::null;
		trade_msg_single["user_id"] = std::to_string(user_id);
		trade_msg_single["message"] = Json::Value::null;
		trade_msg_single["message"]["type"] = MESSAGE_TYPE_TRADE;
		trade_msg_single["message"]["show"] = 1;
		trade_msg_single["message"]["title"] = tmp_warning_json["title"].asString();
		trade_msg_single["message"]["title_tw"] = tmp_warning_json["title_tw"].asString();
		trade_msg_single["message"]["title_en"] = tmp_warning_json["title_en"].asString();
		trade_msg_single["message"]["title_vi"] = tmp_warning_json["title_vi"].asString();
		trade_msg_single["message"]["currency"] = m_server_currency;
		trade_msg_single["message"]["created_at"] = m_time_now;
		m_trade_msg_array.append(trade_msg_single);

		Json::Value email_msg_single = Json::Value::null;
		email_msg_single["user_id"] = user_id;
		email_msg_single["send_type"] = 2;
		email_msg_single["title_cn"] = "强平风险提醒";
		email_msg_single["title_tw"] = "強平風險提醒";
		email_msg_single["title_en"] = "Forced liquidation risk reminder";
		email_msg_single["title_vi"] = "Cảnh báo rủi ro cưỡng chế đóng vị thế";
		email_msg_single["content_cn"] = "充送活动用户您好，您的" + m_server_currency + "交易区账户即将达到充送用户强平线，请您及时入金或关注行情波动，控制交易风险！";
		email_msg_single["content_tw"] = "充送活動用戶您好，您的" + m_server_currency + "交易區賬戶即將達到充送用戶強平線，請您及時入金或關註行情波動，控制交易風險！";
		email_msg_single["content_en"] = "Dear 100% deposit match customer, your " + m_server_currency + " trading market account has reached forced liquidation level, please add margin and control your risks!";
		email_msg_single["content_vi"] = "Xin chào khách hàng tham gia hoạt động nạp bao nhiêu tặng bấy nhiêu, tài khoản khu giao dịch " + m_server_currency + " của bạn đạt đến mức độ cưỡng chế đóng vị thế của khách hàng nạp bao nhiêu tặng bấy nhiêu, vui lòng thanh toán kịp thời hoặc chú ý đến biến động thị trường và kiểm soát rủi ro giao dịch!";
		email_msg_single["short_message_cn"] = "强平风险提醒：充送活动用户您好，您的" + m_server_currency + "交易区账户即将达到充送用户强平线，请您及时入金或关注行情波动，控制交易风险！";
		email_msg_single["short_message_tw"] = "強平風險提醒：充送活動用戶您好，您的" + m_server_currency + "交易區賬戶即將達到充送用戶強平線，請您及時入金或關註行情波動，控制交易風險！";
		email_msg_single["short_message_en"] = "Your " + m_server_currency + " market trading will reach liquidation level soon.";
		email_msg_single["short_message_vi"] = "TK khu giao dịch " + m_server_currency + " của bạn đã tới mức cưỡng chế đóng.";
		m_email_msg_array.append(email_msg_single);
	}

	if (stop_user_id != 0){
		//long double balance = switch_s_to_f(m_accounts[to_string(stop_user_id)]["USDT"]["balance"].asString());
		//long double margin = switch_s_to_f(m_accounts[to_string(stop_user_id)]["USDT"]["margin"].asString());
		//long double frozen_margin = switch_s_to_f(m_accounts[to_string(stop_user_id)]["USDT"]["frozen_margin"].asString());
		//long double profit = switch_s_to_f(m_accounts[to_string(stop_user_id)]["USDT"]["profit"].asString());
		//if (frozen_margin > EPS){
			//TODO 取消订单
			for (int i = 0; i < (int)m_accounts[std::to_string(stop_user_id)]["orders"].size(); i++){
				std::string tmp_order_id = m_accounts[std::to_string(stop_user_id)]["orders"][i].asString();
				Json::Value tmp_msg_json = Json::Value::null;
				tmp_msg_json["msg_type"] = "cancel";
				tmp_msg_json["order_id"] = tmp_order_id;
				tmp_msg_json["user_id"] = stop_user_id;
				std::string tmp_msg_str = Json::writeString(writer, tmp_msg_json);
				m_pending_msg_list.push_back(tmp_msg_str);
			}
		//}
		m_accounts[std::to_string(stop_user_id)]["stop_timestamp"] = m_time_now;

		std::string result = Json::writeString(writer, m_accounts[std::to_string(stop_user_id)]);
		if (redisFormatCommand(&redis_cmd, "SET account_user_%d %s", stop_user_id, result.c_str()) <= 0){
			LOG(ERROR) << "redis format error";
			return false;
		}
		std::string redis_cmd_str = redis_cmd;
		free(redis_cmd);
		m_redis_cmd_list.push_back(redis_cmd_str);

		std::vector<Json::Value> pending_msg_json_list;
		pending_msg_json_list.clear();
		Json::Value::Members position_contract_keys = m_positions[std::to_string(stop_user_id)].getMemberNames();
		for (auto iter1 = position_contract_keys.begin(); iter1 != position_contract_keys.end(); iter1++) {
			std::string contract_id_str = *iter1;
			int contract_id = atoi(contract_id_str.c_str());
			Json::Value::Members position_lever_keys = m_positions[std::to_string(stop_user_id)][contract_id_str].getMemberNames();
			for (auto iter2 = position_lever_keys.begin(); iter2 != position_lever_keys.end(); iter2++) {
				std::string lever_rate = *iter2;
				long double buy_amount = strtold(m_positions[std::to_string(stop_user_id)][contract_id_str][lever_rate]["buy_amount"].asCString(), NULL);
				long double sell_amount = strtold(m_positions[std::to_string(stop_user_id)][contract_id_str][lever_rate]["sell_amount"].asCString(), NULL);
				Json::Value tmp_msg_json = Json::Value::null;
				tmp_msg_json["msg_type"] = "order";
				tmp_msg_json["user_id"] = stop_user_id;
				tmp_msg_json["contract_id"] = contract_id;
				tmp_msg_json["lever_rate"] = lever_rate;
				tmp_msg_json["order_type"] = ORDER_TYPE_MARKET;
				tmp_msg_json["system_type"] = ORDER_SYSTEM_TYPE_STOP;
				tmp_msg_json["is_bbo"] = 0;
				tmp_msg_json["ip"] = "";
				tmp_msg_json["source"] = 0;
				if (buy_amount > 0){
					m_new_order_id = GetListId();
					if (m_new_order_id == EmptyString){
						return false;
					}
					tmp_msg_json["order_id"] = m_new_order_id;
					tmp_msg_json["order_op"] = ORDER_SIDE_CLOSE_LONG;
					tmp_msg_json["origin_amount"] = switch_f_to_s(buy_amount);
					std::string tmp_msg_str = Json::writeString(writer, tmp_msg_json);
					m_pending_msg_list.push_back(tmp_msg_str);
				}
				if (sell_amount > 0){
					m_new_order_id = GetListId();
					if (m_new_order_id == EmptyString){
						return false;
					}
					tmp_msg_json["order_id"] = m_new_order_id;
					tmp_msg_json["order_op"] = ORDER_SIDE_CLOSE_SHORT;
					tmp_msg_json["origin_amount"] = switch_f_to_s(sell_amount);
					std::string tmp_msg_str = Json::writeString(writer, tmp_msg_json);
					m_pending_msg_list.push_back(tmp_msg_str);
				}
			}
		}
		if (stop_msg_state == 1){
			Json::Value tmp_warning_json = Json::Value::null;
			tmp_warning_json["type"] = "trade_msg";
			tmp_warning_json["user_id"] = stop_user_id;
			tmp_warning_json["msg_type"] = MESSAGE_TYPE_TRADE;
			if (stop_type == 1){
				tmp_warning_json["title"] = "您的【" + m_server_currency + "交易区】账户风险率已达到" + m_stop_percent + "%，为减少亏损，已对您的" + m_server_currency + "交易区账户进行强制平仓。请关注行情波动，控制交易风险！";
				tmp_warning_json["title_tw"] = "您的【" + m_server_currency + "交易區】賬戶風險率已達到" + m_stop_percent + "%，為減少虧損，已對您的" + m_server_currency + "交易區賬戶進行強制平倉。請關注行情波動，控制交易風險！";
				tmp_warning_json["title_en"] = "Your【" + m_server_currency + " market】account risk rating has reached " + m_stop_percent + "%. Your positions will be liquidated to control losses. Please be aware of market volatility and control your risks!";
				tmp_warning_json["title_vi"] = "Tỷ lệ rủi ro tài khoản【Khu giao dịch " + m_server_currency + "】của bạn đã đạt mức " + m_stop_percent + "%, nên đã tiến hành cưỡng chế đóng vị thế khu giao dịch " + m_server_currency + " tài khoản bạn, tài. Hãy chú ý đến biến động thị trường và kiểm soát rủi ro giao dịch!";
			}else{
				tmp_warning_json["title"] = "【强平提醒】充送活动用户您好，您的" + m_server_currency + "交易区账户已达到充送用户强平线，为减少亏损，已对您的" + m_server_currency + "交易区账户进行强制平仓。请关注行情波动，控制交易风险！";
				tmp_warning_json["title_tw"] = "【強平提醒】充送活動用戶您好，您的" + m_server_currency + "交易區賬戶已達到充送用户強平線，為減少虧損，已對您的" + m_server_currency + "交易區賬戶進行強制平倉。請關注行情波動，控制交易風險！";
				tmp_warning_json["title_en"] = "【Forced liquidation】Dear 100% deposit match customer, your " + m_server_currency + " trading market account has reached forced liquidation level, and positions will be liquidated to control your losses. Please be aware of market volatility and control your risks!";
				tmp_warning_json["title_vi"] = "【Cảnh báo cưỡng chế đóng vị thế】Xin chào khách hàng tham gia hoạt động nạp bao nhiêu tặng bấy nhiêu , tài khoản khu giao dịch " + m_server_currency + " của bạn đạt đến mức độ cưỡng chế đóng vị thế của khách hàng nạp bao nhiêu tặng bấy nhiêu, để giảm tổn thất, nên đã tiến hành cưỡng chế đóng vị thế khu giao dịch " + m_server_currency + " tài khoản bạn. Hãy chú ý đến biến động thị trường và kiểm soát rủi ro giao dịch!";
			}
			tmp_warning_json["currency"] = m_server_currency;
			m_order_result_list.append(tmp_warning_json);

			Json::Value trade_msg_single = Json::Value::null;
			trade_msg_single["user_id"] = std::to_string(stop_user_id);
			trade_msg_single["message"] = Json::Value::null;
			trade_msg_single["message"]["type"] = MESSAGE_TYPE_TRADE;
			trade_msg_single["message"]["show"] = 1;
			trade_msg_single["message"]["title"] = tmp_warning_json["title"].asString();
			trade_msg_single["message"]["title_tw"] = tmp_warning_json["title_tw"].asString();
			trade_msg_single["message"]["title_en"] = tmp_warning_json["title_en"].asString();
			trade_msg_single["message"]["title_vi"] = tmp_warning_json["title_vi"].asString();
			trade_msg_single["message"]["currency"] = m_server_currency;
			trade_msg_single["message"]["created_at"] = m_time_now;
			m_trade_msg_array.append(trade_msg_single);

			Json::Value email_msg_single = Json::Value::null;
			email_msg_single["user_id"] = stop_user_id;
			if (stop_type == 1){
				email_msg_single["title_cn"] = "合约交易风险预警";
				email_msg_single["title_tw"] = "合約交易風險預警";
				email_msg_single["title_en"] = "Perpetual Contracts Risk warning";
				email_msg_single["title_vi"] = "Dự cảnh báo rủi do Hợp Đồng giao dịch";
				email_msg_single["content_cn"] = "尊敬的用户，您好！您的【" + m_server_currency + "交易区】账户风险率已达到" + m_stop_percent + "%，为减少亏损，已对您的账户进行强制平仓，请登录您的合约交易账户查看强制平仓的结果！";
				email_msg_single["content_tw"] = "尊敬的用戶，您好！您的【" + m_server_currency + "交易區】賬戶風險率已達到" + m_stop_percent + "%，為減少虧損，已對您的賬戶進行強制平倉，請登錄您的合約交易賬戶查看強制平倉的結果！";
				email_msg_single["content_en"] = "Dear User, your【" + m_server_currency + " market】account risk rating has reached " + m_stop_percent + "%. Your positions will be liquidated to control losses. Please login to your Perpetual Contracts account to see results of the forced liquidation process!";
				email_msg_single["content_vi"] = "Kính gửi người dùng, Tỷ lệ rủi ro tài khoản【Khu giao dịch " + m_server_currency + "】của bạn đã đạt mức " + m_stop_percent + "%, nên đã tiến hành cưỡng chế đóng vị thế khu giao dịch " + m_server_currency + " tài khoản bạn, tài. Xin mời bạn đăng nhập tài khoản Hợp Đồng giao dịch để kiểm tra kết quả giao dịch đã thành công!";
			}else{
				email_msg_single["send_type"] = 2;
				email_msg_single["title_cn"] = "强平提醒";
				email_msg_single["title_tw"] = "強平提醒";
				email_msg_single["title_en"] = "Forced liquidation";
				email_msg_single["title_vi"] = "Dự cảnh báo rủi do Hợp Đồng giao dịch";
				email_msg_single["content_cn"] = "充送活动用户您好，您的" + m_server_currency + "交易区账户已达到充送用户强平线，为减少亏损，已对您的" + m_server_currency + "交易区账户进行强制平仓。请关注行情波动，控制交易风险！";
				email_msg_single["content_tw"] = "充送活動用戶您好，您的" + m_server_currency + "交易區賬戶已達到充送用戶強平線，為減少虧損，已對您的" + m_server_currency + "交易區賬戶進行強制平倉。請關註行情波動，控制交易風險！";
				email_msg_single["content_en"] = "Dear 100% deposit match customer, your " + m_server_currency + " trading market account has reached forced liquidation level, and positions will be liquidated to control your losses. Please be aware of market volatility and control your risks!";
				email_msg_single["content_vi"] = "Xin chào khách hàng tham gia hoạt động nạp bao nhiêu tặng bấy nhiêu , tài khoản khu giao dịch " + m_server_currency + " của bạn đạt đến mức độ cưỡng chế đóng vị thế của khách hàng nạp bao nhiêu tặng bấy nhiêu, để giảm tổn thất, nên đã tiến hành cưỡng chế đóng vị thế khu giao dịch " + m_server_currency + " tài khoản bạn. Hãy chú ý đến biến động thị trường và kiểm soát rủi ro giao dịch!";
				email_msg_single["short_message_cn"] = "强平提醒：您的" + m_server_currency + "交易区账户已达到充送用户强平线，为减少亏损，已对您的账户进行强制平仓。请关注行情波动，控制交易风险！";
				email_msg_single["short_message_tw"] = "強平提醒：您的" + m_server_currency + "交易區賬戶已達到充送用戶強平線，為減少虧損，已對您的賬戶進行強制平倉。請關註行情波動，控制交易風險！";
				email_msg_single["short_message_en"] = "Your " + m_server_currency + " market trading will reach liquidation level soon.";
				email_msg_single["short_message_vi"] = "TK khu giao dịch " + m_server_currency + " của bạn đã tới mức cưỡng chế đóng.";
			}
			m_email_msg_array.append(email_msg_single);
		}
		reply = (redisReply*)redisCommand(m_stat_redis, "SADD stop_user_set %d", stop_user_id);
		if (reply == NULL) {
			LOG(ERROR) << "redis reply null";
			redisFree(m_stat_redis);
			m_stat_redis = NULL;
			exit(1);
		}
		for (int i = 0; i < (int)m_pending_msg_list.size(); i++){
			LOG(INFO) << "m_order_id:" << m_order_id << " i: " << i << " m_pending_msg_list: " << m_pending_msg_list[i];
		}
	}else if (profit_loss_user_id != 0){
		//平单
		for (auto iter = profit_loss_cancel_list.begin(); iter != profit_loss_cancel_list.end(); iter++) {
			m_pending_msg_list.push_back(*iter);
		}
		std::string tmp_msg_str = Json::writeString(writer, profit_loss_order_json);
		m_pending_msg_list.push_back(tmp_msg_str);
		LOG(INFO) << "m_order_id:" << m_order_id << " profit_loss_order_json: " << tmp_msg_str;

		int order_op = profit_loss_order_json["order_op"].asInt();
		std::string order_op_str;
		std::string order_op_tw_str;
		std::string order_op_en_str;
		std::string order_op_vi_str;
		if (order_op == ORDER_SIDE_CLOSE_LONG){
			order_op_str = "多单";
			order_op_tw_str = "多單";
			order_op_en_str = "long";
			order_op_vi_str = "long";
		}else{
			order_op_str = "空单";
			order_op_tw_str = "空單";
			order_op_en_str = "short";
			order_op_vi_str = "short";
		}

		Json::Value tmp_warning_json = Json::Value::null;
		tmp_warning_json["type"] = "trade_msg";
		tmp_warning_json["user_id"] = profit_loss_user_id;
		tmp_warning_json["msg_type"] = MESSAGE_TYPE_TRADE;
		char tmp_st[10240];
		snprintf(tmp_st, sizeof(tmp_st), "%.2Lf", switch_s_to_f(profit_loss_order_json["price"].asString()));
		std::string profit_loss_price = tmp_st;
		snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】%s已按%s价【%s】委托挂单，如果市场未能撮合成交，请您先手动撤单，再自主挂单平仓。请关注行情波动，控制交易风险!", m_server_lang_cn.c_str(), profit_loss_contract_name.c_str(), order_op_str.c_str(), profit_loss_side.c_str(), profit_loss_price.c_str());
		tmp_warning_json["title"] = tmp_st;
		snprintf(tmp_st, sizeof(tmp_st), "您的【%s%s】%s已按%s價【%s】委托掛單，如果市場未能撮合成交，請您先手動撤單，再自主掛單平倉。請關註行情波動，控制交易風險!", m_server_lang_tw.c_str(), profit_loss_contract_name.c_str(), order_op_tw_str.c_str(), profit_loss_side_tw.c_str(), profit_loss_price.c_str());
		tmp_warning_json["title_tw"] = tmp_st;
		snprintf(tmp_st, sizeof(tmp_st), "%s is now set at 【%s】 for your 【%s%s】%s position. If the order remains unmatched in the market, you can manually withdraw and re-submit at another price. Please be aware of market fluctuations and control your risks!", profit_loss_side_en.c_str(), profit_loss_price.c_str(), m_server_lang_en.c_str(), profit_loss_contract_name.c_str(), order_op_en_str.c_str());
		tmp_warning_json["title_en"] = tmp_st;
		snprintf(tmp_st, sizeof(tmp_st), "Lệnh %s【%s%s】của bạn đã đặt lệnh limit theo giá cắt %s【%s】, nếu như thị trường chưa thể tổng hợp giao dịch, bạn hãy rút lệnh bằng cách thủ công, rồi tự chủ đặt lệnh limit đóng vị thế. Xin chú ý đến dao động giá thị trường để kiểm soát rủi ro giao dịch!", order_op_vi_str.c_str(), m_server_lang_vi.c_str(), profit_loss_contract_name.c_str(), profit_loss_side_vi.c_str(), profit_loss_price.c_str());
		tmp_warning_json["title_vi"] = tmp_st;
		tmp_warning_json["currency"] = m_server_currency;
		m_order_result_list.append(tmp_warning_json);

		Json::Value trade_msg_single = Json::Value::null;
		trade_msg_single["user_id"] = std::to_string(profit_loss_user_id);
		trade_msg_single["message"] = Json::Value::null;
		trade_msg_single["message"]["type"] = MESSAGE_TYPE_TRADE;
		trade_msg_single["message"]["show"] = 0;
		trade_msg_single["message"]["title"] = tmp_warning_json["title"].asString();
		trade_msg_single["message"]["title_tw"] = tmp_warning_json["title_tw"].asString();
		trade_msg_single["message"]["title_en"] = tmp_warning_json["title_en"].asString();
		trade_msg_single["message"]["title_vi"] = tmp_warning_json["title_vi"].asString();
		trade_msg_single["message"]["currency"] = m_server_currency;
		trade_msg_single["message"]["created_at"] = m_time_now;
		m_trade_msg_array.append(trade_msg_single);

		Json::Value email_msg_single = Json::Value::null;
		email_msg_single["user_id"] = profit_loss_user_id;
		if (profit_loss_side == "止盈"){
			email_msg_single["title_cn"] = "合约交易止盈提示";
			email_msg_single["title_tw"] = "合約交易止盈提示";
			email_msg_single["title_en"] = "Perpetual Contracts Limit Order prompt";
			email_msg_single["title_vi"] = "Nhắc nhở cắt lãi Hợp Đồng giao dịch";
		}else{
			email_msg_single["title_cn"] = "合约交易止损提示";
			email_msg_single["title_tw"] = "合約交易止損提示";
			email_msg_single["title_en"] = "Perpetual Contracts Stop Loss prompt";
			email_msg_single["title_vi"] = "Nhắc nhở cắt lỗ Hợp Đồng giao dịch";
		}
		snprintf(tmp_st, sizeof(tmp_st), "尊敬的用户，您好！您的【%s%s】%s已按%s价【%s】委托挂单，如果市场未能撮合成交，请您先手动撤单，再自主挂单平仓。请关注行情波动，控制交易风险!", m_server_lang_cn.c_str(), profit_loss_contract_name.c_str(), order_op_str.c_str(), profit_loss_side.c_str(), profit_loss_price.c_str());
		email_msg_single["content_cn"] = tmp_st;
		snprintf(tmp_st, sizeof(tmp_st), "尊敬的用戶，您好！您的【%s%s】%s已按%s價【%s】委托掛單，如果市場未能撮合成交，請您先手動撤單，再自主掛單平倉。請關註行情波動，控制交易風險!", m_server_lang_tw.c_str(), profit_loss_contract_name.c_str(), order_op_tw_str.c_str(), profit_loss_side_tw.c_str(), profit_loss_price.c_str());
		email_msg_single["content_tw"] = tmp_st;
		snprintf(tmp_st, sizeof(tmp_st), "Dear User, %s is now set at 【%s】 for your 【%s%s】%s position. If the order remains unmatched in the market, you can manually withdraw and re-submit at another price. Please be aware of market fluctuations and control your risks!", profit_loss_side_en.c_str(), profit_loss_price.c_str(), m_server_lang_en.c_str(), profit_loss_contract_name.c_str(), order_op_en_str.c_str());
		email_msg_single["content_en"] = tmp_st;
		snprintf(tmp_st, sizeof(tmp_st), "Kính gửi người dùng, lệnh %s【%s%s】của bạn đã đặt lệnh limit theo giá cắt %s【%s】, nếu như thị trường chưa thể tổng hợp giao dịch, bạn hãy rút lệnh bằng cách thủ công, rồi tự chủ đặt lệnh limit đóng vị thế. Xin chú ý đến dao động giá thị trường để kiểm soát rủi ro giao dịch!", order_op_vi_str.c_str(), m_server_lang_vi.c_str(), profit_loss_contract_name.c_str(), profit_loss_side_vi.c_str(), profit_loss_price.c_str());
		email_msg_single["content_vi"] = tmp_st;
		m_email_msg_array.append(email_msg_single);
	}

	std::string result = Json::writeString(writer, m_order_result_list);
	std::string redis_cmd_str;
	if (redisFormatCommand(&redis_cmd, "LPUSH order_result_list %s", result.c_str()) <= 0){
		LOG(ERROR) << "redis format error";
		return false;
	}
	redis_cmd_str = redis_cmd;
	free(redis_cmd);
	m_redis_cmd_list.push_back(redis_cmd_str);

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

	if (m_trade_msg_array.size() > 0){
		result = Json::writeString(writer, m_trade_msg_array);
		LOG(INFO) << " trade msg: " << result;
		m_trade->SendTradeMessage(result);
	}

	if (m_email_msg_array.size() > 0){
		result = Json::writeString(writer, m_email_msg_array);
		LOG(INFO) << " email msg: " << result;
		m_trade->SendEmailMessage(result);
	}

	LOG(INFO) << "m_order_id:" << m_order_id << " SettleProfitAndLossLimit end";
	return true;
}

bool Match::NewOrder(){
	if (!PrepareForOrder()) return false;
	
	if (!MatchOrder()) return false;
	
	if (!QueryUserAccount()) return false;
	
	if (!SettleTrade()) return false;

	if (!Frozen()) return false;
	
	if (!InsertOrder()) return false;
	
	if (!WriteRedis()) return false;

	if (!GetContractLastPrice()) return false;

	if (!SettleFundFee()) return false;

	if (!SettleProfitAndLossLimit()) return false;
	
	return true;
}

bool Match::CancelOrder(){
	LOG(INFO) << "m_order_id:" << m_order_id << " cancel start";
	
	m_trade_users_set.insert(m_order_user_id);
	std::set<int> users_lever_rate;
	users_lever_rate.insert(m_lever_rate);
	m_trade_users_lever_rate.insert(std::map<int, std::set<int>>::value_type(m_order_user_id, users_lever_rate));
	
	if (!QueryUserAccount()) return false;
	
	if (!DeleteOrder()) return false;
	
	if (!WriteRedis()) return false;

	//if (!GetContractLastPrice()) return false;

	//if (!SettleProfitAndLossLimit()) return false;
	
	LOG(INFO) << "m_order_id:" << m_order_id << " cancel end";
	return true;
}

bool Match::SetProfitLoss(){
	LOG(INFO) << "m_order_user_id:" << m_order_user_id << " m_order_contract_id:" << m_order_contract_id << " SetProfitLoss start";
	
	GetContractLastPrice();
	
	Json::StreamWriterBuilder writer;
	writer["indentation"] = "";

	m_trade_users_set.insert(m_order_user_id);
	std::set<int> users_lever_rate;
	users_lever_rate.insert(m_lever_rate);
	m_trade_users_lever_rate.insert(std::map<int, std::set<int>>::value_type(m_order_user_id, users_lever_rate));
	if (!QueryUserAccount()) return false;

	if (!m_user_position.isMember(m_order_user_id_str) || !m_user_position[m_order_user_id_str].isMember(m_order_contract_id_str) || !m_user_position[m_order_user_id_str][m_order_contract_id_str].isMember(m_lever_rate_str)){
		LOG(ERROR) << "m_user_position no user or no contract user_id:" << m_order_user_id_str << " contract_id:" << m_order_contract_id_str;
		return false;
	}
	if (!m_contract_config.isMember(m_order_contract_id_str)){
		LOG(ERROR) << "m_contract_config no contract_id:" << m_order_contract_id_str;
		return false;
	}
	if (m_order_op == ORDER_SIDE_OPEN_LONG){
		m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_profit_limit"] = m_order_profit_limit;
		m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_lose_limit"] = m_order_lose_limit;
	}else{
		m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_profit_limit"] = m_order_profit_limit;
		m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_lose_limit"] = m_order_lose_limit;
	}

	char *redis_cmd;
	std::string result = Json::writeString(writer, m_user_position[m_order_user_id_str]);
	if (redisFormatCommand(&redis_cmd, "SET position_user_%d %s", m_order_user_id, result.c_str()) <= 0){
		LOG(ERROR) << "redis format error";
		return false;
	}
	std::string redis_cmd_str = redis_cmd;
	free(redis_cmd);
	m_redis_cmd_list.push_back(redis_cmd_str);

	Json::Value tmp_position_obj = Json::Value::null;
	tmp_position_obj["type"] = "position";
	tmp_position_obj["user_id"] = m_order_user_id;
	tmp_position_obj["contract_id"] = m_order_contract_id;
	tmp_position_obj["contract_name"] = m_contract_config[m_order_contract_id_str]["contract_name"].asString();
	tmp_position_obj["asset_symbol"] = m_contract_config[m_order_contract_id_str]["asset_symbol"].asString();
	tmp_position_obj["unit_amount"] = m_contract_config[m_order_contract_id_str]["unit_amount"].asString();
	tmp_position_obj["lever_rate"] = m_lever_rate;
	tmp_position_obj["settle_asset"] = m_settle_asset;
	tmp_position_obj["buy_frozen"] = m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_frozen"].asString();
	tmp_position_obj["buy_margin"] = m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_margin"].asString();
	tmp_position_obj["buy_amount"] = m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_amount"].asString();
	tmp_position_obj["buy_available"] = m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_available"].asString();
	tmp_position_obj["buy_quote_amount"] = m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_quote_amount"].asString();
	tmp_position_obj["buy_quote_amount_settle"] = m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_quote_amount_settle"].asString();
	tmp_position_obj["buy_take_profit"] = m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_profit_limit"].asString();
	tmp_position_obj["buy_stop_loss"] = m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["buy_lose_limit"].asString();
	tmp_position_obj["sell_frozen"] = m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_frozen"].asString();
	tmp_position_obj["sell_margin"] = m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_margin"].asString();
	tmp_position_obj["sell_amount"] = m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_amount"].asString();
	tmp_position_obj["sell_available"] = m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_available"].asString();
	tmp_position_obj["sell_quote_amount"] = m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_quote_amount"].asString();
	tmp_position_obj["sell_quote_amount_settle"] = m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_quote_amount_settle"].asString();
	tmp_position_obj["sell_take_profit"] = m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_profit_limit"].asString();
	tmp_position_obj["sell_stop_loss"] = m_user_position[m_order_user_id_str][m_order_contract_id_str][m_lever_rate_str]["sell_lose_limit"].asString();
	tmp_position_obj["create_at"] = m_time_now;
	m_order_result_list.append(tmp_position_obj);
	m_statistics["position"].append(tmp_position_obj);


	result = Json::writeString(writer, m_order_result_list);
	if (redisFormatCommand(&redis_cmd, "LPUSH order_result_list %s", result.c_str()) <= 0){
		LOG(ERROR) << "redis format error";
		return false;
	}
	redis_cmd_str = redis_cmd;
	free(redis_cmd);
	m_redis_cmd_list.push_back(redis_cmd_str);
	
	result = Json::writeString(writer, m_statistics);
	m_trade->SendStatisticsMessage(result);
	
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

	Json::Value::Members keys = m_user_position.getMemberNames();
	for (auto iter = keys.begin(); iter != keys.end(); iter++) {
		if (m_user_position[*iter].size() > 0) {
			m_positions[*iter] = m_user_position[*iter];
		} else {
			m_positions.removeMember(*iter);
		}
	}
	
	LOG(INFO) << "m_order_user_id:" << m_order_user_id << " m_order_contract_id:" << m_order_contract_id << " SetProfitLoss end";
	return true;
}

bool Match::Msg(std::string order_str){
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

		m_redis = SentinelRedisConnect(sentinels, redis_config["master_name"].asCString(), password.c_str(), redis_config["database"].asInt());
		if (m_redis == NULL) {
			LOG(ERROR) << "m_redis connect faild";
        	exit(1);
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

		m_stat_redis = SentinelRedisConnect(sentinels, redis_config["master_name"].asCString(), password.c_str(), redis_config["database"].asInt());
		if (m_stat_redis == NULL) {
			LOG(ERROR) << "m_stat_redis connect faild";
        	exit(1);
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

		m_index_redis = SentinelRedisConnect(sentinels, redis_config["master_name"].asCString(), password.c_str(), redis_config["database"].asInt());
		if (m_index_redis == NULL) {
			LOG(ERROR) << "m_index_redis connect faild";
        	exit(1);
		}
	}

	Reposition();

	Json::CharReaderBuilder rbuilder;
	std::unique_ptr<Json::CharReader> const reader(rbuilder.newCharReader());
	JSONCPP_STRING error;

	Json::Value json = Json::Value::null;
	bool ret = reader->parse(order_str.c_str(), order_str.c_str() + order_str.size(), &json, &error);
	if (!(ret && error.size() == 0)) {
		LOG(ERROR) << "json error";
		return false;
	}

	if (!json.isMember("msg_type")){
		LOG(ERROR) << "json no msg_type";
		return false;
	}
	m_msg_type = json["msg_type"].asString();

	if (m_msg_type == "transfer" || m_msg_type == "human") {
		if (!Account(json)) return false;
		return true;
	}

	if (!InitOrder(json)) return false;
	bool res;
	if (m_msg_type == "order"){
		res = NewOrder();
	}else if (m_msg_type == "cancel"){
		res = CancelOrder();
	}else if (m_msg_type == "profit_loss"){
		res = SetProfitLoss();
	}else{
		res = false;
	}
	return res;
}

bool Match::Account(Json::Value& account_json) {
	LOG(INFO) << "Account start";
	
	if (m_msg_type != "transfer" && m_msg_type != "human") {
		LOG(ERROR) << " msg type error";
		return false;
	}

	std::string txno = account_json["id"].asString();
	std::string user_id_str = account_json["user_id"].asString();
	int user_id = atoi(user_id_str.c_str());
	std::string asset = account_json["asset"].asString();
	long double amount = switch_s_to_f(account_json["amount"].asString());
	std::string remark = account_json["remark"].asString();
	std::string remark_tw = "";
	if (account_json.isMember("remark_tw")){
		remark_tw = account_json["remark_tw"].asString();
	}
	std::string remark_en = "";
	if (account_json.isMember("remark_en")){
		remark_en = account_json["remark_en"].asString();
	}
	std::string remark_vi = "";
	if (account_json.isMember("remark_vi")){
		remark_vi = account_json["remark_vi"].asString();
	}
	int jnl_type = 0;
	if (account_json.isMember("jnl_type")){
		jnl_type = account_json["jnl_type"].asInt();
	} else if (m_msg_type == "human") {
		if (amount > 0){
			jnl_type = USER_ACCOUNT_JNL_HUMAN_ADD;
		} else if (amount < 0){
			jnl_type = USER_ACCOUNT_JNL_HUMAN_REDUCE;
		}
	}

	if ((jnl_type == USER_ACCOUNT_JNL_COIN_CONTRACT && amount < EPS) || (jnl_type == USER_ACCOUNT_JNL_CONTRACT_COIN && amount > EPS)) {
		LOG(ERROR) << "amount is error user_id:" << user_id << " jnl:" << jnl_type << " amount:" << amount;
		return false;
	}

	if (!m_accounts.isMember(user_id_str)){
		if (jnl_type == USER_ACCOUNT_JNL_CONTRACT_COIN || jnl_type == USER_ACCOUNT_JNL_HUMAN_REDUCE) {
			LOG(ERROR) << "account is null user_id:" << user_id;
			return false;
		}
	}
	Json::Value user_account = m_accounts[user_id_str];
	if (!user_account.isMember(asset)){
		user_account[asset]["balance"] = switch_f_to_s(0);
		user_account[asset]["margin"] = switch_f_to_s(0);
		user_account[asset]["frozen_margin"] = switch_f_to_s(0);
		user_account[asset]["profit"] = switch_f_to_s(0);
		user_account[asset]["encode_balance"] = HmacSha256Encode(user_id_str + asset + user_account[asset]["balance"].asString());
		user_account[asset]["encode_margin"] = HmacSha256Encode(user_id_str + asset + user_account[asset]["margin"].asString());
		user_account[asset]["encode_frozen_margin"] = HmacSha256Encode(user_id_str + asset + user_account[asset]["frozen_margin"].asString());
		user_account[asset]["encode_profit"] = HmacSha256Encode(user_id_str + asset + user_account[asset]["profit"].asString());
	}

	redisReply* reply = NULL;
	redisAppendCommand(m_redis, "HGETALL last_trade_price");
	redisGetReply(m_redis, (void**)&reply);
	Json::Value contract_price = Json::Value::null;
	if (reply == NULL) {
		LOG(ERROR) << " HGETALL last_trade_price redis reply null";
		redisFree(m_redis);
		m_redis = NULL;
		return false;
	}
	if (reply->type != REDIS_REPLY_ARRAY){
		LOG(ERROR) << " HGETALL last_trade_price redis type error:" << reply->type;
		freeReplyObject(reply);
		return false;
	}
	for (unsigned i = 0; i < reply->elements; i = i + 2) {
		char* contract_id = reply->element[i]->str;
		char* price = reply->element[i+1]->str;
		contract_price[contract_id] = price;
	}
	freeReplyObject(reply);
	
	std::string balance = user_account[asset]["balance"].asString();
	std::string encode_balance = user_account[asset]["encode_balance"].asString();
	std::string margin = user_account[asset]["margin"].asString();
	std::string encode_margin = user_account[asset]["encode_margin"].asString();
	std::string frozen_margin = user_account[asset]["frozen_margin"].asString();
	std::string encode_frozen_margin = user_account[asset]["encode_frozen_margin"].asString();
	std::string profit = user_account[asset]["profit"].asString();
	std::string encode_profit = user_account[asset]["encode_profit"].asString();
	m_available_margin = strtold(balance.c_str(), NULL) + strtold(profit.c_str(), NULL) - strtold(margin.c_str(), NULL) - strtold(frozen_margin.c_str(), NULL);

	long double total_future_profit = 0;
	if (asset == m_settle_asset && m_positions.isMember(user_id_str)) {
		Json::Value user_position = m_positions[user_id_str];
		Json::Value::Members keys = user_position.getMemberNames();
		for (auto iter = keys.begin(); iter != keys.end(); iter++) {
			Json::Value::Members ks = user_position[*iter].getMemberNames();
			for (auto it = ks.begin(); it != ks.end(); it++) {
				long double buy_amount = strtold(user_position[*iter][*it]["buy_amount"].asCString(), NULL);
				long double buy_quote_amount_settle = strtold(user_position[*iter][*it]["buy_quote_amount_settle"].asCString(), NULL);
				long double sell_amount = strtold(user_position[*iter][*it]["sell_amount"].asCString(), NULL);
				long double sell_quote_amount_settle = strtold(user_position[*iter][*it]["sell_quote_amount_settle"].asCString(), NULL);
				long double buy_profit = 0;
				long double sell_profit = 0;
				if (buy_amount > EPS) {
					long double price = strtold(contract_price[*iter].asCString(), NULL);
					buy_profit = buy_amount * price * m_rate - buy_quote_amount_settle; 
				}
				if (sell_amount > EPS) { 
					long double price = strtold(contract_price[*iter].asCString(), NULL);
					sell_profit = sell_quote_amount_settle - sell_amount * price * m_rate; 
				}
				m_available_margin = m_available_margin + buy_profit + sell_profit;
				total_future_profit = total_future_profit + buy_profit + sell_profit;
			}	
		}
	}

	if (jnl_type == USER_ACCOUNT_JNL_CONTRACT_COIN || jnl_type == USER_ACCOUNT_JNL_HUMAN_REDUCE){
		long double available_out = m_available_margin;
		if (switch_s_to_f(profit) > 0){
			available_out = available_out - switch_s_to_f(profit);
		}else{
			available_out = available_out;
		}
		if (total_future_profit > 0){
			available_out = available_out - total_future_profit;
		}else{
			available_out = available_out;
		}
		if (available_out + amount < -EPS){
			LOG(INFO) << "amount is too much! user_id:" << user_id << " jnl:" << jnl_type << " amount:" << amount << " m_available_margin:" << m_available_margin << " profit:" << profit << " total_future_profit:" << total_future_profit << " available_out:" << available_out;
			if (available_out > EPS){
				amount = - available_out;
			}else{
				amount = 0;
			}
		}
	}

	if (switch_s_to_f(balance) + amount < -EPS){
		LOG(ERROR) << "new balance < 0 user_id:" << user_id << " jnl:" << jnl_type << " amount:" << amount << " balance:" << balance;
		return false;
	}

	Json::Value tmp_obj = Json::Value::null;
	tmp_obj["type"] = "account";
	tmp_obj["id"] = txno;
	tmp_obj["user_id"] = user_id;
	tmp_obj["asset"] = asset;
	tmp_obj["contract"] = "";
	tmp_obj["jnl_type"] = jnl_type;
	tmp_obj["remark"] = remark;
	tmp_obj["remark_tw"] = remark_tw;
	tmp_obj["remark_en"] = remark_en;
	tmp_obj["remark_vi"] = remark_vi;
	tmp_obj["update_at"] = m_time_now;
	tmp_obj["change_balance"] = switch_f_to_s(amount);
	tmp_obj["new_balance"] = switch_f_to_s(switch_s_to_f(user_account[asset]["balance"].asString()) + switch_s_to_f(tmp_obj["change_balance"].asString()));
	tmp_obj["change_margin"] = switch_f_to_s(0);
	tmp_obj["new_margin"] = switch_f_to_s(switch_s_to_f(user_account[asset]["margin"].asString()) + switch_s_to_f(tmp_obj["change_margin"].asString()));
	tmp_obj["change_frozen_margin"] = switch_f_to_s(0);
	tmp_obj["new_frozen_margin"] = switch_f_to_s(switch_s_to_f(user_account[asset]["frozen_margin"].asString()) + switch_s_to_f(tmp_obj["change_frozen_margin"].asString()));
	tmp_obj["change_profit"] = switch_f_to_s(0);
	tmp_obj["new_profit"] = switch_f_to_s(switch_s_to_f(user_account[asset]["profit"].asString()) + switch_s_to_f(tmp_obj["change_profit"].asString()));
	tmp_obj["new_encode_balance"] = user_account[asset]["encode_balance"];
	tmp_obj["new_encode_margin"] = user_account[asset]["encode_margin"];
	tmp_obj["new_encode_frozen_margin"] = user_account[asset]["encode_frozen_margin"];
	tmp_obj["new_encode_profit"] = user_account[asset]["encode_profit"];
	Json::Value result_list = Json::Value::null;
	result_list.append(tmp_obj);

	user_account[asset]["balance"] = tmp_obj["new_balance"];
	user_account[asset]["encode_balance"] = HmacSha256Encode(tmp_obj["user_id"].asString() + asset + tmp_obj["new_balance"].asString());
	
	if (m_msg_type == "human") {
		tmp_obj = Json::Value::null;
		tmp_obj["type"] = "human";
		tmp_obj["id"] = txno;
		tmp_obj["jnl_type"] = jnl_type;
		tmp_obj["amount"] = switch_f_to_s(amount);
		result_list.append(tmp_obj);
	}

	LOG(INFO) << "txno:" << txno << " WriteRedis start";

	std::vector<std::string> cmd_list;
	Json::StreamWriterBuilder writer;
	writer["indentation"] = "";
	char *redis_cmd;

	std::string result;
	result = Json::writeString(writer, user_account);
	if (redisFormatCommand(&redis_cmd, "SET account_user_%d %s", user_id, result.c_str()) <= 0){
		LOG(ERROR) << "redis format error";
		return false;
	}
	cmd_list.push_back(redis_cmd);
	free(redis_cmd);
	
	result = Json::writeString(writer, result_list);
	if (redisFormatCommand(&redis_cmd, "LPUSH order_result_list %s", result.c_str()) <= 0){
		LOG(ERROR) << "redis format error";
		return false;
	}
	cmd_list.push_back(redis_cmd);
	free(redis_cmd);

	if (redisFormatCommand(&redis_cmd, "SADD account_set %d", user_id) <= 0){
		LOG(ERROR) << "redis format error";
		return false;
	}
	cmd_list.push_back(redis_cmd);
	free(redis_cmd);

	for (int i = 0; i < (int)cmd_list.size(); i++){
		LOG(INFO) << "redis cmd list " << cmd_list[i];
	}

	redisAppendCommand(m_redis, "MULTI");
	for (int i = 0; i < (int)cmd_list.size(); i++){
		redisAppendFormattedCommand(m_redis, cmd_list[i].c_str(), cmd_list[i].size());
	}
	redisAppendCommand(m_redis, "EXEC");
	redisReply* temp_reply = NULL;
	for (int i = 0; i < (int)cmd_list.size() + 2; i++){
		redisGetReply(m_redis, (void**)&temp_reply);
		if (temp_reply == NULL) {
			LOG(ERROR) << "m_redis reply null";
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

	LOG(INFO) << "txno:" << txno << " WriteRedis end";

	m_accounts[user_id_str] = user_account;

	LOG(INFO) << "Account end";
	return true;
}