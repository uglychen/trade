#include "account.h"
#include "trade.h"
#include "utils.h"
#include "redis_utils.h"
#include <memory>

string switch_f_to_s(long double f){
	char st[1024];
	snprintf(st, sizeof(st), "%.*Lf", VALUE_DECIMAL, f);
	string ans = st;
	return ans;
}

long double switch_s_to_f(string s){
	return strtold(s.c_str(), NULL);
}

Account::Account(Trade* trade) {
	m_trade = trade;
	Init();
}

Account::~Account() {
}


void Account::AccountLock(int user_id) {
	char tmp[1024];
	snprintf(tmp, sizeof(tmp), "lock_account_%d", user_id);
	string str = tmp;
	lock_key_all.insert(str);
	LOG(INFO) << " lock " << str;
	Lock(str);
	return;
}

void Account::AccountUnlock(int user_id) {
	char tmp[1024];
	snprintf(tmp, sizeof(tmp), "lock_account_%d", user_id);
	string str = tmp;
	Unlock(str);
	lock_key_all.erase(str);
	LOG(INFO) << " unlock " << str;
	return;
}

void Account::AllUnlock() {
	for (set<string>::iterator it = lock_key_all.begin(); it != lock_key_all.end(); it++){
		string str = *it;
		Unlock(str);
	}
}

bool Account::Init() {
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

		//m_redis = SentinelRedisConnect(sentinels, redis_config["master_name"].asCString(), password.c_str(), redis_config["database"].asInt());
        for (auto it = sentinels.begin(); it != sentinels.end(); ++it) {
            m_redis = redisConnect(it->first.c_str(), it->second) ;
        }

        if (m_redis == NULL) {
            LOG(ERROR) << "m_redis connect faild";
            exit(1);
        }else{
            //std::cout << "m_redis connect ok" << std::endl;
            LOG(INFO) << "m_redis connect ok";
        }
	}

	LOG(INFO) << "redis ok";

    return true;
}

bool Account::InitAccount(string account_str){
	LOG(INFO) << " InitAccount start";
	
	lock_key_all.clear();
	m_redis_cmd_list.clear();
	m_account_result_list.resize(0);
	m_user_account = Json::Value::null;
	m_time_now = time(0);
	
	Json::CharReaderBuilder rbuilder;
	std::unique_ptr<Json::CharReader> const reader(rbuilder.newCharReader());
	JSONCPP_STRING error;
	
	Json::Value account_json = Json::Value::null;
	bool ret = reader->parse(account_str.c_str(), account_str.c_str() + account_str.size(), &account_json, &error);
	if (!(ret && error.size() == 0)) {
		LOG(ERROR) << "json error";
		return false;
	}
	
	m_msg_type = account_json["msg_type"].asString();
	if (!(m_msg_type == "deposit" || m_msg_type == "apply_withdraw" || m_msg_type == "reject_withdraw" || m_msg_type == "cancel_withdraw" 
		|| m_msg_type == "withdraw" || m_msg_type == "human" || m_msg_type == "transfer")) {
		LOG(ERROR) << " msg type error";
		return false;
	}
	m_account_id = account_json["id"].asString();
	m_user_id = atoi(account_json["user_id"].asString().c_str());
	m_asset = account_json["asset"].asString();
	m_amount = switch_s_to_f(account_json["amount"].asString());
	m_remark = account_json["remark"].asString();
	if (m_msg_type == "human"){
		m_frozen_amount = switch_s_to_f(account_json["frozen_amount"].asString());
	}else{
		m_frozen_amount = 0L;
	}
	if (m_msg_type == "withdraw"){
		m_fee = switch_s_to_f(account_json["fee"].asString());
	}else{
		m_fee = 0L;
	}
	if (m_msg_type == "deposit"){
		m_reward_amount = switch_s_to_f(account_json["reward_amount"].asString());
	}else{
		m_reward_amount = 0L;
	}
	
	if (m_msg_type == "deposit"){
		m_jnl_type = USER_ACCOUNT_JNL_DEPOSIT;
	}else if (m_msg_type == "apply_withdraw"){
		m_jnl_type = USER_ACCOUNT_JNL_WITHDRAW_FROZEN;
	}else if (m_msg_type == "reject_withdraw" || m_msg_type == "cancel_withdraw"){
		m_jnl_type = USER_ACCOUNT_JNL_CANCEL_WITHDRAW;
	}else if (m_msg_type == "withdraw"){
		m_jnl_type = USER_ACCOUNT_JNL_WITHDRAW;
	}else{
		if (account_json.isMember("jnl_type")){
			m_jnl_type = account_json["jnl_type"].asInt();
		}else if (m_amount > 0){
			m_jnl_type = USER_ACCOUNT_JNL_HUMAN_ADD;
		}else if (m_amount < 0){
			m_jnl_type = USER_ACCOUNT_JNL_HUMAN_REDUCE;
		}else if (m_frozen_amount > 0){
			m_jnl_type = USER_ACCOUNT_JNL_HUMAN_ADD;
		}else{
			m_jnl_type = USER_ACCOUNT_JNL_HUMAN_REDUCE;
		}
	}
	
	if (m_jnl_type == USER_ACCOUNT_JNL_LOCK_BALANCE || m_jnl_type == USER_ACCOUNT_JNL_CANCEL_LOCK || m_jnl_type == USER_ACCOUNT_JNL_LOCK_BALANCE_END){
		m_op_id = account_json["op"]["id"].asInt();
	}else{
		m_op_id = 0;
	}
	
	LOG(INFO) << " InitAccount end";
	return true;
}

bool Account::QueryUserAccount(){
	LOG(INFO) << "m_account_id:" << m_account_id << " QueryUserAccount start";
	
	redisReply* reply;

	reply = (redisReply*) redisCommand(m_redis, "GET account_user_%d", m_user_id);
	if (reply == NULL){
		LOG(ERROR) << "m_account_id:" << m_account_id << " redis reply null";
		redisFree(m_redis);
		m_redis = NULL;
		return false;
	}
	if (reply->type != REDIS_REPLY_STRING){
		if (reply->type == REDIS_REPLY_NIL && (m_msg_type == "deposit" || m_msg_type == "human" || (m_msg_type == "transfer" && m_jnl_type == 33))){
			m_user_account[m_asset]["available"] = switch_f_to_s(0);
			m_user_account[m_asset]["frozen"] = switch_f_to_s(0);
			freeReplyObject(reply);
			LOG(INFO) << "m_account_id:" << m_account_id << " QueryUserAccount end";
			return true;
		}else{
			LOG(ERROR) << "m_account_id:" << m_account_id << " redis type error:" << reply->type;
			freeReplyObject(reply);
			return false;
		}
	}
	string user_account_str = reply->str;
	freeReplyObject(reply);
	
	Json::CharReaderBuilder rbuilder;
	std::unique_ptr<Json::CharReader> const reader(rbuilder.newCharReader());
	JSONCPP_STRING error;
	bool ret = reader->parse(user_account_str.c_str(), user_account_str.c_str() + user_account_str.size(), &m_user_account, &error);
	if (!(ret && error.size() == 0)) {
		LOG(ERROR) << "m_account_id:" << m_account_id << " json error";
		return false;
	}
	
	if (!m_user_account.isMember(m_asset)){
		if (m_msg_type == "deposit" || m_msg_type == "human" || (m_msg_type == "transfer" && m_jnl_type == 33)){
			m_user_account[m_asset]["available"] = switch_f_to_s(0);
			m_user_account[m_asset]["frozen"] = switch_f_to_s(0);
			LOG(INFO) << "m_account_id:" << m_account_id << " QueryUserAccount end";
			return true;
		}else{
			LOG(ERROR) << "m_account_id:" << m_account_id << " no this asset";
			return false;
		}
	}
	string available = m_user_account[m_asset]["available"].asString();
	string frozen = m_user_account[m_asset]["frozen"].asString();
	string encode_available = m_user_account[m_asset]["encode_available"].asString();
	string encode_frozen = m_user_account[m_asset]["encode_frozen"].asString();
	if (encode_available != HmacSha256Encode(to_string(m_user_id) + m_asset + available) || encode_frozen != HmacSha256Encode(to_string(m_user_id) + m_asset + frozen)){
		LOG(ERROR) << "m_account_id:" << m_account_id << " encode balance error";
		return false;
	}
	
	LOG(INFO) << "m_account_id:" << m_account_id << " QueryUserAccount end";
	return true;
}


bool Account::WriteRedis(){
	LOG(INFO) << "m_account_id:" << m_account_id << " WriteRedis start";
	
	Json::StreamWriterBuilder writer;
	writer["indentation"] = "";
	char *redis_cmd;
	
	string result;
	result = Json::writeString(writer, m_user_account);
	if (redisFormatCommand(&redis_cmd, "SET account_user_%d %s", m_user_id, result.c_str()) <= 0){
		LOG(ERROR) << "redis format error";
		return false;
	}
	string redis_cmd_str = redis_cmd;
	free(redis_cmd);
	m_redis_cmd_list.push_back(redis_cmd_str);
	
	result = Json::writeString(writer, m_account_result_list);
	if (redisFormatCommand(&redis_cmd, "LPUSH order_result_list %s", result.c_str()) <= 0){
		LOG(ERROR) << "redis format error";
		return false;
	}
	redis_cmd_str = redis_cmd;
	free(redis_cmd);
	m_redis_cmd_list.push_back(redis_cmd_str);
	
	for (int i = 0; i < (int)m_redis_cmd_list.size(); i++){
		LOG(INFO) << "redis cmd list " << m_redis_cmd_list[i];
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
	
	LOG(INFO) << "m_account_id:" << m_account_id << " WriteRedis end";
	return true;
}

bool Account::SettleAccount(){
	LOG(INFO) << "m_account_id:" << m_account_id << " SettleAccount start";
	
	AccountLock(m_user_id);
	if (!QueryUserAccount()) return false;
	
	long double change_available = 0L;
	long double new_available = 0L;
	long double change_frozen = 0L;
	long double new_frozen = 0L;
	
	Json::Value tmp_account_result = Json::Value::null;
	
	if (m_msg_type == "deposit"){
		change_available = m_amount;
		change_frozen = 0L;
	}else if (m_msg_type == "apply_withdraw"){
		change_available = -m_amount;
		change_frozen = m_amount;
	}else if (m_msg_type == "reject_withdraw"){
		change_available = m_amount;
		change_frozen = -m_amount;
	}else if (m_msg_type == "cancel_withdraw"){
		change_available = m_amount;
		change_frozen = -m_amount;
	}else if (m_msg_type == "withdraw"){
		change_available = 0L;
		change_frozen = -m_amount;
	}else if (m_msg_type == "human"){
		change_available = m_amount;
		change_frozen = m_frozen_amount;
	}else if (m_msg_type == "transfer") {
		change_available = m_amount;
		change_frozen = m_frozen_amount;
	} else {
		return false;
	}
	new_available = switch_s_to_f(m_user_account[m_asset]["available"].asString()) + change_available;
	new_frozen = switch_s_to_f(m_user_account[m_asset]["frozen"].asString()) + change_frozen;
	string new_encode_available = HmacSha256Encode(to_string(m_user_id) + m_asset + switch_f_to_s(new_available));
	string new_encode_frozen = HmacSha256Encode(to_string(m_user_id) + m_asset + switch_f_to_s(new_frozen));
	if (new_available + EPS < 0 || new_frozen + EPS < 0){
		LOG(ERROR) << "m_account_id:" << m_account_id << " new_available: " << new_available << " new_frozen: " << new_frozen << " ERROR";
		return false;
	}
	
	tmp_account_result["type"] = "account";
	tmp_account_result["id"] = m_account_id;
	tmp_account_result["user_id"] = m_user_id;
	tmp_account_result["update_at"] = m_time_now;
	tmp_account_result["asset"] = m_asset;
	tmp_account_result["change_available"] = switch_f_to_s(change_available);
	tmp_account_result["new_available"] = switch_f_to_s(new_available);
	tmp_account_result["change_frozen"] = switch_f_to_s(change_frozen);
	tmp_account_result["new_frozen"] = switch_f_to_s(new_frozen);
	tmp_account_result["jnl_type"] = m_jnl_type;
	tmp_account_result["remark"] = m_remark;
	tmp_account_result["new_encode_available"] = new_encode_available;
	tmp_account_result["new_encode_frozen"] = new_encode_frozen;
	m_account_result_list.append(tmp_account_result);
	
	if (m_msg_type == "apply_withdraw"){
		tmp_account_result = Json::Value::null;
		tmp_account_result["type"] = "apply_withdraw";
		tmp_account_result["id"] = m_account_id;
		tmp_account_result["amt"] = switch_f_to_s(change_frozen);
		m_account_result_list.append(tmp_account_result);
	}
	
	if (m_msg_type == "human"){
		tmp_account_result = Json::Value::null;
		tmp_account_result["type"] = "human";
		tmp_account_result["id"] = m_account_id;
		tmp_account_result["jnl_type"] = m_jnl_type;
		tmp_account_result["op_id"] = m_op_id;
		m_account_result_list.append(tmp_account_result);
	}
	
	if (m_msg_type == "deposit" && m_reward_amount > EPS){
		change_available = m_reward_amount;
		change_frozen = 0L;
		new_available += change_available;
		new_frozen += change_frozen;
		
		tmp_account_result["type"] = "account";
		tmp_account_result["id"] = m_account_id;
		tmp_account_result["user_id"] = m_user_id;
		tmp_account_result["update_at"] = m_time_now;
		tmp_account_result["asset"] = m_asset;
		tmp_account_result["change_available"] = switch_f_to_s(change_available);
		tmp_account_result["new_available"] = switch_f_to_s(new_available);
		tmp_account_result["change_frozen"] = switch_f_to_s(change_frozen);
		tmp_account_result["new_frozen"] = switch_f_to_s(new_frozen);
		tmp_account_result["jnl_type"] = USER_ACCOUNT_JNL_DEPOSIT_SEND;
		tmp_account_result["remark"] = "充值赠送";
		new_encode_available = HmacSha256Encode(to_string(m_user_id) + m_asset + switch_f_to_s(new_available));
		new_encode_frozen = HmacSha256Encode(to_string(m_user_id) + m_asset + switch_f_to_s(new_frozen));
		tmp_account_result["new_encode_available"] = new_encode_available;
		tmp_account_result["new_encode_frozen"] = new_encode_frozen;
		m_account_result_list.append(tmp_account_result);
	}
	
	m_user_account[m_asset]["available"] = switch_f_to_s(new_available);
	m_user_account[m_asset]["frozen"] = switch_f_to_s(new_frozen);
	m_user_account[m_asset]["encode_available"] = new_encode_available;
	m_user_account[m_asset]["encode_frozen"] = new_encode_frozen;
	
	if (!WriteRedis()) return false;
	AccountUnlock(m_user_id);
	
	LOG(INFO) << "m_account_id:" << m_account_id << " SettleAccount end";
	return true;
}

bool Account::Msg(string account_str){
	LOG(INFO) << account_str;
	
	if (m_redis) {
		redisReply* reply = (redisReply*) redisCommand(m_redis, "INFO");
		if (reply == NULL){
			LOG(ERROR) << "Connection error: " << m_redis->errstr;
			redisFree(m_redis);
			m_redis = NULL;
		}
	}
	
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
            m_redis = redisConnect(it->first.c_str(), it->second) ;
        }

		if (m_redis == NULL) {
			LOG(INFO) << "redis connect failed!";
			exit(1);
		}
	}

	if (!InitAccount(account_str)) return false;
	bool res = true;
	if (!SettleAccount()){
		res = false;
	}
	AllUnlock();
	return res;
}