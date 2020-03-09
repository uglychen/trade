#ifndef ACCOUNT_H_
#define ACCOUNT_H_
#include "hiredis.h"
#include "json/json.h"
#include <string>
#include <vector>
#include "config.h"
#include "logging.h"
#include <set>
#include <ctime>
#include "lock.h"
class Trade;

using namespace std;

string switch_f_to_s(long double f);
long double switch_s_to_f(string s);

class Account {
	public:
		Account(Trade* trade);
		~Account();
		bool Msg(string order_str);
	private:
		void AccountLock(int user_id);
		void AccountUnlock(int user_id);
		void AllUnlock();
		
		bool Init();
		bool InitAccount(string account_str);
		bool QueryUserAccount();
		bool WriteRedis();
		bool SettleAccount();

		bool TradeQueryUserAccount();
		//bool TradeWriteRedis();
		bool TradeSettleAccount();
		
		Trade* m_trade;										//trade
		redisContext* m_redis;								//redis连接
		
		set<string> lock_key_all;							//所有锁的集合
		vector<string> m_redis_cmd_list;					//事务redis写操作数组
		Json::Value m_account_result_list;					//账务处理结果对象数组
		Json::Value	m_user_account;							//用户余额对象
		int		m_time_now;									//当前时间戳
		
		string		m_msg_type;								//订单
		string		m_account_id;
		int			m_user_id;
		string		m_asset;
		long double	m_amount;
		long double	m_frozen_amount;
		long double	m_fee;
		long double	m_reward_amount;
		int			m_jnl_type;
		int			m_op_id;
		string		m_remark;

		int m_is_trans;
		int m_trade_user_id;
		long double m_trade_amount;
		Json::Value	m_trade_user_account;
		Json::Value m_trade_account_result_list;							
		vector<string> m_trade_redis_cmd_list;

		Json::Value account_json;
};

#endif // ACCOUNT_H_