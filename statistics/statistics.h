#ifndef STATISTICS_H_
#define STATISTICS_H_
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

#define		TICKER_INTERVAL_MIN			3
#define		TICKER_INTERVAL_MAX			30
#define		TRADE_LIST_MAX_LEN			100

#define		ORDER_SIDE_BUY			1
#define		ORDER_SIDE_SELL			2

string switch_f_to_s(long double f);
long double switch_s_to_f(string s);

class Statistics {
	public:
		Statistics(Trade* trade);
		~Statistics();
		bool			Msg(string order_str);
		bool 			SendAllTicker();
	private:
		bool 			Init();
		bool 			Reposition();
		bool 			InitStatistics(string statistics_str);
		bool 			UpdateOneKline(string sortset_key, int time_interval);
		bool 			StatisticsKline();
		bool 			UpdateOneOrder(string sortset_key, string opposite_key, Json::Value order_json, int order_op);
		bool 			StatisticsOrderbook();
		bool 			SendOneTicker(string pair_key);
		bool			StatisticsTrade();
		bool			WriteRedis();
		
		Trade* 			m_trade;				//trade
		redisContext* 	m_redis;				//redis连接
		redisContext* 	m_stat_redis;			//statRedis连接
		int				m_server_id;			//server id
		
		int				m_ticker_last_time;
		
		vector<string>	m_redis_cmd_list;		//事务redis写操作数组
		Json::Value		m_statistic_json;
		string			m_base_asset;
		string			m_quote_asset;
		int				m_time_trade;
		long double		m_first_price;
		long double		m_last_price;
		long double		m_high_price;
		long double		m_low_price;
		long double		m_amount;
		long double		m_quote_amount;
};

#endif // STATISTICS_H_