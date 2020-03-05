#ifndef MATCH_H_
#define MATCH_H_
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


#define		ORDER_SIDE_BUY			1
#define		ORDER_SIDE_SELL			2

#define		ORDER_TYPE_LIMIT		1
#define		ORDER_TYPE_MARKET		2

#define		ORDER_STATUS_NEW		1
#define		ORDER_STATUS_FILLED		2
#define		ORDER_STATUS_PARTIALLY	3
#define		ORDER_STATUS_CANCELED	4

#define		TRADE_PAIR_STATE_ON		1
#define		ORDER_PRICE_LIMIT_SWITCH		1
#define		ORDER_PRICE_LIMIT_PERCENT		0.02

string switch_f_to_s(long double f);
long double switch_s_to_f(string s);
bool CheckDecimal(long double f, int decimal);

class Match {
	public:
		Match(Trade* trade);
		~Match();
		bool Msg(string order_str);
	private:
		void PreLock();
		void AccountLock(int user_id);
		void PreUnlock();
		void AccountUnlock(int user_id);
		void AllUnlock();
		set<string> lock_key_all;							//所有锁的集合
		
		bool DiffPrice(long double p1, long double p2);
		bool Init();
		bool Reposition();
		bool InitOrder(string order_str);
		bool PrepareForOrder();
		bool GetTradeId();
		bool MatchOrder();
		bool QueryUserAccount();
		bool Frozen();
		bool SettleTrade();
		bool InsertOrder();
		bool WriteRedis();
		
		bool GetOrderInfo();
		bool DeleteOrder();
		
		bool NewOrder();
		bool CancelOrder();
		
		Trade* m_trade;										//trade
		redisContext* m_redis;								//redis连接
		redisContext* m_stat_redis;							//statRedis连接
		redisContext* m_index_redis;						//indexRedis连接
		
		long double m_spot_price;							//价格指数
		long double m_order_price_limit_high = 0.02L;		//下单价格比指数允许高的百分比
		long double m_order_price_limit_low = -0.02L;		//下单价格与指数允许低的百分比

		vector<string> m_redis_cmd_list;					//事务redis写操作数组
		Json::Value m_trade_list;							//交易对象数组
		Json::Value m_order_result_list;					//订单处理结果对象数组
		Json::Value m_statistics;							//统计对象，包括orderbook和成交量
		set<int> m_trade_users_set;							//参与交易的用户集合
		vector<int> m_trade_users_list;						//参与交易的用户队列
		Json::Value m_user_account = Json::Value::null;		//参与交易的用户的当前余额
		string m_cur_trade_id;								//当前成交的编号
		
		//for order/cancel order
		int			m_time_now;									//当前时间戳
		string 		m_msg_type;									//订单
		string		m_order_id;									//订单号id
		int			m_order_user_id;							//订单用户id
		string		m_order_base_asset;							//交易货币
		string		m_order_quote_asset;						//定价货币
		int			m_order_type;								//类型(0-限价 1-市价 默认是0)
		int			m_order_op;									//类型(0-买 1-卖 默认是0)
		string		m_order_price_str;							//订单价格
		long double	m_order_price;								//订单价格
		string		m_order_amt_str;							//订单数量
		long double	m_order_amt;								//订单数量
		string		m_order_ip;									//订单ip
		int			m_order_source;								//订单来源

		int m_is_subscribe;	
		int m_unfreeze_status;
		string m_first_unfreeze_at;
		string m_second_unfreeze_at;
		
		long double	m_min_order_amount;							//当前交易对最小下单量
		long double	m_max_order_amount;							//当前交易对最大下单量
		int			m_amount_decimal;							//当前交易对数量精度位数
		int			m_price_decimal;							//当前交易对价格精度位数
		long double	m_maker_fee;								//maker手续费
		long double	m_taker_fee;								//taker手续费
		
		long double	m_price_base_asset_KK;						//base_asset/KK价格
		long double	m_price_quote_asset_KK;						//quote_asset/KK价格
		long double	m_price_base_asset_bitCNY;					//quote_asset/bitCNY价格
		long double m_usd_cny_value;							//usd/cny汇率数值
		int 		m_usd_cny_timestamp;						//usd/cny汇率时间
		
		long double	m_price_base_asset_GLA; ////base_asset/GLA价格
		long double	m_price_quote_asset_GLA;

		long double	m_remain_amount;							//当前订单剩余数量（未达成交易）
		long double	m_executed_quote_amount;					//当前订单成交额
};

#endif // MATCH_H_