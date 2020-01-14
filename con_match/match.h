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

class Trade;

#define		ORDER_SIDE_OPEN_LONG			1
#define		ORDER_SIDE_OPEN_SHORT			2
#define     ORDER_SIDE_CLOSE_LONG			3
#define     ORDER_SIDE_CLOSE_SHORT			4

#define		ORDER_TYPE_LIMIT				1
#define		ORDER_TYPE_MARKET				2

#define		ORDER_STATUS_NEW				1
#define		ORDER_STATUS_FILLED				2
#define		ORDER_STATUS_PARTIALLY			3
#define		ORDER_STATUS_CANCELED			4
#define		ORDER_STATUS_PARTIALLY_CANCELED	5

#define		TRADE_PAIR_STATE_ON				1

#define		MESSAGE_TYPE_TRADE				3

#define		ORDER_SYSTEM_TYPE_NORMAL		1
#define		ORDER_SYSTEM_TYPE_STOP			2
#define		ORDER_SYSTEM_TYPE_PROFIT		3
#define		ORDER_SYSTEM_TYPE_LOSS			4

#define		ORDER_PRICE_LIMIT_SWITCH		1
#define		ORDER_PRICE_LIMIT_PERCENT		0.02

#define   ORDER_MARKET_COIN		1
#define   ORDER_MARKET_USA		2
#define   ORDER_MARKET_HK			3

#define BTC_RATE 0.0001L	// 比特币固定汇率
#define ETH_RATE 0.001L		// 以太坊固定汇率

std::string switch_f_to_s(long double f);
long double switch_s_to_f(std::string s);
bool CheckDecimal(long double f, int decimal);
bool JudgeOneUtcDay(time_t t1, time_t t2);

const std::string EmptyString = "";

class Match {
	public:
		Match(Trade* trade);
		~Match();
		bool Msg(std::string order_str);
		std::vector<std::string> m_pending_msg_list;
	private:
		bool DiffPrice(long double p1, long double p2);		
		bool Init();
		bool Reposition();
		bool InitOrder(Json::Value& order_json);
		bool PrepareForOrder();
		std::string GetListId();
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
		bool SetProfitLoss();

		bool Account(Json::Value& account_json);

		bool GetContractLastPrice();
		bool SettleFundFee();
		bool SettleProfitAndLossLimit();
		
		Trade* m_trade;										//trade
		redisContext* m_redis;								//redis连接
		redisContext* m_stat_redis;							//statRedis连接
		redisContext* m_index_redis;						//indexRedis连接

		std::map<int, int> m_makers;					  //做市用户contract_id:user_id
		std::set<int> maker_user_set;							//做市用户的集合
		int		m_match_stop_interval;						//强平的时间间隔
		long double m_order_price_limit_high = 0.05L;		//下单价格比指数允许高的百分比
		long double m_order_price_limit_low = -0.05L;		//下单价格与指数允许低的百分比
		long double m_send_stop_relative_rate = 0.4L;		//赠金平仓相对百分比
		long double m_send_stop_warning_rate = 0.8L;		//赠金平仓相对百分比
		long double m_send_stop_absolute_rate = 1.1L;		//赠金平仓绝对百分比
		
		std::string m_settle_asset;												// 结算币种
		std::string m_server_currency;											// 交易区币种
		std::string m_server_lang_cn;											// 交易区中文文案
		std::string m_server_lang_tw;											// 交易区繁体文案
		std::string m_server_lang_en;											// 交易区英文文案
		std::string m_server_lang_vi;											// 交易区越南语文案
		long double m_rate;														// 汇率
		Json::Value m_positions = Json::Value::null;		//所有用户的当前持仓
		Json::Value m_accounts = Json::Value::null;			//所有用户的当前账户
		std::vector<std::string> m_redis_cmd_list;					//事务redis写操作数组
		Json::Value m_trade_list;							//交易对象数组
		Json::Value m_order_result_list;					//订单处理结果对象数组
		Json::Value m_statistics;							//统计对象，包括orderbook和成交量
		std::set<int> m_trade_users_set;							//参与交易的用户集合
		std::vector<int> m_trade_users_list;						//参与交易的用户队列
		std::map<int, std::set<int>> m_trade_users_lever_rate;		//参与交易的用户的持仓
		Json::Value m_user_account = Json::Value::null;		//参与交易的用户的当前余额
		Json::Value m_user_position = Json::Value::null;	//参与交易的用户的当前持仓
		std::string m_cur_trade_id;								//当前成交的编号
		Json::Value m_contract_price = Json::Value::null;	//最新成交价

    std::vector<int> m_contract_id_arr;                      //合约id的数组
		Json::Value m_contract_config = Json::Value::null;	//所有合约详情，连带价格 {"1":{"contract_name":"BTC001", "asset_symbol":"BTC", "price":"7000"}}
		int			m_contract_config_timestamp = 0;		//获取合约详情的时间戳
		long double m_warning_rate_1 = 1.0;                 //预警风险率1
		long double m_warning_rate_2 = 0.7;                 //预警风险率2
    long double m_stop_rate = 0.5;                      //强平风险率
		std::string m_warning_percent_1 = "100";					//预警百分比1
		std::string m_warning_percent_2 = "70";					//预警百分比2
		std::string m_stop_percent = "50";						//强平百分比
		Json::Value m_fund_fee_rate_json = Json::Value::null;	//资金费率对象

		long double m_spot_price;							//现货指数

		std::string m_new_order_id;								//新建平仓单的id

		Json::Value m_trade_msg_array;						//trade消息数组
		Json::Value m_email_msg_array;						//email消息数组
		
		//for order/cancel order
		int			m_time_now;									//当前时间戳
		std::string 		m_msg_type;									//订单
		std::string		m_order_id;									//订单号id
		std::string		m_order_user_id_str;						//订单用户id
		int			m_order_user_id;							//订单用户id
		std::string		m_order_contract_id_str;					//订单合约id
		int			m_order_contract_id;						//订单合约id
		std::string		m_order_base_asset;							//交易货币
		int			m_order_type;								//类型(1-限价 2-市价 默认是1)
		int			m_order_isbbo;								//类型(0-否 1-是)
		int			m_order_op;									//类型(1开多，2开空，3平多，4平空)
		int			m_order_system_type;						//类型(1普通订单，2强平，3止盈，4止损)
		std::string		m_order_price_str;							//订单价格
		long double	m_order_price;								//订单价格
		std::string		m_order_amt_str;							//订单数量
		long double	m_order_amt;								//订单数量
		std::string		m_order_ip;									//订单ip
		int			m_order_source;								//订单来源
		std::string		m_order_profit_limit;						//订单止盈
		std::string		m_order_lose_limit;							//订单止损
		int 		m_lever_rate;								//杠杆倍数
		std::string  m_lever_rate_str;
		
		std::string 		m_contract_name;							//合约代码
		long double m_unit_amount;								//交易单位
		int			m_price_decimal;							//当前交易对价格精度位数
		long double	m_max_order_amount;							//当前交易对最大下单量
		long double	m_max_hold_amount;							//当前交易对最大持仓量
		long double	m_maker_fee;								//maker手续费
		long double	m_taker_fee;								//taker手续费
		long double m_stop_amount;							//止盈止损系统接单上限
		long double m_max_stop_amount;					//止盈止损系统日内接单上限
		
		long double m_usd_cny_value;							//usd/cny汇率数值
		int 		m_usd_cny_timestamp;						//usd/cny汇率时间
		
		long double m_available_margin;							//可用保证金
		long double	m_remain_amount;							//当前订单剩余数量（未达成交易）
		long double	m_executed_quote_amount;					//当前订单成交额
		long double	m_executed_settle_amount;					//当前订单结算额
		long double	m_frozen_margin;							//当前订单冻结金额
};

#endif // MATCH_H_