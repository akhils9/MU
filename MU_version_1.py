import sys
import os
import tables
from datetime import datetime, timedelta
from calendar import monthrange
import time
import pandas as pd
import numpy as np
import tables
import pytz

exchange = 'CME'

expiry_month = 'U9'
current_symbol = 'ZT' + expiry_month
big_trade_size = 250

date_list = ['2019-08-14', '2019-08-16', '2019-08-22','2019-08-23']
for d in date_list:

    date = d

    qrm_data_all = pd.DataFrame(columns=['captureTime', 'captureTimeStr', 'messageTime', 'messageTimeStr',
                                         'packetSeqNum', 'messageId', 'entryId', 'eventId', 'rptSeq',
                                         'CME:ZN' + expiry_month + '_bidPrice_1', 'CME:ZN' + expiry_month + '_bidOrders_1',
                                         'CME:ZN' + expiry_month + '_bidQty_1',
                                         'CME:ZN' + expiry_month + '_tradeType', 'CME:ZN' + expiry_month + '_tradeAggSide',
                                         'CME:ZN' + expiry_month + '_tradePrice',
                                         'CME:ZN' + expiry_month + '_tradeQty', 'CME:ZN' + expiry_month + '_tradeNumOrders',
                                         'CME:ZN' + expiry_month + '_tradeOrderId',
                                         'CME:ZN' + expiry_month + '_askPrice_1', 'CME:ZN' + expiry_month + '_askOrders_1',
                                         'CME:ZN' + expiry_month + '_askQty_1',
                                         'CME:ZT' + expiry_month + '_bidPrice_1', 'CME:ZT' + expiry_month + '_bidOrders_1',
                                         'CME:ZT' + expiry_month + '_bidQty_1',
                                         'CME:ZT' + expiry_month + '_tradeType', 'CME:ZT' + expiry_month + '_tradeAggSide',
                                         'CME:ZT' + expiry_month + '_tradePrice',
                                         'CME:ZT' + expiry_month + '_tradeQty', 'CME:ZT' + expiry_month + '_tradeNumOrders',
                                         'CME:ZT' + expiry_month + '_tradeOrderId',
                                         'CME:ZT' + expiry_month + '_askPrice_1', 'CME:ZT' + expiry_month + '_askOrders_1',
                                         'CME:ZT' + expiry_month + '_askQty_1',
                                         'CME:ZF' + expiry_month + '_bidPrice_1', 'CME:ZF' + expiry_month + '_bidOrders_1',
                                         'CME:ZF' + expiry_month + '_bidQty_1',
                                         'CME:ZF' + expiry_month + '_tradeType', 'CME:ZF' + expiry_month + '_tradeAggSide',
                                         'CME:ZF' + expiry_month + '_tradePrice',
                                         'CME:ZF' + expiry_month + '_tradeQty', 'CME:ZF' + expiry_month + '_tradeNumOrders',
                                         'CME:ZF' + expiry_month + '_tradeOrderId',
                                         'CME:ZF' + expiry_month + '_askPrice_1', 'CME:ZF' + expiry_month + '_askOrders_1',
                                         'CME:ZF' + expiry_month + '_askQty_1',
                                         'CME:ZB' + expiry_month + '_bidPrice_1', 'CME:ZB' + expiry_month + '_bidOrders_1',
                                         'CME:ZB' + expiry_month + '_bidQty_1',
                                         'CME:ZB' + expiry_month + '_tradeType', 'CME:ZB' + expiry_month + '_tradeAggSide',
                                         'CME:ZB' + expiry_month + '_tradePrice',
                                         'CME:ZB' + expiry_month + '_tradeQty', 'CME:ZB' + expiry_month + '_tradeNumOrders',
                                         'CME:ZB' + expiry_month + '_tradeOrderId',
                                         'CME:ZB' + expiry_month + '_askPrice_1', 'CME:ZB' + expiry_month + '_askOrders_1',
                                         'CME:ZB' + expiry_month + '_askQty_1',
                                         'CME:UB' + expiry_month + '_bidPrice_1', 'CME:UB' + expiry_month + '_bidOrders_1',
                                         'CME:UB' + expiry_month + '_bidQty_1',
                                         'CME:UB' + expiry_month + '_tradeType', 'CME:UB' + expiry_month + '_tradeAggSide',
                                         'CME:UB' + expiry_month + '_tradePrice',
                                         'CME:UB' + expiry_month + '_tradeQty', 'CME:UB' + expiry_month + '_tradeNumOrders',
                                         'CME:UB' + expiry_month + '_tradeOrderId',
                                         'CME:UB' + expiry_month + '_askPrice_1', 'CME:UB' + expiry_month + '_askOrders_1',
                                         'CME:UB' + expiry_month + '_askQty_1'])

    qrm_symbols = ['CME:ZN' + expiry_month, 'CME:ZT' + expiry_month, 'CME:ZF' + expiry_month, 'CME:ZB' + expiry_month,
                   'CME:UB' + expiry_month]

    qrm_file_name = 'qrm_data_' + date + '_all.pkl.gz'
    qrm_data = pd.read_pickle(qrm_file_name, compression='gzip')

    qrm_data['captureTimeStr'] = pd.to_datetime(qrm_data['captureTime'], unit='ns')
    qrm_data['messageTimeStr'] = pd.to_datetime(qrm_data['messageTime'], unit='ns')
    qrm_data['captureTimeStr'] = qrm_data['captureTimeStr'].dt.tz_localize('GMT').dt.tz_convert('US/Central')
    qrm_data['messageTimeStr'] = qrm_data['messageTimeStr'].dt.tz_localize('GMT').dt.tz_convert('US/Central')


    col = qrm_data.columns[qrm_data.columns.str.contains(current_symbol + '_trade')]
    col = ['captureTimeStr', 'messageTimeStr', 'eventId', 'packetSeqNum'] + col.tolist()

    qrm_zn_big = qrm_data[col]
    qrm_zn_big = qrm_zn_big[(qrm_zn_big['CME:' + current_symbol + '_tradeQty'] >= big_trade_size) & (
            qrm_zn_big['CME:' + current_symbol + '_tradeType'] == 6) & (
                                    qrm_zn_big['CME:' + current_symbol + '_tradeAggSide'] != 0)]
    qrm_zn_big = qrm_zn_big.drop(
        columns=['CME:' + current_symbol + '_tradeType', 'CME:' + current_symbol + '_tradeOrderId'])

    all_df = pd.DataFrame(columns=['captureTimeStr', 'messageTimeStr', 'eventId', 'packetSeqNum',
                                   'CME:' + current_symbol + '_tradeAggSide', 'CME:' + current_symbol + '_tradePrice',
                                   'CME:' + current_symbol + '_tradeQty',
                                   'CME:' + current_symbol + '_tradeNumOrders', 'captureTimeStr', 'messageTimeStr',
                                   'eventId', 'packetSeqNum', 'CME:ZNU9_tradeAggSide',
                                   'CME:ZNU9_tradePrice', 'CME:ZNU9_tradeQty', 'CME:ZNU9_tradeNumOrders',
                                   'CME:ZTU9_tradeAggSide', 'CME:ZTU9_tradePrice', 'CME:ZTU9_tradeQty',
                                   'CME:ZTU9_tradeNumOrders', 'CME:ZFU9_tradeAggSide',
                                   'CME:ZFU9_tradePrice', 'CME:ZFU9_tradeQty', 'CME:ZFU9_tradeNumOrders',
                                   'CME:ZBU9_tradeAggSide', 'CME:ZBU9_tradePrice', 'CME:ZBU9_tradeQty',
                                   'CME:ZBU9_tradeNumOrders', 'CME:UBU9_tradeAggSide',
                                   'CME:UBU9_tradePrice', 'CME:UBU9_tradeQty', 'CME:UBU9_tradeNumOrders',
                                   'Events_delta', 'time_delta_in_mics', 'count', 'WR_ZN_1Sec',
                                   'WR_ZN_5Sec', 'WR_ZN_30Sec', 'WR_ZN_60Sec', 'WR_ZN_300Sec',
                                   'WR_ZT_1Sec', 'WR_ZT_5Sec', 'WR_ZT_30Sec', 'WR_ZT_60Sec',
                                   'WR_ZT_300Sec', 'WR_ZF_1Sec', 'WR_ZF_5Sec', 'WR_ZF_30Sec',
                                   'WR_ZF_60Sec', 'WR_ZF_300Sec', 'WR_ZB_1Sec', 'WR_ZB_5Sec',
                                   'WR_ZB_30Sec', 'WR_ZB_60Sec', 'WR_ZB_300Sec', 'WR_UB_1Sec',
                                   'WR_UB_5Sec', 'WR_UB_30Sec', 'WR_UB_60Sec', 'WR_UB_300Sec',
                                   'ZN_1_Sec_MU', 'ZN_5_Sec_MU', 'ZN_30_Sec_MU', 'ZN_60_Sec_MU',
                                   'ZN_300_Sec_MU', 'ZT_1_Sec_MU', 'ZT_5_Sec_MU', 'ZT_30_Sec_MU',
                                   'ZT_60_Sec_MU', 'ZT_300_Sec_MU', 'ZF_1_Sec_MU', 'ZF_5_Sec_MU',
                                   'ZF_30_Sec_MU', 'ZF_60_Sec_MU', 'ZF_300_Sec_MU', 'ZB_1_Sec_MU',
                                   'ZB_5_Sec_MU', 'ZB_30_Sec_MU', 'ZB_60_Sec_MU', 'ZB_300_Sec_MU',
                                   'UB_1_Sec_MU', 'UB_5_Sec_MU', 'UB_30_Sec_MU', 'UB_60_Sec_MU',
                                   'UB_300_Sec_MU', 'Total_1_Sec_MU', 'Total_5_Sec_MU', 'Total_30_Sec_MU',
                                   'Total_60_Sec_MU', 'Total_300_Sec_MU'])

    for num_zn_big in range(len(qrm_zn_big)):
        # getting trades that immediately follow big trade

        transactTime = qrm_zn_big['messageTimeStr'].iloc[num_zn_big]
        capture_transactTime = qrm_zn_big['captureTimeStr'].iloc[num_zn_big]
        trade_event = qrm_zn_big['eventId'].iloc[num_zn_big]

        time_delta_trade_after = 1000

        col = qrm_data.columns[qrm_data.columns.str.contains('trade')]  # get all columns pertaining to trade
        col = ['captureTimeStr', 'messageTimeStr', 'eventId', 'packetSeqNum'] + col.tolist()

        trades_after_df = qrm_data[col]
        trades_after_df = trades_after_df[(trades_after_df['messageTimeStr'] > capture_transactTime) & (
                    trades_after_df['messageTimeStr'] <= (
                        capture_transactTime + timedelta(microseconds=time_delta_trade_after)))]
        trades_after_df = trades_after_df.reset_index(drop=True)

        trade_type = qrm_data.columns[qrm_data.columns.str.contains('tradeType')]

        trades_after_df = trades_after_df[(trades_after_df[trade_type[0]] == 6) | (trades_after_df[trade_type[1]] == 6) | (
                    trades_after_df[trade_type[2]] == 6) | (trades_after_df[trade_type[3]] == 6) | (
                                                      trades_after_df[trade_type[4]] == 6)]

        trades_after_df['Events_delta'] = 0
        # trades_after_df['Events_delta_of_considering_only_concerned_ORs'] = 0
        trades_after_df['time_delta_in_mics'] = 0

        mics = (trades_after_df['messageTimeStr'] - capture_transactTime).dt.microseconds
        nanos = (trades_after_df['messageTimeStr'] - capture_transactTime).dt.nanoseconds
        trades_after_df['time_delta_in_mics'] = mics + nanos / 1000
        trades_after_df['Events_delta'] = trades_after_df['eventId'] - trade_event

        # trades_after_df.columns = trades_after_df.columns + '_trades_after_' + str(time_delta_trade_after) + 'µs'

        # tradeAggSide = trades_after_df.columns[trades_after_df.columns.str.contains('tradeAggSide')]
        tradeOrderId = trades_after_df.columns[trades_after_df.columns.str.contains('_tradeOrderId')]

        trade_col = trade_type.tolist() + tradeOrderId.tolist()

        trades_after_df = trades_after_df.drop(columns=trade_col)

        all_df_temp = trades_after_df[
            (trades_after_df['time_delta_in_mics'] <= 5.5) & (trades_after_df['time_delta_in_mics'] >= 3.5)]
        all_df_temp = all_df_temp.reset_index(drop=True)

        if (len(all_df_temp) > 0):
            all_df_temp['count'] = num_zn_big

            MU = pd.DataFrame(columns=['captureTimeStr_MU_1Sec', 'messageTimeStr_MU_1Sec',
                                       'CME:ZNU9_bidPrice_1_MU_1Sec', 'CME:ZNU9_bidOrders_1_MU_1Sec',
                                       'CME:ZNU9_bidQty_1_MU_1Sec', 'CME:ZNU9_askPrice_1_MU_1Sec',
                                       'CME:ZNU9_askOrders_1_MU_1Sec', 'CME:ZNU9_askQty_1_MU_1Sec',
                                       'captureTimeStr_MU_5Sec', 'messageTimeStr_MU_5Sec',
                                       'CME:ZNU9_bidPrice_1_MU_5Sec', 'CME:ZNU9_bidOrders_1_MU_5Sec',
                                       'CME:ZNU9_bidQty_1_MU_5Sec', 'CME:ZNU9_askPrice_1_MU_5Sec',
                                       'CME:ZNU9_askOrders_1_MU_5Sec', 'CME:ZNU9_askQty_1_MU_5Sec',
                                       'captureTimeStr_MU_30Sec', 'messageTimeStr_MU_30Sec',
                                       'CME:ZNU9_bidPrice_1_MU_30Sec', 'CME:ZNU9_bidOrders_1_MU_30Sec',
                                       'CME:ZNU9_bidQty_1_MU_30Sec', 'CME:ZNU9_askPrice_1_MU_30Sec',
                                       'CME:ZNU9_askOrders_1_MU_30Sec', 'CME:ZNU9_askQty_1_MU_30Sec',
                                       'captureTimeStr_MU_60Sec', 'messageTimeStr_MU_60Sec',
                                       'CME:ZNU9_bidPrice_1_MU_60Sec', 'CME:ZNU9_bidOrders_1_MU_60Sec',
                                       'CME:ZNU9_bidQty_1_MU_60Sec', 'CME:ZNU9_askPrice_1_MU_60Sec',
                                       'CME:ZNU9_askOrders_1_MU_60Sec', 'CME:ZNU9_askQty_1_MU_60Sec',
                                       'captureTimeStr_MU_300Sec', 'messageTimeStr_MU_300Sec',
                                       'CME:ZNU9_bidPrice_1_MU_300Sec', 'CME:ZNU9_bidOrders_1_MU_300Sec',
                                       'CME:ZNU9_bidQty_1_MU_300Sec', 'CME:ZNU9_askPrice_1_MU_300Sec',
                                       'CME:ZNU9_askOrders_1_MU_300Sec', 'CME:ZNU9_askQty_1_MU_300Sec',
                                       'captureTimeStr_MU_1Sec', 'messageTimeStr_MU_1Sec',
                                       'CME:ZTU9_bidPrice_1_MU_1Sec', 'CME:ZTU9_bidOrders_1_MU_1Sec',
                                       'CME:ZTU9_bidQty_1_MU_1Sec', 'CME:ZTU9_askPrice_1_MU_1Sec',
                                       'CME:ZTU9_askOrders_1_MU_1Sec', 'CME:ZTU9_askQty_1_MU_1Sec',
                                       'captureTimeStr_MU_5Sec', 'messageTimeStr_MU_5Sec',
                                       'CME:ZTU9_bidPrice_1_MU_5Sec', 'CME:ZTU9_bidOrders_1_MU_5Sec',
                                       'CME:ZTU9_bidQty_1_MU_5Sec', 'CME:ZTU9_askPrice_1_MU_5Sec',
                                       'CME:ZTU9_askOrders_1_MU_5Sec', 'CME:ZTU9_askQty_1_MU_5Sec',
                                       'captureTimeStr_MU_30Sec', 'messageTimeStr_MU_30Sec',
                                       'CME:ZTU9_bidPrice_1_MU_30Sec', 'CME:ZTU9_bidOrders_1_MU_30Sec',
                                       'CME:ZTU9_bidQty_1_MU_30Sec', 'CME:ZTU9_askPrice_1_MU_30Sec',
                                       'CME:ZTU9_askOrders_1_MU_30Sec', 'CME:ZTU9_askQty_1_MU_30Sec',
                                       'captureTimeStr_MU_60Sec', 'messageTimeStr_MU_60Sec',
                                       'CME:ZTU9_bidPrice_1_MU_60Sec', 'CME:ZTU9_bidOrders_1_MU_60Sec',
                                       'CME:ZTU9_bidQty_1_MU_60Sec', 'CME:ZTU9_askPrice_1_MU_60Sec',
                                       'CME:ZTU9_askOrders_1_MU_60Sec', 'CME:ZTU9_askQty_1_MU_60Sec',
                                       'captureTimeStr_MU_300Sec',
                                       'messageTimeStr_MU_300Sec', 'CME:ZTU9_bidPrice_1_MU_300Sec',
                                       'CME:ZTU9_bidOrders_1_MU_300Sec', 'CME:ZTU9_bidQty_1_MU_300Sec',
                                       'CME:ZTU9_askPrice_1_MU_300Sec', 'CME:ZTU9_askOrders_1_MU_300Sec',
                                       'CME:ZTU9_askQty_1_MU_300Sec', 'captureTimeStr_MU_1Sec',
                                       'messageTimeStr_MU_1Sec', 'CME:ZFU9_bidPrice_1_MU_1Sec',
                                       'CME:ZFU9_bidOrders_1_MU_1Sec', 'CME:ZFU9_bidQty_1_MU_1Sec',
                                       'CME:ZFU9_askPrice_1_MU_1Sec', 'CME:ZFU9_askOrders_1_MU_1Sec',
                                       'CME:ZFU9_askQty_1_MU_1Sec', 'captureTimeStr_MU_5Sec',
                                       'messageTimeStr_MU_5Sec', 'CME:ZFU9_bidPrice_1_MU_5Sec',
                                       'CME:ZFU9_bidOrders_1_MU_5Sec', 'CME:ZFU9_bidQty_1_MU_5Sec',
                                       'CME:ZFU9_askPrice_1_MU_5Sec', 'CME:ZFU9_askOrders_1_MU_5Sec',
                                       'CME:ZFU9_askQty_1_MU_5Sec', 'captureTimeStr_MU_30Sec',
                                       'messageTimeStr_MU_30Sec', 'CME:ZFU9_bidPrice_1_MU_30Sec',
                                       'CME:ZFU9_bidOrders_1_MU_30Sec', 'CME:ZFU9_bidQty_1_MU_30Sec',
                                       'CME:ZFU9_askPrice_1_MU_30Sec', 'CME:ZFU9_askOrders_1_MU_30Sec',
                                       'CME:ZFU9_askQty_1_MU_30Sec', 'captureTimeStr_MU_60Sec',
                                       'messageTimeStr_MU_60Sec', 'CME:ZFU9_bidPrice_1_MU_60Sec',
                                       'CME:ZFU9_bidOrders_1_MU_60Sec', 'CME:ZFU9_bidQty_1_MU_60Sec',
                                       'CME:ZFU9_askPrice_1_MU_60Sec', 'CME:ZFU9_askOrders_1_MU_60Sec',
                                       'CME:ZFU9_askQty_1_MU_60Sec', 'captureTimeStr_MU_300Sec',
                                       'messageTimeStr_MU_300Sec', 'CME:ZFU9_bidPrice_1_MU_300Sec',
                                       'CME:ZFU9_bidOrders_1_MU_300Sec', 'CME:ZFU9_bidQty_1_MU_300Sec',
                                       'CME:ZFU9_askPrice_1_MU_300Sec', 'CME:ZFU9_askOrders_1_MU_300Sec',
                                       'CME:ZFU9_askQty_1_MU_300Sec', 'captureTimeStr_MU_1Sec',
                                       'messageTimeStr_MU_1Sec', 'CME:ZBU9_bidPrice_1_MU_1Sec',
                                       'CME:ZBU9_bidOrders_1_MU_1Sec', 'CME:ZBU9_bidQty_1_MU_1Sec',
                                       'CME:ZBU9_askPrice_1_MU_1Sec', 'CME:ZBU9_askOrders_1_MU_1Sec',
                                       'CME:ZBU9_askQty_1_MU_1Sec', 'captureTimeStr_MU_5Sec',
                                       'messageTimeStr_MU_5Sec', 'CME:ZBU9_bidPrice_1_MU_5Sec',
                                       'CME:ZBU9_bidOrders_1_MU_5Sec', 'CME:ZBU9_bidQty_1_MU_5Sec',
                                       'CME:ZBU9_askPrice_1_MU_5Sec', 'CME:ZBU9_askOrders_1_MU_5Sec',
                                       'CME:ZBU9_askQty_1_MU_5Sec', 'captureTimeStr_MU_30Sec',
                                       'messageTimeStr_MU_30Sec', 'CME:ZBU9_bidPrice_1_MU_30Sec',
                                       'CME:ZBU9_bidOrders_1_MU_30Sec', 'CME:ZBU9_bidQty_1_MU_30Sec',
                                       'CME:ZBU9_askPrice_1_MU_30Sec', 'CME:ZBU9_askOrders_1_MU_30Sec',
                                       'CME:ZBU9_askQty_1_MU_30Sec', 'captureTimeStr_MU_60Sec',
                                       'messageTimeStr_MU_60Sec', 'CME:ZBU9_bidPrice_1_MU_60Sec',
                                       'CME:ZBU9_bidOrders_1_MU_60Sec', 'CME:ZBU9_bidQty_1_MU_60Sec',
                                       'CME:ZBU9_askPrice_1_MU_60Sec', 'CME:ZBU9_askOrders_1_MU_60Sec',
                                       'CME:ZBU9_askQty_1_MU_60Sec', 'captureTimeStr_MU_300Sec',
                                       'messageTimeStr_MU_300Sec', 'CME:ZBU9_bidPrice_1_MU_300Sec',
                                       'CME:ZBU9_bidOrders_1_MU_300Sec', 'CME:ZBU9_bidQty_1_MU_300Sec',
                                       'CME:ZBU9_askPrice_1_MU_300Sec', 'CME:ZBU9_askOrders_1_MU_300Sec',
                                       'CME:ZBU9_askQty_1_MU_300Sec', 'captureTimeStr_MU_1Sec',
                                       'messageTimeStr_MU_1Sec', 'CME:UBU9_bidPrice_1_MU_1Sec',
                                       'CME:UBU9_bidOrders_1_MU_1Sec', 'CME:UBU9_bidQty_1_MU_1Sec',
                                       'CME:UBU9_askPrice_1_MU_1Sec', 'CME:UBU9_askOrders_1_MU_1Sec',
                                       'CME:UBU9_askQty_1_MU_1Sec', 'captureTimeStr_MU_5Sec',
                                       'messageTimeStr_MU_5Sec', 'CME:UBU9_bidPrice_1_MU_5Sec',
                                       'CME:UBU9_bidOrders_1_MU_5Sec', 'CME:UBU9_bidQty_1_MU_5Sec',
                                       'CME:UBU9_askPrice_1_MU_5Sec', 'CME:UBU9_askOrders_1_MU_5Sec',
                                       'CME:UBU9_askQty_1_MU_5Sec', 'captureTimeStr_MU_30Sec',
                                       'messageTimeStr_MU_30Sec', 'CME:UBU9_bidPrice_1_MU_30Sec',
                                       'CME:UBU9_bidOrders_1_MU_30Sec', 'CME:UBU9_bidQty_1_MU_30Sec',
                                       'CME:UBU9_askPrice_1_MU_30Sec', 'CME:UBU9_askOrders_1_MU_30Sec',
                                       'CME:UBU9_askQty_1_MU_30Sec', 'captureTimeStr_MU_60Sec',
                                       'messageTimeStr_MU_60Sec', 'CME:UBU9_bidPrice_1_MU_60Sec',
                                       'CME:UBU9_bidOrders_1_MU_60Sec', 'CME:UBU9_bidQty_1_MU_60Sec',
                                       'CME:UBU9_askPrice_1_MU_60Sec', 'CME:UBU9_askOrders_1_MU_60Sec',
                                       'CME:UBU9_askQty_1_MU_60Sec', 'captureTimeStr_MU_300Sec',
                                       'messageTimeStr_MU_300Sec', 'CME:UBU9_bidPrice_1_MU_300Sec',
                                       'CME:UBU9_bidOrders_1_MU_300Sec', 'CME:UBU9_bidQty_1_MU_300Sec',
                                       'CME:UBU9_askPrice_1_MU_300Sec', 'CME:UBU9_askOrders_1_MU_300Sec',
                                       'CME:UBU9_askQty_1_MU_300Sec'])

            for n in range(len(all_df_temp)):
                new_transactTime = all_df_temp['messageTimeStr'].iloc[n]
                new_captureTime = all_df_temp['captureTimeStr'].iloc[n]
                qrm_book_after = qrm_data[(qrm_data['messageTimeStr'] >= new_transactTime) & (
                        qrm_data['messageTimeStr'] <= (new_transactTime + timedelta(seconds=301)))]
                sym_MU_all_t = []
                time_delta_after_capture = [1, 5, 30, 60, 300]  # in Seconds
                symbols = ['ZN', 'ZT', 'ZF', 'ZB', 'UB']

                for sym in symbols:
                    sym_MU = []
                    col = qrm_book_after.columns[
                        qrm_book_after.columns.str.contains(sym)]  # get all column s pertaining to a symbol
                    col = ['captureTimeStr', 'messageTimeStr'] + col.tolist()
                    col = qrm_book_after[col].columns[
                        ~qrm_book_after[col].columns.str.contains('trade')]  # get rid of trade related columns

                    bk_after_sym = qrm_book_after[col].dropna()
                    bk_after_sym = bk_after_sym.reset_index(drop=True)

                    for i in range(len(time_delta_after_capture)):
                        temp = bk_after_sym[(bk_after_sym['messageTimeStr'] >= new_captureTime) & (
                                bk_after_sym['messageTimeStr'] <= (
                                new_captureTime + timedelta(seconds=time_delta_after_capture[i])))]
                        temp = temp.sort_values('captureTimeStr')
                        temp.columns = temp.columns + '_MU_' + str(time_delta_after_capture[i]) + 'Sec'
                        sym_MU.append(pd.DataFrame(temp.iloc[-1:].reset_index(drop=True)))
                    sym_MU_all_t.append(pd.concat([sym_MU[0], sym_MU[1], sym_MU[2], sym_MU[3], sym_MU[4]], axis=1))

                MU_temp = pd.concat([sym_MU_all_t[0], sym_MU_all_t[1], sym_MU_all_t[2], sym_MU_all_t[3], sym_MU_all_t[4]],
                                    axis=1)
                MU = pd.concat([MU, MU_temp])
                MU = MU.reset_index(drop=True)

            all_df_temp = pd.concat([all_df_temp, MU], axis=1)
            # all_df_temp.loc[len(all_df_temp)] = '.'
            all_df_temp = all_df_temp.reset_index(drop=True)

            time_delta = ['1', '5', '30', '60', '300']  # times for MU after trade
            # Get WR for different times
            for ele in symbols:
                for delta in time_delta:
                    all_df_temp['WR_' + ele + '_' + delta + 'Sec'] = ((all_df_temp[
                                                                           'CME:' + ele + 'U9_bidPrice_1_MU_' + delta + 'Sec'] *
                                                                       all_df_temp[
                                                                           'CME:' + ele + 'U9_askQty_1_MU_' + delta + 'Sec']) + \
                                                                      (all_df_temp[
                                                                           'CME:' + ele + 'U9_askPrice_1_MU_' + delta + 'Sec'] *
                                                                       all_df_temp[
                                                                           'CME:' + ele + 'U9_bidQty_1_MU_' + delta + 'Sec'])) / \
                                                                     (all_df_temp[
                                                                          'CME:' + ele + 'U9_bidQty_1_MU_' + delta + 'Sec'] +
                                                                      all_df_temp[
                                                                          'CME:' + ele + 'U9_askQty_1_MU_' + delta + 'Sec'])

            # getting rid of columns which contains books info
            col = all_df_temp.columns[~all_df_temp.columns.str.contains('MU')].tolist()

            all_df_temp = all_df_temp[col]

            # WR = 0 for the symbols that are not traded at that instant

            for ele in symbols:
                all_df_temp.loc[
                    all_df_temp['CME:' + ele + 'U9_tradeQty'].isna(), ['WR_' + ele + '_1Sec', 'WR_' + ele + '_5Sec',
                                                                       'WR_' + ele + '_30Sec', 'WR_' + ele + '_60Sec',
                                                                       'WR_' + ele + '_300Sec']] = 0
                all_df_temp = all_df_temp[all_df_temp['CME:' + ele + 'U9_tradeAggSide'] != 0]  # remove implied trades

            # WR - buy_price or sell_price - WR
            for ele in symbols:
                for delta in time_delta:
                    all_df_temp[ele + '_' + delta + '_Sec_MU'] = all_df_temp['WR_' + ele + '_' + delta + 'Sec'] - \
                                                                 all_df_temp['CME:' + ele + 'U9_tradePrice']

            for ele in symbols:
                all_df_temp.loc[all_df_temp['CME:' + ele + 'U9_tradeAggSide'] == 2, [ele + '_1_Sec_MU', ele + '_5_Sec_MU',
                                                                                     ele + '_30_Sec_MU', ele + '_60_Sec_MU',
                                                                                     ele + '_300_Sec_MU']] = -1 * \
                                                                                                             all_df_temp.loc[
                                                                                                                 all_df_temp[
                                                                                                                     'CME:' + ele + 'U9_tradeAggSide'] == 2, [
                                                                                                                     ele + '_1_Sec_MU',
                                                                                                                     ele + '_5_Sec_MU',
                                                                                                                     ele + '_30_Sec_MU',
                                                                                                                     ele + '_60_Sec_MU',
                                                                                                                     ele + '_300_Sec_MU']]

            """
    
    
    
            path =  'Y:\\2019_08_14\\securityDefs.h5'
            dt = tables.open_file(path, mode="r")
            secdef = pd.DataFrame(dt.get_node('/SecurityDefinitions').read())
            secdef = secdef.applymap(lambda x: x.decode() if isinstance(x, bytes) else x)
    
            secdef = secdef[['securityId', 'symbol', 'displayFactorInt','displayFactorRaw', 'minPriceIncrement', 'asset', 'rawToIntMult','priceRatio']]
            secdef[secdef['symbol']=="UBU9"]['minPriceIncrement']
    
    
            zn_tick_size = 0.015625
            zn_tick_value = 15.625
    
    
            zt_tick_size = 0.003906
            zt_tick_value = 7.8125
    
            zf_tick_size = 0.007812
            zf_tick_value = 7.8125
    
    
            zb_tick_size = 0.03125
            zb_tick_value = 31.25
    
            ub_tick_size = 0.03125
            ub_tick_value = 31.25
    
            """

            # tick value/ tick size multiplied by price difference
            for ele in symbols:
                for delta in time_delta:
                    if (ele == 'ZT'):
                        all_df_temp.loc[:, ele + '_' + delta + '_Sec_MU'] = all_df_temp.loc[:,
                                                                            ele + '_' + delta + '_Sec_MU'] * 200 * all_df_temp.loc[
                                                                                                                   :,
                                                                                                                   'CME:' + ele + 'U9_tradeQty']
                    else:
                        all_df_temp.loc[:, ele + '_' + delta + '_Sec_MU'] = all_df_temp.loc[:,
                                                                            ele + '_' + delta + '_Sec_MU'] * 1000 * all_df_temp.loc[
                                                                                                                    :,
                                                                                                                    'CME:' + ele + 'U9_tradeQty']

            for delta in time_delta:
                all_df_temp['Total_' + delta + '_Sec_MU'] = all_df_temp[
                    ['ZN_' + delta + '_Sec_MU', 'ZT_' + delta + '_Sec_MU', 'ZF_' + delta + '_Sec_MU',
                     'ZB_' + delta + '_Sec_MU', 'UB_' + delta + '_Sec_MU']].sum(axis=1)

            if (num_zn_big == len(qrm_zn_big)):
                all_df_temp = pd.concat([qrm_zn_big.iloc[num_zn_big:].reset_index(drop=True), all_df_temp], axis=1)
            else:
                all_df_temp = pd.concat([qrm_zn_big.iloc[num_zn_big:(num_zn_big + 1)].reset_index(drop=True), all_df_temp],
                                        axis=1)

            all_df = pd.concat([all_df, all_df_temp], ignore_index=True, sort=False)
            all_df = all_df.drop_duplicates()

    all_df.loc[all_df.duplicated(subset=all_df.columns[8:34]), all_df.columns[12:]] = np.nan


    file_name = current_symbol[:2] + '_' + date + '.csv'
    all_df.to_csv(file_name, index = False)




