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

current_symbol = 'UBU9' 
big_trade_size = 20

load_symbols = ['ZNU9', 'ZTU9', 'ZFU9', 'ZBU9', 'UBU9','ZNZ9', 'ZTZ9', 'ZFZ9', 'ZBZ9', 'UBZ9']

date_list = ['2019-08-27']
for d in date_list:

    date = d

    qrm_columns = ['captureTime', 'captureTimeStr', 'messageTime', 'messageTimeStr',
                      'packetSeqNum', 'messageId', 'entryId', 'eventId', 'rptSeq']

    for sym in load_symbols:
        col = ['CME:' + sym + '_bidPrice_1', 'CME:' + sym + '_bidOrders_1', 'CME:' + sym + '_bidQty_1',
                  'CME:' + sym + '_tradeType', 'CME:' + sym + '_tradeAggSide', 'CME:' + sym + '_tradePrice',
                  'CME:' + sym + '_tradeQty', 'CME:' + sym + '_tradeNumOrders', 'CME:' + sym + '_tradeOrderId',
                  'CME:' + sym + '_askPrice_1', 'CME:' + sym + '_askOrders_1', 'CME:' + sym + '_askQty_1']

        qrm_columns = qrm_columns + col
        col = None


    qrm_symbols = []
    for sym in load_symbols:
        qrm_symbols.append('CME:'+sym)


    
    qrm_file_name = 'qrm_data_' + date + '_all.pkl.gz'
    qrm_data = pd.read_pickle(qrm_file_name, compression='gzip')

    qrm_data = qrm_data.loc[:,qrm_columns]

    qrm_data['captureTimeStr'] = pd.to_datetime(qrm_data['captureTime'], unit='ns')
    qrm_data['messageTimeStr'] = pd.to_datetime(qrm_data['messageTime'], unit='ns')
    qrm_data['captureTimeStr'] = qrm_data['captureTimeStr'].dt.tz_localize('GMT').dt.tz_convert('US/Central')
    qrm_data['messageTimeStr'] = qrm_data['messageTimeStr'].dt.tz_localize('GMT').dt.tz_convert('US/Central')


    col = qrm_data.columns[qrm_data.columns.str.contains(current_symbol + '_trade')]
    col = ['captureTimeStr', 'messageTimeStr', 'eventId', 'packetSeqNum'] + col.tolist()

    qrm_zn_big = qrm_data[col]
    qrm_zn_big = qrm_zn_big[(qrm_zn_big['CME:' + current_symbol + '_tradeQty'] >= big_trade_size) & (qrm_zn_big['CME:' + current_symbol + '_tradeType'] == 6) & (
                                    qrm_zn_big['CME:' + current_symbol + '_tradeAggSide'] != 0)]
    qrm_zn_big = qrm_zn_big.drop(columns=['CME:' + current_symbol + '_tradeType', 'CME:' + current_symbol + '_tradeOrderId'])

        
    
    all_df_columns = ['captureTimeStr', 'messageTimeStr', 'eventId', 'packetSeqNum',
                      'CME:' + current_symbol + '_tradeAggSide', 'CME:' + current_symbol + '_tradePrice',
                      'CME:' + current_symbol + '_tradeQty',
                      'CME:' + current_symbol + '_tradeNumOrders', 'captureTimeStr', 'messageTimeStr',
                      'eventId', 'packetSeqNum']

    for sym in load_symbols:
        col = ['CME:' + sym + '_tradeAggSide', 'CME:' + sym + '_tradePrice',
               'CME:' + sym + '_tradeQty', 'CME:' + sym + '_tradeNumOrders',
               'WR_' + sym + '_1Sec', 'WR_' + sym + '_5Sec', 'WR_' + sym + '_30Sec',
               'WR_' + sym + '_60Sec', 'WR_' + sym + '_300Sec',
               sym + '_1_Sec_MU', sym + '_5_Sec_MU', sym+ '_30_Sec_MU',
               sym + '_60_Sec_MU', sym + '_300_Sec_MU']

        all_df_columns = all_df_columns + col
        col = None




    all_df_columns = all_df_columns + ['Events_delta', 'time_delta_in_mics', 'count','Total_1_Sec_MU', 'Total_5_Sec_MU', 'Total_30_Sec_MU',
                                       'Total_60_Sec_MU', 'Total_300_Sec_MU']


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

            mu_columns = []
            for ele in load_symbols:
                col = []
                for delta in time_delta_after_capture:
                    col = col + ['captureTimeStr_MU_' + str(delta) + 'Sec', 'messageTimeStr_MU_' + str(delta) + 'Sec',
                                 'CME:' + ele + '_bidPrice_1_MU_' + str(delta) + 'Sec',
                                 'CME:' + ele + '_bidOrders_1_MU_' + str(delta) + 'Sec',
                                 'CME:' + ele + '_bidQty_1_MU_' + str(delta) + 'Sec',
                                 'CME:' + ele + '_askPrice_1_MU_' + str(delta) + 'Sec',
                                 'CME:' + ele + '_askOrders_1_MU_' + str(delta) + 'Sec',
                                 'CME:' + ele + '_askQty_1_MU_' + str(delta) + 'Sec']

                mu_columns = mu_columns + col

            MU = pd.DataFrame(columns=[mu_columns])

            for n in range(len(all_df_temp)):
                new_transactTime = all_df_temp['messageTimeStr'].iloc[n]
                new_captureTime = all_df_temp['captureTimeStr'].iloc[n]
                qrm_book_after = qrm_data[(qrm_data['messageTimeStr'] >= new_transactTime) & (
                        qrm_data['messageTimeStr'] <= (new_transactTime + timedelta(seconds=301)))]
                sym_MU_all_t = []
                time_delta_after_capture = [1, 5, 30, 60, 300]  # in Seconds

                symbols = []
                for ele in load_symbols:
                    symbols.append(ele[:2])

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
            for ele in load_symbols:
                for delta in time_delta:
                    all_df_temp['WR_' + ele + '_' + delta + 'Sec'] = ((all_df_temp[
                                                                           'CME:' + ele + '_bidPrice_1_MU_' + delta + 'Sec'] *
                                                                       all_df_temp[
                                                                           'CME:' + ele + '_askQty_1_MU_' + delta + 'Sec']) + \
                                                                      (all_df_temp[
                                                                           'CME:' + ele + '_askPrice_1_MU_' + delta + 'Sec'] *
                                                                       all_df_temp[
                                                                           'CME:' + ele + '_bidQty_1_MU_' + delta + 'Sec'])) / \
                                                                     (all_df_temp[
                                                                          'CME:' + ele + '_bidQty_1_MU_' + delta + 'Sec'] +
                                                                      all_df_temp[
                                                                          'CME:' + ele + '_askQty_1_MU_' + delta + 'Sec'])

            # getting rid of columns which contains books info
            col = all_df_temp.columns[~all_df_temp.columns.str.contains('MU')].tolist()

            all_df_temp = all_df_temp[col]

            # WR = 0 for the symbols that are not traded at that instant

            for ele in load_symbols:
                all_df_temp.loc[
                    all_df_temp['CME:' + ele + '_tradeQty'].isna(), ['WR_' + ele + '_1Sec', 'WR_' + ele + '_5Sec',
                                                                       'WR_' + ele + '_30Sec', 'WR_' + ele + '_60Sec',
                                                                       'WR_' + ele + '_300Sec']] = 0
                all_df_temp = all_df_temp[all_df_temp['CME:' + ele + '_tradeAggSide'] != 0]  # remove implied trades

            # WR - buy_price or sell_price - WR
            for ele in load_symbols:
                for delta in time_delta:
                    all_df_temp[ele + '_' + delta + '_Sec_MU'] = all_df_temp['WR_' + ele + '_' + delta + 'Sec'] - \
                                                                 all_df_temp['CME:' + ele + '_tradePrice']

            for ele in load_symbols:
                all_df_temp.loc[all_df_temp['CME:' + ele + '_tradeAggSide'] == 2, [ele + '_1_Sec_MU', ele + '_5_Sec_MU',
                                                                                     ele + '_30_Sec_MU', ele + '_60_Sec_MU',
                                                                                     ele + '_300_Sec_MU']] = -1 * \
                                                                                                             all_df_temp.loc[
                                                                                                                 all_df_temp[
                                                                                                                     'CME:' + ele + '_tradeAggSide'] == 2, [
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
            for ele in load_symbols:
                for delta in time_delta:
                    if (ele[:2] == 'ZT'):
                        all_df_temp.loc[:, ele + '_' + delta + '_Sec_MU'] = all_df_temp.loc[:,
                                                                            ele + '_' + delta + '_Sec_MU'] * 200 * all_df_temp.loc[
                                                                                                                   :,
                                                                                                                   'CME:' + ele + '_tradeQty']
                    else:
                        all_df_temp.loc[:, ele + '_' + delta + '_Sec_MU'] = all_df_temp.loc[:,
                                                                            ele + '_' + delta + '_Sec_MU'] * 1000 * all_df_temp.loc[
                                                                                                                    :,
                                                                                                                    'CME:' + ele + '_tradeQty']

            for delta in time_delta:
                x = 0
                for ele in load_symbols:
                    x+= all_df_temp[ele + '_' + delta + '_Sec_MU']
                all_df_temp['Total_' + delta + '_Sec_MU'] = x 

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




