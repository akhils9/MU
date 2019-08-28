
load_symbols = ['ZNU9', 'ZTU9', 'ZFU9', 'ZBU9', 'UBU9','ZNZ9','ZTZ9','ZFZ9','ZBZ9','UBZ9']

range_dates = pd.date_range(start = '2019-08-01', end =  '2019-08-10', freq=BDay())


date_list = pd.Series(range_dates.format()).tolist() # list of dates

#

for date in date_list:


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


       qrm_data_all = pd.DataFrame(columns=qrm_columns)

       from datetime import datetime, timedelta

       #date = datetime.strptime(date,'%Y-%m-%d')

       start = datetime.strptime(date,'%Y-%m-%d') - timedelta(hours=7) # previous day 17:00:00
       end = datetime.strptime(date,'%Y-%m-%d') + timedelta(minutes=975) # date 16:15:00

       from datetime import timedelta
       step = timedelta(minutes=15)
       seconds = (end - start).total_seconds()
       array = []
       for i in range(0, int(seconds), int(step.total_seconds())):
           array.append(start + timedelta(seconds=i))

       array = [i.strftime("%Y-%m-%d %H:%M:%S") for i in array]



       for i in range(len(array)-1):
           temp_qrm = qrm(qrm_symbols,array[i],array[i+1])
           qrm_data_all = pd.concat([qrm_data_all,temp_qrm],sort = False, ignore_index=True)
       ed = time.time()

       path = 'Z:\\Akhil\\Python\\qrm_data_' + date +'.pkl.gz'
       qrm_data_all.to_pickle(path,compression = 'gzip')


