from module.ConnectDB import ConnectDB
import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings('ignore')


def get_price(term, stock_code=None):
    if term[5] == '1':  # 1분기 (1월~3월)
        start_date = term[0:4] + '-01-01'
        end_date = term[0:4] + '-03-31'
        period = term[0:4] + 'Q1'

    elif term[5] == '2':  # 2분기 (4월~6월)
        start_date = term[0:4] + '-04-01'
        end_date = term[0:4] + '-06-30'
        period = term[0:4] + 'Q2'

    elif term[5] == '3':  # 3분기 (7월~9월)
        start_date = term[0:4] + '-07-01'
        end_date = term[0:4] + '-09-30'
        period = term[0:4] + 'Q3'

    elif term[5] == '4':  # 4분기 (10월~12월)
        start_date = term[0:4] + '-10-01'
        end_date = term[0:4] + '-12-31'
        period = term[0:4] + 'Q4'

    # DB 연결
    cd = ConnectDB()
    # 주가데이터가 원시주가 테이블과, 수정주가 테이블로 나뉘어져 있다. 두 테이블중 필요한 값을 join해서 불러온다.
    sql = "SELECT srp.*,  sop.시장구분, sop.거래대금, sop.시가총액, sop.상장주식수 FROM stock_revise_price srp " \
          "LEFT JOIN stock_origin_price sop " \
          "ON (srp.종목코드, srp.날짜) = (sop.종목코드, sop.날짜) " \
          "WHERE srp.날짜 BETWEEN '{}' AND '{}'".format(start_date, end_date)
    # 종목이 지정된 경우
    if stock_code is not None and \
            (stock_code.upper() != 'LATER' and stock_code.upper() != 'ALL' and stock_code.upper() != 'LOAD'):
        sql += " AND srp.종목코드={}".format(stock_code)

    df = cd.executeQuery(sql)
    cd.closeConnection()

    df['날짜'] = df['날짜'].apply(lambda x: x.strftime('%Y-%m-%d'))
    print('start_date({}) ~ end_date({})'.format(start_date, end_date))

    df = df.sort_values(by=['종목코드', '날짜'], axis=0)  # stock_code, date 별로 정렬

    # 결측치 처리
    df[['시가', '고가', '저가', '종가']] = df[['시가', '고가', '저가', '종가']].replace(0, np.nan)
    df['시가'] = np.where(pd.notnull(df['시가']) == True, df['시가'], df['종가'])
    df['고가'] = np.where(pd.notnull(df['고가']) == True, df['고가'], df['종가'])
    df['저가'] = np.where(pd.notnull(df['저가']) == True, df['저가'], df['종가'])
    df['종가'] = np.where(pd.notnull(df['종가']) == True, df['종가'], df['종가'])

    groups = df.groupby('종목코드')

    df_ohlc = pd.DataFrame()
    df_ohlc['종목명'] = groups.max()['종목명']
    df_ohlc['시장'] = groups.max()['시장구분']
    df_ohlc['기간'] = period
    df_ohlc['고가'] = groups.max()['고가']  # 분기별 고가
    df_ohlc['저가'] = groups.min()['저가']  # 분기별 저가
    df_ohlc['시가'], df_ohlc['종가'], df_ohlc['거래량'] = np.nan, np.nan, np.nan
    df_ohlc['거래대금'], df_ohlc['시가총액'], df_ohlc['상장주식수'] = np.nan, np.nan, np.nan
    df_ohlc['종목코드'] = df_ohlc.index

    df_ohlc = df_ohlc.reset_index(drop=True)

    for i in range(len(df_ohlc)):
        df_ohlc['시가'][i] = int(df[df['종목코드'] == df_ohlc['종목코드'][i]].head(1)['시가'])
        df_ohlc['종가'][i] = int(df[df['종목코드'] == df_ohlc['종목코드'][i]].tail(1)['종가'])
        df_ohlc['거래량'][i] = int(df[df['종목코드'] == df_ohlc['종목코드'][i]].tail(1)['거래량'])
        df_ohlc['거래대금'][i] = int(df[df['종목코드'] == df_ohlc['종목코드'][i]].tail(1)['거래대금'])
        df_ohlc['시가총액'][i] = int(df[df['종목코드'] == df_ohlc['종목코드'][i]].tail(1)['시가총액'])
        df_ohlc['상장주식수'][i] = int(df[df['종목코드'] == df_ohlc['종목코드'][i]].tail(1)['상장주식수'])

    df_ohlc = df_ohlc[['종목코드', '종목명', '시장', '기간', '시가', '고가', '저가', '종가', '거래량', '거래대금', '시가총액', '상장주식수']]

    return df_ohlc

# 재무데이터를 불러온다.
def get_finance(table, stock_code, period, column):
    cd = ConnectDB()
    sql = "SELECT * FROM {} WHERE 종목코드='{}' AND 기간 LIKE '{}' AND 컬럼='{}'".format(
        table, stock_code, period, column)
    df = cd.executeQuery(sql)
    cd.closeConnection()
    return df

# 종목 리스트를 불러온다.
def get_stockcode():
    cd = ConnectDB()
    sql = "SELECT Symbol as '종목코드' FROM stock_list"
    df = cd.executeQuery(sql)
    cd.closeConnection()
    df = df.sort_values(by=['종목코드'])

    # df = df.head(10) #사용데이터 축소

    df = df.values.tolist()
    return df


# 데이터가 존재하는지 확인
def CheckDataExists(table, stock_code, period, column):
    cd = ConnectDB()
    sql = "SELECT EXISTS (SELECT 종목코드, 컬럼, 기간 " \
          "FROM {} WHERE 종목코드='{}' AND 컬럼='{}' AND 기간 LIKE '{}')" \
          " as 'IsExists'".format(table[0], stock_code, column, period)
    exist = cd.executeQuery(sql)
    if exist.values == 0:  # is_kr 데이터가 존재하지 않음
        return False

    sql = "SELECT EXISTS (SELECT 종목코드, 컬럼, 기간 " \
          "FROM {} WHERE 종목코드='{}' AND 컬럼='{}' AND 기간 LIKE '{}')" \
          " as 'IsExists'".format(table[1], stock_code, column, period)
    exist = cd.executeQuery(sql)
    if exist.values == 0:  # bs_kr 데이터가 존재하지 않음
        return False

    sql = "SELECT EXISTS (SELECT 종목코드, 컬럼, 기간 " \
          "FROM {} WHERE 종목코드='{}' AND 컬럼='{}' AND 기간 LIKE '{}')" \
          " as 'IsExists'".format(table[2], stock_code, column, period)
    exist = cd.executeQuery(sql)
    if exist.values == 0:  # cf_kr 데이터가 존재하지 않음
        return False

    return True  # 모든 데이터 존재


def get_trailing(stock_code, period, by='Consolidated', freq='Q'):
    if freq.upper() == 'A':
        print('연간 데이터로 트레일링 데이터를 만들 수 없습니다.')
        return -1

    df_quat = {}
    period_quat = period
    column = by.lower() + '_' + freq.lower()
    period_quat = period_quat[0:4] + '-' + str((int(period_quat[5])) * 3).zfill(2) + '%'

    # print('\n{} '.format(stock_code), end='')
    for i in range(4):
        # 데이터가 존재하지 않으면 종료
        if not CheckDataExists(['is_kr', 'bs_kr', 'cf_kr'], stock_code, period_quat, column):
            print('{}: {}로 트레일링 데이터를 만들 수 없습니다'.format(stock_code, period))
            return
        # print(period_quat[0:4] + '년 ' + str(int(int(period_quat[5:7]) / 3)) + '분기', end=' ')
        df_is = get_finance(table='is_kr', stock_code=stock_code, period=period_quat, column=column)
        df_bs = get_finance(table='bs_kr', stock_code=stock_code, period=period_quat, column=column)
        df_cf = get_finance(table='cf_kr', stock_code=stock_code, period=period_quat, column=column)

        df_merge = pd.merge(df_is, df_bs, how='left', on=['종목코드', '종목명', '시장', '기간', '컬럼'])
        df_merge = pd.merge(df_merge, df_cf, how='left', on=['종목코드', '종목명', '시장', '기간', '컬럼'])
        df_merge['기간'] = period_quat[0:4] + 'Q' + str(int(int(period_quat[5:7]) / 3))
        df_quat[i] = df_merge

        if int(int(period_quat[5:7]) / 3) - 1 == 0:
            period_quat = str(int(period_quat[0:4]) - 1) + '-12%'
        else:
            period_quat = period_quat[0:4] + '-' + str((int(int(period_quat[5:7]) / 3) - 1) * 3).zfill(2) + '%'

    df_trailing = pd.DataFrame(df_quat[0], columns=['종목코드', '종목명', '시장', '기간', '컬럼'])
    df_trailing[df_quat[0].columns[5:]] = 0

    for i in range(len(df_quat)):
        df_trailing[df_quat[0].columns[5:]] += df_quat[i][df_quat[i].columns[5:]]

    return df_trailing

def LoadTrailingData():
    cd = ConnectDB()
    sql = "SELECT * FROM trailingData"
    df = cd.executeQuery(sql)
    cd.closeConnection()
    return df