import pandas as pd
import json, requests
from datetime import datetime, timedelta
import FinanceDataReader as fdr
import sqlalchemy  # sql 접근 및 관리를 도와주는 패키지
from module.ConnectDB import EngineDB


def getStockList(table_name='', save=False):
    stocks = fdr.StockListing('KRX').dropna()

    if save:
        if table_name == '':
            return stocks;
        en = EngineDB()
        engine = en.getEngine()

        # 종목 리스트 데이터 DB 저장
        stocks.to_sql(name=table_name, con=engine, if_exists='append', index=False,
                      dtype={  # sql에 저장할 때, 데이터 유형도 설정할 수 있다.
                          # Symbol	Market	Name	Sector	Industry
                          # ListingDate	SettleMonth	Representative	HomePage	Region
                          'Symbol': sqlalchemy.types.VARCHAR(10),
                          'Market': sqlalchemy.types.VARCHAR(20),
                          'Name': sqlalchemy.types.VARCHAR(45),
                          'Sector': sqlalchemy.types.TEXT(),
                          'Industry': sqlalchemy.types.TEXT(),
                          'ListingDate': sqlalchemy.types.DATE(),
                          'SettleMonth': sqlalchemy.types.VARCHAR(20),
                          'Representative': sqlalchemy.types.TEXT(),
                          'HomePage': sqlalchemy.types.TEXT(),
                          'Region': sqlalchemy.types.TEXT()
                      })
        engine.dispose()
        print('save complete')
    return stocks


def getPriceData(date):
    date = date[0:4] + date[5:7] + date[8:10]
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) \
                Chrome/100.0.4896.151 Whale/3.14.134.62 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    res = requests.post(url, headers=headers, data={
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',  # 기본통계 - 주식 - 종목시세 - 전종목시세
        'locale': 'ko_KR',
        'mktId': 'ALL',  # 전체 데이터
        'trdDd': date,  # 수집 날짜
        'share': '1',  # 주식 수 단위
        'money': '1',  # 금액 단위
        'csvxls_isNo': 'false'
    })

    html_text = res.content
    html_json = json.loads(html_text)
    html_jsons = html_json['OutBlock_1']

    daily = []
    if len(html_jsons) > 0:
        for html_json in html_jsons:
            if html_json['TDD_OPNPRC'] == '-':  # 시장이 열리지 않아 값이 없는 경우
                continue;

            ISU_SRT_CD = html_json['ISU_SRT_CD']
            ISU_ABBRV = html_json['ISU_ABBRV']
            MKT_NM = html_json['MKT_NM']
            TRD_DD = datetime.strptime(date, '%Y%m%d')

            FLUC_RT = float(html_json['FLUC_RT'].replace(',', '')) / 100
            TDD_CLSPRC = int(html_json['TDD_CLSPRC'].replace(',', ''))
            TDD_OPNPRC = int(html_json['TDD_OPNPRC'].replace(',', ''))
            TDD_HGPRC = int(html_json['TDD_HGPRC'].replace(',', ''))
            TDD_LWPRC = int(html_json['TDD_LWPRC'].replace(',', ''))

            ACC_TRDVOL = int(html_json['ACC_TRDVOL'].replace(',', ''))
            ACC_TRDVAL = int(html_json['ACC_TRDVAL'].replace(',', ''))
            MKTCAP = int(html_json['MKTCAP'].replace(',', ''))
            LIST_SHRS = int(html_json['LIST_SHRS'].replace(',', ''))

            daily.append((ISU_SRT_CD, ISU_ABBRV, MKT_NM, TRD_DD, FLUC_RT,
                          TDD_OPNPRC, TDD_HGPRC, TDD_LWPRC, TDD_CLSPRC,
                          ACC_TRDVOL, ACC_TRDVAL, MKTCAP, LIST_SHRS))
    else:
        pass

    if len(daily) > 0:
        daily = pd.DataFrame(daily)
        daily.columns = ['종목코드', '종목명', '시장구분', '날짜', '등락률', \
                         '시가', '고가', '저가', '종가', '거래량', \
                         '거래대금', '시가총액', '상장주식수']
        # daily = daily.sort_values(by='날짜').reset_index(drop=True)
    return daily;


def getStartDate(table_name):
    en = EngineDB()
    engine = en.getEngine()
    # 테이블 존재 확인
    sql = "SELECT count(*) FROM Information_schema.tables WHERE table_schema = 'krx_stock_data' " \
          "AND table_name = '{}'".format(table_name)

    exist = pd.read_sql_query(sql, engine)

    if exist.values == 0:
        return '2016-01-01'

    # 저장된 마지막 날짜 확인
    sql = 'select 날짜 from {} ORDER BY 날짜 DESC LIMIT 1'.format(table_name)
    df = pd.read_sql_query(sql, engine)

    start_date = df + timedelta(days=1)
    start_date = (start_date['날짜'].iloc[0]).strftime('%Y-%m-%d')

    return start_date  # 주가를 조회할 첫 날짜


def getOriginPrice(table_name='stock_origin_price', start_date='', end_date='', save=False):
    if start_date == '':
        start_date = getStartDate(table_name)
    if end_date == '':
        now = datetime.now()
        end_date = now.strftime('%Y-%m-%d')

    period = pd.date_range(start=start_date, end=end_date)
    period = period.strftime("%Y-%m-%d").tolist()
    print(f'start_date: {start_date} end_date: {end_date}')

    originPrice = pd.DataFrame()
    for p in period:
        ret = getPriceData(p)
        originPrice = originPrice.append(ret)  # 데이터 추가
    originPrice = originPrice.sort_values(by='날짜').reset_index(drop=True)  # 날짜 기준 정렬

    if save:
        en = EngineDB()
        engine = en.getEngine()
        originPrice.to_sql(name=table_name, con=engine, if_exists='append', index=False,
                           dtype={  # sql에 저장할 때, 데이터 유형도 설정할 수 있다.
                               '종목코드': sqlalchemy.types.VARCHAR(10),
                               '종목명': sqlalchemy.types.TEXT(),
                               '시장구분': sqlalchemy.types.VARCHAR(20),
                               '날짜': sqlalchemy.types.DATE(),
                               '등락률': sqlalchemy.types.FLOAT(),
                               '시가': sqlalchemy.types.BIGINT(),
                               '고가': sqlalchemy.types.BIGINT(),
                               '저가': sqlalchemy.types.BIGINT(),
                               '종가': sqlalchemy.types.BIGINT(),
                               '거래량': sqlalchemy.types.BIGINT(),
                               '거래대금': sqlalchemy.types.BIGINT(),
                               '시가총액': sqlalchemy.types.BIGINT(),
                               '상장주식수': sqlalchemy.types.BIGINT()
                           }
                           )
        engine.dispose()
        print('save complete')
    return originPrice


def getRevisePrice(table_name='stock_revise_price', start_date='', end_date='', save=False):
    if start_date == '':
        start_date = getStartDate(table_name)
    if end_date == '':
        now = datetime.now()
        end_date = now.strftime('%Y-%m-%d')
    print(f'start_date: {start_date} end_date: {end_date}')

    stock_list = getStockList()

    revisePrice = pd.DataFrame()
    for code, name in stock_list[['Symbol', 'Name']].values:
        ohlcv = fdr.DataReader(code, start_date, end_date)  # 데이터 불러오기
        ohlcv.columns = ['시가', '고가', '저가', '종가', '거래량', '등락률']
        ohlcv.insert(0, '종목코드', code)
        ohlcv.insert(1, '종목명', name)
        date = []
        for i in range(0, len(ohlcv.index)):
            date.append(datetime.date(ohlcv.index[i]))
        ohlcv.insert(2, '날짜', date)
        revisePrice = pd.concat([revisePrice, ohlcv])

    if save:
        en = EngineDB()
        engine = en.getEngine()
        revisePrice.to_sql(name=table_name, con=engine, if_exists='append', index=False,
                           dtype={  # sql에 저장할 때, 데이터 유형도 설정할 수 있다.
                               '종목코드': sqlalchemy.types.VARCHAR(10),
                               '종목명': sqlalchemy.types.TEXT(),
                               '날짜': sqlalchemy.types.DATE(),
                               '시가': sqlalchemy.types.BIGINT(),
                               '고가': sqlalchemy.types.BIGINT(),
                               '저가': sqlalchemy.types.BIGINT(),
                               '종가': sqlalchemy.types.BIGINT(),
                               '거래량': sqlalchemy.types.BIGINT(),
                               '등락률': sqlalchemy.types.FLOAT()
                           }
                           )
        engine.dispose()
        print('save complete')
    return revisePrice
