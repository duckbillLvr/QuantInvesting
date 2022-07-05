import pandas as pd
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
import sqlalchemy
import module.GetPrice as GetPrice
from module.ConnectDB import EngineDB


def getISData(stock, rpt_type, freq):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.151 "
                      "Whale/3.14.134.62 Safari/537.36"
    }
    if rpt_type.upper() == 'CONSOLIDATED':  # 연결 연간/분기 손익 계산서
        url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=D&NewMenuID=103&stkGb=701".format(
            stock[0])
    else:  # 별도 연간/분기 손익계산서
        url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=B&NewMenuID=103&stkGb=701".format(
            stock[0])

    req = Request(url=url, headers=headers)
    html = urlopen(req).read()
    soup = BeautifulSoup(html, 'html.parser')

    if freq.upper() == 'A':  # 연간 손익계산서 영역
        is_a = soup.find(id='divSonikY')
        num_col = 4  # 최근 4개 데이터
    else:  # 분기 손익계산서 영역
        is_a = soup.find(id='divSonikQ')
        num_col = 4  # 최근 4개 데이터
    is_a = is_a.find_all(['tr'])

    items_kr = [is_a[m].find(['th']).get_text().replace('\n', '').replace('\xa0', '').replace('계산에 참여한 계정 펼치기', '')
                for m in range(1, len(is_a))]

    period = [is_a[0].find_all('th')[n].get_text() for n in range(1, num_col + 1)]
    period = [period[m].replace('/', '-') for m in range(0, len(period))]
    _period = []
    for p in period:
        _period.append(datetime.strptime(str(p)[:7], '%Y-%m').date())

    globals()['period'] = _period

    for item, i in zip(items_kr, range(1, len(is_a))):
        temps = []
        for j in range(0, num_col):
            temp = [float(is_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '')) \
                        if is_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') != '' \
                        else (0 if is_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') == '-0' \
                                  else 0)]
            temps.append(temp[0])

        globals()[item] = temps

    if rpt_type.upper() == 'CONSOLIDATED':
        pass
    else:  # 별도 연간 손익계산서 해당 항목을 NULL값으로 채움
        globals()['지배주주순이익'], globals()['비지배주주순이익'] = [np.NaN] * num_col, [np.NaN] * num_col

    is_domestic = pd.DataFrame({
        '종목코드': stock[0],
        '종목명': stock[2],
        '시장': stock[1],
        '기간': _period,
        '컬럼': rpt_type.lower() + '_' + freq.lower(),
        '매출액': 매출액,
        '매출원가': 매출원가,
        '매출총이익': 매출총이익,
        '판매비': 판매비,
        '관리비': 관리비,
        '영업이익': 영업이익,
        '금융수익': 금융수익,
        '금융원가': 금융원가,
        '기타수익': 기타수익,
        '기타비용': 기타비용,
        '관계기업관련손익': globals()['종속기업,공동지배기업및관계기업관련손익'],
        'EBIT': 세전계속사업이익,
        '법인세비용': 법인세비용,
        '계속영업이익': 계속영업이익,
        '중단영업이익': 중단영업이익,
        '당기순이익': 당기순이익,
        '지배주주순이익': globals()['지배주주순이익'],
        '비지배주주순이익': globals()['비지배주주순이익']
    }, index=_period)
    return is_domestic  # DataFrame 생성


# 저장된 데이터의 최종 날짜 확인
def dataTimeCheck(code, date, column, table):
    en = EngineDB()
    engine = en.getEngine()
    sql = "SELECT count(*) FROM Information_schema.tables WHERE table_schema = 'krx_stock_data' " \
          "AND table_name = '{}'".format(table)
    exist = pd.read_sql_query(sql, engine)
    if exist.values == 0:
        return True

    sql = "select 기간 FROM {} where 종목코드 = '{}' and 컬럼 = '{}' order by 기간 desc limit 1".format(table, code, column)
    fdate = pd.read_sql_query(sql, engine)
    engine.dispose()
    if fdate.empty:
        return True

    fdate = fdate.iloc[0].values
    if fdate < date:
        return True
    else:
        return False


def getIS(rpt_type, freq, table='is_kr', save=False):
    df = pd.DataFrame()

    stock_list = GetPrice.getStockList()
    for stock in stock_list.values:
        ret = getISData(stock, rpt_type, freq)
        df = df.append(ret)

    df = df.reset_index(drop=True)

    if not save:
        return df
    if table == '':
        print('저장될 table 이름을 입력해 주세요')
        return df
    try:
        if rpt_type.upper() == 'CONSOLIDATED':
            dtype = {
                '종목코드': sqlalchemy.types.VARCHAR(10),
                '종목명': sqlalchemy.types.TEXT(),
                '시장': sqlalchemy.types.VARCHAR(20),
                '기간': sqlalchemy.types.DATE(),
                '컬럼': sqlalchemy.types.VARCHAR(45),
                '매출액': sqlalchemy.types.FLOAT(),
                '매출원가': sqlalchemy.types.FLOAT(),
                '매출총이익': sqlalchemy.types.FLOAT(),
                '판매비': sqlalchemy.types.FLOAT(),
                '관리비': sqlalchemy.types.FLOAT(),
                '영업이익': sqlalchemy.types.FLOAT(),
                '금융수익': sqlalchemy.types.FLOAT(),
                '금융원가': sqlalchemy.types.FLOAT(),
                '기타수익': sqlalchemy.types.FLOAT(),
                '기타비용': sqlalchemy.types.FLOAT(),
                '관계기업관련손익': sqlalchemy.types.FLOAT(),
                'EBIT': sqlalchemy.types.FLOAT(),
                '법인세비용': sqlalchemy.types.FLOAT(),
                '계속영업이익': sqlalchemy.types.FLOAT(),
                '중단영업이익': sqlalchemy.types.FLOAT(),
                '당기순이익': sqlalchemy.types.FLOAT(),
                '지배주주순이익': sqlalchemy.types.FLOAT(),
                '비지배주주순이익': sqlalchemy.types.FLOAT()
            }
        else:
            dtype = {
                '종목코드': sqlalchemy.types.VARCHAR(10),
                '종목명': sqlalchemy.types.TEXT(),
                '시장': sqlalchemy.types.VARCHAR(20),
                '기간': sqlalchemy.types.DATE(),
                '컬럼': sqlalchemy.types.VARCHAR(45),
                '매출액': sqlalchemy.types.FLOAT(),
                '매출원가': sqlalchemy.types.FLOAT(),
                '매출총이익': sqlalchemy.types.FLOAT(),
                '판매비': sqlalchemy.types.FLOAT(),
                '관리비': sqlalchemy.types.FLOAT(),
                '영업이익': sqlalchemy.types.FLOAT(),
                '금융수익': sqlalchemy.types.FLOAT(),
                '금융원가': sqlalchemy.types.FLOAT(),
                '기타수익': sqlalchemy.types.FLOAT(),
                '기타비용': sqlalchemy.types.FLOAT(),
                '관계기업관련손익': sqlalchemy.types.FLOAT(),
                'EBIT': sqlalchemy.types.FLOAT(),
                '법인세비용': sqlalchemy.types.FLOAT(),
                '계속영업이익': sqlalchemy.types.FLOAT(),
                '중단영업이익': sqlalchemy.types.FLOAT(),
                '당기순이익': sqlalchemy.types.FLOAT()
            }

        en = EngineDB()
        engine = en.getEngine()

        for i in range(0, len(df)):
            if dataTimeCheck(df['종목코드'].iloc[i], df['기간'].iloc[i], df['컬럼'].iloc[i], table):
                # 저장된 데이터 없으면 db 저장 실행
                input = pd.DataFrame(df.iloc[i])
                input = input.transpose()
                input.to_sql(name=table, con=engine, if_exists='append', index=False, dtype=dtype)

        engine.dispose()
        print('get_is save complete {}_{}'.format(rpt_type.upper(), freq.upper()))
    except:
        print('get_is save failed {}_{}'.format(rpt_type.upper(), freq.upper()))
        return df
    return df


def getBSData(stock, rpt_type, freq):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.151 "
                      "Whale/3.14.134.62 Safari/537.36"
    }
    if rpt_type.upper() == 'CONSOLIDATED':  # 연결 연간/분기 손익 계산서
        url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=D&NewMenuID=103&stkGb=701".format(
            stock[0])
    else:  # 별도 연간/분기 손익계산서
        url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=B&NewMenuID=103&stkGb=701".format(
            stock[0])

    req = Request(url=url, headers=headers)
    html = urlopen(req).read()
    soup = BeautifulSoup(html, 'html.parser')

    if freq.upper() == 'A':
        bs_a = soup.find(id='divDaechaY')
        num_col = 4
    else:
        bs_a = soup.find(id='divDaechaQ')
        num_col = 4
    bs_a = bs_a.find_all(['tr'])

    items_kr = [bs_a[m].find(['th']).get_text().replace('\n', '').replace('\xa0', '').replace('계산에 참여한 계정 펼치기', '')
                for m in range(1, len(bs_a))]

    period = [bs_a[0].find_all('th')[n].get_text() for n in range(1, num_col + 1)]
    period = [period[m].replace('/', '-') for m in range(0, len(period))]
    _period = []
    for p in period:
        _period.append(datetime.strptime(str(p)[:7], '%Y-%m').date())
    globals()['period'] = _period

    for item, i in zip(items_kr, range(1, len(bs_a))):
        temps = []
        for j in range(0, num_col):
            temp = [float(bs_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '')) \
                        if bs_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') != '' \
                        else (0 if bs_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') == '-0' \
                                  else 0)]
            temps.append(temp[0])

        globals()[item] = temps

    if rpt_type.upper() == 'CONSOLIDATED':
        pass
    else:
        globals()['지배기업주주지분'], globals()['비지배주주지분'] = [np.NaN] * num_col, [np.NaN] * num_col

    bs_domestic = pd.DataFrame({
        '종목코드': stock[0],
        '종목명': stock[2],
        '시장': stock[1],
        '기간': _period,
        '컬럼': rpt_type.lower() + '_' + freq.lower(),
        '자산': 자산,
        '유동자산': 유동자산,
        '재고자산': 재고자산,
        '매출채권및기타유동채권': 매출채권및기타유동채권,
        '비유동자산': 비유동자산,
        '장기매출채권및기타비유동채권': 장기매출채권및기타비유동채권,
        '기타금융업자산': 기타금융업자산,
        '부채': 부채,
        '유동부채': 유동부채,
        '단기차입금': 단기차입금,
        '비유동부채': 비유동부채,
        '장기차입금': 장기차입금,
        '기타금융업부채': 기타금융업부채,
        '자본': 자본,
        '지배기업주주지분': globals()['지배기업주주지분'],
        '비지배주주지분': globals()['비지배주주지분']
    }, index=_period)
    return bs_domestic  # DataFrame 생성


def getBS(rpt_type, freq, table='bs_kr', save=False):
    df = pd.DataFrame()

    stock_list = GetPrice.getStockList()
    for stock in stock_list.values:
        ret = getBSData(stock, rpt_type, freq)
        df = df.append(ret)

    df = df.reset_index(drop=True)

    if not save:
        return df
    if table == '':
        print('저장될 table 이름을 입력해 주세요')
        return df
    try:
        if rpt_type.upper() == 'CONSOLIDATED':
            dtype = {
                '종목코드': sqlalchemy.types.VARCHAR(10),
                '종목명': sqlalchemy.types.TEXT(),
                '시장': sqlalchemy.types.VARCHAR(20),
                '기간': sqlalchemy.types.DATE(),
                '컬럼': sqlalchemy.types.VARCHAR(45),
                '자산': sqlalchemy.types.FLOAT(),
                '유동자산': sqlalchemy.types.FLOAT(),
                '재고자산': sqlalchemy.types.FLOAT(),
                '매출채권및기타유동채권': sqlalchemy.types.FLOAT(),
                '비유동자산': sqlalchemy.types.FLOAT(),
                '장기매출채권및기타비유동채권': sqlalchemy.types.FLOAT(),
                '기타금융업자산': sqlalchemy.types.FLOAT(),
                '부채': sqlalchemy.types.FLOAT(),
                '유동부채': sqlalchemy.types.FLOAT(),
                '단기차입금': sqlalchemy.types.FLOAT(),
                '비유동부채': sqlalchemy.types.FLOAT(),
                '장기차입금': sqlalchemy.types.FLOAT(),
                '기타금융업부채': sqlalchemy.types.FLOAT(),
                '자본': sqlalchemy.types.FLOAT(),
                '지배주주순지분': sqlalchemy.types.FLOAT(),
                '비지배주주순지분': sqlalchemy.types.FLOAT()
            }
        else:
            dtype = {
                '종목코드': sqlalchemy.types.VARCHAR(10),
                '종목명': sqlalchemy.types.TEXT(),
                '시장': sqlalchemy.types.VARCHAR(20),
                '기간': sqlalchemy.types.DATE(),
                '컬럼': sqlalchemy.types.VARCHAR(45),
                '자산': sqlalchemy.types.FLOAT(),
                '유동자산': sqlalchemy.types.FLOAT(),
                '재고자산': sqlalchemy.types.FLOAT(),
                '매출채권및기타유동채권': sqlalchemy.types.FLOAT(),
                '비유동자산': sqlalchemy.types.FLOAT(),
                '장기매출채권및기타비유동채권': sqlalchemy.types.FLOAT(),
                '기타금융업자산': sqlalchemy.types.FLOAT(),
                '부채': sqlalchemy.types.FLOAT(),
                '유동부채': sqlalchemy.types.FLOAT(),
                '단기차입금': sqlalchemy.types.FLOAT(),
                '비유동부채': sqlalchemy.types.FLOAT(),
                '장기차입금': sqlalchemy.types.FLOAT(),
                '기타금융업부채': sqlalchemy.types.FLOAT(),
                '자본': sqlalchemy.types.FLOAT(),
            }

        en = EngineDB()
        engine = en.getEngine()

        for i in range(0, len(df)):
            if dataTimeCheck(df['종목코드'].iloc[i], df['기간'].iloc[i], df['컬럼'].iloc[i], table):
                # 저장된 데이터 없으면 db 저장 실행
                input = pd.DataFrame(df.iloc[i])
                input = input.transpose()
                input.to_sql(name=table, con=engine, if_exists='append', index=False, dtype=dtype)

        # df.to_sql(name=table, con=engine, if_exists='append', index=False, dtype=dtype)

        engine.dispose()
        print('get_bs save complete {}_{}'.format(rpt_type.upper(), freq.upper()))
    except:
        print('get_bs save failed {}_{}'.format(rpt_type.upper(), freq.upper()))
        return df
    return df


def getCFData(stock, rpt_type, freq):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.151 "
                      "Whale/3.14.134.62 Safari/537.36"
    }
    if rpt_type.upper() == 'CONSOLIDATED':  # 연결 연간/분기 손익 계산서
        url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=D&NewMenuID=103&stkGb=701".format(
            stock[0])
    else:  # 별도 연간/분기 손익계산서
        url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=B&NewMenuID=103&stkGb=701".format(
            stock[0])

    req = Request(url=url, headers=headers)
    html = urlopen(req).read()
    soup = BeautifulSoup(html, 'html.parser')

    if freq.upper() == 'A':
        cf_a = soup.find(id='divCashY')
        num_col = 3  # 최근 3개년 연간 데이터
    else:
        cf_a = soup.find(id='divCashQ')
        num_col = 4  # 최근 4분기 연간 데이터

    cf_a = cf_a.find_all(['tr'])

    items_kr = [cf_a[m].find(['th']).get_text().replace('\n', '').replace('\xa0', '').replace('계산에 참여한 계정 펼치기', '')
                for m in range(1, len(cf_a))]
    period = [cf_a[0].find_all('th')[n].get_text() for n in range(1, num_col + 1)]
    period = [period[m].replace('/', '-') for m in range(0, len(period))]
    _period = []
    for p in period:
        _period.append(datetime.strptime(str(p)[:7], '%Y-%m').date())
    globals()['period'] = _period

    idx = [1, 2, 3, 4, 9, 39, 70, 75, 76, 84, 85, 99, 113, 121, 122, 134, 145, 153, 154, 155, 156, 157, 158]
    for i in idx:
        temps = []
        for j in range(0, num_col):
            temp = [float(cf_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '')) \
                        if cf_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') != '' \
                        else (0 if cf_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') == '-0' \
                                  else 0)]
            temps.append(temp[0])

            globals()[items_kr[i - 1]] = temps

    cf_domestic = pd.DataFrame({
        '종목코드': stock[0],
        '종목명': stock[2],
        '시장': stock[1],
        '기간': _period,
        '컬럼': rpt_type.lower() + '_' + freq.lower(),
        '영업활동으로인한현금흐름': 영업활동으로인한현금흐름,
        '당기순손익': 당기순손익,
        '법인세비용차감전계속사업이익': 법인세비용차감전계속사업이익,
        '현금유출이없는비용등가산': 현금유출이없는비용등가산,
        '감가상각비': 감가상각비,
        '현금유입이없는수익등차감': globals()['(현금유입이없는수익등차감)'],
        '영업활동으로인한자산부채변동(운전자본변동)': globals()['영업활동으로인한자산부채변동(운전자본변동)'],
        '영업에서창출된현금흐름': globals()['*영업에서창출된현금흐름'],
        '기타영업활동으로인한현금흐름': 기타영업활동으로인한현금흐름,
        '투자활동으로인한현금흐름': 투자활동으로인한현금흐름,
        '투자활동으로인한현금유입액': 투자활동으로인한현금유입액,
        '투자활동으로인한현금유출액': globals()['(투자활동으로인한현금유출액)'],
        '기타투자활동으로인한현금흐름': 기타투자활동으로인한현금흐름,
        '재무활동으로인한현금흐름': 재무활동으로인한현금흐름,
        '재무활동으로인한현금유입액': 재무활동으로인한현금유입액,
        '재무활동으로인한현금유출액': globals()['(재무활동으로인한현금유출액)'],
        '기타재무활동으로인한현금흐름': 기타재무활동으로인한현금흐름,
        '영업투자재무활동기타현금흐름': 영업투자재무활동기타현금흐름,
        '연결범위변동으로인한현금의증가': 연결범위변동으로인한현금의증가,
        '환율변동효과': 환율변동효과,
        '현금및현금성자산의증가': 현금및현금성자산의증가,
        '기초현금및현금성자산': 기초현금및현금성자산,
        '기말현금및현금성자산': 기말현금및현금성자산
    })
    return cf_domestic


def getCF(rpt_type, freq, table='cf_kr', save=False):
    df = pd.DataFrame()
    stock_list = GetPrice.getStockList()
    for stock in stock_list.values:
        ret = getCFData(stock, rpt_type, freq)
        df = df.append(ret)

    df = df.reset_index(drop=True)

    if not save:
        return df
    if table == '':
        print('저장될 table 이름을 입력해 주세요')
        return df

    try:
        dtype = {
            '종목코드': sqlalchemy.types.VARCHAR(10),
            '종목명': sqlalchemy.types.TEXT(),
            '시장': sqlalchemy.types.VARCHAR(20),
            '기간': sqlalchemy.types.DATE(),
            '컬럼': sqlalchemy.types.VARCHAR(45),
            '영업활동으로인한현금흐름': sqlalchemy.types.FLOAT(),
            '당기순손익': sqlalchemy.types.FLOAT(),
            '법인세비용차감전계속사업이익': sqlalchemy.types.FLOAT(),
            '현금유출이없는비용등가산': sqlalchemy.types.FLOAT(),
            '감가상각비': sqlalchemy.types.FLOAT(),
            '현금유입이없는수익등차감': sqlalchemy.types.FLOAT(),
            '영업활동으로인한자산부채변동(운전자본변동)': sqlalchemy.types.FLOAT(),
            '영업에서창출된현금흐름': sqlalchemy.types.FLOAT(),
            '기타영업활동으로인한현금흐름': sqlalchemy.types.FLOAT(),
            '투자활동으로인한현금흐름': sqlalchemy.types.FLOAT(),
            '투자활동으로인한현금유입액': sqlalchemy.types.FLOAT(),
            '투자활동으로인한현금유출액': sqlalchemy.types.FLOAT(),
            '기타투자활동으로인한현금흐름': sqlalchemy.types.FLOAT(),
            '재무활동으로인한현금흐름': sqlalchemy.types.FLOAT(),
            '재무활동으로인한현금유입액': sqlalchemy.types.FLOAT(),
            '재무활동으로인한현금유출액': sqlalchemy.types.FLOAT(),
            '기타재무활동으로인한현금흐름': sqlalchemy.types.FLOAT(),
            '영업투자재무활동기타현금흐름': sqlalchemy.types.FLOAT(),
            '연결범위변동으로인한현금의증가': sqlalchemy.types.FLOAT(),
            '환율변동효과': sqlalchemy.types.FLOAT(),
            '현금및현금성자산의증가': sqlalchemy.types.FLOAT(),
            '기초현금및현금성자산': sqlalchemy.types.FLOAT(),
            '기말현금및현금성자산': sqlalchemy.types.FLOAT(),
        }
        en = EngineDB()
        engine = en.getEngine()

        for i in range(0, len(df)):
            if dataTimeCheck(df['종목코드'].iloc[i], df['기간'].iloc[i], df['컬럼'].iloc[i], table):
                # 저장된 데이터 없으면 db 저장 실행
                input = pd.DataFrame(df.iloc[i])
                input = input.transpose()
                input.to_sql(name=table, con=engine, if_exists='append', index=False, dtype=dtype)

        # df.to_sql(name=table, con=engine, if_exists='append', index=False, dtype=dtype)

        engine.dispose()
        print('get_cf save complete {}_{}'.format(rpt_type.upper(), freq.upper()))
    except:
        print('get_cf save failed {}_{}'.format(rpt_type.upper(), freq.upper()))
        return df
    return df
