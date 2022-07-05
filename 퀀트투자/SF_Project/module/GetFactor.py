import numpy as np
import pandas as pd
from module import GetDBData
from module import ConnectDB
import sqlalchemy
from functools import reduce


class getFactor:
    won = 100000000

    def __init__(self, start_quarter, end_quarter=None, stock_code=None, by='consolidated', freq='q'):
        self.stock_code = stock_code
        self.by = by
        self.freq = freq

        self.start_quarter = start_quarter
        if end_quarter is None:
            end_quarter = start_quarter
        self.end_quarter = end_quarter
        self.by = by
        self.freq = freq

        # 수정주가 데이터 불러오기
        print('수정주가 데이터 로딩')
        self.priceData = GetDBData.get_price(start_quarter, stock_code=stock_code)
        if start_quarter != end_quarter:
            quarter = self.getNextQuarter(start_quarter)
            while quarter <= end_quarter:
                temp = GetDBData.get_price(quarter, stock_code=stock_code)
                self.priceData = pd.concat([self.priceData, temp])
                quarter = self.getNextQuarter(quarter)
        print('수정주가 데이터 로딩 성공')
        self.priceData.sort_values(['종목코드', '기간'])

        # 트레일링 데이터 나중에 불러오기
        if stock_code.upper() == 'LATER':
            return
        # 저장된 모든 트레일링 데이터 불러오기
        if stock_code.upper() == 'LOAD':
            print('저장된 트레일링 데이터 로딩')
            self.trailingData = GetDBData.LoadTrailingData()
            self.Factor = pd.DataFrame(self.trailingData, columns=['종목코드', '종목명', '시장', '기간', '컬럼'])
            return
        # 모든 트레일링 데이터 생성
        if stock_code.upper() == 'ALL':  # 모든 종목의 트레일링 데이터 조회
            # 주식의 종목코드 목록
            print('모든 종목의 트레일링 데이터 생성')
            self.stockList = GetDBData.get_stockcode()  # db상의 종목 데이터 조회
            self.trailingData = pd.DataFrame()
            # 손익, 재무, 현금흐름
            for s in self.stockList:
                temp = GetDBData.get_trailing(s[0], start_quarter, by, freq)
                self.trailingData = pd.concat([self.trailingData, temp])

            quarter = self.getNextQuarter(start_quarter)
            while quarter <= end_quarter:
                for s in self.stockList:
                    temp = GetDBData.get_trailing(s[0], quarter, by, freq)
                    self.trailingData = pd.concat([self.trailingData, temp])
                quarter = self.getNextQuarter(quarter)
            print('트레일링 데이터 로딩 성공')
        # 종목코드가 주어졌을때 한 종목 트레일링 데이터 생성
        elif stock_code is not None:
            # 손익, 재무, 현금흐름 데이터 불러오기
            self.trailingData = GetDBData.get_trailing(stock_code, start_quarter, by, freq)

            quarter = self.getNextQuarter(start_quarter)
            while quarter <= end_quarter:
                temp = GetDBData.get_trailing(stock_code, quarter, by, freq)
                self.trailingData = pd.concat([self.trailingData, temp])
                quarter = self.getNextQuarter(quarter)

        self.trailingData.sort_values(by=['종목코드', '종목명'])
        # 트레일링 데이터의 [종목코드, 종목명, 시장, 기간, 컬럼]을 이용하여 팩터 DataFrame 생성
        self.Factor = pd.DataFrame(self.trailingData, columns=['종목코드', '종목명', '시장', '기간', '컬럼'])

    # 다음 분기
    def getNextQuarter(self, quarter):
        quarter = quarter[0:5] + str(int(quarter[5]) + 1)

        if int(quarter[5]) == 5:
            quarter = str(int(quarter[0:4]) + 1) + 'Q1'
        return quarter

    # 이전 분기
    def getPrevQuarter(self, quarter):
        quarter = quarter[0:5] + str(int(quarter[5]) - 1)

        if int(quarter[5]) == 0:
            quarter = str(int(quarter[0:4]) - 1) + 'Q4'
        return quarter

    def getFilteredRow(self, idx):
        valid = True
        tf = self.trailingData[(self.trailingData['종목코드'] == self.Factor['종목코드'].iloc[idx]) & (
                self.trailingData['기간'] == self.Factor['기간'].iloc[idx])]
        pf = self.priceData[(self.priceData['종목코드'] == self.Factor['종목코드'].iloc[idx]) & (
                self.priceData['기간'] == self.Factor['기간'].iloc[idx])]
        # 데이터 유효성 확인
        if tf.empty or pf.empty:
            valid = False
        return tf, pf, valid

    # 주가수익비율 PER
    def getPER(self):
        self.Factor['PER'] = np.NaN
        self.Factor['EPS'] = np.NaN
        for i in range(len(self.Factor)):
            (tf, pf, valid) = self.getFilteredRow(i)
            if not valid:
                continue
            self.Factor['EPS'].iloc[i] = (tf['당기순이익'].iloc[0] * getFactor.won) / pf['상장주식수'].iloc[0]
            self.Factor['PER'].iloc[i] = pf['종가'].iloc[0] / self.Factor['EPS'].iloc[i]

        self.Factor = self.Factor.replace([np.inf, -np.inf], np.NaN)
        return self.Factor[['종목코드', '종목명', '시장', '기간', '컬럼', 'EPS', 'PER']].dropna().reset_index(drop=True)

    # 주가순자산비율 PBR
    def getPBR(self):
        self.Factor['PBR'] = np.NaN
        self.Factor['BPS'] = np.NaN
        for i in range(len(self.Factor)):
            (tf, pf, valid) = self.getFilteredRow(i)
            if not valid:
                continue
            self.Factor['BPS'].iloc[i] = (tf['자본'].iloc[0] * getFactor.won) / pf['상장주식수'].iloc[0]
            self.Factor['PBR'].iloc[i] = pf['종가'].iloc[0] / self.Factor['BPS'].iloc[i]
        self.Factor.loc[self.Factor['BPS'] < 0, 'BPS'] = 0
        self.Factor = self.Factor.replace([np.inf, -np.inf], np.NaN)
        return self.Factor[['종목코드', '종목명', '시장', '기간', '컬럼', 'BPS', 'PBR']].dropna().reset_index(drop=True)

    # 주가매출비율 PSR
    def getPSR(self):
        self.Factor['SPS'] = np.NaN
        self.Factor['PSR'] = np.NaN
        for i in range(len(self.Factor)):
            (tf, pf, valid) = self.getFilteredRow(i)
            if not valid:
                continue
            self.Factor['SPS'].iloc[i] = (tf['매출액'].iloc[0] * getFactor.won) / pf['상장주식수'].iloc[0]
            self.Factor['PSR'].iloc[i] = pf['종가'].iloc[0] / self.Factor['SPS'].iloc[i]
        self.Factor.loc[self.Factor['PSR'] < 0, 'PSR'] = 0
        self.Factor = self.Factor.replace([np.inf, -np.inf], np.NaN)
        return self.Factor[['종목코드', '종목명', '시장', '기간', '컬럼', 'SPS', 'PSR']].dropna().reset_index(drop=True)

    # 주가현금흐름비율 PCR
    def getPCR(self):
        self.Factor['CFPS'] = np.NaN
        self.Factor['PCR'] = np.NaN
        for i in range(len(self.Factor)):
            (tf, pf, valid) = self.getFilteredRow(i)
            if not valid:
                continue
            self.Factor['CFPS'].iloc[i] = (tf['영업활동으로인한현금흐름'].iloc[0] * getFactor.won) / pf['상장주식수'].iloc[0]
            self.Factor['PCR'].iloc[i] = pf['종가'].iloc[0] / self.Factor['CFPS'].iloc[i]

        self.Factor = self.Factor.replace([np.inf, -np.inf], np.NaN)
        return self.Factor[['종목코드', '종목명', '시장', '기간', '컬럼', 'CFPS', 'PCR']].dropna().reset_index(drop=True)

    # 가치지표 결합하기(2개 팩터 PER, PBR) 상대, 절대점수 산정
    def get2Combo(self, mod, n=None):
        self.getPER()
        self.getPBR()
        s3 = pd.DataFrame()
        # 상대점수 계산
        if mod.upper() == 'RELATIVE':
            s1 = self.getScore(mod=mod, by='PER', floor=1, cap=10, asc=True)
            s2 = self.getScore(mod=mod, by='PBR', floor=0.1, cap=1, asc=True)
            s3 = pd.merge(s1, s2, how='inner')
            s3['2Combo'] = s3['PER_score'] + s3['PBR_score']
        # 절대점수 계산
        elif mod.upper() == 'ABSOLUTE':
            s1 = self.getScore(mod=mod, by='PER', floor=1, cap=10, asc=True)
            s2 = self.getScore(mod=mod, by='PBR', floor=0.1, cap=1, asc=True)
            s3 = pd.merge(s1, s2, how='inner')
            s3['2Combo'] = s3['PER_score'] + s3['PBR_score']

        if n is None:
            return s3
        else:
            return s3.head(n)

    # 가치투자 4대장 콤보
    def get4Combo(self, n=None, asc=True):
        # 상위 종목의 트레일링 데이터 생성
        def getComboData():
            stocks = self.priceData.sort_values(by=['시가총액'], ascending=asc)
            if n is not None:
                stocks = stocks.head(n)

            print('선택 종목의 트레일링 데이터 로딩')
            stockList = pd.DataFrame(stocks, columns=['종목코드', '종목명'])
            comboData = pd.DataFrame()
            # 손익, 재무, 현금흐름
            for s in stockList.values:
                temp = GetDBData.get_trailing(s[0], self.start_quarter, self.by, self.freq)
                comboData = pd.concat([comboData, temp])
            quarter = self.getNextQuarter(self.start_quarter)
            while quarter <= self.end_quarter:
                for s in stockList:
                    temp = GetDBData.get_trailing(s[0], quarter, self.by, self.freq)
                    ComboData = pd.concat([ComboData, temp])
                quarter = self.getNextQuarter(quarter)
            print('트레일링 데이터 로딩 성공')
            return comboData

        # 모든 종목의 트레일링 데이터가 없을 경우 생성
        if self.stock_code.upper() != 'ALL' and self.stock_code.upper() != 'LOAD':
            self.trailingData = getComboData()
            self.Factor = pd.DataFrame(self.trailingData, columns=['종목코드', '종목명', '시장', '기간', '컬럼'])

        # 필요 지표 계산
        self.getPER()
        self.getPBR()
        self.getPSR()
        self.getPCR()
        # 가치 지표의 상대 점수 계산
        s1 = self.getScore('relative', by='PER', floor=1, asc=True)
        s2 = self.getScore('relative', by='PBR', floor=0.1, asc=True)
        s3 = self.getScore('relative', by='PSR', floor=0.1, asc=True)
        s4 = self.getScore('relative', by='PCR', floor=0.1, asc=True)
        df = [s1, s2, s3, s4]
        s5 = reduce(lambda left, right: pd.merge(left, right, on=['종목코드', '종목명', '시장', '기간', '컬럼'], how='inner'), df)
        s5['4Combo'] = s5['PER_score'] + s5['PBR_score'] + s5['PSR_score'] + s5['PCR_score']
        s5.sort_values(by=['4Combo'])
        s5.reset_index(drop=True)
        if n is not None:
            s5 = s5.head(n)
        if not asc:
            s5 = s5.tail(n)
        return s5

    def getEVEBITDA(self):
        self.Factor['EV'] = np.NaN
        self.Factor['EBITDA'] = np.NaN
        self.Factor['EV/EBITDA'] = np.NaN
        for i in range(len(self.Factor)):
            (tf, pf, valid) = self.getFilteredRow(i)
            if not valid:
                continue
            self.Factor['EV'].iloc[i] = pf['시가총액'].iloc[0] + (
                    tf['단기차입금'].iloc[0] + tf['장기차입금'].iloc[0] - tf['기말현금및현금성자산'].iloc[0]) * getFactor.won
            self.Factor['EBITDA'].iloc[i] = (tf['영업이익'].iloc[0] + tf['감가상각비'].iloc[0]) * getFactor.won
            self.Factor['EV/EBITDA'].iloc[i] = self.Factor['EV'].iloc[i] / self.Factor['EBITDA'].iloc[0]
        self.Factor = self.Factor.replace([np.inf, -np.inf], np.NaN)
        return self.Factor[['종목코드', '종목명', '시장', '기간', '컬럼', 'EV', 'EBITDA', 'EV/EBITDA']].dropna().reset_index(drop=True)

    def getEVSales(self):
        self.Factor['EV'] = np.NaN
        self.Factor['EV/Sales'] = np.NaN
        for i in range(len(self.Factor)):
            (tf, pf, valid) = self.getFilteredRow(i)
            if not valid:
                continue
            self.Factor['EV'].iloc[i] = pf['시가총액'].iloc[0] + (
                    tf['단기차입금'].iloc[0] + tf['장기차입금'].iloc[0] - tf['기말현금및현금성자산'].iloc[0]) * getFactor.won
            self.Factor['EV/Sales'].iloc[i] = self.Factor['EV'].iloc[i] / (tf['매출액'].iloc[0] * getFactor.won)
        self.Factor = self.Factor.replace([np.inf, -np.inf], np.NaN)
        return self.Factor[['종목코드', '종목명', '시장', '기간', '컬럼', 'EV', 'EV/Sales']].dropna().reset_index(drop=True)

    def getGraham(self):
        self.Factor['Graham'] = np.NaN
        self.Factor['NCAV'] = np.NaN
        for i in range(len(self.Factor)):
            (tf, pf, valid) = self.getFilteredRow(i)
            if not valid:
                continue
            self.Factor['NCAV'].iloc[i] = (tf['유동자산'].iloc[0] - tf['부채'].iloc[0]) * getFactor.won
            self.Factor['Graham'].iloc[i] = self.Factor['NCAV'].iloc[i] - (pf['시가총액'].iloc[0] * 1.5)
        self.Factor = self.Factor.replace([np.inf, -np.inf], np.NaN)
        return self.Factor[['종목코드', '종목명', '시장', '기간', '컬럼', 'NCAV', 'Graham']].dropna().reset_index(drop=True)

    # 주가수익성장비율 PEG
    def getPEG(self):
        def get4PreQ(period):  # 4분기전 데이터
            return str(int(period[0:5]) - 1) + period[5:]

        self.Factor['EPS Growth'] = np.NaN
        self.Factor['PEG'] = np.NaN
        self.getPER()
        for i in range(0, len(self.Factor)):
            (tf, pf, valid) = self.getFilteredRow(i)
            if not valid:
                continue

            sq4 = get4PreQ(self.Factor['기간'].iloc[i])  # 4분기 이전의 기간
            # 4분기 이전 팩터
            sff = self.Factor[(self.Factor['종목코드'] == self.Factor['종목코드'].iloc[i]) & (
                    self.Factor['기간'] == sq4)]
            # 4분기 이전의 값 존재하지 않을 경우
            if sff.empty:
                continue

            self.Factor['EPS Growth'].iloc[i] = (self.Factor['EPS'].iloc[i] - sff['EPS'].iloc[0]) / abs(
                sff['EPS'].iloc[0]) * 100
            self.Factor['PEG'].iloc[i] = (pf['종가'].iloc[0] / self.Factor['EPS'].iloc[i]) / \
                                         self.Factor['EPS Growth'].iloc[i]
        self.Factor = self.Factor.replace([np.inf, -np.inf], np.NaN)
        return self.Factor[['종목코드', '종목명', '시장', '기간', '컬럼', 'EPS', 'EPS Growth', 'PEG']].dropna().reset_index(drop=True)

    # 자산대비이익 ROA
    def getROA(self):
        self.Factor['ROA'] = np.NaN
        for i in range(len(self.Factor)):
            (tf, pf, valid) = self.getFilteredRow(i)
            if not valid:
                continue
            self.Factor['ROA'].iloc[i] = tf['당기순이익'].iloc[0] / (
                    (tf['기초현금및현금성자산'].iloc[0] + tf['기말현금및현금성자산'].iloc[0]) / 2)
        self.Factor = self.Factor.replace([np.inf, -np.inf], np.NaN)
        return self.Factor[['종목코드', '종목명', '시장', '기간', '컬럼', 'ROA']].dropna().reset_index(drop=True)

    # 자본대비이익 ROE
    def getROE(self):
        def get4PreQ(period):
            return str(int(period[0:5]) - 1) + period[5:]

        self.Factor['ROE'] = np.NaN
        for i in range(0, len(self.Factor)):
            (tf, pf, valid) = self.getFilteredRow(i)
            if not valid:
                continue
            ''' 4분기 전의 값 존재시
            sq4 = get4PreQ(self.Factor['기간'].iloc[i]) # 4분기 이전의 기간
            # 4분기 이전 트레일링 데이터
            stf = self.trailingData[(self.trailingData['종목코드'] == self.Factor['종목코드'].iloc[i]) & (
                    self.trailingData['기간'] == sq4)]
            # 4분기 이전의 값 존재하지 않을 경우
            if stf.empty:
                continue
            self.Factor['ROE'].iloc[i] = tf['당기순이익'].iloc[0] / ((tf['자본'].iloc[0] + stf['자본'].iloc[0]) / 2)
            '''
            # 4분기 이전 값 미존재
            self.Factor['ROE'].iloc[i] = tf['당기순이익'].iloc[0] / tf['자본'].iloc[0]
        self.Factor = self.Factor.replace([np.inf, -np.inf], np.NaN)
        return self.Factor[['종목코드', '종목명', '시장', '기간', '컬럼', 'ROE']].dropna().reset_index(drop=True)

    # 잔여이익모델 RIM, P/RIM
    def getRIM(self, con):
        def getBPS():
            self.Factor['BPS'] = np.NaN
            for i in range(len(self.Factor)):
                (tf, pf, valid) = self.getFilteredRow(i)
                if not valid:
                    continue
                self.Factor['BPS'].iloc[i] = (tf['자본'].iloc[0] * self.won) / pf['상장주식수'].iloc[0]

        cons = '{:.2f}'.format(con)
        self.Factor['RIM' + str(cons)[-2:]] = np.NaN
        self.Factor['P/RIM']
        getBPS()
        self.getROE()
        self.Factor['ROE3AVG'] = self.Factor['ROE'].rolling(12).mean()
        self.Factor['RIM' + str(cons)[-2:]] = self.Factor['BPS'] * self.Factor['ROE3AVG'] / con
        self.Factor.loc[(self.Factor['당기순이익'] < 0) | (self.Factor['자본'] < 0)] = np.NaN
        for i in range(len(self.Factor)):
            (tf, pf, valid) = self.getFilteredRow(i)
            if not valid:
                continue
            self.Factor['P/RIM'].iloc[i] = pf['종가'].iloc[0] / self.Factor['RIM' + str(cons)[-2:]]
        self.Factor = self.Factor.replace([np.inf, -np.inf], np.NaN)
        return self.Factor[['종목코드', '종목명', '시장', '기간', '컬럼', 'BPS', 'ROE', 'ROE3AVG', 'RIM']].dropna().reset_index(drop=True)

    def getGPA(self):
        self.Factor['GP/A'] = np.NaN
        for i in range(len(self.Factor)):
            (tf, pf, valid) = self.getFilteredRow(i)
            if not valid:
                continue
            self.Factor['GP/A'].iloc[i] = tf['매출총이익'].iloc[0] / tf['자산'].iloc[0]
        self.Factor = self.Factor.replace([np.inf, -np.inf], np.NaN)
        return self.Factor[['종목코드', '종목명', '시장', '기간', '컬럼', 'GP/A']].dropna().reset_index(drop=True)

    # 안정성지표
    def getStabilityIdx(self):
        self.Factor['부채비율'] = np.NaN
        self.Factor['차입금비율'] = np.NaN
        for i in range(len(self.Factor)):
            (tf, pf, valid) = self.getFilteredRow(i)
            if not valid:
                continue
            self.Factor['부채비율'].iloc[i] = tf['부채'].iloc[0] / tf['자본'].iloc[0] * 100
            self.Factor['차입금비율'].iloc[i] = (tf['단기차입금'].iloc[0] + tf['장기차입금'].iloc[0]) / tf['자본'].iloc[0] * 100
        self.Factor = self.Factor.replace([np.inf, -np.inf], np.NaN)
        return self.Factor[['종목코드', '종목명', '시장', '기간', '컬럼', '부채비율', '차입금비율']].dropna().reset_index(drop=True)

    # 성장율 지표
    def getGrowthRate(self):
        def get12PreQ(period):
            return str(int(period[0:5]) - 3) + period[5:]

        self.Factor['매출액증가율'] = np.NaN
        self.Factor['자산증가율'] = np.NaN
        self.Factor['영업이익증가율'] = np.NaN
        self.Factor['당기순이익증가율'] = np.NaN

        for i in range(len(self.Factor)):
            (tf, pf, valid) = self.getFilteredRow(i)
            if not valid:
                continue
            sq12 = get12PreQ(self.Factor['기간'].iloc[i])  # 12분기 이전의 기간(3년)
            # 4분기 이전 트레일링 데이터
            stf = self.trailingData[(self.trailingData['종목코드'] == self.Factor['종목코드'].iloc[i]) & (
                    self.trailingData['기간'] == sq12)]
            # 4분기 이전의 값 존재하지 않을 경우
            if stf.empty:
                continue
            self.Factor['매출액증가율'].iloc[i] = (tf['매출액'].iloc[0] + stf['매출액'].iloc[0]) / abs(
                stf['매출액'].iloc[0])
            self.Factor['자산증가율'].iloc[i] = (tf['자산'].iloc[0] + stf['자산'].iloc[0]) / abs(
                stf['자산'].iloc[i])
            self.Factor['당기순이익증가율'].iloc[i] = (tf['당기순이익'].iloc[0] + stf['당기순이익'].iloc[0]) / abs(
                stf['당기순이익'].iloc[0])
            self.Factor['영업이익증가율'].iloc[i] = (tf['영업이익'].iloc[0] + stf['영업이익'].iloc[0]) / abs(
                stf['영업이익'].iloc[0])
        self.Factor = self.Factor.replace([np.inf, -np.inf], np.NaN)
        return self.Factor[['종목코드', '종목명', '시장', '기간', '컬럼', '매출액증가율', '자산증가율', '영업이익증가율', '당기순이익증가율']].dropna().reset_index(drop=True)

    # 회전율 지표
    def getTurnoverRatio(self):
        def get4PreQ(period):
            return str(int(period[0:5]) - 3) + period[5:]

        self.Factor['총자산회전율'] = np.NaN
        self.Factor['매출채권회전율'] = np.NaN
        self.Factor['재고자산회전율'] = np.NaN

        for i in range(len(self.Factor)):
            (tf, pf, valid) = self.getFilteredRow(i)
            if not valid:
                continue

            sq4 = get4PreQ(self.Factor['기간'].iloc[i])  # 4분기 이전의 기간
            # 4분기 이전 트레일링 데이터
            stf = self.trailingData[(self.trailingData['종목코드'] == self.Factor['종목코드'].iloc[i]) & (
                    self.trailingData['기간'] == sq4)]
            # 4분기 이전의 값 존재하지 않을 경우
            if stf.empty:
                continue
            self.Factor['총자산회전율'].iloc[i] = tf['매출액'].iloc[0] / ((tf['자본'].iloc[0] + stf['자본'].iloc) / 2)
            self.Factor['매출채권회전율'].iloc[i] = tf['매출액'].iloc[0] / (
                    ((tf['매출채권및기타유동채권'].iloc[0] + tf['장기매출채권및기타비유동채권'].iloc[0]) + (
                            stf['매출채권및기타유동채권'].iloc[0] + stf['장기매출채권및기타비유동채권'].iloc[0])) / 2)
            self.Factor['재고자산회전율'].iloc[i] = tf['매출원가'].iloc[0] / ((tf['재고자산'].iloc[0] + stf['재고자산'].iloc[0]) / 2)

        self.Factor = self.Factor.replace([np.inf, -np.inf], np.NaN)
        return self.Factor[['종목코드', '종목명', '시장', '기간', '컬럼', '총자산회전율', '매출채권회전율', '재고자산회전율']].dropna().reset_index(drop=True)

    # 혜자가 있는 기업
    def getMoatIdx(self):
        self.Factor['매출총이익율'] = np.NaN
        self.Factor['영업이익율'] = np.NaN
        self.Factor['순이익율'] = np.NaN

        for i in range(len(self.Factor)):
            (tf, pf, valid) = self.getFilteredRow(i)
            if not valid:
                continue
            self.Factor['매출총이익율'].iloc[i] = tf['매출총이익'].iloc[0] / tf['매출액'].iloc[0]
            self.Factor['영업이익율'].iloc[i] = tf['영업이익'].iloc[0] / tf['매출액'].iloc[0]
            self.Factor['순이익율'].iloc[i] = tf['당기순이익'].iloc[0] / tf['매출액'].iloc[0]

        self.Factor = self.Factor.replace([np.inf, -np.inf], np.NaN)
        return self.Factor[['종목코드', '종목명', '시장', '기간', '컬럼', '매출총이익율', '영업이익율', '순이익율']].dropna().reset_index(drop=True)

    # 팩터를 floor, cap으로 필터링 하여 반환
    def getFiltered(self, by, floor=None, cap=None, asc=True):
        # floor, cap으로 필터링 해준다.
        if floor is not None and cap is not None:
            filtered = self.Factor.loc[(self.Factor[by] >= floor) & (self.Factor[by] < cap)]
        elif floor is not None and cap is None:
            filtered = self.Factor.loc[(self.Factor[by] >= floor)]
        elif floor is None and cap is not None:
            filtered = self.Factor.loc[(self.Factor[by] < cap)]
        else:
            filtered = self.Factor

        filtered = filtered.sort_values(by=by, ascending=asc)
        filtered = filtered.dropna()  # 결측치 제거
        filtered = filtered.reset_index(drop=True)  # 인덱스 리셋

        return filtered

    # 지표에 대하여 절대/상대 점수 산출
    def getScore(self, mod, by, floor=None, cap=None, asc=True):
        calc = self.getFiltered(by, floor, cap, asc)
        calc[by + '_score'] = np.NaN  # 팩터의 상대 점수를 저장할 컬럼
        # 상대 점수 계산 함수
        if mod.upper() == 'RELATIVE':
            # 전체 종목에 대하여 순위를 매기고 이에 대하여 상대 점수를 매긴다.
            for i in range(len(calc)):
                calc[by + '_score'].iloc[i] = 100 - (((calc.index[i] + 1) / len(calc)) * 100)

        # 절대점수 계산 floor, cap 존재 필요
        if mod.upper() == 'ABSOLUTE':
            div = cap - floor
            for i in range(len(calc)):
                temp = int((calc[by].iloc[i] - floor) / div * 1000)
                if temp < 0 or temp == np.inf:
                    temp = 0
                calc[by + '_score'].iloc[i] = (1000 - temp) / 10
        return calc[['종목코드', '종목명', '시장', '기간', '컬럼', by + '_score']]

    # 결측치 처리
    def setMissingValue(self):
        self.Factor = self.Factor.replace([np.inf, -np.inf], np.NaN)
        self.Factor = self.Factor.dropna()
        self.Factor = self.Factor.reset_index(drop=True)

    # DB에 데이터가 저장되어 있는지 확인
    def dataExistCheck(code, stock_code, column, period):
        en = ConnectDB.EngineDB()
        engine = en.getEngine()
        sql = "SELECT EXISTS (SELECT 종목코드, 컬럼, 기간 FROM trailingData " \
              "where 종목코드='{}' and 컬럼='{}' and 기간='{}') as exist".format(stock_code, column, period)
        exist = pd.read_sql_query(sql, engine)
        if exist.values == 1:
            return False
        else:
            return True

    # 트레일링 데이터 저장
    def SaveTrailingData(self):
        dtype = {
            # 손익계산서
            '종목코드': sqlalchemy.types.VARCHAR(10),
            '종목명': sqlalchemy.types.TEXT(),
            '시장': sqlalchemy.types.VARCHAR(20),
            '기간': sqlalchemy.types.VARCHAR(20),
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
            '비지배주주순이익': sqlalchemy.types.FLOAT(),
            # 재무상태표
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
            '비지배주주순지분': sqlalchemy.types.FLOAT(),
            # 현금흐름표
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
        en = ConnectDB.EngineDB()
        engine = en.getEngine()

        for i in range(0, len(self.trailingData)):
            if self.dataExistCheck(self.trailingData['종목코드'].iloc[i], self.trailingData['컬럼'].iloc[i],
                                   self.trailingData['기간'].iloc[i]):
                # 저장된 데이터 없으면 db 저장 실행
                input = pd.DataFrame(self.trailingData.iloc[i])
                input.to_sql(name='trailingData', con=engine, if_exists='append', index=False, dtype=dtype)

        # df.to_sql(name=table, con=engine, if_exists='append', index=False, dtype=dtype)

        engine.dispose()
