from module import GetFactor

# 한개 종목 데이터 불러오기
# s1 = GetFactor.getFactor(stock_code='005930', start_quarter='2022Q1')
# # 한개 종목 팩터 불러오기
# s1.getPER()
# s1.getPBR()
# s1.getPSR()
# s1.getPCR()
# s1.getEVEBITDA()
# s1.getEVSales()
# s1.getGraham()

# 모든 종목 데이터 불러오기
# stocks = GetFactor.getFactor(stock_code='ALL', start_quarter='2022Q1')
# 모든 종목의 트레일링 데이터와 주가 데이터를 불러오는데 시간이 오래 소요되므로 DB에 저장된 데이터 불러옴
''' Params
start_quarter: 시작 기간
end_quarter: None, 종료 기간
stock_code: None, 'ALL': 모든 데이터 조회, 'LATER': 데이터 나중에 조회, 'LOAD': 모든 데이터 로딩, 종목코드
by: 'consolidated',
freq: 'q'
'''
stocks = GetFactor.getFactor(stock_code='LOAD', start_quarter='2022Q1')
per = stocks.getPER()
pbr = stocks.getPBR()
psr = stocks.getPSR()
pcr = stocks.getPCR()
evebitda = stocks.getEVEBITDA()
evsales = stocks.getEVSales()
graham = stocks.getGraham()
roa = stocks.getROA()
# ROE 4분기전 데이터 필요
#stocks.getROE()
# RIM 계수 입력을 통해 여러 계수에 따른 RIM 계산 가능
# 12분기 데이터 필요
#stocks.getRIM(0.1)
gpa = stocks.getGPA()
stable = stocks.getStabilityIdx()
# 성장율 지표 12분기간 데이터 필요
# stocks.getGrowthRate()
# 회전율 지표 4분기전 데이터 필요
# stocks.getTurnoverRatio()
moat = stocks.getMoatIdx()

Combo2_absolute = stocks.get2Combo('absolute')
Combo2_relative = stocks.get2Combo('relative')
Combo4 = stocks.get4Combo()
stocks.setMissingValue()