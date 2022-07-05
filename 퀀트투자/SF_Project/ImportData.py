from module import GetPrice, GetFinance
from module import GetDBData

# # 종목 데이터 수집
# ''' Params
# table: 저장될 테이블 이름
# save: DB 저장여부
# return 수집데이터
# '''
# stock_list = GetPrice.getStockList(table_name='stock_list2', save=False)
# # 주가데이터 저장 최종 날짜 조회
# ''' Params
# table_name: 저장된 테이블명
# column: 기간 column명
# '''
# origin_last_date = GetPrice.getStartDate(table_name='stock_origin_price') # 원시주가 마지막 저장 날짜 조회
# revise_last_date = GetPrice.getStartDate(table_name='stock_revise_price') # 수정주가 마지막 저장 날짜 조회
#
# # 원시주가 수집
# ''' Params
# table_name: 테이블이름,
# start_date: 주가수집시작일('%Y%m%d)
# end_date: 주가수집종료일('%Y%m%d)
# save: DB 저장여부(True: 저장, False: 저장X)
# return: 수집데이터 dataframe
# '''
# origin_price = GetPrice.getOriginPrice(start_date='2016-01-01', end_date='2018-01-01', save=True) # 기간 데이터 수집
# origin_price = GetPrice.getOriginPrice(save=True) # 최종 저장일 ~ 현재까지 데이터 수집
#
# # 수정주가 수집
# ''' Params
# table_name: 테이블이름,
# start_date: 주가수집시작일('%Y-%m-%d)
# end_date: 주가수집종료일('%Y-%m-%d)
# save: DB 저장여부(True: 저장, False: 저장X)
# return: 수집데이터 dataframe
# '''
# revise_price = GetPrice.getRevisePrice(start_date='2016-01-01', end_date='2018-01-01', save=True) # 기간 데이터 수집
# GetPrice.getRevisePrice(save=True) # 최종 저장일 ~ 현재까지 데이터 수집
#
# # 손익계산서 수집
# ''' Params
# rpt_type: Consolidated, Unconsolidated(연결/별도)
# freq: A, Q(연간/분기)
# table: 저장할 테이블 이름
# save: DB저장여부(True: 저장, False: 저장X)
# return: 수집데이터 dataframe
# '''
# GetFinance.getIS('consolidated', 'a', 'is_kr', save=True)
# GetFinance.getIS('consolidated', 'q', 'is_kr', save=True)
# GetFinance.getIS('unconsolidated', 'a', 'is_kr', save=True)
# GetFinance.getIS('unconsolidated', 'q', 'is_kr', save=True)
#
# # 재무상태표 수집
# ''' Params
# rpt_type: Consolidated, Unconsolidated(연결/별도)
# freq: A, Q(연간/분기)
# table: 저장할 테이블 이름
# save: DB저장여부(True: 저장, False: 저장X)
# return: 수집데이터 dataframe
# '''
# GetFinance.getBS('consolidated', 'a', 'bs_kr', save=True)
bs = GetFinance.getBS('consolidated', 'q', 'bs_kr', save=True)
# GetFinance.getBS('unconsolidated', 'a', 'bs_kr', save=True)
# GetFinance.getBS('unconsolidated', 'q', 'bs_kr', save=True)
#
# # 현금흐름표 수집
# ''' Params
# rpt_type: Consolidated, Unconsolidated(연결/별도)
# freq: A, Q(연간/분기)
# table: 저장할 테이블 이름
# save: DB저장여부(True: 저장, False: 저장X)
# return: 수집데이터 dataframe
# '''
# GetFinance.getCF('consolidated', 'a', 'cf_kr', save=True)
# GetFinance.getCF('consolidated', 'q', 'cf_kr', save=True)
# GetFinance.getCF('unconsolidated', 'a', 'cf_kr', save=True)
# GetFinance.getCF('unconsolidated', 'q', 'cf_kr', save=True)
#
# # 저장된 수정주가 데이터 확인하기
# ''' Params
# terms: %Y/%s(연도/분기)
# return: 수정주가데이터, 기간
# '''
# stock_price_data = GetDBData.get_price('2018/1')
#
# # 저장된 트레일링 데이터 조회