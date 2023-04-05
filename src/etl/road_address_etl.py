# TODO
# local test : 8 min 58 sec
import os
import sys
import time
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), 'lib'))
from LogManager import LogManager
from DBManager import PgsqlConnection

def road_name_address_etl(logger, f_path, f_name, field_sep, line_sep):
    result = False
    logger.trace_log("info", f"{f_name} road_name_address etl Start")
    copy_f_path = os.path.join(f_path, f"{f_name}.copy")
    # DB Setting
    db = PgsqlConnection()
    try:
        s_time = time.time()
        # rnaddr file format(파일 변환 시작)
        # 도로명주소관리번호|법정동코드|시도명|시군구명|읍면동명|리명|산여부|번지|호|도로명코드|도로명|지하여부|건물본번|건물부번|
        # 행정동코드|행정동명|기초구역번호(우편번호)|이전도로명주소|효력발생일|공동주택구분|이동사유코드|건축물대장건물명|시군구용건물명|비고
        with open(copy_f_path, 'w') as wf:
            with open(os.path.join(f_path, f_name), 'r') as rf:
                for line in rf.readlines():
                    #wf.write(line.replace('|', field_sep) + line_sep)
                    now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    # Sep 방어로직
                    line = line.replace('\t', ' ').replace('\n', '\\n')
                    line_list = line.split('|')
                    extn_line_list = list()
                    extn_line_list.append(line_list[0])     # rna_mgmt_num | 도로명주소관리번호
                    extn_line_list.append(line_list[1])     # legal_dong_code | 법정동코드
                    extn_line_list.append(line_list[2])     # sido | 시도명
                    extn_line_list.append(line_list[3])     # sigungu | 시군구명
                    extn_line_list.append(line_list[4])     # eupmyeondong | 읍면동명
                    extn_line_list.append(line_list[5])     # li | 리명
                    extn_line_list.append(line_list[6])     # san_ysno | 산여부
                    extn_line_list.append(line_list[7])     # land_num | 번지
                    extn_line_list.append(line_list[8])     # ho | 호
                    extn_line_list.append(line_list[9])     # rna_code | 도로명코드
                    extn_line_list.append(line_list[10])    # road_name | 도로명
                    extn_line_list.append(line_list[11])    # basement_ysno | 지하여부
                    extn_line_list.append(line_list[12])    # bldg_main_num | 건물본번
                    extn_line_list.append(line_list[13])    # bldg_sub_num | 건물부번
                    extn_line_list.append(line_list[14])    # admin_dong_code | 행정동코드
                    extn_line_list.append(line_list[15])    # admin_dong_name | 행정동명
                    extn_line_list.append(line_list[16])    # zip_code | 기초구역번호(우편번호)
                    extn_line_list.append(line_list[17])    # prev_rna | 이전도로명주소
                    extn_line_list.append(line_list[18])    # effect_date | 효력발생일
                    extn_line_list.append(line_list[19])    # apt_ysno | 공동주택구분
                    extn_line_list.append(line_list[21])    # bldg_name | 건축물대장건물명
                    extn_line_list.append("python-batch")   # cret_id | 생성자ID
                    extn_line_list.append(now_time)         # cret_dttm | 생성일시
                    extn_line_list.append("python-batch")   # amnd_id | 수정자ID
                    extn_line_list.append(now_time)         # amnd_dttm | 수정일시
                    wf.write(field_sep.join(extn_line_list) + line_sep)
        convert_to_copy_time = time.time() - s_time
        logger.trace_log("debug", f"{f_name} road_name_address convert to copy file {convert_to_copy_time:.4f} sec.")
        # DB 적재 시작
        copy_result = db.copy_expert(2, 'dev.tb_road_name_addr', copy_f_path)
        if copy_result:
            result = copy_result
            copy_to_db_time = time.time() - s_time - convert_to_copy_time
            logger.trace_log("debug", f"{f_name} road_name_address copy to db {copy_to_db_time:.4f} sec.")
        else:
            raise Exception(f"{f_name} road_name_address copy to db Fail")
    except Exception as e:
        logger.trace_log("error", f"Exception: {e}")
    finally:
        logger.trace_log("info", f"{f_name} road_name_address etl END. total elapse time : {time.time() - s_time:.4f} sec. Status : {str(copy_result)}")
        db.conn.commit()
        del(db)
        return result

def jibun_address_etl(logger, f_path, f_name, field_sep, line_sep):
    result = False
    logger.trace_log("info", f"{f_name} jibun_address etl Start")
    copy_f_path = os.path.join(f_path, f"{f_name}.copy")
    # DB Setting
    db = PgsqlConnection()
    try:
        s_time = time.time()
        # jibun rnaddr file format(파일변환 시작)
        # 도로명주소관리번호|법정동코드|시도명|시군구명|법정읍면동명|법정리명|산여부|
        # 지번본번(번지)|지번부번(호)|도로명코드|지하여부|건물본번|건물부번|이동사유코드
        with open(copy_f_path, 'w') as wf:
            with open(os.path.join(f_path, f_name), 'r') as rf:
                for line in rf.readlines():
                    # wf.write(line.replace('|', field_sep) + line_sep)
                    now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    # Sep 방어로직
                    line = line.replace('\t', ' ').replace('\n', '\\n')
                    line_list = line.split('|')
                    extn_line_list = list()
                    extn_line_list.append(line_list[0])  # rna_mgmt_num | 도로명주소관리번호
                    extn_line_list.append(line_list[1])  # legal_dong_code | 법정동코드
                    extn_line_list.append(line_list[2])  # sido | 시도명
                    extn_line_list.append(line_list[3])  # sigungu | 시군구명
                    extn_line_list.append(line_list[4])  # eupmyeondong | 읍면동명
                    extn_line_list.append(line_list[5])  # li | 리명
                    extn_line_list.append(line_list[6])  # san_ysno | 산여부
                    extn_line_list.append(line_list[7])  # land_main_num | 지번본번(번지)
                    extn_line_list.append(line_list[8])  # land_sub_num | 지번부번(호)
                    extn_line_list.append(line_list[9])  # rna_code | 도로명코드
                    extn_line_list.append(line_list[10])  # basement_ysno | 지하여부
                    extn_line_list.append(line_list[11])  # bldg_main_num | 건물본번
                    extn_line_list.append(line_list[12])  # bldg_sub_num | 건물부번
                    extn_line_list.append("python-batch")  # cret_id | 생성자ID
                    extn_line_list.append(now_time)  # cret_dttm | 생성일시
                    extn_line_list.append("python-batch")  # amnd_id | 수정자ID
                    extn_line_list.append(now_time)  # amnd_dttm | 수정일시
                    wf.write(field_sep.join(extn_line_list) + line_sep)
        convert_to_copy_time = time.time() - s_time
        logger.trace_log("debug", f"{f_name} jibun_address convert to copy file {convert_to_copy_time:.4f} sec.")
        # DB 적재 시작
        copy_result = db.copy_expert(2, 'dev.tb_lot_number_addr', copy_f_path)
        if copy_result:
            result = copy_result
            copy_to_db_time = time.time() - s_time - convert_to_copy_time
            logger.trace_log("debug", f"{f_name} jibun_address copy to db {copy_to_db_time:.4f} sec.")
        else:
            raise Exception(f"{f_name} jibun_address copy to db Fail")
    except Exception as e:
        logger.trace_log("error", f"Exception: {e}")
    finally:
        logger.trace_log("info", f"{f_name} jibun_address etl END. elapse time : {time.time() - s_time:.4f} sec. Status : {str(copy_result)}")
        db.conn.commit()
        del (db)
        return result

def main():
    # Custom logger setting
    logger = LogManager(FILENAME='road_address_etl', LEVEL='DEBUG')

    # variable setting
    work_dir = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), 'data', 'road_address')
    work_file_list = os.listdir(work_dir)
    logger.trace_log("debug", f"road_address_etl work directory set {work_dir}")
    field_sep = '\t'
    line_sep = '\n'

    # road name address Start
    rnaddr_file_list = [x for x in work_file_list if x.startswith("rnaddrkor") and x.endswith(".txt")]
    for f_name in rnaddr_file_list:
        rnaddr_result = road_name_address_etl(logger, work_dir, f_name, field_sep, line_sep)
        if not rnaddr_result:
            logger.trace_log("error", f"etl error occurred in {f_name} file")
            break

    # jibun address Start
    jibun_file_list = [x for x in work_file_list if x.startswith("jibun_rnaddrkor") and x.endswith(".txt")]
    for f_name in jibun_file_list:
        rnaddr_result = jibun_address_etl(logger, work_dir, f_name, field_sep, line_sep)
        if not rnaddr_result:
            logger.trace_log("error", f"etl error occurred in {f_name} file")
            break


if __name__ == "__main__":
    main()