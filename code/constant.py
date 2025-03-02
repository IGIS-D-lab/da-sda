# asset info


ifundaum_aureum_cols = [
    "펀드코드",
    "약칭",
    "펀드명",
    "투자자산유형",
    "자산수",
    "연면적(평)",
    "자산명",
    "운용상태",
    "설정일",
    "만기일",
    "FS_기준일자",
    "FS_설정일",
    "FS_해지일",
    "FS_전결산일",
    "FS_차기결산일",
    "FS_기준가",
    "FS_통화",
    "FS_환율",
    "FS_순자산총액",
    "FS_총계정_원본액",
    "FS_총계정_차입금",
    "FS_총계정_임대보증금",
    "FS_총계정_자산",
    "FS_사무수탁사",
    "AUM 입력일자",
    "약정_Equity 총액(원)",
    "약정_Loan 총액(원)",
    "약정_기준일자 임대보증금(원)",
    "약정_AUM(원)",
    "투입_Equity 총액(원)",
    "투입_Loan 총액(원)",
    "투입_기준일자 임대보증금(원)",
    "투입_AUM(원)",
    "수익자정보",
    "대주정보",
    "자동화여부",
    "투자담당자",
    "투자담당팀장",
    "담당부서",
    "담당자",
    "담당자확인",
    "담당자확인일시",
    "담당팀장",
    "담당팀장확인",
    "담당팀장확인일시",
    "국내/해외",
    "펀드구분",
    "위탁운용여부",
    "투자유형",
    "투자전략",
    "모자구분",
    "멀티클래스구분",
    "공모사모구분",
    "펀드구조",
    "당사펀드재간접포함여부",
    "사무수탁사",
    "수탁사"
]

# 펀드코드	약칭	펀드명	투자자산유형	자산수	연면적(평)	자산명	운용상태	설정일	만기일

# # FS 붙이기
# 기준일자	설정일	해지일	전결산일	차기결산일	기준가	통화	환율	순자산총액	총계정_원본액	총계정_차입금	총계정_임대보증금	총계정_자산	사무수탁사


# AUM 입력일자


# # 약정 붙이기
# Equity 총액(원)	Loan 총액(원)	기준일자 임대보증금(원)	AUM(원)

# # 투입 붙이기
# Equity 총액(원)	Loan 총액(원)	기준일자 임대보증금(원)	AUM(원)

# 수익자정보	대주정보	자동화여부

# 투자담당자	투자담당팀장	담당부서	담당자	담당자확인	담당자확인일시	담당팀장	담당팀장확인	담당팀장확인일시	국내/해외	펀드구분	위탁운용여부	투자유형	투자전략	모자구분	멀티클래스구분	공모사모구분	펀드구조	당사펀드재간접포함여부	사무수탁사	수탁사

view_en_kr_dict = {
    "v_a_fund_mgt": "펀드관리",
    "v_a_fund_aum_mgt": "펀드AUM관리",
    "v_a_asset_info": "투자자산조회",
    "v_a_bene_info": "수익자정보조회",
    "v_a_lend_info": "대주정보조회"
}

view_name_en_col_kr_col_dict = {
    "v_a_fund_mgt": {
        "FUND_CD": "펀드코드",
        "FUND_NUM": "약칭",
        "FUND_NM": "펀드명",
        "INVT_ASET_NM": "자산명",
        "BENE_INFO": "수익자 정보",
        "LNDR_INFO": "대주정보",
        "MNGT_STAT": "운용상태코드",
        "MNGT_STAT_NM": "운용상태",
        "DVLP_YN": "개발여부",
        "DMAB_CLS": "국내해외구분코드",
        "DMAB_CLS_NM": "국내해외구분명",
        "INVT_ASET_TYPE": "투자자산유형코드",
        "PROP_TYPE_NM": "투자자산유형",
        "FUND_CLS": "펀드구분코드",
        "FUND_CLS_NM": "펀드구분",
        "CNCO": "수탁사코드",
        "CNCO_NM": "수탁사",
        "AGCO": "사무수탁사코드",
        "AGCO_NM": "사무수탁사",
        "PM": "PM",
        "VENDOR": "판매사",
        "POPE_CLS": "공모사모구분코드",
        "POPE_CLS_NM": "공모사모구분",
        "ACNG_AUDT_YN": "회계감사여부",
        "MANG_INCR_EMPL_NO": "운용담당자사번",
        "MANG_INCR_EMPL_NM": "운용담당자명",
        "MANG_PTPR_EMPL_NO": "운용담당팀장사번",
        "MANG_PTPR_EMPL_NM": "운용담당팀장명",
        "MANG_DEPT_NM": "운용담당부서",
        "INCR_EMPL_NO": "입력담당자사번",
        "EMPL_NM": "입력담당자명",
        "INCH_DEPT_CD": "입력담당부서코드",
        "DEPT_NM": "입력담당팀",
        "PTPR_EMPL_NO": "입력담당팀장사번",
        "PART_EMPL_NM": "입력담당팀장명",
        "USE_YN": "사용여부",
        "FST_ESDT": "펀드설정일",
        "MNGT_BGDT": "운용개시일",
        "MTRT_PRDT": "만기예정일",
        "MNGT_EXDT": "운용만료일",
        "FUND_LQDT": "펀드청산일",
        "INVT_TYPE": "투자유형코드",
        "INVT_TYPE_NM": "투자유형",
        "INVT_STTG": "투자전략코드",
        "INVT_STTG_NM": "투자전략",
        "MOJA_CLS": "모자구분코드",
        "MOJA_CLS_NM": "모자구분",
        "MULTI_CLAS_CLS": "멀티클래스구분코드",
        "MULTI_CLAS_CLS_NM": "멀티클래스구분",
        "FUND_STRR": "펀드구조코드",
        "FUND_STRRNM": "펀드구조",
        "THCO_FUND_FOF_INCU_YN": "당사펀드재간접포함여부",
        "SDYN": "SHAREDEAL여부",
        "SDST_TLDT": "SHAREDEAL체결일",
        "CPCL_MNNR_YN": "CAPITALCALL방식여부"
}
,
    "v_a_fund_aum_mgt": {
        "STD_DT": "기준일자",
        "FUND_CD": "펀드코드",
        "FUND_NO": "약칭",
        "FUND_NM": "펀드명",
        "INVT_ASET_NM": "자산명",
        "RLES_CNT": "자산갯수",
        "PROP_SUM_PY": "자산면적합계(평)",
        "INVT_ASET_TYPE": "투자자산유형코드",
        "PROP_TYPE_NM": "투자자산유형",
        "MNGT_STAT": "운용상태코드",
        "MNGT_STAT_NM": "운용상태",
        "FST_ESDT": "펀드설정일",
        "MTRT_PRDT": "만기예정일",
        "EQTY_TTAM_STPL_STND": "EQUITY총액_약정",
        "LOAN_TTAM_STPL_STND": "LOAN총액_약정",
        "DPST_STPL_STND": "기준일자보증금_약정",
        "EQTY_TTAM_INPT_STND": "EQUITY총액_투입",
        "LOAN_TTAM_INPT_STND": "LOAN총액_투입",
        "DPST_INPT_STND": "기준일자보증금_투입",
        "MANG_INCR_EMPL_NO": "운용담당자_사번",
        "MANG_INCR_EMPL_NM": "운용담당자",
        "MANG_PTPR_EMPL_NO": "운용담당팀장_사번",
        "MANG_PTPR_EMPL_NM": "운용담당팀장",
        "INCR_EMPL_NO": "입력담당자_사번",
        "INCR_EMPL_NM": "입력담당자",
        "INCR_CFMN_YN": "입력담당자_확인여부",
        "INCR_CFMN_LMSM": "입력담당자_확인일자",
        "PTPR_EMPL_NO": "입력자담당팀장_사번",
        "PTPR_EMPL_NM": "입력자담당팀장",
        "PTPR_CFMN_YN": "입력자담당팀장_확인여부",
        "PTPR_CFMN_LMSM": "입력자담당팀장_확인일자"
},
    "v_a_asset_info": {
        "FUND_CD": "펀드코드",
        "FUND_NUM": "약칭",
        "FUND_NM": "펀드명",
        "SQNO": "순번",
        "INVT_ASET_TYPE": "자산유형코드",
        "PROP_TYPE_NM": "투자자산유형",
        "ASET_CD": "자산코드",
        "INVT_ASET_TYPE_NM": "자산유형",
        "INVT_ASET_NM": "자산명",
        "INVT_TRGT_LCTN": "국내해외구분코드",
        "INVT_TRGT_LCTN_NM": "국내해외구분명",
        "INVT_TRGT_LCTN_AREA": "투자지역코드",
        "INVT_TRGT_LCTN_AREANM": "투자지역",
        "INVT_TRGT_LCTN_ADDR": "이하주소",
        "INDV_BLDG_YEAR_SQMS_SQMT": "M²",
        "INDV_BLDG_YEAR_SQMS_PYNG": "평",
        "RMNU": "객실수(실)",
        "RLES_CNT": "부동산수(동)",
        "CMPN_DATE": "준공(예정일)",
        "REMD_YN": "여부",
        "REMD_CMPN_DATE": "완료일",
        "CAP_RATE": "CAP.RATE(%)",
        "BYNG_NOI": "NOI",
        "BYNG_VCRT": "공실률(%)",
        "NOTE": "비고",
        "LOAN_CRNC": "대출통화",
        "LOAN_CRNCNM": "대출통화명",
        "LOAN_AMT": "대출금액",
        "MNGT_STAT": "운용상태코드",
        "MNGT_STAT_NM": "운용상태",
        "FUND_CLS": "펀드구분코드",
        "FUND_CLS_NM": "펀드구분",
        "POPE_CLS": "공모사모구분코드",
        "POPE_CLS_NM": "공모사모구분",
        "FST_ESDT": "펀드설정일",
        "MNGT_BGDT": "운용개시일",
        "INVT_TYPE": "투자유형코드",
        "INVT_TYPE_NM": "투자유형",
        "INVT_STTG": "투자전략코드",
        "INVT_STTG_NM": "투자전략",
        "MOJA_CLS": "모자구분코드",
        "MOJA_CLS_NM": "모자구분",
        "MULTI_CLAS_CLS": "멀티클래스구분코드",
        "MULTI_CLAS_CLS_NM": "멀티클래스구분",
        "THCO_FUND_FOF_INCU_YN": "당사펀드재간접포함여부"
},
    "v_a_bene_info": {
        "STD_DT": "기준일",
        "FUND_CD": "펀드코드",
        "FUND_NUM": "약칭",
        "FUND_NM": "펀드명",
        "INVT_ASET_TYPE": "투자자산유형코드",
        "PROP_TYPE_NM": "투자자산유형",
        "INVT_ASET_NM": "자산명",
        "MNGT_STAT": "운용상태코드",
        "MNGT_STAT_NM": "운용상태",
        "FST_ESDT": "펀드설정일",
        "MTRT_PRDT": "만기예정일",
        "BNFC_CD": "수익자코드",
        "BNFC_NM": "수익자",
        "INVR_CLCD_NM": "수익자구분",
        "BNFC_DVCD_NM": "수익자분류",
        "FST_STPL_DAY": "최초약정일",
        "STPL_CALL_DATE": "약정콜일자",
        "TOT_STPL_AMT_WNCR": "총약정금액_원화",
        "STPL_CALL_BURN_AMT_WNCR": "약정콜소진금액_원화",
        "NOTE": "비고"
},
    "v_a_lend_info": {
        "STD_DT": "기준일",
        "FUND_CD": "펀드코드",
        "FUND_NUM": "약칭",
        "FUND_NM": "펀드명",
        "INVT_ASET_TYPE": "투자자산유형코드",
        "INVT_ASET_TYPENM": "투자자산유형",
        "INVT_ASET_NM": "자산명",
        "MNGT_STAT": "운용상태코드",
        "MNGT_STATNM": "운용상태",
        "FST_ESDT": "펀드설정일",
        "MTRT_PRDT": "만기예정일",
        "LNDR_CD": "대주코드",
        "LNDR_NM": "대주명",
        "SQNO": "순번",
        "LOAN_TTAM_WNCR": "대출약정금액(원)",
        "LOAN_AMT_WNCR": "대출인출잔액(원)",
        "LOAN_ITDT": "대출실행일",
        "LOAN_MRDT": "대출만기일",
        "BROW_TYPE": "차입유형코드",
        "BROW_NM": "차입유형",
        "INTS_TYPE": "이자유형코드",
        "INTS_NM": "이자유형",
        "BSRT": "기준금리",
        "SPRD": "스프레드(가산금리)",
        "LOAN_INRT": "대출금리",
        "ALLIN_INRT": "ALL-IN금리",
        "LOAN_TYPE": "대출유형코드",
        "LOAN_NM": "대출유형",
        "NOTE": "비고"
}
}