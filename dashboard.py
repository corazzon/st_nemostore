import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
import os
import json
import re
from datetime import datetime, timezone
import plotly.express as px
from bs4 import BeautifulSoup

# 페이지 설정
st.set_page_config(page_title="Nemo Store Analytics Pro", layout="wide", initial_sidebar_state="expanded")

# --- 유틸리티 함수 ---
def format_currency_pro(amount_won):
    """원을 억/만 혼합 및 만원 단위로 변환"""
    if pd.isna(amount_won) or amount_won is None: 
        return {"raw": "-", "man": "-", "uk_man": "-"}
    
    amount_man = amount_won / 10000
    
    # 억/만 표기
    if amount_won >= 100000000:
        uk = int(amount_won // 100000000)
        man = int((amount_won % 100000000) // 10000)
        uk_man_str = f"{uk}억 {man:,}만" if man > 0 else f"{uk}억"
    else:
        uk_man_str = f"{amount_man:,.0f}만"
        
    return {
        "raw": f"{int(amount_won):,}원",
        "man": f"{amount_man:,.1f}만원",
        "uk_man": uk_man_str
    }

def extract_region_from_title(title):
    """제목에서 [지역명] 추출"""
    if not isinstance(title, str): return "기타"
    match = re.search(r'\[(.*?)\]', title)
    return match.group(1) if match else "기타"

# --- HTML 파싱 엔진 ---
class NemoHtmlParser:
    @staticmethod
    def parse_facilities(html_content):
        """주변 500m 시설 정보 추출"""
        if not html_content: return []
        soup = BeautifulSoup(html_content, 'html.parser')
        facilities = []
        items = soup.select('.around-facility-content')
        for item in items:
            name_tag = item.select_one('p.font-14')
            dist_tag = item.select_one('p.text-gray-60')
            if name_tag and dist_tag:
                facilities.append({"시설명": name_tag.text, "거리정보": dist_tag.text})
        return facilities

    @staticmethod
    def parse_building_register(html_content):
        """건축물 대장 정보 추출"""
        if not html_content: return {}
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.select_one('.building-register-information table')
        if not table: return {}
        
        data = {}
        rows = table.find_all('tr')
        for row in rows:
            th = row.find('th')
            td = row.find('td')
            if th and td:
                data[th.text.strip()] = td.text.strip()
        return data

# --- 데이터 로드 및 전처리 ---
@st.cache_data
def load_and_preprocess_data():
    json_path = os.path.join(os.path.dirname(__file__), "sample_response.json")
    db_path = os.path.join(os.path.dirname(__file__), "data", "nemo_store.db")
    
    # 1. JSON 데이터 로드
    items = []
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                items = data.get("items", [])
        except: pass
            
    df_json = pd.DataFrame(items)
    
    # 2. DB 데이터 로드
    df_db = pd.DataFrame()
    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            df_db = pd.read_sql_query("SELECT * FROM nemo_stores", conn)
            conn.close()
        except: pass

    # 3. 데이터 통합 로직
    # DB의 snake_case 컬럼명을 JSON의 camelCase 필드명으로 매핑
    db_to_json_map = {
        'business_large_code_name': 'businessLargeCodeName',
        'business_middle_code_name': 'businessMiddleCodeName',
        'price_type_name': 'priceTypeName',
        'maintenance_fee': 'maintenanceFee',
        'near_subway_station': 'nearSubwayStation',
        'view_count': 'viewCount',
        'favorite_count': 'favoriteCount',
        'created_date_utc': 'createdDateUtc',
        'monthly_rent': 'monthlyRent'
    }
    
    if not df_db.empty:
        df_db = df_db.rename(columns=db_to_json_map)

    if not df_json.empty:
        if not df_db.empty:
            # ID 기준 통합
            df = pd.concat([df_json, df_db[~df_db['id'].isin(df_json['id'])]], ignore_index=True)
        else:
            df = df_json
    else:
        df = df_db

    if df.empty: return pd.DataFrame()

    # 필수 컬럼 보장
    required_cols = ['id', 'number', 'title', 'deposit', 'monthlyRent', 'premium', 'maintenanceFee', 
                    'size', 'businessLargeCodeName', 'businessMiddleCodeName', 'previewPhotoUrl', 
                    'nearSubwayStation', 'viewCount', 'favoriteCount', 'createdDateUtc']
    for col in required_cols:
        if col not in df.columns: df[col] = None

    # NaN 처리 (st.image 등 오류 방지)
    df = df.replace({np.nan: None})

    # 4. 금액 단위 변환 (천원 -> 원)
    money_map = {'deposit': 'deposit_krw', 'monthlyRent': 'monthly_rent_krw', 
                 'premium': 'premium_krw', 'maintenanceFee': 'maintenance_fee_krw'}
    for src, dst in money_map.items():
        val = pd.to_numeric(df[src], errors='coerce').fillna(0)
        df[dst] = val * 1000
        
    # 5. 파생 변수 생성
    df['total_monthly_cost'] = df['monthly_rent_krw'] + df['maintenance_fee_krw']
    df['size'] = pd.to_numeric(df['size'], errors='coerce').fillna(0)
    df['size_pyeong'] = df['size'] / 3.3057
    df['rent_per_size'] = df['monthly_rent_krw'] / df['size'].replace(0, np.nan)
    df['region'] = df['title'].apply(extract_region_from_title)
    
    # 6. 날짜 처리
    try:
        df['created_at_kst'] = pd.to_datetime(df['createdDateUtc']).dt.tz_convert('Asia/Seoul')
    except:
        df['created_at_kst'] = pd.to_datetime(datetime.now())
    
    return df

@st.cache_data
def get_html_data_for_item(item_id):
    """지정된 매물 ID에 대한 HTML 데이터 로드 (현재는 data_json_html.md를 샘플로 사용)"""
    try:
        md_path = os.path.join(os.path.dirname(__file__), "data_json_html.md")
        with open(md_path, "r", encoding="utf-8") as f:
            content = f.read()
            if str(item_id) in content:
                parts = content.split("위 정보에 매핑되는 데이터는 다음 html에 들어 있습니다.")
                if len(parts) > 1:
                    return parts[1]
    except: pass
    return None

# --- UI 컴포넌트 ---
def sidebar_filters(df):
    st.sidebar.header("🏢 필터 설정")
    
    # 업종 필터
    biz_col = 'businessLargeCodeName'
    all_large = ["전체"] + sorted([str(x) for x in df[biz_col].unique() if x is not None])
    selected_large = st.sidebar.multiselect("업종 대분류", all_large, default=["전체"])
    
    filtered_df = df.copy()
    if "전체" not in selected_large and selected_large:
        filtered_df = filtered_df[filtered_df[biz_col].isin(selected_large)]
        
    # 금액 범위
    st.sidebar.subheader("💰 월세 범위 (만원)")
    max_rent = int(pd.to_numeric(df['monthlyRent'], errors='coerce').max() or 1000)
    rent_range = st.sidebar.slider("월세", 0, max_rent, (0, max_rent))
    filtered_df = filtered_df[(pd.to_numeric(filtered_df['monthlyRent'], errors='coerce').fillna(0) >= rent_range[0]) & 
                               (pd.to_numeric(filtered_df['monthlyRent'], errors='coerce').fillna(0) <= rent_range[1])]
    
    return filtered_df

def tab_overview(df):
    st.header("📊 시장 현황 및 핵심 지표")
    if df.empty:
        st.warning("데이터가 없습니다.")
        return

    m1, m2, m3, m4 = st.columns(4)
    with m1: st.metric("총 매물 수", f"{len(df)}건")
    with m2: st.metric("평균 월세", format_currency_pro(df['monthly_rent_krw'].mean())['uk_man'])
    with m3: st.metric("평균 권리금", format_currency_pro(df['premium_krw'].mean())['uk_man'])
    with m4: st.metric("평균 전용면적", f"{df['size'].mean():.1f}㎡")
        
    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📍 지역별 매물 분포")
        region_counts = df['region'].value_counts().reset_index()
        fig_region = px.pie(region_counts, values='count', names='region', hole=0.4)
        st.plotly_chart(fig_region, use_container_width=True)
    with c2:
        st.subheader("🍱 업종별 비중 (Top 10)")
        biz_col = 'businessMiddleCodeName'
        biz_counts = df[biz_col].value_counts().head(10).reset_index()
        fig_biz = px.bar(biz_counts, x='count', y=biz_col, orientation='h', color='count')
        st.plotly_chart(fig_biz, use_container_width=True)

    st.divider()
    st.subheader("📈 가격 데이터 기술통계 및 분포")
    stat_col, hist_col = st.columns([1, 2])
    with stat_col:
        st.write("**기술통계 요약**")
        price_stats = df[['monthly_rent_krw', 'deposit_krw', 'premium_krw']].describe()
        price_stats.columns = ['월세', '보증금', '권리금']
        display_stats = price_stats.copy()
        for col in display_stats.columns:
            display_stats[col] = display_stats[col].apply(lambda x: format_currency_pro(x)['uk_man'] if pd.notna(x) else "-")
        st.table(display_stats)
    with hist_col:
        selected_price = st.selectbox("분포 확인할 지표 선택", ["월세", "보증금", "권리금"])
        price_key = {"월세": "monthly_rent_krw", "보증금": "deposit_krw", "권리금": "premium_krw"}[selected_price]
        fig_hist = px.histogram(df, x=price_key, nbins=30, marginal="box")
        st.plotly_chart(fig_hist, use_container_width=True)

def tab_industry(df):
    st.header("🏢 업종별 시장 분석")
    if df.empty: return
    biz_col = 'businessLargeCodeName'
    large_codes = sorted([str(x) for x in df[biz_col].unique() if x is not None])
    selected_large = st.selectbox("업종 대분류 선택", large_codes)
    sub_df = df[df[biz_col] == selected_large]
    
    st.subheader(f"📍 {selected_large} 부문 중분류 현황")
    agg_df = sub_df.groupby('businessMiddleCodeName').agg({
        'id': 'count',
        'monthly_rent_krw': ['mean', 'median'],
        'premium_krw': 'median',
        'size': 'mean'
    }).reset_index()
    agg_df.columns = ['업종 중분류', '매물 수', '평균 월세', '중앙값 월세', '중앙값 권리금', '평균 면적(㎡)']
    for col in ['평균 월세', '중앙값 월세', '중앙값 권리금']:
        agg_df[col] = agg_df[col].apply(lambda x: format_currency_pro(x)['uk_man'])
    st.dataframe(agg_df, use_container_width=True)

def tab_location(df):
    st.header("🚇 지역 및 역세권 탐색")
    if df.empty: return
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("인기 지하철역 TOP 15")
        subway_stats = df['nearSubwayStation'].value_counts().reset_index()
        fig_subway = px.bar(subway_stats.head(15), x='count', y='nearSubwayStation', orientation='h')
        st.plotly_chart(fig_subway, use_container_width=True)
    with c2:
        st.subheader("동별 평균 임대료")
        region_agg = df.groupby('region')['monthly_rent_krw'].mean().sort_values(ascending=False).reset_index()
        fig_region_bar = px.bar(region_agg, x='region', y='monthly_rent_krw', color='monthly_rent_krw')
        st.plotly_chart(fig_region_bar, use_container_width=True)

def tab_deal_finder(df):
    st.header("🔍 매물 상세 검색")
    if df.empty: return
    col_s1, col_s2 = st.columns([1, 2])
    with col_s1: sort_by = st.selectbox("정렬 기준", ["최신순", "월세 낮은순", "보증금 낮은순", "면적 넓은순"])
    with col_s2: search_query = st.text_input("검색어 입력 (제목, 지역, 역세권)", "")
    
    if search_query:
        mask = df['title'].str.contains(search_query, case=False, na=False) | \
               df['region'].str.contains(search_query, case=False, na=False) | \
               df['nearSubwayStation'].str.contains(search_query, case=False, na=False)
        df = df[mask]
    
    sort_map = {
        "최신순": ("created_at_kst", False),
        "월세 낮은순": ("monthly_rent_krw", True),
        "보증금 낮은순": ("deposit_krw", True),
        "면적 넓은순": ("size", False)
    }
    col, asc = sort_map[sort_by]
    df = df.sort_values(by=col, ascending=asc)
    
    st.write(f"총 {len(df)}건의 매물이 발견되었습니다.")
    for i in range(0, len(df), 3):
        cols = st.columns(3)
        for j in range(3):
            if i + j < len(df):
                item = df.iloc[i + j]
                with cols[j]:
                    with st.container(border=True):
                        img_url = item.get('previewPhotoUrl')
                        if pd.notna(img_url) and isinstance(img_url, str) and img_url.startswith('http'):
                            st.image(img_url, use_container_width=True)
                        else: st.info("이미지 없음")
                        st.markdown(f"### {item['title'] or '제목 없음'}")
                        st.caption(f"{item['businessMiddleCodeName'] or '-'} | {item['size']}㎡")
                        st.write(f"**월세 {int(item['monthlyRent'] or 0)}만 / 보증금 {format_currency_pro(item['deposit_krw'])['uk_man']}**")
                        if st.button("상세 보기", key=f"btn_{item['id']}"):
                            st.session_state.selected_item_id = item['id']
                            st.success(f"{item['title']} 선택됨. '매물 상세' 탭으로 이동하세요.")

def tab_detail(df):
    st.header("🏠 매물 상세 분석")
    selected_id = st.session_state.get('selected_item_id')
    if not selected_id:
        st.info("매물 리스트 탭에서 매물을 선택해 주세요.")
        return
    items = df[df['id'] == selected_id]
    if items.empty:
        st.error("해당 매물을 찾을 수 없습니다.")
        return
    item = items.iloc[0]
    html_content = get_html_data_for_item(selected_id)
    
    st.markdown(f"## {item['title'] or '제목 없음'}")
    c1, c2 = st.columns([1, 1])
    with c1:
        img_url = item.get('previewPhotoUrl')
        if pd.notna(img_url) and isinstance(img_url, str) and img_url.startswith('http'):
            st.image(img_url, use_container_width=True)
        else: st.info("이미지 없음")
            
    with c2:
        st.subheader("💰 상세 가격")
        st.write(f"**보증금**: {format_currency_pro(item['deposit_krw'])['uk_man']}")
        st.write(f"**월세**: {format_currency_pro(item['monthly_rent_krw'])['uk_man']}")
        st.write(f"**권리금**: {format_currency_pro(item['premium_krw'])['uk_man']}")
        st.write(f"**관리비**: {format_currency_pro(item['maintenance_fee_krw'])['uk_man']}")
        st.divider()
        st.markdown(f"### 💵 실질 월 비용: **{format_currency_pro(item['total_monthly_cost'])['uk_man']}**")

    if html_content:
        st.divider()
        st.subheader("📋 건축물 및 입지 정보 (HTML 파싱)")
        parser = NemoHtmlParser()
        t1, t2 = st.tabs(["🏗️ 건축물 정보", "🏥 주변 시설(500m)"])
        with t1:
            build_info = parser.parse_building_register(html_content)
            if build_info: st.table(pd.DataFrame(build_info.items(), columns=["항목", "내용"]))
        with t2:
            facilities = parser.parse_facilities(html_content)
            if facilities: st.table(pd.DataFrame(facilities))

def main():
    df = load_and_preprocess_data()
    if df.empty:
        st.error("데이터 로드 실패")
        return
    filtered_df = sidebar_filters(df)
    tabs = st.tabs(["📊 개요", "🏢 업종 비교", "🚇 지역/역세권", "🔍 매물 리스트", "🏠 매물 상세"])
    with tabs[0]: tab_overview(filtered_df)
    with tabs[1]: tab_industry(filtered_df)
    with tabs[2]: tab_location(filtered_df)
    with tabs[3]: tab_deal_finder(filtered_df)
    with tabs[4]: tab_detail(df)

if __name__ == "__main__":
    main()
