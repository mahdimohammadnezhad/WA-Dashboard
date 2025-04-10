import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
import os
import yaml # For authenticator config
from yaml.loader import SafeLoader # For authenticator config
import streamlit_authenticator as stauth # For authentication
import geopandas as gpd # For shapefile handling
import zipfile # For unzipping shapefiles
import io # For handling file streams
import plotly.graph_objects as go # For maps
import folium
from streamlit_folium import st_folium

# --- Configuration ---
st.set_page_config(layout="wide", page_title="داشبورد حسابداری آب")

# --- File Paths ---
# Define base paths
try:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    BASE_DIR = os.getcwd() # Fallback

DAM_DATA_PATH = os.path.join(BASE_DIR, 'data/Dam_6Apr25.txt')
GW_DATA_PATH = os.path.join(BASE_DIR, 'data/GW_6Apr25.txt')
TRANSFER_DATA_PATH = os.path.join(BASE_DIR, 'Transfer_Data.txt')
WASTEWATER_DATA_PATH = os.path.join(BASE_DIR, 'Wastewater_Data.txt')

names = ['John Smith', 'Rebecca Briggs']
usernames = ['jsmith', 'rbriggs']
passwords = ['123', '456']

hashed_passwords = stauth.Hasher(passwords).generate()

authenticator = stauth.Authenticate(names, usernames, hashed_passwords,
    'some_cookie_name', 'some_signature_key', cookie_expiry_days=30)

# name, authentication_status, username = authenticator.login('Login', 'main')


# --- Login Form ---
# name, authentication_status, username = authenticator.login('main') # Original call

# FIX: Modified call with check for None return value
login_result = authenticator.login('main')
if login_result:
    name, authentication_status, username = login_result
else:
    # If login returns None, something is wrong, likely config.
    # Set defaults to avoid further errors and show a message.
    name, authentication_status, username = (None, None, None)
    st.error("خطا در پردازش ورود. لطفاً فایل `config.yaml` را بررسی کنید و از صحت آن اطمینان حاصل نمایید. سپس برنامه را مجدداً اجرا کنید.")
    st.stop() # Stop execution if login failed fundamentally

# --- Main App Logic (Gated by Authentication) ---
if authentication_status:
    # --- Logout Button in Sidebar ---
    st.sidebar.write(f'خوش آمدید *{st.session_state["name"]}*')
    authenticator.logout('خروج', 'sidebar')
    # --- Helper Functions ---
    @st.cache_data
    def load_and_preprocess_data(file_path, expected_cols, rename_map, source_type, extraction_source_col=None, usage_col='Usage_Type', county_col='County', year_col='Water_Year_Str', id_col_standard='ID', renewable_col='Renewable_Status'):
        """Loads and preprocesses data, handling missing files, units, and adding necessary columns."""
        if not os.path.exists(file_path):
            essential_cols = [usage_col, county_col, 'Extraction_MCM', id_col_standard, 'Source_Type', 'Source_Name', year_col, renewable_col]
            return pd.DataFrame(columns=essential_cols)

        try:
            try:
                df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='cp1256', low_memory=False)

            missing_cols = [col for col in expected_cols if col not in df.columns]
            if missing_cols:
                st.error(f"خطا در بارگذاری فایل {os.path.basename(file_path)}. ستون‌های مورد انتظار یافت نشدند: {missing_cols}.")
                essential_cols = [usage_col, county_col, 'Extraction_MCM', id_col_standard, 'Source_Type', 'Source_Name', year_col, renewable_col]
                return pd.DataFrame(columns=essential_cols)

            df = df.rename(columns=rename_map)

            # --- Standardize Essential Columns ---
            # ID Column
            if id_col_standard not in df.columns:
                st.error(f"ستون ID استاندارد ('{id_col_standard}') پس از تغییر نام در فایل {os.path.basename(file_path)} یافت نشد.")
                if 'ID' in df.columns: df[id_col_standard] = df['ID'].astype(str); st.warning("از ستون 'ID' موجود استفاده شد.")
                else: df[id_col_standard] = 'نامشخص'
            else: df[id_col_standard] = df[id_col_standard].astype(str)

            # Extraction Column & Unit Conversion
            extraction_col_found = False
            if extraction_source_col and extraction_source_col in df.columns:
                df['Extraction_MCM'] = safe_to_numeric(df[extraction_source_col]).fillna(0)
                extraction_col_found = True
                if source_type == 'Groundwater':
                    df['Extraction_MCM'] = df['Extraction_MCM'] / 1_000_000
            elif 'Extraction_MCM' in df.columns:
                df['Extraction_MCM'] = safe_to_numeric(df['Extraction_MCM']).fillna(0)
                extraction_col_found = True
                if source_type == 'Groundwater':
                    if not df['Extraction_MCM'].empty and df['Extraction_MCM'].max() > 10000:
                        df['Extraction_MCM'] = df['Extraction_MCM'] / 1_000_000
            elif not df.empty and 'Extraction_MCM' in df.columns and not df['Extraction_MCM'].empty and df['Extraction_MCM'].max() > 10000 and source_type == 'Groundwater':
                df['Extraction_MCM'] = df['Extraction_MCM'] / 1_000_000

            if not extraction_col_found:
                df['Extraction_MCM'] = 0
                if not (source_type == 'Surface' and extraction_source_col == 'Dam_Extraction_Value'):
                    st.warning(f"ستون برداشت ('{extraction_source_col}' یا 'Extraction_MCM') برای فایل {os.path.basename(file_path)} یافت نشد. مقدار صفر در نظر گرفته شد.")

            # Source Type
            df['Source_Type'] = source_type

            # Source Name
            if 'Dam_Name' in df.columns: df['Source_Name'] = df['Dam_Name']
            elif source_type == 'Groundwater': df['Source_Name'] = 'منبع زیرزمینی ' + df[id_col_standard]
            elif source_type == 'Transfer' and 'Transfer_Source_Name' in df.columns: df['Source_Name'] = df['Transfer_Source_Name']
            elif source_type == 'Wastewater' and 'WW_Plant_Name' in df.columns: df['Source_Name'] = df['WW_Plant_Name']
            else: df['Source_Name'] = source_type + ' ' + df[id_col_standard]

            # Usage Type, County, Year, Renewable Status
            if usage_col not in df.columns: df[usage_col] = 'نامشخص'
            df[usage_col] = df[usage_col].fillna('نامشخص')
            if county_col not in df.columns: df[county_col] = 'نامشخص'
            df[county_col] = df[county_col].fillna('نامشخص')
            if year_col not in df.columns: df[year_col] = 'نامشخص'
            df[year_col] = df[year_col].astype(str).fillna('نامشخص')
            if renewable_col not in df.columns: df[renewable_col] = 'نامشخص'
            df[renewable_col] = df[renewable_col].fillna('نامشخص')

            # --- Specific Preprocessing ---
            if source_type == 'Surface' or source_type == 'Transfer':
                if 'Dam_Name' in df.columns:
                    df['Source_Type'] = np.where(df['Dam_Name'].isin(TRANSFER_DAM_NAMES), 'Transfer', 'Surface')
                    df.loc[df['Source_Type'] == 'Transfer', 'Source_Name'] = df['Dam_Name']
                    df.loc[df['Source_Type'] == 'Surface', 'Source_Name'] = df['Dam_Name']
            elif source_type == 'Groundwater':
                if 'Smart_Meter' in df.columns: df['Smart_Meter'] = df['Smart_Meter'].replace({'دارد': 'Yes', 'ندارد': 'No', 0: 'No', 1: 'Yes'}).fillna('نامشخص')
                if 'Study_Area' not in df.columns: df['Study_Area'] = 'نامشخص'
                df['Study_Area'] = df['Study_Area'].fillna('نامشخص')
                if 'Well_ID_Orig' in df.columns: df['Well_ID_Orig'] = df['Well_ID_Orig'].astype(str)

            # Select standardized essential columns plus extras
            essential_cols = ['Extraction_MCM', id_col_standard, 'Source_Type', 'Source_Name', usage_col, county_col, year_col, renewable_col]
            if source_type == 'Groundwater': essential_cols.extend(['Study_Area', 'Well_Type', 'Well_Status', 'Well_Depth_m', 'Operating_Hours', 'Flow_Rate_ls', 'Well_ID_Orig'])
            if source_type == 'Surface' or source_type == 'Transfer': essential_cols.extend(['Volume_Start_Year', 'Volume_End_Year', 'Level_Start_Year', 'Level_End_Year', 'Inflow', 'Leakage', 'Pumping_Out', 'Drainage', 'Evaporation', 'Sediment_Discharge', 'Intake_Discharge', 'Spillway_Discharge'])

            final_cols = [col for col in essential_cols if col in df.columns]
            df_final = df[final_cols].copy()
            df_final = df_final.rename(columns={id_col_standard: 'ID'})

            return df_final

        except FileNotFoundError:
            # st.error(f"خطا: فایل در مسیر {file_path} یافت نشد.") # Already handled by os.path.exists
            essential_cols = [usage_col, county_col, 'Extraction_MCM', 'ID', 'Source_Type', 'Source_Name', year_col, renewable_col]
            return pd.DataFrame(columns=essential_cols)
        except Exception as e:
            st.error(f"خطایی در پردازش {os.path.basename(file_path)} رخ داد: {e}")
            essential_cols = [usage_col, county_col, 'Extraction_MCM', 'ID', 'Source_Type', 'Source_Name', year_col, renewable_col]
            return pd.DataFrame(columns=essential_cols)


    def safe_to_numeric(series):
        """Converts a pandas Series to numeric, coercing errors to NaN."""
        return pd.to_numeric(series, errors='coerce')

    # --- Define Mappings and Constants ---
    TRANSFER_DAM_NAMES = ['سد دوستی']

    dam_expected_cols = ['Year', 'Name of Dam', 'تراز انتهای سال آبی', 'تراز ابتدای سال آبی', 'حجم انتهای سال آبی', 'حجم ابتدای سال آبی', 'ورودی', 'سایر', 'كل', 'نشتي', 'پمپاژ', 'زهكش', 'تبخير', 'تخلیه رسوب', 'دريچه آبگيري', 'سرريز', 'کل', 'Type of Use', 'ID', 'Value', 'sharestan']
    dam_rename_map = {'Year': 'Water_Year_Str', 'Name of Dam': 'Dam_Name', 'تراز انتهای سال آبی': 'Level_End_Year', 'تراز ابتدای سال آبی': 'Level_Start_Year', 'حجم انتهای سال آبی': 'Volume_End_Year', 'حجم ابتدای سال آبی': 'Volume_Start_Year', 'ورودی': 'Inflow', 'سایر': 'Other_Input', 'كل': 'Total_Input', 'نشتي': 'Leakage', 'پمپاژ': 'Pumping_Out', 'زهكش': 'Drainage', 'تبخير': 'Evaporation', 'تخلیه رسوب': 'Sediment_Discharge', 'دريچه آبگيري': 'Intake_Discharge', 'سرريز': 'Spillway_Discharge', 'کل': 'Total_Outflow', 'Type of Use': 'Usage_Type', 'ID': 'SubBasin_ID', 'Value': 'Dam_Extraction_Value', 'sharestan': 'County'}

    gw_expected_cols = ['سال آبي', 'اشتراک', 'امور', 'اشتراک برق', 'محدوده مطالعاتي', 'شهرستان', 'MA_XUTM', 'MA_YUTM', 'عمق چاه', 'دبي', 'ساعت کارکرد', 'اضافه کسربرداشت', 'تخليه مترمکعب', 'نوع چاه', 'نوع مصرف', 'نيرو محرکه', 'وضعيت چاه', 'برداشت واقعي', 'کنتور هوشمند', 'conat', 'ID']
    gw_rename_map = {'سال آبي': 'Water_Year_Str', 'اشتراک': 'Subscription_ID', 'امور': 'Department', 'اشتراک برق': 'Electricity_Subscription', 'محدوده مطالعاتي': 'Study_Area', 'شهرستان': 'County', 'MA_XUTM': 'X_UTM', 'MA_YUTM': 'Y_UTM', 'عمق چاه': 'Well_Depth_m', 'دبي': 'Flow_Rate_ls', 'ساعت کارکرد': 'Operating_Hours', 'اضافه کسربرداشت': 'Over_Under_Extraction_m3', 'تخليه مترمکعب': 'Discharge_m3', 'نوع چاه': 'Well_Type', 'نوع مصرف': 'Usage_Type', 'نيرو محرکه': 'Power_Source', 'وضعيت چاه': 'Well_Status', 'برداشت واقعي': 'Actual_Extraction_m3', 'کنتور هوشمند': 'Smart_Meter', 'conat': 'Coordinates_Text', 'ID': 'SubBasin_ID'}

    transfer_expected_cols = ['Water_Year', 'Source_Name', 'Extraction_MCM', 'Usage_Type', 'County', 'ID', 'Renewable_Status']
    transfer_rename_map = {'Water_Year': 'Water_Year_Str', 'Source_Name': 'Transfer_Source_Name', 'Extraction_MCM': 'Extraction_MCM', 'Usage_Type': 'Usage_Type', 'County': 'County', 'ID': 'SubBasin_ID', 'Renewable_Status': 'Renewable_Status'}

    ww_expected_cols = ['Water_Year', 'Plant_Name', 'Treated_Volume_MCM', 'Usage_Type', 'County', 'ID', 'Renewable_Status']
    ww_rename_map = {'Water_Year': 'Water_Year_Str', 'Plant_Name': 'WW_Plant_Name', 'Treated_Volume_MCM': 'Extraction_MCM', 'Usage_Type': 'Usage_Type', 'County': 'County', 'ID': 'SubBasin_ID', 'Renewable_Status': 'Renewable_Status'}


    # --- Load All Data ---
    df_dam_raw = load_and_preprocess_data(DAM_DATA_PATH, dam_expected_cols, dam_rename_map, 'Surface', extraction_source_col='Dam_Extraction_Value', id_col_standard='SubBasin_ID', year_col='Water_Year_Str')
    df_gw_raw = load_and_preprocess_data(GW_DATA_PATH, gw_expected_cols, gw_rename_map, 'Groundwater', extraction_source_col='Actual_Extraction_m3', id_col_standard='SubBasin_ID', year_col='Water_Year_Str')
    df_transfer_raw = load_and_preprocess_data(TRANSFER_DATA_PATH, transfer_expected_cols, transfer_rename_map, 'Transfer', extraction_source_col='Extraction_MCM', id_col_standard='SubBasin_ID', year_col='Water_Year_Str')
    df_wastewater_raw = load_and_preprocess_data(WASTEWATER_DATA_PATH, ww_expected_cols, ww_rename_map, 'Wastewater', extraction_source_col='Extraction_MCM', id_col_standard='SubBasin_ID', year_col='Water_Year_Str')

    df_all_data = pd.concat([df_dam_raw, df_gw_raw, df_transfer_raw, df_wastewater_raw], ignore_index=True)


    # --- Sidebar Navigation and Filters ---
    st.sidebar.title("راهبری")
    app_mode = st.sidebar.radio("انتخاب صفحه داشبورد", ["تحلیل جزئی", "خلاصه بیلان آب"])
    st.sidebar.divider()
    st.sidebar.header("فیلترهای عمومی")

    all_water_years_options = []
    latest_year = None
    if not df_all_data.empty and 'Water_Year_Str' in df_all_data.columns:
         valid_years_list = sorted(df_all_data['Water_Year_Str'].dropna().unique(), reverse=True)
         all_water_years_options = [yr for yr in valid_years_list if yr not in ['nan', 'نامشخص', 'None']]
         if all_water_years_options: latest_year = all_water_years_options[0]
    selected_water_years = st.sidebar.multiselect("انتخاب سال(های) آبی", options=all_water_years_options, default=[latest_year] if latest_year else [])

    all_counties = ['همه']
    if not df_all_data.empty and 'County' in df_all_data.columns:
        all_counties.extend(sorted(list(set(c for c in df_all_data['County'].dropna().unique() if c != 'نامشخص'))))
    selected_county_sidebar = st.sidebar.selectbox("انتخاب شهرستان", options=all_counties, key="county_sidebar_filter")


    # --- Filter DataFrames Globally ---
    df_filtered = df_all_data.copy()
    if selected_water_years: df_filtered = df_filtered[df_filtered['Water_Year_Str'].isin(selected_water_years)]
    if selected_county_sidebar != "همه": df_filtered = df_filtered[df_filtered['County'] == selected_county_sidebar]

    df_dam_detailed = df_filtered[df_filtered['Source_Type'].isin(['Surface', 'Transfer'])].copy()
    df_gw_detailed = df_filtered[df_filtered['Source_Type'] == 'Groundwater'].copy()


    # --- Page Display Functions ---

    def display_detailed_analysis(df_dam_viz, df_gw_viz):
        """Displays the detailed charts and tables."""
        st.title("💧 داشبورد حسابداری آب - تحلیل جزئی")

        st.header("🌊 تحلیل داده‌های سد و آب انتقالی")

        if df_dam_viz is None or df_dam_viz.empty:
            st.warning(f"داده‌ای برای سد/انتقالی با فیلترهای انتخاب شده یافت نشد (سال آبی: {selected_water_years}, شهرستان: {selected_county_sidebar}).")
        else:
            dam_names = ['همه'] + sorted(df_dam_viz['Source_Name'].dropna().unique())
            selected_dam = st.selectbox("انتخاب سد / منبع انتقالی", dam_names, key="dam_select_detail")
            df_dam_viz_filtered = df_dam_viz if selected_dam == "همه" else df_dam_viz[df_dam_viz['Source_Name'] == selected_dam]

            if not df_dam_viz_filtered.empty:
                plot_numeric_cols = ['Volume_Start_Year', 'Volume_End_Year', 'Level_Start_Year', 'Level_End_Year', 'Inflow', 'Leakage', 'Pumping_Out', 'Drainage', 'Evaporation', 'Sediment_Discharge', 'Intake_Discharge', 'Spillway_Discharge', 'Extraction_MCM']
                for col in plot_numeric_cols:
                    if col not in df_dam_viz_filtered.columns: df_dam_viz_filtered[col] = 0
                    else: df_dam_viz_filtered[col] = safe_to_numeric(df_dam_viz_filtered[col]).fillna(0)

                col1, col2 = st.columns(2)
                if 'Volume_Start_Year' in df_dam_viz_filtered.columns and 'Volume_End_Year' in df_dam_viz_filtered.columns:
                    with col1:
                        st.subheader("حجم آب سد (MCM)")
                        fig_dam_vol = px.line(df_dam_viz_filtered, x='Water_Year_Str', y=['Volume_Start_Year', 'Volume_End_Year'], title=f"حجم آب برای {selected_dam}", labels={'Water_Year_Str': 'سال آبی', 'value': 'حجم (میلیون متر مکعب)', 'variable': 'اندازه‌گیری'}, markers=True)
                        st.plotly_chart(fig_dam_vol, use_container_width=True)
                else: # Indentation fixed
                    with col1:
                        st.info("داده‌های حجم برای نمایش موجود نیست.")
                if 'Level_Start_Year' in df_dam_viz_filtered.columns and 'Level_End_Year' in df_dam_viz_filtered.columns:
                    with col2:
                        st.subheader("تراز آب سد (m)")
                        fig_dam_level = px.line(df_dam_viz_filtered, x='Water_Year_Str', y=['Level_Start_Year', 'Level_End_Year'], title=f"تراز آب برای {selected_dam}", labels={'Water_Year_Str': 'سال آبی', 'value': 'تراز (متر)', 'variable': 'اندازه‌گیری'}, markers=True)
                        st.plotly_chart(fig_dam_level, use_container_width=True)
                else: # Indentation fixed
                    with col2:
                        st.info("داده‌های تراز برای نمایش موجود نیست.")

                st.subheader(f"مولفه‌های بیلان آب برای {selected_dam} (MCM)")
                balance_cols = ['Inflow', 'Leakage', 'Pumping_Out', 'Drainage', 'Evaporation', 'Sediment_Discharge', 'Intake_Discharge', 'Spillway_Discharge', 'Extraction_MCM']
                balance_cols_present = [col for col in balance_cols if col in df_dam_viz_filtered.columns]
                if balance_cols_present:
                    df_balance = df_dam_viz_filtered.groupby('Water_Year_Str')[balance_cols_present].sum().reset_index() if selected_dam == "همه" else df_dam_viz_filtered[['Water_Year_Str'] + balance_cols_present].copy()
                    title_suffix = "(تجمیعی)" if selected_dam == "همه" else f"برای {selected_dam}"
                    df_balance_melt = df_balance.melt(id_vars='Water_Year_Str', value_vars=balance_cols_present, var_name='مولفه', value_name='حجم (MCM)')
                    fig_balance = px.bar(df_balance_melt, x='Water_Year_Str', y='حجم (MCM)', color='مولفه', title=f"مولفه‌های بیلان آب {title_suffix} ({', '.join(selected_water_years)})", labels={'Water_Year_Str': 'سال آبی'}, barmode='group').update_xaxes(categoryorder='array', categoryarray=sorted(df_balance_melt['Water_Year_Str'].unique())) # Sort x-axis
                    st.plotly_chart(fig_balance, use_container_width=True)
                else: st.info("داده‌های مولفه‌های بیلان برای نمایش موجود نیست.")

                st.subheader(f"داده‌های فیلتر شده سد/انتقالی ({selected_dam})")
                st.dataframe(df_dam_viz_filtered)
            else: st.warning(f"داده‌ای برای سد/انتقالی با فیلترهای انتخاب شده یافت نشد (سال آبی: {selected_water_years}, شهرستان: {selected_county_sidebar}, منبع: {selected_dam}).")

        st.divider()
        st.header("🌍 تحلیل داده‌های آب زیرزمینی")
        if df_gw_viz is None or df_gw_viz.empty:
            st.warning(f"داده‌ای برای آب زیرزمینی با فیلترهای انتخاب شده یافت نشد (سال آبی: {selected_water_years}, شهرستان: {selected_county_sidebar}).")
        else:
            gw_usage_types = ['همه'] + sorted(df_gw_viz['Usage_Type'].dropna().unique())
            selected_gw_usage = st.selectbox("انتخاب نوع کاربری آب زیرزمینی", gw_usage_types, key="gw_usage_detail")
            selected_well_type = "همه"
            if 'Well_Type' in df_gw_viz.columns:
                gw_well_types = ['همه'] + sorted(df_gw_viz['Well_Type'].dropna().unique())
                selected_well_type = st.selectbox("انتخاب نوع چاه", gw_well_types, key="gw_well_type_detail")
            selected_well_status = "همه"
            if 'Well_Status' in df_gw_viz.columns:
                gw_well_status = ['همه'] + sorted(df_gw_viz['Well_Status'].dropna().unique())
                selected_well_status = st.selectbox("انتخاب وضعیت چاه", gw_well_status, key="gw_status_detail")

            df_gw_viz_filtered = df_gw_viz
            if selected_gw_usage != "همه": df_gw_viz_filtered = df_gw_viz_filtered[df_gw_viz_filtered['Usage_Type'] == selected_gw_usage]
            if selected_well_type != "همه" and 'Well_Type' in df_gw_viz_filtered.columns: df_gw_viz_filtered = df_gw_viz_filtered[df_gw_viz_filtered['Well_Type'] == selected_well_type]
            if selected_well_status != "همه" and 'Well_Status' in df_gw_viz_filtered.columns: df_gw_viz_filtered = df_gw_viz_filtered[df_gw_viz_filtered['Well_Status'] == selected_well_status]

            if not df_gw_viz_filtered.empty:
                total_extraction_mcm = df_gw_viz_filtered['Extraction_MCM'].sum()
                avg_depth = df_gw_viz_filtered['Well_Depth_m'].mean() if 'Well_Depth_m' in df_gw_viz_filtered.columns else np.nan
                num_subbasins = df_gw_viz_filtered['ID'].nunique()

                st.subheader("مقادیر خلاصه (فیلتر شده)")
                mcol1, mcol2, mcol3 = st.columns(3)
                mcol1.metric("مجموع برداشت (میلیون متر مکعب)", f"{total_extraction_mcm:,.2f}")
                mcol2.metric("میانگین عمق چاه (متر)", f"{avg_depth:.1f}" if not pd.isna(avg_depth) else "N/A")
                mcol3.metric("تعداد زیرحوضه‌های فعال", f"{num_subbasins}")

                st.subheader("مجموع برداشت آب زیرزمینی بر اساس سال آبی و نوع کاربری (MCM)")
                df_gw_agg_usage = df_gw_viz_filtered.groupby(['Water_Year_Str', 'Usage_Type'])['Extraction_MCM'].sum().reset_index()
                fig_gw_usage = px.bar(df_gw_agg_usage, x='Water_Year_Str', y='Extraction_MCM', color='Usage_Type', title=f"برداشت سالانه آب زیرزمینی بر اساس نوع کاربری ({', '.join(selected_water_years)})", labels={'Water_Year_Str': 'سال آبی', 'Extraction_MCM': 'مجموع برداشت (میلیون متر مکعب)'}).update_xaxes(categoryorder='array', categoryarray=sorted(df_gw_agg_usage['Water_Year_Str'].unique())) # Sort x-axis
                st.plotly_chart(fig_gw_usage, use_container_width=True)

                col3, col4 = st.columns(2)
                if 'Well_Type' in df_gw_viz_filtered.columns:
                    with col3:
                        st.subheader("توزیع نوع چاه (بر اساس تعداد)")
                        count_col = 'Well_ID_Orig' if 'Well_ID_Orig' in df_gw_viz_filtered.columns else 'ID'
                        df_gw_count_type = df_gw_viz_filtered.groupby('Well_Type')[count_col].nunique().reset_index().rename(columns={count_col: 'Count'})
                        fig_gw_type = px.pie(df_gw_count_type, names='Well_Type', values='Count', title="توزیع انواع چاه", hole=0.3)
                        st.plotly_chart(fig_gw_type, use_container_width=True)
                # FIX: Correct indentation for else block (Line 297 original)
                else:
                    with col3:
                        st.info("داده نوع چاه موجود نیست.")
                if 'Well_Status' in df_gw_viz_filtered.columns:
                    with col4:
                        st.subheader("توزیع وضعیت چاه (بر اساس تعداد)")
                        count_col = 'Well_ID_Orig' if 'Well_ID_Orig' in df_gw_viz_filtered.columns else 'ID'
                        df_gw_count_status = df_gw_viz_filtered.groupby('Well_Status')[count_col].nunique().reset_index().rename(columns={count_col: 'Count'})
                        fig_gw_status = px.pie(df_gw_count_status, names='Well_Status', values='Count', title="توزیع وضعیت چاه‌ها", hole=0.3)
                        st.plotly_chart(fig_gw_status, use_container_width=True)
                # FIX: Correct indentation for else block (Line 305 original)
                else:
                    with col4:
                        st.info("داده وضعیت چاه موجود نیست.")

                scatter_cols_exist = all(c in df_gw_viz_filtered.columns for c in ['Extraction_MCM', 'Operating_Hours', 'Flow_Rate_ls'])
                if scatter_cols_exist:
                    df_scatter = df_gw_viz_filtered[(df_gw_viz_filtered['Extraction_MCM'] > 0) & (df_gw_viz_filtered['Operating_Hours'] > 0)]
                    if not df_scatter.empty:
                        st.subheader("برداشت (MCM) در مقابل ساعات کارکرد")
                        fig_scatter = px.scatter(df_scatter, x='Operating_Hours', y='Extraction_MCM', color='Usage_Type', size='Flow_Rate_ls', hover_name='ID', title="برداشت در مقابل ساعات کارکرد (اندازه بر اساس دبی)", labels={'Operating_Hours': 'ساعات کارکرد', 'Extraction_MCM': 'برداشت (میلیون متر مکعب)'})
                        st.plotly_chart(fig_scatter, use_container_width=True)
                    else: st.info("داده‌ای با برداشت و ساعات کارکرد مثبت برای نمودار پراکندگی وجود ندارد.")
                else: st.info("ستون‌های لازم برای نمودار پراکندگی موجود نیستند.")

                st.subheader("داده‌های فیلتر شده آب زیرزمینی")
                st.dataframe(df_gw_viz_filtered)
            else: st.warning(f"داده‌ای برای آب زیرزمینی با فیلترهای انتخاب شده یافت نشد.")

    @st.cache_data # Cache shapefile reading
    def load_shapefile(uploaded_file):
        """Loads a shapefile from an uploaded zip file."""
        try:
            zip_buffer = io.BytesIO(uploaded_file.getvalue())
            with zipfile.ZipFile(zip_buffer) as z:
                shp_file_path = None
                for filename in z.namelist():
                    if filename.lower().endswith(".shp"): shp_file_path = filename; break
                if shp_file_path is None: st.error("فایل .shp در فایل فشرده یافت نشد."); return None
                import tempfile
                with tempfile.TemporaryDirectory() as tmpdir:
                    z.extractall(path=tmpdir)
                    shp_full_path = os.path.join(tmpdir, shp_file_path)
                    if os.path.exists(shp_full_path):
                        gdf = gpd.read_file(shp_full_path)
                        if gdf.crs is None: gdf.set_crs("EPSG:4326", inplace=True); st.warning("سیستم مختصات (CRS) برای شیپ‌فایل مشخص نشده بود. EPSG:4326 (WGS84) به عنوان پیش‌فرض در نظر گرفته شد.")
                        gdf = gdf.to_crs("EPSG:4326")
                        return gdf
                    else: st.error("خطا در استخراج یا یافتن فایل .shp در مسیر موقت."); return None
        except zipfile.BadZipFile: st.error("فایل آپلود شده یک فایل فشرده (zip) معتبر نیست."); return None
        except ImportError: st.error("کتابخانه geopandas یافت نشد. لطفاً آن را نصب کنید: pip install geopandas"); return None
        except Exception as e: st.error(f"خطا در خواندن شیپ‌فایل: {e}"); return None
        
    def display_water_balance_summary(df_summary_data):
        """Displays the new water balance summary page with filters, metrics, table, and chart."""
        st.title("💧 داشبورد حسابداری آب - خلاصه بیلان آب")
        st.markdown("خلاصه برداشت آب (میلیون متر مکعب - MCM) بر اساس فیلترهای انتخابی.")
        st.info("نکته: داده‌های جریان برگشتی، ضرایب برگشت در دسترس نیستند. ستون تجدیدپذیری Placeholder است.")

        # --- Filters ---
        col_f1, col_f2, col_f3, col_f4 = st.columns(4)
        with col_f1: # County
            county_options = ["همه"]
            if not df_summary_data.empty and 'County' in df_summary_data.columns: county_options.extend(sorted(list(set(c for c in df_summary_data['County'].dropna().unique() if c != 'نامشخص'))))
            disabled_county = selected_county_sidebar != "همه"
            selected_county_summary = st.selectbox("شهرستان", options=county_options, key="county_summary_filter", index=county_options.index(selected_county_sidebar) if disabled_county else 0, disabled=disabled_county)
            if disabled_county: st.caption(f"فیلتر شهرستان '{selected_county_sidebar}' اعمال شده است.")
        with col_f2: # Study Area
            study_areas = ["همه"]
            df_gw_summary = df_summary_data[df_summary_data['Source_Type'] == 'Groundwater']
            current_county = selected_county_summary if not disabled_county else selected_county_sidebar
            if current_county != "همه": df_gw_summary = df_gw_summary[df_gw_summary['County'] == current_county]
            if not df_gw_summary.empty and 'Study_Area' in df_gw_summary.columns: study_areas.extend(sorted(df_gw_summary['Study_Area'].dropna().unique()))
            selected_study_area = st.selectbox("محدوده مطالعاتی", options=list(set(study_areas)), key="study_area_filter")
        with col_f3: # Usage Type
            usage_types = ["همه"]
            if not df_summary_data.empty and 'Usage_Type' in df_summary_data.columns: usage_types.extend(sorted(list(set(u for u in df_summary_data['Usage_Type'].dropna().unique() if u != 'نامشخص'))))
            selected_usage_type = st.selectbox("نوع کاربری", options=usage_types, key="usage_type_filter")
        with col_f4: # Source Classification
            source_options_dict = {"همه": "All", "آب سطحی (سد)": "Surface", "آب زیرزمینی": "Groundwater", "آب انتقالی": "Transfer", "تصفیه خانه": "Wastewater"}
            available_sources = df_summary_data['Source_Type'].unique() if not df_summary_data.empty else []
            display_source_options = ["همه"] + [k for k, v in source_options_dict.items() if v in available_sources and v != "All"]
            selected_source_type_display = st.selectbox("طبقه‌بندی منبع", options=display_source_options, key="source_type_filter")
            selected_source_type_val = source_options_dict.get(selected_source_type_display, "All")

        # Renewable Filter
        renewable_options = ["همه", "تجدیدپذیر", "تجدیدناپذیر", "نامشخص"]
        selected_renewable_status = st.selectbox("تجدیدپذیری", options=renewable_options, key="renewable_filter")

        # --- Filter data ---
        df_summary_filtered = df_summary_data.copy()
        if not disabled_county and selected_county_summary != "همه": df_summary_filtered = df_summary_filtered[df_summary_filtered['County'] == selected_county_summary]
        if selected_study_area != "همه" and 'Study_Area' in df_summary_filtered.columns: df_summary_filtered = df_summary_filtered[~((df_summary_filtered['Source_Type'] == 'Groundwater') & (df_summary_filtered['Study_Area'] != selected_study_area))]
        if selected_usage_type != "همه": df_summary_filtered = df_summary_filtered[df_summary_filtered['Usage_Type'] == selected_usage_type]
        if selected_source_type_val != "All": df_summary_filtered = df_summary_filtered[df_summary_filtered['Source_Type'] == selected_source_type_val]
        if selected_renewable_status != "همه":
            if 'Renewable_Status' in df_summary_filtered.columns:
                status_to_check = ['نامشخص', 'Unknown', None] if selected_renewable_status == "نامشخص" else [selected_renewable_status]
                df_summary_filtered = df_summary_filtered[df_summary_filtered['Renewable_Status'].isin(status_to_check)]
            else: st.warning("ستون 'Renewable_Status' برای اعمال فیلتر تجدیدپذیری یافت نشد.")

        # --- Display Metrics (in MCM) ---
        st.subheader("خلاصه مقادیر برداشت (میلیون متر مکعب - MCM)")
        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
        total_surface = df_summary_filtered[df_summary_filtered['Source_Type'] == 'Surface']['Extraction_MCM'].sum()
        metric_col1.metric("برداشت آب سطحی (سدها)", f"{total_surface:,.2f}")
        total_gw = df_summary_filtered[df_summary_filtered['Source_Type'] == 'Groundwater']['Extraction_MCM'].sum()
        metric_col2.metric("برداشت آب زیرزمینی", f"{total_gw:,.2f}")
        total_transfer = df_summary_filtered[df_summary_filtered['Source_Type'] == 'Transfer']['Extraction_MCM'].sum()
        transfer_available = not df_all_data[df_all_data['Source_Type'] == 'Transfer'].empty # Check if data ever existed
        metric_col3.metric("برداشت آب انتقالی", f"{total_transfer:,.2f}" if transfer_available else "N/A")
        total_wastewater = df_summary_filtered[df_summary_filtered['Source_Type'] == 'Wastewater']['Extraction_MCM'].sum()
        wastewater_available = not df_all_data[df_all_data['Source_Type'] == 'Wastewater'].empty # Check if data ever existed
        metric_col4.metric("تصفیه خانه", f"{total_wastewater:,.2f}" if wastewater_available else "N/A")

        # --- Prepare and Display Aggregated Table (in MCM) --- FIX: Aggregate before display
        st.subheader("جدول خلاصه داده‌های فیلتر شده") # Changed title to reflect aggregation
        if not df_summary_filtered.empty:
            # Define columns to group by - ensure they exist
            group_by_cols = ['طبقه‌بندی منبع', 'نام منبع', 'شناسه زیرحوضه', 'شهرستان', 'کاربری', 'وضعیت تجدیدپذیری']
            # Rename columns *before* grouping
            df_renamed_for_grouping = df_summary_filtered.rename(columns={
                'Extraction_MCM': 'برداشت (MCM)',
                'ID': 'شناسه زیرحوضه',
                'Usage_Type': 'کاربری', 'County': 'شهرستان',
                'Source_Type': 'طبقه‌بندی منبع', 'Source_Name': 'نام منبع',
                'Renewable_Status': 'وضعیت تجدیدپذیری'
            })
            # Ensure all group columns exist
            actual_group_cols = [col for col in group_by_cols if col in df_renamed_for_grouping.columns]

            if 'برداشت (MCM)' in df_renamed_for_grouping.columns and actual_group_cols:
                # Aggregate the data
                aggregated_table = df_renamed_for_grouping.groupby(actual_group_cols, observed=False)['برداشت (MCM)'].sum().reset_index()

                # Display the aggregated table
                st.dataframe(aggregated_table[actual_group_cols + ['برداشت (MCM)']].style.format({'برداشت (MCM)': '{:,.2f}'}))

                # --- Chart Generation (based on aggregated data) ---
                st.divider()
                st.subheader("نمودار داده‌های خلاصه شده")
                if not aggregated_table.empty:
                    chart_type = st.radio("انتخاب نوع نمودار:", ('میله‌ای', 'خطی', 'دایره‌ای'), key="chart_select", horizontal=True)
                    try:
                        plot_data = aggregated_table.copy()
                        plot_data['برداشت (MCM)'] = pd.to_numeric(plot_data['برداشت (MCM)'], errors='coerce').fillna(0)
                        if chart_type == 'میله‌ای':
                            fig_chart = px.bar(plot_data, x='شهرستان', y='برداشت (MCM)', color='طبقه‌بندی منبع', title="برداشت تجمیعی (MCM) بر اساس شهرستان و طبقه‌بندی منبع", labels={'شهرستان': 'شهرستان', 'برداشت (MCM)': 'مجموع برداشت (میلیون متر مکعب)', 'طبقه‌بندی منبع': 'طبقه‌بندی منبع'}, barmode='group')
                            fig_chart.update_layout(xaxis={'categoryorder':'total descending'})
                            st.plotly_chart(fig_chart, use_container_width=True)
                        elif chart_type == 'خطی':
                            if len(selected_water_years) > 1:
                                line_plot_data = df_summary_filtered.groupby(['Water_Year_Str', 'Source_Type'])['Extraction_MCM'].sum().reset_index()
                                fig_chart = px.line(line_plot_data, x='Water_Year_Str', y='Extraction_MCM', color='Source_Type', title="روند برداشت (MCM) در طول زمان بر اساس نوع منبع", labels={'Water_Year_Str': 'سال آبی', 'Extraction_MCM': 'مجموع برداشت (میلیون متر مکعب)', 'Source_Type': 'نوع منبع'}, markers=True).update_xaxes(categoryorder='array', categoryarray=sorted(line_plot_data['Water_Year_Str'].unique()))
                                st.plotly_chart(fig_chart, use_container_width=True)
                            else: st.warning("نمودار خطی برای نمایش روند، نیاز به انتخاب حداقل دو سال آبی در فیلتر عمومی دارد.")
                        elif chart_type == 'دایره‌ای':
                            pie_col = st.selectbox("نمایش توزیع بر اساس:", ('طبقه‌بندی منبع', 'کاربری', 'شهرستان'), key="pie_col_select")
                            if pie_col in plot_data.columns:
                                pie_data = plot_data.groupby(pie_col)['برداشت (MCM)'].sum().reset_index()
                                # Filter out zero values for better pie chart visibility
                                pie_data = pie_data[pie_data['برداشت (MCM)'] > 0]
                                if not pie_data.empty:
                                    fig_chart = px.pie(pie_data, names=pie_col, values='برداشت (MCM)', title=f"توزیع درصد برداشت (MCM) بر اساس {pie_col}", hole=0.3)
                                    fig_chart.update_traces(textposition='inside', textinfo='percent+label')
                                    st.plotly_chart(fig_chart, use_container_width=True)
                                else:
                                    st.warning(f"داده‌ای با مقدار برداشت مثبت برای نمایش نمودار دایره‌ای بر اساس '{pie_col}' وجود ندارد.")
                            else: st.warning(f"ستون '{pie_col}' برای رسم نمودار دایره‌ای در داده‌های تجمیع شده یافت نشد.")
                    except Exception as e: st.error(f"خطا در رسم نمودار: {e}")
                else: st.warning("داده‌ای در جدول خلاصه برای رسم نمودار وجود ندارد.")

                # --- Shapefile Upload and Map Display ---
                st.divider()
                st.subheader("نقشه محدوده و برداشت")
                uploaded_shp_zip = st.file_uploader("آپلود شیپ‌فایل محدوده (فایل .zip)", type="zip", key="shp_uploader")
                if uploaded_shp_zip is not None:
                    gdf = load_shapefile(uploaded_shp_zip)
                    if gdf is not None:
                        st.success("شیپ‌فایل با موفقیت بارگذاری و خوانده شد.")
                        shp_cols = gdf.columns.tolist()
                        likely_id_cols = [col for col in shp_cols if col.upper() in ('ID', 'SUBBASINID', 'SUBBASIN_I', 'IDENTIFIER', 'CODE')]
                        default_index = shp_cols.index(likely_id_cols[0]) if likely_id_cols else 0
                        id_col_shp = st.selectbox("انتخاب ستون شناسه (ID) در شیپ‌فایل برای اتصال:", options=shp_cols, index=default_index)

                        if id_col_shp and not aggregated_table.empty:
                            try:
                                map_data = aggregated_table[['شناسه زیرحوضه', 'برداشت (MCM)']].copy()
                                map_data['شناسه زیرحوضه'] = map_data['شناسه زیرحوضه'].astype(str)
                                map_data_agg = map_data.groupby('شناسه زیرحوضه')['برداشت (MCM)'].sum().reset_index()
                                gdf_map = gdf[[id_col_shp, 'geometry']].copy()
                                gdf_map[id_col_shp] = gdf_map[id_col_shp].astype(str)
                                merged_gdf = gdf_map.merge(map_data_agg, left_on=id_col_shp, right_on='شناسه زیرحوضه', how='left')
                                merged_gdf['برداشت (MCM)'] = merged_gdf['برداشت (MCM)'].fillna(0)

                                # Classification
                                color_col = 'برداشت (MCM)'
                                color_map = "Viridis"
                                try:
                                    non_zero_values = merged_gdf['برداشت (MCM)'][merged_gdf['برداشت (MCM)'] > 0]
                                    if non_zero_values.nunique() >= 4:
                                        merged_gdf['کلاس_برداشت'] = pd.qcut(non_zero_values, q=4, labels=False, duplicates='drop')
                                        merged_gdf['کلاس_برداشت'] = 'کلاس ' + (merged_gdf['کلاس_برداشت'] + 1).astype(str)
                                        merged_gdf['کلاس_برداشت'].fillna('بدون برداشت', inplace=True)
                                        color_col = 'کلاس_برداشت'
                                    else: st.info("تعداد مقادیر منحصر به فرد برای طبقه‌بندی کوانتایل کافی نیست. از مقادیر خام استفاده می‌شود.")
                                except Exception as e_class: st.warning(f"خطا در طبقه‌بندی داده‌ها: {e_class}. از مقادیر خام استفاده می‌شود.")

                                # Plot Map
                                st.write("نقشه رنگ‌بندی شده بر اساس برداشت (MCM):")
                                try: center_lat = merged_gdf.geometry.centroid.y.mean(); center_lon = merged_gdf.geometry.centroid.x.mean()
                                except: center_lat = 36.0; center_lon = 58.0
                                fig_map = px.choropleth_mapbox(merged_gdf, geojson=merged_gdf.geometry, locations=merged_gdf.index, color=color_col,
                                                            mapbox_style="carto-positron", zoom=7, center={"lat": center_lat, "lon": center_lon}, opacity=0.6,
                                                            hover_name=id_col_shp, hover_data={'برداشت (MCM)': ':.2f'},
                                                            color_continuous_scale=color_map if color_col == 'برداشت (MCM)' else None,
                                                            category_orders={'کلاس_برداشت': sorted(merged_gdf['کلاس_برداشت'].unique())} if color_col == 'کلاس_برداشت' else None,
                                                            title="نقشه برداشت بر اساس زیرحوضه")
                                fig_map.update_layout(margin={"r":0,"t":30,"l":0,"b":0})
                                st.plotly_chart(fig_map, use_container_width=True)
                            except KeyError as e: st.error(f"خطا در اتصال داده‌ها به شیپ‌فایل: ستون شناسه '{e}' یافت نشد.")
                            except Exception as e: st.error(f"خطا در ایجاد نقشه: {e}")
                        else: st.warning("لطفاً ستون شناسه در شیپ‌فایل را انتخاب کنید و مطمئن شوید داده‌ای برای اتصال وجود دارد.")


    # --- Main App Logic ---
    if app_mode == "تحلیل جزئی":
        display_detailed_analysis(df_dam_detailed, df_gw_detailed)
    elif app_mode == "خلاصه بیلان آب":
        display_water_balance_summary(df_filtered)

    # --- Footer ---
    st.sidebar.divider()
    st.sidebar.info("داشبورد ایجاد شده با Streamlit.")
# --- Handle Authentication Status ---
elif authentication_status == False:
    st.error('نام کاربری یا رمز عبور اشتباه است')
elif authentication_status == None:
    st.warning('لطفاً نام کاربری و رمز عبور خود را وارد کنید')
