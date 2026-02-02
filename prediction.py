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

# --- Authentication Setup (Example) ---
names = ['John Smith', 'Rebecca Briggs']
usernames = ['jsmith', 'rbriggs']
passwords = ['123', '456'] # In a real app, use hashed passwords from a secure source

hashed_passwords = stauth.Hasher(passwords).generate()

authenticator = stauth.Authenticate(names, usernames, hashed_passwords,
    'some_cookie_name', 'some_signature_key', cookie_expiry_days=30)


# --- Login Form ---
login_result = authenticator.login('main')
if login_result:
    name, authentication_status, username = login_result
else:
    name, authentication_status, username = (None, None, None)


# --- Main App Logic (Gated by Authentication) ---
if authentication_status:
    # --- Logout Button in Sidebar ---
    st.sidebar.write(f'خوش آمدید *{st.session_state["name"]}*')
    authenticator.logout('خروج', 'sidebar')

    # --- Helper Functions ---
    def safe_to_numeric(series):
        """Converts a pandas Series to numeric, coercing errors to NaN."""
        return pd.to_numeric(series, errors='coerce')

    def format_water_year(year_str):
        """Converts a year string (e.g., '1402') to water year format 'YYYY-(YYYY-1)' (e.g., '1402-1401')."""
        try:
            year = int(year_str)
            return f"{year}-{year-1}"
        except (ValueError, TypeError):
            return year_str # Return original if conversion fails

    @st.cache_data
    def load_and_preprocess_data(file_path, expected_cols, rename_map, source_type, extraction_source_col=None, usage_col='Usage_Type', county_col='County', year_col='Water_Year_Str', id_col_standard='ID', renewable_col='Renewable_Status'):
        """Loads and preprocesses data, handling missing files, units, adding necessary columns, and setting renewable status."""
        essential_cols_definition = [usage_col, county_col, 'Extraction_MCM', id_col_standard, 'Source_Type', 'Source_Name', year_col, renewable_col]
        
        if not os.path.exists(file_path):
            st.warning(f"فایل داده در مسیر {file_path} یافت نشد. یک دیتافریم خالی برگردانده می‌شود.")
            return pd.DataFrame(columns=essential_cols_definition + ['Water_Year_Formatted', 'Water_Year_Numeric'])

        try:
            try:
                df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='cp1256', low_memory=False)
            except Exception as e:
                 st.error(f"خطا در خواندن فایل {os.path.basename(file_path)}: {e}")
                 return pd.DataFrame(columns=essential_cols_definition + ['Water_Year_Formatted', 'Water_Year_Numeric'])

            original_year_col_name = [k for k, v in rename_map.items() if v == year_col][0]

            missing_cols = [col for col in expected_cols if col not in df.columns]
            if missing_cols:
                st.error(f"خطا در بارگذاری فایل {os.path.basename(file_path)}. ستون‌های مورد انتظار یافت نشدند: {missing_cols}.")
                return pd.DataFrame(columns=essential_cols_definition + ['Water_Year_Formatted', 'Water_Year_Numeric'])

            df = df.rename(columns=rename_map)
            
            # --- Add Numeric Year Column for Forecasting ---
            df['Water_Year_Numeric'] = safe_to_numeric(df[year_col])


            # --- Standardize Essential Columns ---
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
                if source_type == 'Groundwater' and not df['Extraction_MCM'].empty and df['Extraction_MCM'].max() > 100000:
                    df['Extraction_MCM'] = df['Extraction_MCM'] / 1_000_000
            elif 'Extraction_MCM' in df.columns:
                 df['Extraction_MCM'] = safe_to_numeric(df['Extraction_MCM']).fillna(0)
                 extraction_col_found = True
                 if source_type == 'Groundwater' and not df['Extraction_MCM'].empty and df['Extraction_MCM'].max() > 100000:
                     df['Extraction_MCM'] = df['Extraction_MCM'] / 1_000_000

            if not extraction_col_found:
                df['Extraction_MCM'] = 0
                if not (source_type == 'Surface' and extraction_source_col == 'Dam_Extraction_Value'):
                     st.warning(f"ستون برداشت ('{extraction_source_col}' یا 'Extraction_MCM') برای فایل {os.path.basename(file_path)} یافت نشد. مقدار صفر در نظر گرفته شد.")

            df['Source_Type'] = source_type

            if 'Dam_Name' in df.columns: df['Source_Name'] = df['Dam_Name']
            elif source_type == 'Groundwater': df['Source_Name'] = 'منبع زیرزمینی ' + df[id_col_standard]
            elif source_type == 'Transfer' and 'Transfer_Source_Name' in df.columns: df['Source_Name'] = df['Transfer_Source_Name']
            elif source_type == 'Wastewater' and 'WW_Plant_Name' in df.columns: df['Source_Name'] = df['WW_Plant_Name']
            else: df['Source_Name'] = source_type + ' ' + df[id_col_standard]

            for col, default in [(usage_col, 'نامشخص'), (county_col, 'نامشخص'), (year_col, 'نامشخص')]:
                if col not in df.columns: df[col] = default
                df[col] = df[col].fillna(default)
            df[year_col] = df[year_col].astype(str)

            if source_type in ['Surface', 'Groundwater']:
                df[renewable_col] = 'تجدیدپذیر'
            elif renewable_col not in df.columns:
                 df[renewable_col] = 'نامشخص'
            else:
                 df[renewable_col] = df[renewable_col].fillna('نامشخص')

            df['Water_Year_Formatted'] = df[year_col].apply(format_water_year)

            if source_type in ['Surface', 'Transfer']:
                 if 'Dam_Name' in df.columns and 'Source_Type' in df.columns:
                     TRANSFER_DAM_NAMES = ['سد دوستی']
                     df['Source_Type'] = np.where(df['Dam_Name'].isin(TRANSFER_DAM_NAMES), 'Transfer', df['Source_Type'])
            elif source_type == 'Groundwater':
                if 'Smart_Meter' in df.columns: df['Smart_Meter'] = df['Smart_Meter'].replace({'دارد': 'Yes', 'ندارد': 'No', 0: 'No', 1: 'Yes'}).fillna('نامشخص')
                if 'Study_Area' not in df.columns: df['Study_Area'] = 'نامشخص'
                df['Study_Area'] = df['Study_Area'].fillna('نامشخص')
                if 'Well_ID_Orig' in df.columns: df['Well_ID_Orig'] = df['Well_ID_Orig'].astype(str)

            essential_cols = ['Extraction_MCM', id_col_standard, 'Source_Type', 'Source_Name', usage_col, county_col, year_col, 'Water_Year_Formatted', 'Water_Year_Numeric', renewable_col]
            extra_cols = []
            if source_type == 'Groundwater': extra_cols.extend(['Study_Area', 'Well_Type', 'Well_Status', 'Well_Depth_m', 'Operating_Hours', 'Flow_Rate_ls', 'Well_ID_Orig', 'X_UTM', 'Y_UTM'])
            if source_type in ['Surface', 'Transfer']: extra_cols.extend(['Volume_Start_Year', 'Volume_End_Year', 'Level_Start_Year', 'Level_End_Year', 'Inflow', 'Leakage', 'Pumping_Out', 'Drainage', 'Evaporation', 'Sediment_Discharge', 'Intake_Discharge', 'Spillway_Discharge'])

            final_cols = essential_cols + [col for col in extra_cols if col in df.columns]
            df_final = df[final_cols].copy()
            df_final = df_final.rename(columns={id_col_standard: 'ID'})

            numeric_cols_to_check = ['Extraction_MCM', 'Well_Depth_m', 'Operating_Hours', 'Flow_Rate_ls', 'Volume_Start_Year', 'Volume_End_Year', 'Level_Start_Year', 'Level_End_Year', 'Inflow', 'Leakage', 'Pumping_Out', 'Drainage', 'Evaporation', 'Sediment_Discharge', 'Intake_Discharge', 'Spillway_Discharge', 'X_UTM', 'Y_UTM']
            for col in numeric_cols_to_check:
                if col in df_final.columns:
                    df_final[col] = safe_to_numeric(df_final[col])
            return df_final
        except Exception as e:
            st.error(f"خطایی در پردازش فایل {os.path.basename(file_path)} رخ داد: {e}")
            return pd.DataFrame(columns=essential_cols_definition + ['Water_Year_Formatted', 'Water_Year_Numeric'])

    # --- Data Loading Mappings ---
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
    
    essential_cols_final = ['Extraction_MCM', 'ID', 'Source_Type', 'Source_Name', 'Usage_Type', 'County', 'Water_Year_Str', 'Water_Year_Formatted', 'Water_Year_Numeric', 'Renewable_Status']
    for col in essential_cols_final:
        if col not in df_all_data.columns:
            df_all_data[col] = pd.NA
            
    # --- Sidebar Navigation and Filters ---
    st.sidebar.title("راهبری")
    app_mode = st.sidebar.radio("انتخاب صفحه داشبورد", ["تحلیل جزئی", "خلاصه بیلان آب", "پیش‌بینی منابع آب"])
    st.sidebar.divider()
    st.sidebar.header("فیلترهای عمومی")

    all_water_years_formatted = []
    latest_year_formatted = None
    if not df_all_data.empty and 'Water_Year_Formatted' in df_all_data.columns:
         valid_years_list = sorted([yr for yr in df_all_data['Water_Year_Formatted'].dropna().unique() if yr not in ['nan', 'نامشخص', 'None']], key=lambda x: int(x.split('-')[0]) if isinstance(x, str) and '-' in x else -1, reverse=True)
         all_water_years_formatted = valid_years_list
         if all_water_years_formatted: latest_year_formatted = all_water_years_formatted[0]

    selected_water_years_formatted = st.sidebar.multiselect("انتخاب سال(های) آبی", options=all_water_years_formatted, default=all_water_years_formatted) # Default to all years
    
    all_counties = ['همه']
    if not df_all_data.empty and 'County' in df_all_data.columns:
        all_counties.extend(sorted(list(set(c for c in df_all_data['County'].dropna().unique() if c != 'نامشخص'))))
    selected_county_sidebar = st.sidebar.selectbox("انتخاب شهرستان", options=all_counties, key="county_sidebar_filter")

    df_filtered = df_all_data.copy()
    if selected_water_years_formatted:
        df_filtered = df_filtered[df_filtered['Water_Year_Formatted'].isin(selected_water_years_formatted)]
    if selected_county_sidebar != "همه":
        df_filtered = df_filtered[df_filtered['County'] == selected_county_sidebar]

    df_dam_detailed = df_filtered[df_filtered['Source_Type'].isin(['Surface', 'Transfer'])].copy()
    df_gw_detailed = df_filtered[df_filtered['Source_Type'] == 'Groundwater'].copy()

    # --- Page Display Functions ---
    def display_detailed_analysis(df_dam_viz, df_gw_viz):
        """Displays the detailed charts and tables."""
        st.title("💧 داشبورد حسابداری آب - تحلیل جزئی")
        st.header("🌊 تحلیل داده‌های سد و آب انتقالی")

        if df_dam_viz is None or df_dam_viz.empty:
            st.warning(f"داده‌ای برای سد/انتقالی با فیلترهای انتخاب شده یافت نشد (سال آبی: {', '.join(selected_water_years_formatted)}, شهرستان: {selected_county_sidebar}).")
        else:
            dam_names = ['همه'] + sorted(df_dam_viz['Source_Name'].dropna().unique())
            selected_dam = st.selectbox("انتخاب سد / منبع انتقالی", dam_names, key="dam_select_detail")
            df_dam_viz_filtered = df_dam_viz if selected_dam == "همه" else df_dam_viz[df_dam_viz['Source_Name'] == selected_dam]

            if not df_dam_viz_filtered.empty:
                plot_numeric_cols = ['Volume_Start_Year', 'Volume_End_Year', 'Level_Start_Year', 'Level_End_Year', 'Inflow', 'Leakage', 'Pumping_Out', 'Drainage', 'Evaporation', 'Sediment_Discharge', 'Intake_Discharge', 'Spillway_Discharge', 'Extraction_MCM']
                for col in plot_numeric_cols:
                    if col not in df_dam_viz_filtered.columns: df_dam_viz_filtered[col] = 0
                    else: df_dam_viz_filtered[col] = safe_to_numeric(df_dam_viz_filtered[col]).fillna(0)
                df_dam_viz_filtered = df_dam_viz_filtered.sort_values(by='Water_Year_Formatted', key=lambda x: x.str.split('-').str[0].astype(int))

                col1, col2 = st.columns(2)
                if 'Volume_Start_Year' in df_dam_viz_filtered.columns and 'Volume_End_Year' in df_dam_viz_filtered.columns:
                    with col1:
                        st.subheader("حجم آب سد (MCM)")
                        fig_dam_vol = px.line(df_dam_viz_filtered, x='Water_Year_Formatted', y=['Volume_Start_Year', 'Volume_End_Year'], title=f"حجم آب برای {selected_dam}", labels={'Water_Year_Formatted': 'سال آبی', 'value': 'حجم (میلیون متر مکعب)', 'variable': 'اندازه‌گیری'}, markers=True)
                        fig_dam_vol.update_xaxes(categoryorder='array', categoryarray=df_dam_viz_filtered['Water_Year_Formatted'].unique())
                        st.plotly_chart(fig_dam_vol, use_container_width=True)
                else:
                    with col1:
                        st.info("داده‌های حجم برای نمایش موجود نیست.")
                if 'Level_Start_Year' in df_dam_viz_filtered.columns and 'Level_End_Year' in df_dam_viz_filtered.columns:
                    with col2:
                        st.subheader("تراز آب سد (m)")
                        fig_dam_level = px.line(df_dam_viz_filtered, x='Water_Year_Formatted', y=['Level_Start_Year', 'Level_End_Year'], title=f"تراز آب برای {selected_dam}", labels={'Water_Year_Formatted': 'سال آبی', 'value': 'تراز (متر)', 'variable': 'اندازه‌گیری'}, markers=True)
                        fig_dam_level.update_xaxes(categoryorder='array', categoryarray=df_dam_viz_filtered['Water_Year_Formatted'].unique())
                        st.plotly_chart(fig_dam_level, use_container_width=True)
                else:
                    with col2:
                        st.info("داده‌های تراز برای نمایش موجود نیست.")

                st.subheader(f"مولفه‌های بیلان آب برای {selected_dam} (MCM)")
                balance_cols = ['Inflow', 'Leakage', 'Pumping_Out', 'Drainage', 'Evaporation', 'Sediment_Discharge', 'Intake_Discharge', 'Spillway_Discharge', 'Extraction_MCM']
                balance_cols_present = [col for col in balance_cols if col in df_dam_viz_filtered.columns and df_dam_viz_filtered[col].sum() > 0]
                if balance_cols_present:
                    df_balance = df_dam_viz_filtered.groupby('Water_Year_Formatted')[balance_cols_present].sum().reset_index() if selected_dam == "همه" else df_dam_viz_filtered[['Water_Year_Formatted'] + balance_cols_present].copy()
                    title_suffix = "(تجمیعی)" if selected_dam == "همه" else f"برای {selected_dam}"
                    df_balance_melt = df_balance.melt(id_vars='Water_Year_Formatted', value_vars=balance_cols_present, var_name='مولفه', value_name='حجم (MCM)')
                    df_balance_melt = df_balance_melt.sort_values(by='Water_Year_Formatted', key=lambda x: x.str.split('-').str[0].astype(int))
                    fig_balance = px.bar(df_balance_melt, x='Water_Year_Formatted', y='حجم (MCM)', color='مولفه', title=f"مولفه‌های بیلان آب {title_suffix} ({', '.join(selected_water_years_formatted)})", labels={'Water_Year_Formatted': 'سال آبی'}, barmode='group')
                    fig_balance.update_xaxes(categoryorder='array', categoryarray=df_balance_melt['Water_Year_Formatted'].unique())
                    st.plotly_chart(fig_balance, use_container_width=True)
                else:
                    st.info("داده‌های مولفه‌های بیلان برای نمایش موجود نیست.")
                st.subheader(f"داده‌های فیلتر شده سد/انتقالی ({selected_dam})")
                st.dataframe(df_dam_viz_filtered)
            else:
                st.warning(f"داده‌ای برای سد/انتقالی با فیلترهای انتخاب شده یافت نشد (سال آبی: {', '.join(selected_water_years_formatted)}, شهرستان: {selected_county_sidebar}, منبع: {selected_dam}).")

        st.divider()
        st.header("🌍 تحلیل داده‌های آب زیرزمینی")
        if df_gw_viz is None or df_gw_viz.empty:
            st.warning(f"داده‌ای برای آب زیرزمینی با فیلترهای انتخاب شده یافت نشد (سال آبی: {', '.join(selected_water_years_formatted)}, شهرستان: {selected_county_sidebar}).")
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
            df_gw_viz_filtered = df_gw_viz.copy()
            if selected_gw_usage != "همه": df_gw_viz_filtered = df_gw_viz_filtered[df_gw_viz_filtered['Usage_Type'] == selected_gw_usage]
            if selected_well_type != "همه" and 'Well_Type' in df_gw_viz_filtered.columns: df_gw_viz_filtered = df_gw_viz_filtered[df_gw_viz_filtered['Well_Type'] == selected_well_type]
            if selected_well_status != "همه" and 'Well_Status' in df_gw_viz_filtered.columns: df_gw_viz_filtered = df_gw_viz_filtered[df_gw_viz_filtered['Well_Status'] == selected_well_status]

            if not df_gw_viz_filtered.empty:
                df_gw_viz_filtered = df_gw_viz_filtered.sort_values(by='Water_Year_Formatted', key=lambda x: x.str.split('-').str[0].astype(int))
                total_extraction_mcm = df_gw_viz_filtered['Extraction_MCM'].sum()
                avg_depth = df_gw_viz_filtered['Well_Depth_m'].mean() if 'Well_Depth_m' in df_gw_viz_filtered.columns else np.nan
                num_subbasins = df_gw_viz_filtered['ID'].nunique()

                st.subheader("مقادیر خلاصه (فیلتر شده آب زیرزمینی)")
                mcol1, mcol2, mcol3 = st.columns(3)
                mcol1.metric("مجموع برداشت (میلیون متر مکعب)", f"{total_extraction_mcm:,.2f}")
                mcol2.metric("میانگین عمق چاه (متر)", f"{avg_depth:.1f}" if not pd.isna(avg_depth) else "N/A")
                mcol3.metric("تعداد زیرحوضه‌های فعال", f"{num_subbasins}")
                st.subheader("مجموع برداشت آب زیرزمینی بر اساس سال آبی و نوع کاربری (MCM)")
                df_gw_agg_usage = df_gw_viz_filtered.groupby(['Water_Year_Formatted', 'Usage_Type'])['Extraction_MCM'].sum().reset_index()
                df_gw_agg_usage = df_gw_agg_usage.sort_values(by='Water_Year_Formatted', key=lambda x: x.str.split('-').str[0].astype(int))
                fig_gw_usage = px.bar(df_gw_agg_usage, x='Water_Year_Formatted', y='Extraction_MCM', color='Usage_Type', title=f"برداشت سالانه آب زیرزمینی بر اساس نوع کاربری ({', '.join(selected_water_years_formatted)})", labels={'Water_Year_Formatted': 'سال آبی', 'Extraction_MCM': 'مجموع برداشت (میلیون متر مکعب)'})
                fig_gw_usage.update_xaxes(categoryorder='array', categoryarray=df_gw_agg_usage['Water_Year_Formatted'].unique())
                st.plotly_chart(fig_gw_usage, use_container_width=True)

                col3, col4 = st.columns(2)
                count_col = 'Well_ID_Orig' if 'Well_ID_Orig' in df_gw_viz_filtered.columns and df_gw_viz_filtered['Well_ID_Orig'].nunique() > 0 else 'ID'
                if 'Well_Type' in df_gw_viz_filtered.columns:
                    with col3:
                        st.subheader("توزیع نوع چاه (بر اساس تعداد)")
                        df_gw_count_type = df_gw_viz_filtered.groupby('Well_Type')[count_col].nunique().reset_index().rename(columns={count_col: 'Count'})
                        df_gw_count_type = df_gw_count_type[df_gw_count_type['Count'] > 0]
                        if not df_gw_count_type.empty:
                             fig_gw_type = px.pie(df_gw_count_type, names='Well_Type', values='Count', title="توزیع انواع چاه", hole=0.3)
                             st.plotly_chart(fig_gw_type, use_container_width=True)
                        else: st.info("داده‌ای برای توزیع نوع چاه موجود نیست.")
                else:
                    with col3:
                        st.info("داده نوع چاه موجود نیست.")
                if 'Well_Status' in df_gw_viz_filtered.columns:
                    with col4:
                        st.subheader("توزیع وضعیت چاه (بر اساس تعداد)")
                        df_gw_count_status = df_gw_viz_filtered.groupby('Well_Status')[count_col].nunique().reset_index().rename(columns={count_col: 'Count'})
                        df_gw_count_status = df_gw_count_status[df_gw_count_status['Count'] > 0]
                        if not df_gw_count_status.empty:
                            fig_gw_status = px.pie(df_gw_count_status, names='Well_Status', values='Count', title="توزیع وضعیت چاه‌ها", hole=0.3)
                            st.plotly_chart(fig_gw_status, use_container_width=True)
                        else: st.info("داده‌ای برای توزیع وضعیت چاه موجود نیست.")
                else:
                    with col4:
                        st.info("داده وضعیت چاه موجود نیست.")

                scatter_cols_exist = all(c in df_gw_viz_filtered.columns for c in ['Extraction_MCM', 'Operating_Hours', 'Flow_Rate_ls'])
                if scatter_cols_exist:
                    df_scatter = df_gw_viz_filtered[(df_gw_viz_filtered['Extraction_MCM'] > 0) & (df_gw_viz_filtered['Operating_Hours'] > 0)].copy()
                    if not df_scatter.empty:
                        st.subheader("برداشت (MCM) در مقابل ساعات کارکرد")
                        hover_name_col = 'Well_ID_Orig' if 'Well_ID_Orig' in df_scatter.columns else 'ID'
                        fig_scatter = px.scatter(df_scatter, x='Operating_Hours', y='Extraction_MCM', color='Usage_Type', size='Flow_Rate_ls', hover_name=hover_name_col, title="برداشت در مقابل ساعات کارکرد (اندازه بر اساس دبی)", labels={'Operating_Hours': 'ساعات کارکرد', 'Extraction_MCM': 'برداشت (میلیون متر مکعب)'})
                        st.plotly_chart(fig_scatter, use_container_width=True)
                    else:
                        st.info("داده‌ای با برداشت و ساعات کارکرد مثبت برای نمودار پراکندگی وجود ندارد.")
                else:
                    st.info("ستون‌های لازم ('Extraction_MCM', 'Operating_Hours', 'Flow_Rate_ls') برای نمودار پراکندگی موجود نیستند.")

                st.subheader("داده‌های فیلتر شده آب زیرزمینی")
                st.dataframe(df_gw_viz_filtered)
            else:
                st.warning(f"داده‌ای برای آب زیرزمینی با فیلترهای انتخاب شده یافت نشد.")

    @st.cache_data
    def load_shapefile(uploaded_file):
        """Loads a shapefile from an uploaded zip file."""
        if uploaded_file is None: return None
        try:
            zip_buffer = io.BytesIO(uploaded_file.getvalue())
            with zipfile.ZipFile(zip_buffer) as z:
                shp_file_path = None
                for filename in z.namelist():
                    if filename.lower().endswith(".shp"):
                        shp_file_path = filename
                        break
                if shp_file_path is None:
                    st.error("فایل .shp در فایل فشرده یافت نشد.")
                    return None
                import tempfile
                with tempfile.TemporaryDirectory() as tmpdir:
                    z.extractall(path=tmpdir)
                    shp_full_path = os.path.join(tmpdir, shp_file_path)
                    if os.path.exists(shp_full_path):
                        gdf = gpd.read_file(shp_full_path)
                        if gdf.crs is None:
                            gdf.set_crs("EPSG:4326", inplace=True)
                            st.warning("سیستم مختصات (CRS) برای شیپ‌فایل مشخص نشده بود. EPSG:4326 (WGS84) به عنوان پیش‌فرض در نظر گرفته شد.")
                        else:
                            gdf = gdf.to_crs("EPSG:4326")
                        return gdf
                    else:
                        st.error("خطا در استخراج یا یافتن فایل .shp در مسیر موقت.")
                        return None
        except zipfile.BadZipFile:
            st.error("فایل آپلود شده یک فایل فشرده (zip) معتبر نیست.")
            return None
        except ImportError:
            st.error("کتابخانه geopandas یافت نشد. لطفاً آن را نصب کنید: pip install geopandas fiona")
            return None
        except Exception as e:
            st.error(f"خطا در خواندن شیپ‌فایل: {e}")
            return None

    def display_water_balance_summary(df_summary_data):
        """Displays the water balance summary page with return flow coefficients."""
        st.title("💧 داشبورد حسابداری آب - خلاصه بیلان آب")
        st.markdown("خلاصه برداشت و جریان برگشتی آب (میلیون متر مکعب - MCM) بر اساس فیلترهای انتخابی.")

        col_f1, col_f2, col_f3, col_f4 = st.columns(4)
        with col_f1:
            county_options = ["همه"]
            if not df_summary_data.empty and 'County' in df_summary_data.columns:
                county_options.extend(sorted(list(set(c for c in df_summary_data['County'].dropna().unique() if c != 'نامشخص'))))
            disabled_county = selected_county_sidebar != "همه"
            default_county_index = county_options.index(selected_county_sidebar) if disabled_county and selected_county_sidebar in county_options else 0
            selected_county_summary = st.selectbox("شهرستان", options=county_options, key="county_summary_filter", index=default_county_index, disabled=disabled_county)
            if disabled_county: st.caption(f"فیلتر شهرستان '{selected_county_sidebar}' از نوار کناری اعمال شده است.")
            active_county_filter = selected_county_sidebar if disabled_county else selected_county_summary
        with col_f2:
            study_areas = ["همه"]
            df_gw_for_study_area = df_summary_data[df_summary_data['Source_Type'] == 'Groundwater']
            if active_county_filter != "همه":
                df_gw_for_study_area = df_gw_for_study_area[df_gw_for_study_area['County'] == active_county_filter]
            if not df_gw_for_study_area.empty and 'Study_Area' in df_gw_for_study_area.columns:
                study_areas.extend(sorted(df_gw_for_study_area['Study_Area'].dropna().unique()))
            selected_study_area = st.selectbox("محدوده مطالعاتی (آب زیرزمینی)", options=list(set(study_areas)), key="study_area_filter")
        with col_f3:
            usage_types = ["همه"]
            if not df_summary_data.empty and 'Usage_Type' in df_summary_data.columns:
                usage_types.extend(sorted(list(set(u for u in df_summary_data['Usage_Type'].dropna().unique() if u != 'نامشخص'))))
            selected_usage_type = st.selectbox("نوع کاربری", options=usage_types, key="usage_type_filter")
        with col_f4:
            source_options_dict = {"همه": "All", "آب سطحی (سد)": "Surface", "آب زیرزمینی": "Groundwater", "آب انتقالی": "Transfer", "تصفیه خانه": "Wastewater"}
            available_sources = df_summary_data['Source_Type'].unique() if not df_summary_data.empty else []
            display_source_options = ["همه"] + [k for k, v in source_options_dict.items() if v in available_sources and v != "All"]
            selected_source_type_display = st.selectbox("طبقه‌بندی منبع", options=display_source_options, key="source_type_filter")
            selected_source_type_val = source_options_dict.get(selected_source_type_display, "All")

        renewable_options = ["همه", "تجدیدپذیر", "تجدیدناپذیر", "نامشخص"]
        selected_renewable_status = st.selectbox("تجدیدپذیری", options=renewable_options, key="renewable_filter")
        st.caption("توجه: طبق تعریف جدید، آب سطحی و زیرزمینی تجدیدپذیر در نظر گرفته می‌شوند.")

        df_summary_filtered = df_summary_data.copy()
        if active_county_filter != "همه":
             df_summary_filtered = df_summary_filtered[df_summary_filtered['County'] == active_county_filter]
        if selected_study_area != "همه" and 'Study_Area' in df_summary_filtered.columns:
             df_summary_filtered = df_summary_filtered[(df_summary_filtered['Source_Type'] != 'Groundwater') | (df_summary_filtered['Study_Area'] == selected_study_area)]
        if selected_usage_type != "همه":
             df_summary_filtered = df_summary_filtered[df_summary_filtered['Usage_Type'] == selected_usage_type]
        if selected_source_type_val != "All":
             df_summary_filtered = df_summary_filtered[df_summary_filtered['Source_Type'] == selected_source_type_val]
        if selected_renewable_status != "همه":
            if 'Renewable_Status' in df_summary_filtered.columns:
                status_to_check = ['نامشخص', 'Unknown', None, pd.NA] if selected_renewable_status == "نامشخص" else [selected_renewable_status]
                df_summary_filtered = df_summary_filtered[df_summary_filtered['Renewable_Status'].isin(status_to_check)]
            else:
                st.warning("ستون 'Renewable_Status' برای اعمال فیلتر تجدیدپذیری یافت نشد.")

        st.divider()
        st.subheader("تعیین ضرایب بازگشت آب بر اساس نوع کاربری")
        available_usage_types = sorted([u for u in df_summary_filtered['Usage_Type'].dropna().unique() if u != 'نامشخص']) if not df_summary_filtered.empty and 'Usage_Type' in df_summary_filtered.columns else []
        
        return_flow_coeffs = {}
        if available_usage_types:
            st.write("برای هر نوع کاربری، ضریب بازگشت (عددی بین 0 و 1) را وارد کنید:")
            num_cols = min(len(available_usage_types), 5) # Display max 5 per row
            cols_coeffs = st.columns(num_cols)
            for i, usage in enumerate(available_usage_types):
                with cols_coeffs[i % num_cols]:
                    return_flow_coeffs[usage] = st.number_input(label=usage, min_value=0.0, max_value=1.0, value=0.0, step=0.05, format="%.2f", key=f"coeff_{usage}")
        else:
            st.info("نوع کاربری برای تعیین ضرایب بازگشت در داده‌های فیلتر شده یافت نشد.")

        if not df_summary_filtered.empty and return_flow_coeffs:
            df_summary_filtered['Return_Flow_MCM'] = df_summary_filtered.apply(lambda row: row['Extraction_MCM'] * return_flow_coeffs.get(row['Usage_Type'], 0), axis=1)
            df_summary_filtered['Net_Consumption_MCM'] = df_summary_filtered['Extraction_MCM'] - df_summary_filtered['Return_Flow_MCM']
        else:
            df_summary_filtered['Return_Flow_MCM'] = 0
            df_summary_filtered['Net_Consumption_MCM'] = df_summary_filtered['Extraction_MCM']

        st.divider()
        st.subheader("خلاصه مقادیر (میلیون متر مکعب - MCM)")
        metric_col1, metric_col2, metric_col3, metric_col4, metric_col5 = st.columns(5)
        total_extraction = df_summary_filtered['Extraction_MCM'].sum()
        metric_col1.metric("کل برداشت", f"{total_extraction:,.2f}")
        total_return_flow = df_summary_filtered['Return_Flow_MCM'].sum()
        metric_col2.metric("کل جریان برگشتی محاسبه‌شده", f"{total_return_flow:,.2f}")
        total_net_consumption = df_summary_filtered['Net_Consumption_MCM'].sum()
        metric_col3.metric("کل مصرف خالص محاسبه‌شده", f"{total_net_consumption:,.2f}")
        total_surface = df_summary_filtered[df_summary_filtered['Source_Type'] == 'Surface']['Extraction_MCM'].sum()
        metric_col4.metric("برداشت سطحی (سد)", f"{total_surface:,.2f}")
        total_gw = df_summary_filtered[df_summary_filtered['Source_Type'] == 'Groundwater']['Extraction_MCM'].sum()
        metric_col5.metric("برداشت زیرزمینی", f"{total_gw:,.2f}")

        st.subheader("جدول خلاصه داده‌های فیلتر شده (با احتساب بازگشت)")
        if not df_summary_filtered.empty:
            display_cols_renamed = {'Source_Type': 'طبقه‌بندی منبع', 'Source_Name': 'نام منبع', 'ID': 'شناسه زیرحوضه/منبع', 'County': 'شهرستان', 'Usage_Type': 'کاربری', 'Renewable_Status': 'وضعیت تجدیدپذیری', 'Extraction_MCM': 'برداشت (MCM)', 'Return_Flow_MCM': 'جریان برگشتی (MCM)', 'Net_Consumption_MCM': 'مصرف خالص (MCM)'}
            cols_to_display = [col for col in display_cols_renamed.keys() if col in df_summary_filtered.columns]
            df_display = df_summary_filtered[cols_to_display].rename(columns=display_cols_renamed)
            st.dataframe(df_display.style.format({'برداشت (MCM)': '{:,.2f}', 'جریان برگشتی (MCM)': '{:,.2f}', 'مصرف خالص (MCM)': '{:,.2f}'}))

            st.divider()
            st.subheader("نمودار داده‌های خلاصه شده")
            if not df_display.empty:
                chart_agg_cols = ['شهرستان', 'طبقه‌بندی منبع', 'کاربری', 'Water_Year_Formatted']
                chart_numeric_cols = ['برداشت (MCM)', 'جریان برگشتی (MCM)', 'مصرف خالص (MCM)']
                df_for_chart_agg = df_summary_filtered[['County', 'Source_Type', 'Usage_Type', 'Water_Year_Formatted', 'Extraction_MCM', 'Return_Flow_MCM', 'Net_Consumption_MCM']].copy()
                df_for_chart_agg = df_for_chart_agg.rename(columns={'County': 'شهرستان', 'Source_Type': 'طبقه‌بندی منبع', 'Usage_Type': 'کاربری', 'Extraction_MCM': 'برداشت (MCM)', 'Return_Flow_MCM': 'جریان برگشتی (MCM)', 'Net_Consumption_MCM': 'مصرف خالص (MCM)'})
                
                grouping_cols_present = [col for col in chart_agg_cols if col in df_for_chart_agg.columns]
                numeric_cols_present = [col for col in chart_numeric_cols if col in df_for_chart_agg.columns]

                if grouping_cols_present and numeric_cols_present:
                    aggregated_chart_data = df_for_chart_agg.groupby(grouping_cols_present, observed=False, as_index=False)[numeric_cols_present].sum()
                    if 'Water_Year_Formatted' in aggregated_chart_data.columns:
                        aggregated_chart_data = aggregated_chart_data.sort_values(by='Water_Year_Formatted', key=lambda x: x.str.split('-').str[0].astype(int))

                    chart_type = st.radio("انتخاب نوع نمودار:", ('میله‌ای (برداشت)', 'خطی (روند)', 'دایره‌ای (سهم)'), key="chart_select", horizontal=True)
                    try:
                        if chart_type == 'میله‌ای (برداشت)':
                            bar_data = aggregated_chart_data.groupby(['شهرستان', 'طبقه‌بندی منبع'], observed=False)[['برداشت (MCM)', 'مصرف خالص (MCM)']].sum().reset_index()
                            bar_data_melt = bar_data.melt(id_vars=['شهرستان', 'طبقه‌بندی منبع'], value_vars=['برداشت (MCM)', 'مصرف خالص (MCM)'], var_name='نوع مقدار', value_name='حجم (MCM)')
                            fig_chart = px.bar(bar_data_melt, x='شهرستان', y='حجم (MCM)', color='طبقه‌بندی منبع', facet_col='نوع مقدار', title="برداشت و مصرف خالص تجمیعی (MCM) بر اساس شهرستان و طبقه‌بندی منبع", labels={'شهرستان': 'شهرستان', 'حجم (MCM)': 'مجموع حجم (میلیون متر مکعب)', 'طبقه‌بندی منبع': 'طبقه‌بندی منبع'}, barmode='group')
                            fig_chart.update_layout(xaxis={'categoryorder':'total descending'})
                            st.plotly_chart(fig_chart, use_container_width=True)
                        elif chart_type == 'خطی (روند)':
                            if len(selected_water_years_formatted) > 1 and 'Water_Year_Formatted' in aggregated_chart_data.columns:
                                line_plot_data = aggregated_chart_data.groupby(['Water_Year_Formatted', 'طبقه‌بندی منبع'], observed=False)[['برداشت (MCM)', 'مصرف خالص (MCM)']].sum().reset_index()
                                line_plot_data_melt = line_plot_data.melt(id_vars=['Water_Year_Formatted', 'طبقه‌بندی منبع'], value_vars=['برداشت (MCM)', 'مصرف خالص (MCM)'], var_name='نوع مقدار', value_name='حجم (MCM)')
                                fig_chart = px.line(line_plot_data_melt, x='Water_Year_Formatted', y='حجم (MCM)', color='طبقه‌بندی منبع', line_dash='نوع مقدار', title="روند برداشت و مصرف خالص (MCM) در طول زمان بر اساس نوع منبع", labels={'Water_Year_Formatted': 'سال آبی', 'حجم (MCM)': 'مجموع حجم (میلیون متر مکعب)', 'طبقه‌بندی منبع': 'نوع منبع'}, markers=True)
                                fig_chart.update_xaxes(categoryorder='array', categoryarray=sorted(line_plot_data_melt['Water_Year_Formatted'].unique(), key=lambda x: int(x.split('-')[0])))
                                st.plotly_chart(fig_chart, use_container_width=True)
                            else:
                                st.warning("نمودار خطی برای نمایش روند، نیاز به انتخاب حداقل دو سال آبی در فیلتر عمومی دارد.")
                        elif chart_type == 'دایره‌ای (سهم)':
                            pie_col_options = [col for col in ['طبقه‌بندی منبع', 'کاربری', 'شهرستان'] if col in aggregated_chart_data.columns]
                            if pie_col_options:
                                pie_col = st.selectbox("نمایش توزیع بر اساس:", pie_col_options, key="pie_col_select")
                                pie_value_col = st.selectbox("مقدار برای نمایش سهم:", ['برداشت (MCM)', 'مصرف خالص (MCM)'], key="pie_value_select")
                                if pie_col in aggregated_chart_data.columns and pie_value_col in aggregated_chart_data.columns:
                                    pie_data = aggregated_chart_data.groupby(pie_col, observed=False)[pie_value_col].sum().reset_index()
                                    pie_data = pie_data[pie_data[pie_value_col] > 0]
                                    if not pie_data.empty:
                                        fig_chart = px.pie(pie_data, names=pie_col, values=pie_value_col, title=f"توزیع درصد {pie_value_col} بر اساس {pie_col}", hole=0.3)
                                        fig_chart.update_traces(textposition='inside', textinfo='percent+label')
                                        st.plotly_chart(fig_chart, use_container_width=True)
                                    else:
                                        st.warning(f"داده‌ای با مقدار {pie_value_col} مثبت برای نمایش نمودار دایره‌ای بر اساس '{pie_col}' وجود ندارد.")
                                else:
                                    st.warning(f"ستون '{pie_col}' یا '{pie_value_col}' برای رسم نمودار دایره‌ای یافت نشد.")
                            else:
                                st.warning("ستونی برای گروه‌بندی نمودار دایره‌ای در داده‌های تجمیع شده یافت نشد.")
                    except Exception as e:
                        st.error(f"خطا در رسم نمودار: {e}")
                else:
                    st.warning("داده‌ای در جدول خلاصه برای رسم نمودار وجود ندارد (یا ستون‌های لازم یافت نشدند).")

            st.divider()
            st.subheader("نقشه محدوده و برداشت")
            uploaded_shp_zip = st.file_uploader("آپلود شیپ‌فایل محدوده (فایل .zip شامل .shp, .dbf, .shx)", type="zip", key="shp_uploader")
            if uploaded_shp_zip is not None:
                gdf = load_shapefile(uploaded_shp_zip)
                if gdf is not None:
                    st.success("شیپ‌فایل با موفقیت بارگذاری و به سیستم مختصات WGS84 تبدیل شد.")
                    shp_cols = gdf.columns.tolist()
                    likely_id_cols = [col for col in shp_cols if any(sub in col.upper() for sub in ['ID', 'SUBBASIN', 'CODE', 'IDENTIFIER', 'NAME'])]
                    default_index = shp_cols.index(likely_id_cols[0]) if likely_id_cols else 0
                    id_col_shp = st.selectbox("انتخاب ستون شناسه در شیپ‌فایل برای اتصال:", options=shp_cols, index=default_index)
                    if id_col_shp and 'شناسه زیرحوضه/منبع' in df_display.columns:
                        try:
                            map_data = df_display[['شناسه زیرحوضه/منبع', 'برداشت (MCM)', 'مصرف خالص (MCM)']].copy()
                            map_data['شناسه زیرحوضه/منبع'] = map_data['شناسه زیرحوضه/منبع'].astype(str)
                            map_data_agg = map_data.groupby('شناسه زیرحوضه/منبع')[['برداشت (MCM)', 'مصرف خالص (MCM)']].sum().reset_index()
                            gdf_map = gdf[[id_col_shp, 'geometry']].copy()
                            gdf_map[id_col_shp] = gdf_map[id_col_shp].astype(str)
                            merged_gdf = gdf_map.merge(map_data_agg, left_on=id_col_shp, right_on='شناسه زیرحوضه/منبع', how='left')
                            merged_gdf['برداشت (MCM)'] = merged_gdf['برداشت (MCM)'].fillna(0)
                            merged_gdf['مصرف خالص (MCM)'] = merged_gdf['مصرف خالص (MCM)'].fillna(0)
                            map_value_col = st.selectbox("انتخاب مقدار برای نمایش روی نقشه:", ['برداشت (MCM)', 'مصرف خالص (MCM)'], key="map_value_select")
                            st.write(f"نقشه رنگ‌بندی شده بر اساس {map_value_col}:")
                            try:
                                center_lat = merged_gdf.geometry.centroid.y.mean()
                                center_lon = merged_gdf.geometry.centroid.x.mean()
                            except Exception:
                                center_lat = 36.0
                                center_lon = 58.0
                            fig_map = px.choropleth_mapbox(merged_gdf, geojson=merged_gdf.geometry, locations=merged_gdf.index, color=map_value_col, mapbox_style="carto-positron", zoom=6, center={"lat": center_lat, "lon": center_lon}, opacity=0.7, hover_name=id_col_shp, hover_data={'برداشت (MCM)': ':.2f', 'مصرف خالص (MCM)': ':.2f'}, color_continuous_scale="Viridis", title=f"نقشه {map_value_col} بر اساس محدوده")
                            fig_map.update_layout(margin={"r":0,"t":30,"l":0,"b":0})
                            st.plotly_chart(fig_map, use_container_width=True)
                        except KeyError as e:
                            st.error(f"خطا در اتصال داده‌ها به شیپ‌فایل: ستون '{e}' یافت نشد. لطفاً ستون شناسه صحیح را در شیپ‌فایل و داده‌ها بررسی کنید.")
                        except Exception as e:
                            st.error(f"خطا در ایجاد نقشه: {e}")
                    else:
                        st.warning("لطفاً ستون شناسه در شیپ‌فایل را انتخاب کنید و مطمئن شوید ستون 'شناسه زیرحوضه/منبع' در داده‌های جدول وجود دارد.")
                else:
                    st.warning("شیپ‌فایل بارگذاری نشد یا در خواندن آن خطا رخ داد.")

    def display_forecasting_page(df_filtered_for_forecast):
        """Displays the forecasting page for water resources based on filtered data."""
        st.title("📈 پیش‌بینی منابع آب")
        st.markdown("پیش‌بینی مقادیر آینده بر اساس روند داده‌های تاریخی و میانگین متحرک.")

        # --- User Inputs for Forecasting (these can stay in the sidebar) ---
        st.sidebar.header("تنظیمات پیش‌بینی")
        forecast_source_map = {
            "ورودی آب به سدها (MCM)": ("Surface", "Inflow"),
            "برداشت آب زیرزمینی (MCM)": ("Groundwater", "Extraction_MCM")
        }
        selected_source_display = st.sidebar.selectbox(
            "انتخاب منبع داده برای پیش‌بینی:",
            options=list(forecast_source_map.keys())
        )
        source_type_filter, metric_to_forecast = forecast_source_map[selected_source_display]

        years_to_forecast = st.sidebar.slider("تعداد سال‌های پیش‌بینی:", min_value=1, max_value=20, value=5)
        window_size = st.sidebar.slider("پنجره میانگین متحرک:", min_value=2, max_value=10, value=3)

        # --- Data Preparation ---
        # The incoming dataframe is already filtered by year and county from the sidebar.
        # We just need to filter by the selected source type.
        df_source_data = df_filtered_for_forecast[df_filtered_for_forecast['Source_Type'] == source_type_filter].copy()
        
        if df_source_data.empty or metric_to_forecast not in df_source_data.columns:
            st.warning(f"داده‌ای برای '{selected_source_display}' با فیلترهای کنونی یافت نشد.")
            return

        # Aggregate data by year
        historical_data = df_source_data.groupby('Water_Year_Numeric')[metric_to_forecast].sum().reset_index()
        historical_data = historical_data.dropna(subset=['Water_Year_Numeric', metric_to_forecast])
        historical_data = historical_data.sort_values('Water_Year_Numeric').reset_index(drop=True)

        if len(historical_data) < 2:
            st.warning(f"برای انجام پیش‌بینی، لطفاً حداقل دو سال آبی را در فیلتر عمومی نوار کناری انتخاب کنید. داده‌های فعلی فقط برای {len(historical_data)} سال موجود است.")
            return

        # --- Forecasting Logic ---
        # 1. Moving Average
        historical_data['Moving_Average'] = historical_data[metric_to_forecast].rolling(window=window_size, min_periods=1).mean()

        # 2. Linear Trend Forecasting
        x = historical_data['Water_Year_Numeric']
        y = historical_data[metric_to_forecast]
        
        # Fit the model
        model = np.polyfit(x, y, 1)
        predict = np.poly1d(model)

        # 3. Generate Future Years and Predictions
        last_year = int(historical_data['Water_Year_Numeric'].max())
        future_years_numeric = np.arange(last_year + 1, last_year + 1 + years_to_forecast)
        
        all_years_numeric = np.concatenate([historical_data['Water_Year_Numeric'], future_years_numeric])
        trend_line = predict(all_years_numeric)
        
        df_forecast = pd.DataFrame({
            'Water_Year_Numeric': future_years_numeric,
            'Forecasted_Value': predict(future_years_numeric)
        })
        df_forecast['Water_Year_Formatted'] = df_forecast['Water_Year_Numeric'].apply(lambda y: f"{int(y)}-{int(y)-1}")

        # --- Visualization ---
        st.subheader(f"پیش‌بینی برای: {selected_source_display}")
        
        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=historical_data['Water_Year_Numeric'],
            y=historical_data[metric_to_forecast],
            name="داده تاریخی",
            marker_color='royalblue'
        ))
        fig.add_trace(go.Scatter(
            x=historical_data['Water_Year_Numeric'],
            y=historical_data['Moving_Average'],
            mode='lines',
            name=f"میانگین متحرک ({window_size} ساله)",
            line=dict(color='orange', width=2, dash='dash')
        ))
        fig.add_trace(go.Scatter(
            x=all_years_numeric,
            y=trend_line,
            mode='lines',
            name="خط روند و پیش‌بینی",
            line=dict(color='firebrick', width=3)
        ))
        fig.update_layout(
            title=f"روند تاریخی و پیش‌بینی آینده برای {selected_source_display}",
            xaxis_title="سال آبی",
            yaxis_title=f"مقدار ({metric_to_forecast.replace('_', ' ')})",
            legend_title="راهنما"
        )
        fig.add_vline(x=last_year + 0.5, line_width=2, line_dash="dot", line_color="grey", annotation_text="شروع پیش‌بینی")
        st.plotly_chart(fig, use_container_width=True)

        # --- Display Forecast Table ---
        st.subheader("جدول مقادیر پیش‌بینی شده")
        st.dataframe(df_forecast[['Water_Year_Formatted', 'Forecasted_Value']].rename(columns={
            'Water_Year_Formatted': 'سال آبی',
            'Forecasted_Value': 'مقدار پیش‌بینی شده (MCM)'
        }).style.format({'مقدار پیش‌بینی شده (MCM)': '{:,.2f}'}))


    # --- Main App Logic ---
    if app_mode == "تحلیل جزئی":
        display_detailed_analysis(df_dam_detailed, df_gw_detailed)
    elif app_mode == "خلاصه بیلان آب":
        display_water_balance_summary(df_filtered)
    elif app_mode == "پیش‌بینی منابع آب":
        display_forecasting_page(df_filtered)

    # --- Footer ---
    st.sidebar.divider()
    st.sidebar.info("داشبورد حسابداری آب | توسعه‌یافته با Streamlit")

# --- Handle Authentication Status ---
elif authentication_status == False:
    st.error('نام کاربری یا رمز عبور اشتباه است')
elif authentication_status == None:
    st.warning('لطفاً نام کاربری و رمز عبور خود را وارد کنید')
