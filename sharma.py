import streamlit as st
from typing import Any
from streamlit_option_menu import option_menu
import plotly.express as px
import pandas as pd
import os
import mysql.connector
import json
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="sharma123@",
    database="phonepe"
)
cur = db.cursor()



def agg_trans_file(t_path, agg_trans_list):
    Dict_trans = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [],
                  'Transaction_amount': []}
    for state in agg_trans_list:
        state_path = t_path + state + "/"
        year_list = os.listdir(state_path)

        for year in year_list:
            # lst_year.append(year)
            year_path = state_path + year + "/"
            Q_file = os.listdir(year_path)

            for data in Q_file:
                # lst_Q.append(data)
                file_path = year_path + data
                file_content = open(file_path, "r")
                file_data = json.load(file_content)
                
                try:
                    for i in file_data["data"]["transactionData"]:
                        trans_name = i["name"]
                        trans_count = i["paymentInstruments"][0]["count"]
                        trans_amount = i["paymentInstruments"][0]["amount"]
                        Dict_trans['Transaction_type'].append(trans_name)
                        Dict_trans['Transaction_count'].append(trans_count)
                        Dict_trans['Transaction_amount'].append(trans_amount)
                        Dict_trans['State'].append(state)
                        Dict_trans['Year'].append(year)
                        Dict_trans['Quarter'].append(int(data.strip('.json')))
                except:
                    pass

    df_agg_trans = pd.DataFrame(Dict_trans)

    df_agg_trans.to_csv('agg_trans.csv', index=False)
    return df_agg_trans


def a_trans(trans_df):
    cur.execute("DROP table IF EXISTS agg_trans;")
    db.commit()
    cur.execute(
        "CREATE table agg_trans (State varchar(100), Year int, Quarter int, Transaction_type varchar(100), Transaction_count int, Transaction_amount real)")
    for i, row in trans_df.iterrows():
        sql = "INSERT INTO agg_trans VALUES (%s,%s,%s,%s,%s,%s)"
        cur.execute(sql, tuple(row))
        db.commit()


# table 2
def agg_user_file(t_path, agg_user_list):
    Dict_user = {'State': [], 'Year': [], 'Quarter': [], 'Brand': [], 'Count': [], 'Percentage': []}
    for state in agg_user_list:
        state_path = t_path + state + "/"
        year_list = os.listdir(state_path)
        for year in year_list:
            year_path = state_path + year + "/"
            Q_file = os.listdir(year_path)
            for data in Q_file:
                file_path = year_path + data
                file_content = open(file_path, "r")
                file_data = json.load(file_content)
                if (file_data["data"]["usersByDevice"] == None):
                    pass
                else:
                    try:
                        for i in file_data["data"]["usersByDevice"]:
                            user_brand = i["brand"]
                            user_count = i["count"]
                            user_percentage = i["percentage"]
                            Dict_user['Brand'].append(user_brand)
                            Dict_user['Count'].append(user_count)
                            Dict_user['Percentage'].append(user_percentage)
                            Dict_user['State'].append(state)
                            Dict_user['Year'].append(year)
                            Dict_user['Quarter'].append(int(data.strip('.json')))
                    except:
                        pass
    df_agg_user = pd.DataFrame(Dict_user)
    df_agg_user.to_csv('agg_user.csv', index=False)
    return df_agg_user


def a_user(user_df):
    cur.execute("DROP table IF EXISTS agg_user;")
    db.commit()
    cur.execute(
        "create table agg_user (State varchar(100), Year int, Quarter int, Brand varchar(100), Transaction_Count int, Percentage float)")
    for i, row in user_df.iterrows():
        sql = "INSERT INTO agg_user VALUES (%s,%s,%s,%s,%s,%s)"
        cur.execute(sql, tuple(row))
        db.commit()


# table 3
def map_trans_file(t_path, map_trans_list):
    Dict_trans = {'State': [], 'Year': [], 'Quarter': [], 'Name': [], 'Count': [], 'Amount': []}
    for state in map_trans_list:
        state_path = t_path + state + "/"
        year_list = os.listdir(state_path)
        # print(year_list)
        for year in year_list:
            year_path = state_path + year + "/"
            Q_file = os.listdir(year_path)
            # print(Q_file)
            for data in Q_file:
                file_path = year_path + data
                file_content = open(file_path, "r")
                file_data = json.load(file_content)
                try:
                    for i in file_data["data"]["hoverDataList"]:
                        trans_name = i["name"]
                        trans_count = i["metric"][0]["count"]
                        trans_amount = i["metric"][0]["amount"]
                        Dict_trans['Name'].append(trans_name)
                        Dict_trans['Count'].append(trans_count)
                        Dict_trans['Amount'].append(trans_amount)
                        Dict_trans['State'].append(state)
                        Dict_trans['Year'].append(year)
                        Dict_trans['Quarter'].append(int(data.strip('.json')))
                except:
                    pass
    df_map_trans = pd.DataFrame(Dict_trans)
    df_map_trans.to_csv('agg_trans.csv', index=False)
    return df_map_trans


def m_trans(trans_df):
    cur.execute("DROP table IF EXISTS map_trans;")
    db.commit()
    cur.execute(
        "CREATE table map_trans (State varchar(100), Year int, Quarter int, District varchar(100), Count int, Amount real)")
    for i, row in trans_df.iterrows():
        sql = "INSERT INTO map_trans VALUES (%s,%s,%s,%s,%s,%s)"
        cur.execute(sql, tuple(row))
        db.commit()


# table 4
def map_user_file(t_path, map_user_list):
    Dict_user = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'RegisteredUsers': [], 'AppOpens': []}
    for state in map_user_list:
        state_path = t_path + state + "/"
        year_list = os.listdir(state_path)
        for year in year_list:
            year_path = state_path + year + "/"
            Q_file = os.listdir(year_path)
            for data in Q_file:
                file_path = year_path + data
                file_content = open(file_path, "r")
                file_data = json.load(file_content)
                if (file_data["data"]["hoverData"] == None):
                    pass
                else:
                    try:
                        for i in file_data["data"]["hoverData"].items():
                            user_dist = i[0]
                            user_reg = i[1]["registeredUsers"]
                            user_app = i[1]["appOpens"]
                            Dict_user['District'].append(user_dist)
                            Dict_user['RegisteredUsers'].append(user_reg)
                            Dict_user['AppOpens'].append(user_app)
                            Dict_user['State'].append(state)
                            Dict_user['Year'].append(year)
                            Dict_user['Quarter'].append(int(data.strip('.json')))
                    except:
                        pass

    df_map_user = pd.DataFrame(Dict_user)
    df_map_user.to_csv('map_user.csv', index=False)
    return df_map_user


def m_user(user_df):
    cur.execute("DROP table IF EXISTS map_user;")
    db.commit()
    cur.execute(
        "CREATE table map_user (State varchar(100), Year int, Quarter int, District varchar(100), RegisteredUsers int, AppOpens int)")
    for i, row in user_df.iterrows():
        sql = "INSERT INTO map_user VALUES (%s,%s,%s,%s,%s,%s)"
        cur.execute(sql, tuple(row))
        db.commit()


# table 5
def top_dist_trans_file(t_path, top_dist_trans_list):
    Dict_trans = {'State': [], 'Year': [], 'Quarter': [], 'District_Name': [], 'Count': [], 'Amount': []}
    for state in top_dist_trans_list:
        state_path = t_path + state + "/"
        year_list = os.listdir(state_path)
        for year in year_list:
            year_path = state_path + year + "/"
            Q_file = os.listdir(year_path)
            for data in Q_file:
                file_path = year_path + data
                file_content = open(file_path, "r")
                file_data = json.load(file_content)

                for i in file_data["data"]["districts"]:
                    trans_dist_name = i["entityName"]
                    trans_count = i["metric"]["count"]
                    trans_amount = i["metric"]["amount"]
                    Dict_trans['District_Name'].append(trans_dist_name)
                    Dict_trans['Count'].append(trans_count)
                    Dict_trans['Amount'].append(trans_amount)
                    Dict_trans['State'].append(state)
                    Dict_trans['Year'].append(year)
                    Dict_trans['Quarter'].append(int(data.strip('.json')))

    df_top_dist_trans = pd.DataFrame(Dict_trans)

    df_top_dist_trans.to_csv('top_dist_trans.csv', index=False)
    return df_top_dist_trans


def top_dist_trans(top_dist_trans_df):
    cur.execute("DROP table IF EXISTS top_dist_trans;")
    db.commit()
    cur.execute(
        "CREATE table top_dist_trans (State varchar(100), Year int, Quarter int, District varchar(100), Count int, Amount real)")
    for i, row in top_dist_trans_df.iterrows():
        sql = "INSERT INTO top_dist_trans VALUES (%s,%s,%s,%s,%s,%s)"
        cur.execute(sql, tuple(row))
        db.commit()


# table 6
def top_pin_trans_file(t_path, top_pin_trans_list):
    Dict_trans = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Count': [], 'Amount': []}
    for state in top_pin_trans_list:
        state_path = t_path + state + "/"
        year_list = os.listdir(state_path)
        for year in year_list:
            year_path = state_path + year + "/"
            Q_file = os.listdir(year_path)
            for data in Q_file:
                file_path = year_path + data
                file_content = open(file_path, "r")
                file_data = json.load(file_content)
                try:
                    for i in file_data["data"]["pincodes"]:
                        trans_pin_name = i["entityName"]
                        trans_count = i["metric"]["count"]
                        trans_amount = i["metric"]["amount"]
                        Dict_trans['Pincode'].append(trans_pin_name)
                        Dict_trans['Count'].append(trans_count)
                        Dict_trans['Amount'].append(trans_amount)
                        Dict_trans['State'].append(state)
                        Dict_trans['Year'].append(year)
                        Dict_trans['Quarter'].append(int(data.strip('.json')))
                except:
                    pass

    df_top_pin_trans = pd.DataFrame(Dict_trans)

    df_top_pin_trans.to_csv('top_pin_trans.csv', index=False)
    return df_top_pin_trans


def top_pin_trans(top_pin_trans_df):
    cur.execute("DROP table IF EXISTS top_pin_trans;")
    db.commit()
    cur.execute(
        "CREATE table top_pin_trans (State varchar(100), Year int, Quarter int, Pincode int, Count int, Amount real)")
    for i, row in top_pin_trans_df.iterrows():
        sql = "INSERT INTO top_pin_trans VALUES (%s,%s,%s,%s,%s,%s)"
        cur.execute(sql, tuple(row))
        db.commit()


# table 7
def top_dist_user_file(t_path, top_dist_user_list):
    Dict_user = {'State': [], 'Year': [], 'Quarter': [], 'District_Name': [], 'RegisteredUsers': []}
    for state in top_dist_user_list:
        state_path = t_path + state + "/"
        year_list = os.listdir(state_path)
        for year in year_list:
            year_path = state_path + year + "/"
            Q_file = os.listdir(year_path)
            for data in Q_file:
                file_path = year_path + data
                file_content = open(file_path, "r")
                file_data = json.load(file_content)
                for i in file_data["data"]["districts"]:
                    user_dist_name = i["name"]
                    reg_users_count = i["registeredUsers"]
                    Dict_user['District_Name'].append(user_dist_name)
                    Dict_user['RegisteredUsers'].append(reg_users_count)
                    Dict_user['State'].append(state)
                    Dict_user['Year'].append(year)
                    Dict_user['Quarter'].append(int(data.strip('.json')))

    df_top_dist_user = pd.DataFrame(Dict_user)

    df_top_dist_user.to_csv('top_dist_user.csv', index=False)

    return df_top_dist_user


def top_dist_user(top_dist_user_df):
    cur.execute("DROP table IF EXISTS top_dist_user;")
    db.commit()
    cur.execute(
        "CREATE table top_dist_user (State varchar(100), Year int, Quarter int, District varchar(100), Registered_Users int)")
    for i, row in top_dist_user_df.iterrows():
        sql = "INSERT INTO top_dist_user VALUES (%s,%s,%s,%s,%s)"
        cur.execute(sql, tuple(row))
        db.commit()


# table 8
def top_pin_user_file(t_path, top_pin_user_list):
    Dict_user = {'State': [], 'Year': [], 'Quarter': [], 'pincode': [], 'RegisteredUsers': []}
    for state in top_pin_user_list:
        state_path = t_path + state + "/"
        year_list = os.listdir(state_path)
    
        for year in year_list:
            year_path = state_path + year + "/"
            Q_file = os.listdir(year_path)
    
            for data in Q_file:
                file_path = year_path + data
                file_content = open(file_path, "r")
                file_data = json.load(file_content)
    

                for i in file_data["data"]["pincodes"]:
                    user_pin = i["name"]
                    reg_users_count = i["registeredUsers"]
                    Dict_user['pincode'].append(user_pin)
                    Dict_user['RegisteredUsers'].append(reg_users_count)
                    Dict_user['State'].append(state)
                    Dict_user['Year'].append(year)
                    Dict_user['Quarter'].append(int(data.strip('.json')))

    df_top_pin_user = pd.DataFrame(Dict_user)
    df_top_pin_user.to_csv('top_dist_user.csv', index=False)
  
    return df_top_pin_user


def top_pin_user(top_pin_user_df):
    cur.execute("DROP table IF EXISTS top_pin_user;")
    db.commit()
    cur.execute(
        "CREATE table top_pin_user (State varchar(100), Year int, Quarter int, Pincode varchar(100), Registered_Users int)")
    for i, row in top_pin_user_df.iterrows():
        sql = "INSERT INTO top_pin_user VALUES (%s,%s,%s,%s,%s)"
        cur.execute(sql, tuple(row))
        db.commit()


# 1.Aggregated transaction
aggre_trans_path = "C:\\Users\\I NOT STUDIOS\\Desktop\\sam important\\project\\pulse\\data\\aggregated\\transaction\\country\\india\\state\\"
aggre_trans_list = os.listdir(aggre_trans_path)
# print(aggri_trans_list)

trans_df = agg_trans_file(aggre_trans_path, aggre_trans_list)
a_trans(trans_df)

# 2.Aggregated User
aggre_user_path = "C:\\Users\\I NOT STUDIOS\\Desktop\\sam important\\project\pulse\\data\\aggregated\\user\\country\\india\\state\\"
aggre_user_list = os.listdir(aggre_user_path)

user_df = agg_user_file(aggre_user_path, aggre_user_list)
a_user(user_df)

# 3.Map Trans
map_trans_path = "C:\\Users\\I NOT STUDIOS\\Desktop\\sam important\\project\\pulse\\data\\map\\transaction\\hover\\country\\india\\state\\"
map_trans_list = os.listdir(map_trans_path)

m_trans_df = map_trans_file(map_trans_path, map_trans_list)
m_trans(m_trans_df)

# 4.Map User
map_user_path = "C:\\Users\\I NOT STUDIOS\\Desktop\\sam important\\project\\pulse\\data\\map\\user\\hover\\country\\india\\state\\"
map_user_list = os.listdir(map_user_path)

m_user_df = map_user_file(map_user_path, map_user_list)
m_user(m_user_df)

# 5top dist trans
top_dist_trans_path = "C:\\Users\\I NOT STUDIOS\\Desktop\\sam important\\project\\pulse\\data\\top\\transaction\\country\\india\\state\\"
top_dist_trans_list = os.listdir(top_dist_trans_path)

top_dist_trans_df = top_dist_trans_file(top_dist_trans_path, top_dist_trans_list)
top_dist_trans(top_dist_trans_df)

# 6top pincode trans
top_pin_trans_path = "C:\\Users\\I NOT STUDIOS\\Desktop\\sam important\\project\\pulse\\data\\top\\transaction\\country\\india\\state\\"
top_pin_trans_list = os.listdir(top_pin_trans_path)

top_pin_trans_df = top_pin_trans_file(top_pin_trans_path, top_pin_trans_list)
top_pin_trans(top_pin_trans_df)

# 7top dist user
top_dist_user_path = "C:\\Users\\I NOT STUDIOS\\Desktop\\sam important\\project\\pulse\\data\\top\\user\\country\\india\\state\\"
top_dist_user_list = os.listdir(top_dist_user_path)

top_dist_user_df = top_dist_user_file(top_dist_user_path, top_dist_user_list)
top_dist_user(top_dist_user_df)

# 8top pin user
top_pin_user_path = "C:\\Users\\I NOT STUDIOS\\Desktop\\sam important\\project\\pulse\\data\\top\\user\\country\\india\\state\\"
top_pin_user_list = os.listdir(top_pin_user_path)

top_pin_user_df = top_pin_user_file(top_pin_user_path, top_pin_user_list)
top_pin_user(top_pin_user_df)

state_in_sql: Any = {"Andaman & Nicobar": "andaman-&-nicobar-islands", "Andhra Pradesh": "andhra-pradesh",
                "Arunachal Pradesh": "arunachal-pradesh",
                "Assam": "assam", "Bihar": "bihar", "Chandigarh": "chandigarh", "Chhattisgarh": "chhattisgarh",
                "Dadra & Nagar-Haweli & Daman & Diu": "dadra-&-nagar-haveli-&-daman-&-diu", "Delhi": "delhi",
                "Goa": "goa", "Gujarat": "gujarat", "Haryana": "haryana",
                "Himachal Pradesh": "himachal-pradesh", "Jammu & Kashmir": "jammu-&-kashmir", "Jharkhand": "jharkhand",
                "Karnataka": "karnataka",
                "Kerala": "kerala", "Ladak": "ladakh", "Lakshadweep": "lakshadweep", "Madhya Pradesh": "madhya-pradesh",
                "Maharastra": "maharashtra",
                "Manipur": "manipur", "Meghalaya": "meghalaya", "Mizoram": "mizoram", "Nagaland": "nagaland",
                "Odisha": "odisha",
                "Pudhuchery": "puducherry", "Punjab": "punjab", "Rajasthan": "rajasthan", "Sikkim": "sikkim",
                "TamilNadu": "tamil-nadu", "Telangana": "telangana",
                "Tiripura": "tripura", "Uttarkhand": "uttarakhand", "Uttar Pradesh": "uttar-pradesh",
                "West Bengal": "west-bengal"}


# create a Streamlit app
st.set_page_config(
    page_title="PhonePe Pulse Data Visualization and Exploration",
    page_icon="🧊",
    layout="wide"
)
with st.sidebar:
    opt = option_menu(
        menu_title="Menu",
        options=[ "Home","Aggregated Analytics", "Top Analytics", "Insights"],
        icons=["house", "bar-chart", "pie-chart", "geo-alt"],
        menu_icon="cast",
        default_index=0)
if opt == "Home":
    st.title("Phonepe Pulse data Visualization and Exploration")
    st.write("Welcome to our Phonepe data analysis webpage! We have analyzed 5 years of Phonepe user data from "
             "2018 to 2022 in India, across all four quarters of each year, and have identified the states, districts, "
             "and pincodes with the highest transaction volume. Our analysis provides insights into the usage "
             "patterns of Phonepe users across the country.")
    st.write("By examining this data, we are able to pinpoint the specific regions in India where Phonepe usage is "
             "most prevalent. Our analysis includes state-level data as well as district and pincode-level data, so"
             " you can get a comprehensive understanding of how Phonepe is being used across the country.")
    st.write("Our website provides easy-to-use visualizations that allow you to explore the data and gain insights "
             "into the transaction patterns of Phonepe users. Whether you're a business owner looking to expand your "
             "customer base, or an individual interested in understanding your own usage patterns, our website has "
             "something for you. So why wait? Start exploring our data analysis today and gain valuable insights into "
             "the world of Phonepe transactions!")

elif opt == "Aggregated Analytics":
    st.title("Phonepe Pulse Data Visualization")
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        st.subheader("State")
        state_list = ["All", "Andaman & Nicobar", "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar",
                      "Chandigarh", "Chhattisgarh", "Dadra & Nagar-Haweli & Daman & Diu", "Delhi", "Goa",
                      "Gujarat", "Haryana", "Himachal Pradesh", "Jammu & Kashmir", "Jharkhand", "Karnataka",
                      "Kerala", "Ladak", "Lakshadweep", "Madhya Pradesh", "Maharastra", "Manipur", "Meghalaya",
                      "Mizoram", "Nagaland", "Odisha", "Pudhuchery", "Punjab", "Rajasthan", "Sikkim",
                      "TamilNadu", "Telangana", "Tiripura", "Uttarkhand", "Uttar Pradesh", "West Bengal"]
        state = st.selectbox(label="Select state", options=state_list)

        if state != "All":
            state = state_in_sql[state]
    with col2:
        st.subheader("Year")
        year_list = ["All", 2018, 2019, 2020, 2021, 2022]
        year = st.selectbox(label="Select year", options=year_list)
    with col3:
        st.subheader("Quarter")
        quarter_list = ["All", 1, 2, 3, 4]
        quarter = st.selectbox(label="Select quarter", options=quarter_list)

    search = st.button("Search")
    if search:
        # agg trans chart
        if state == "All" and year == "All" and quarter == "All":
            cur.execute("SELECT * FROM AGG_TRANS")
            a_t = cur.fetchall()
        elif state != "All" and year == "All" and quarter == "All":
            cur.execute(f"SELECT * FROM AGG_TRANS WHERE STATE = '{state}'")
            a_t = cur.fetchall()
        elif state == "All" and year != "All" and quarter == "All":
            cur.execute(f"SELECT * FROM AGG_TRANS WHERE YEAR = {year}")
            a_t = cur.fetchall()
        elif state == "All" and year == "All" and quarter != "All":
            cur.execute(f"SELECT * FROM AGG_TRANS WHERE QUARTER = {quarter}")
            a_t = cur.fetchall()
        elif state != "All" and year != "All" and quarter == "All":
            cur.execute(f"SELECT * FROM AGG_TRANS WHERE STATE = '{state}' AND YEAR = {year}")
            a_t = cur.fetchall()
        elif state == "All" and year != "All" and quarter != "All":
            cur.execute(f"SELECT * FROM AGG_TRANS WHERE YEAR = {year} AND QUARTER = {quarter}")
            a_t = cur.fetchall()
        else:
            cur.execute(
                f"SELECT * FROM AGG_TRANS WHERE STATE = '{state}' AND YEAR = {year} AND QUARTER = {quarter}")
            a_t = cur.fetchall()
        df_AT = pd.DataFrame(a_t, columns=["State", "Year", "Quarter", "Trans_type", "Trans_count", "Trans_amount"])
        fig = px.bar(df_AT, x="Trans_type", y="Trans_amount", color="Trans_type",
                     labels={"Trans_type": "Transaction type", "Trans_amount": "Transaction amount"},
                     title="Aggregated Transaction")
        st.plotly_chart(fig)

elif opt == "Top Analytics":
    st.title("Phonepe Pulse Data Visualization")
    col1, col2 = st.columns([2, 2])
    with col1:
        st.subheader("Year")
        year_list = ["All", 2018, 2019, 2020, 2021, 2022]
        year = st.selectbox(label="Select year", options=year_list)
    with col2:
        st.subheader("Quarter")
        quarter_list = ["All", 1, 2, 3, 4]
        quarter = st.selectbox(label="Select quarter", options=quarter_list)

    page_names = ["State", "District", "Pincode"]
    page = st.radio("select", page_names, horizontal=True, label_visibility="hidden")
    res1 = st.button("Search")

    if page == "State" and res1:
        if year == "All" and quarter == "All":
            cur.execute(
                "SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM TOP_DIST_TRANS GROUP BY "
                "STATE ORDER BY SUM(AMOUNT) DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["State", "Total Amount", "Total count"])
            fig = px.pie(df, values="Total Amount", names="State", color="State",
                         title="Top ten state-wise transactions for the last five years and all four quarters")
            st.plotly_chart(fig)

            cur.execute("SELECT STATE, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS FROM TOP_DIST_USER GROUP BY STATE "
                        "ORDER BY TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["State", "Reg_users"])
            fig_1 = px.pie(df_u, values="Reg_users", names="State", color="State",
                           title="Top ten state-wise users for the last five years and all four quarters")
            st.plotly_chart(fig_1)

        elif year != "All" and quarter == "All":
            cur.execute(f"SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM TOP_DIST_TRANS "
                        f"WHERE YEAR={year} GROUP BY STATE ORDER BY TOTAL_AMOUNT DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["State", "Total Amount", "Total count"])
            fig = px.pie(df, values="Total Amount", names="State", color="State",
                         title=f"Top ten state-wise transactions for {year} year and all four quarters ")
            st.plotly_chart(fig)

            cur.execute(f"SELECT STATE, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS FROM TOP_DIST_USER WHERE YEAR={year} "
                        f"GROUP BY STATE ORDER BY TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["State", "Reg_users"])
            fig_1 = px.pie(df_u, values="Reg_users", names="State", color="State",
                           title=f"Top ten state-wise users for {year} year and all four quarters")
            st.plotly_chart(fig_1)

        elif year == "All" and quarter != "All":
            cur.execute(f"SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM TOP_DIST_TRANS "
                        f"WHERE QUARTER={quarter} GROUP BY STATE ORDER BY TOTAL_AMOUNT DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["State", "Total Amount", "Total count"])
            fig = px.pie(df, values="Total Amount", names="State", color="State",
                         title=f"Top ten state-wise transactions for all years and {quarter} quarter")
            st.plotly_chart(fig)

            cur.execute(f"SELECT STATE, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS FROM TOP_DIST_USER WHERE "
                        f"QUARTER={quarter} GROUP BY STATE ORDER BY TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["State", "Reg Users"])
            fig_1 = px.pie(df_u, values="Reg Users", names="State", color="State",
                           title=f"Top ten state-wise users for all years and {quarter} quarter")
            st.plotly_chart(fig_1)

        else:
            cur.execute(f"SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"TOP_DIST_TRANS WHERE YEAR={year} AND QUARTER={quarter} GROUP BY STATE ORDER BY TOTAL_AMOUNT "
                        f"DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["State", "Total Amount", "Total count"])
            fig = px.pie(df, values="Total Amount", names="State", color="State",
                         title=f"Top ten state-wise transactions for year {year} and quarter {quarter}")
            st.plotly_chart(fig)

            cur.execute(f"SELECT STATE, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS "
                        f"FROM TOP_DIST_USER WHERE YEAR={year} AND QUARTER={quarter} GROUP BY STATE ORDER BY "
                        f"TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["State", "Reg_users"])
            fig_1 = px.pie(df_u, values="Reg_users", names="State", color="State",
                           title=f"Top ten state-wise users for year {year} and quarter {quarter}")
            st.plotly_chart(fig_1)


    if page == "Pincode" and res1:
        if year == "All" and quarter == "All":
            cur.execute("SELECT PINCODE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        "TOP_PIN_TRANS GROUP BY PINCODE ORDER BY TOTAL_AMOUNT DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["Pincode", "Total_Amount", "Total Count"])
            fig = px.pie(df, values="Total_Amount", names="Pincode",
                         title=f"Top ten pincode-wise transactions for all the five years and four quarters")
            st.plotly_chart(fig)

            cur.execute("SELECT PINCODE, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS FROM TOP_PIN_USER "
                        "GROUP BY PINCODE ORDER BY TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["Pincode", "Total_reg_users"])
            fig_u = px.pie(df_u, values="Total_reg_users", names="Pincode",
                           title=f"Top ten pincode-wise transactions for all the five years and four quarters")
            st.plotly_chart(fig_u)

        elif year != "All" and quarter == "All":
            cur.execute(f"SELECT PINCODE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"TOP_PIN_TRANS WHERE YEAR={year} GROUP BY PINCODE ORDER BY TOTAL_AMOUNT DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["Pincode", "Total_Amount", "Total Count"])
            fig = px.pie(df, values="Total_Amount", names="Pincode",
                         title=f"Top ten pincode-wise transactions for {year} year and all four quarters")
            st.plotly_chart(fig)

            cur.execute(f"SELECT PINCODE, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS FROM TOP_PIN_USER "
                        f"WHERE YEAR={year} GROUP BY PINCODE  ORDER BY TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["Pincode", "Total_reg_users"])
            fig_u = px.pie(df_u, values="Total_reg_users", names="Pincode",
                           title=f"Top ten pincode-wise transactions for {year} year and all four quarters")
            st.plotly_chart(fig_u)

        elif year == "All" and quarter != "All":
            cur.execute(f"SELECT PINCODE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"TOP_PIN_TRANS WHERE QUARTER={quarter} GROUP BY PINCODE ORDER BY TOTAL_AMOUNT "
                        f"DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["Pincode", "Total_Amount", "Total Count"])
            fig = px.pie(df, values="Total_Amount", names="Pincode",
                         title=f"Top ten pincode-wise transactions for all the five years and quarter {quarter}")
            st.plotly_chart(fig)

            cur.execute(f"SELECT PINCODE, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS FROM TOP_PIN_USER "
                        f"WHERE QUARTER={quarter} GROUP BY PINCODE  ORDER BY TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["Pincode", "Total_reg_users"])
            fig_u = px.pie(df_u, values="Total_reg_users", names="Pincode",
                           title=f"Top ten pincode-wise transactions for all the five years and quarter {quarter} ")
            st.plotly_chart(fig_u)

        else:
            cur.execute(f"SELECT PINCODE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"TOP_PIN_TRANS WHERE YEAR={year} AND QUARTER={quarter} GROUP BY PINCODE "
                        f"ORDER BY TOTAL_AMOUNT DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["Pincode", "Total_Amount", "Total Count"])
            fig = px.pie(df, values="Total_Amount", names="Pincode",
                         title=f"Top ten pincode-wise transactions for year  {year} and quarter {quarter}")
            st.plotly_chart(fig)

            cur.execute(f"SELECT PINCODE, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS FROM TOP_PIN_USER "
                        f"WHERE YEAR={year} AND QUARTER={quarter} GROUP BY PINCODE  ORDER BY TOTAL_REG_USERS "
                        f"DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["Pincode", "Total_reg_users"])
            fig_u = px.pie(df_u, values="Total_reg_users", names="Pincode",
                           title=f"Top ten pincode-wise transactions for year {year} and quarter {quarter}")
            st.plotly_chart(fig_u)

    if page == "District" and res1:
        if year == "All" and quarter == "All":
            cur.execute("SELECT DISTRICT, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM TOP_DIST_TRANS "
                        "GROUP BY DISTRICT ORDER BY TOTAL_AMOUNT DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["District", "Total_Amount", "Total Count"])
            fig = px.pie(df, values="Total_Amount", names="District",
                         title="Top ten district-wise transactions for all the five years and four quarters")
            st.plotly_chart(fig)

            cur.execute("SELECT DISTRICT, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS  FROM TOP_DIST_USER "
                        "GROUP BY DISTRICT ORDER BY TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["District", "Total Reg users"])
            fig_u = px.pie(df_u, values="Total Reg users", names="District",
                           title="Top ten district-wise users for all the five years and four quarters")
            st.plotly_chart(fig_u)

        elif year != "All" and quarter == "All":
            cur.execute(f"SELECT DISTRICT, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM TOP_DIST_TRANS "
                        f"WHERE YEAR={year} GROUP BY DISTRICT ORDER BY TOTAL_AMOUNT DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["District", "Total_Amount", "Total Count"])
            fig = px.pie(df, values="Total_Amount", names="District",
                         title=f"Top ten district-wise transactions for {year} year and all four quarters")
            st.plotly_chart(fig)

            cur.execute(f"SELECT DISTRICT, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS  FROM TOP_DIST_USER WHERE "
                        f"YEAR={year} GROUP BY DISTRICT ORDER BY TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["District", "Total Reg users"])
            fig_u = px.pie(df_u, values="Total Reg users", names="District",
                           title=f"Top ten district-wise users for {year} year and all four quarters")
            st.plotly_chart(fig_u)

        elif year == "All" and quarter != "All":
            cur.execute(f"SELECT DISTRICT, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM TOP_DIST_TRANS "
                        f"WHERE QUARTER={quarter} GROUP BY DISTRICT ORDER BY TOTAL_AMOUNT DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["District", "Total_Amount", "Total Count"])
            fig = px.pie(df, values="Total_Amount", names="District",
                         title=f"Top ten district-wise transactions for all the five years and quarter {quarter}")
            st.plotly_chart(fig)

            cur.execute(f"SELECT DISTRICT, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS  FROM TOP_DIST_USER WHERE "
                        f"QUARTER={quarter} GROUP BY DISTRICT ORDER BY TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["District", "Total Reg users"])
            fig_u = px.pie(df_u, values="Total Reg users", names="District",
                           title=f"Top ten district-wise users for all the five years and quarter {quarter}")
            st.plotly_chart(fig_u)

        else:
            cur.execute(f"SELECT DISTRICT, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM TOP_DIST_TRANS "
                        f"WHERE YEAR={year} AND QUARTER={quarter} GROUP BY DISTRICT ORDER BY TOTAL_AMOUNT "
                        f"DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["District", "Total_Amount", "Total Count"])
            fig = px.pie(df, values="Total_Amount", names="District",
                         title=f"Top ten district-wise transactions for year {year} and quarter {quarter}")
            st.plotly_chart(fig)

            cur.execute(f"SELECT DISTRICT, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS  FROM TOP_DIST_USER WHERE "
                        f"YEAR={year} AND QUARTER={quarter} GROUP BY DISTRICT ORDER BY TOTAL_REG_USERS "
                        f"DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["District", "Total Reg users"])
            fig_u = px.pie(df_u, values="Total Reg users", names="District",
                           title=f"Top ten district-wise users for {year} year and quarter {quarter} ")
            st.plotly_chart(fig_u)
else:
    st.title("Phonepe Pulse Data Visualization")
    col1, col2, col3 = st.columns([3, 2, 2])
    with col1:
        st.subheader("State")
        state_list = ["All", "Andaman & Nicobar", "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar",
                      "Chandigarh", "Chhattisgarh", "Dadra & Nagar-Haweli & Daman & Diu", "Delhi", "Goa",
                      "Gujarat", "Haryana", "Himachal Pradesh", "Jammu & Kashmir", "Jharkhand", "Karnataka",
                      "Kerala", "Ladak", "Lakshadweep", "Madhya Pradesh", "Maharastra", "Manipur", "Meghalaya",
                      "Mizoram", "Nagaland", "Odisha", "Pudhuchery", "Punjab", "Rajasthan", "Sikkim",
                      "TamilNadu", "Telangana", "Tiripura", "Uttarkhand", "Uttar Pradesh", "West Bengal"]
        state = st.selectbox(label="Select state", options=state_list)
        if state != "All":
            state = state_in_sql[state]
    with col2:
        st.subheader("Year")
        year_list = ["All", 2018, 2019, 2020, 2021, 2022]
        year = st.selectbox(label="Select year", options=year_list)
    with col3:
        st.subheader("Quarter")
        quarter_list = ["All", 1, 2, 3, 4]
        quarter = st.selectbox(label="Select quarter", options=quarter_list)

    result = st.button("Search")
    if result:
        state_name = {'All': 'All', 'andaman-&-nicobar-islands': 'Andaman & Nicobar',
                      'andhra-pradesh': 'Andhra Pradesh',
                      'arunachal-pradesh': 'Arunachal Pradesh', 'assam': 'Assam', 'bihar': 'Bihar',
                      'chandigarh': 'Chandigarh',
                      'chhattisgarh': 'Chhattisgarh',
                      'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
                      'delhi': 'Delhi', 'goa': 'Goa', 'gujarat': 'Gujarat',
                      'haryana': 'Haryana', 'himachal-pradesh': 'Himachal Pradesh',
                      'jammu-&-kashmir': 'Jammu & Kashmir',
                      'jharkhand': 'Jharkhand',
                      'karnataka': 'Karnataka', 'kerala': 'Kerala', 'ladakh': 'Ladakh', 'lakshadweep': 'Lakshadweep',
                      'madhya-pradesh': 'Madhya Pradesh',
                      'maharashtra': 'Maharashtra', 'manipur': 'Manipur', 'meghalaya': 'Meghalaya',
                      'mizoram': 'Mizoram',
                      'nagaland': 'Nagaland',
                      'odisha': 'Odisha', 'puducherry': 'Puducherry', 'punjab': 'Punjab', 'rajasthan': 'Rajasthan',
                      'sikkim': 'Sikkim',
                      'tamil-nadu': 'Tamil Nadu', 'telangana': 'Telangana', 'tripura': 'Tripura',
                      'uttar-pradesh': 'Uttar Pradesh',
                      'uttarakhand': 'Uttarakhand', 'west-bengal': 'West Bengal'}

        if state == "All" and year == "All" and quarter == "All":
            cur.execute("SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT "
                        "FROM MAP_TRANS GROUP BY STATE")
            fetch = cur.fetchall()

        elif state != "All" and year == "All" and quarter == "All":
            cur.execute(f"SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"MAP_TRANS WHERE STATE='{state}' GROUP BY STATE")
            fetch = cur.fetchall()

        elif state == "All" and year != "All" and quarter == "All":
            cur.execute(f"SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"MAP_TRANS WHERE YEAR={year} GROUP BY STATE")
            fetch = cur.fetchall()

        elif state == "All" and year == "All" and quarter != "All":
            cur.execute(f"SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"MAP_TRANS WHERE QUARTER={quarter} GROUP BY STATE")
            fetch = cur.fetchall()

        elif state != "All" and year != "All" and quarter == "All":
            cur.execute(f"SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"MAP_TRANS WHERE STATE='{state}' AND YEAR={year} GROUP BY STATE")
            fetch = cur.fetchall()

        elif state == "All" and year != "All" and quarter != "All":
            cur.execute(f"SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"MAP_TRANS WHERE YEAR={year} AND QUARTER={quarter} GROUP BY STATE")
            fetch = cur.fetchall()

        else:
            cur.execute(f"SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"MAP_TRANS WHERE STATE='{state}' AND YEAR={year} AND QUARTER={quarter} GROUP BY "
                        f"STATE")
            fetch = cur.fetchall()

        df = pd.DataFrame(fetch, columns=["State", "Total Amount", "Total count"])
        df["State"] = df["State"].replace(state_name)
        fig = px.choropleth(df,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112"
                                    "/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey="properties.ST_NM",
                            locations="State",
                            color="Total Amount",
                            hover_data=["State", "Total Amount", "Total count"],
                            color_continuous_scale="Bluyl")
        fig.update_geos(fitbounds='locations', visible=False)
        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        fig.show()
