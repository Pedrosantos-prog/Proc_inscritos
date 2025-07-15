import streamlit as st
import mysql.connector
from mysql.connector import Error

def connect_bd():
    """Conecta ao banco de dados usando secrets do Streamlit"""
    try:
        connection = mysql.connector.connect(
            host=st.secrets["database"]["host"],
            port=st.secrets["database"]["port"],
            database=st.secrets["database"]["database"],
            user=st.secrets["database"]["username"],
            password=st.secrets["database"]["password"]
        )
        return connection
    except Error as e:
        st.error(f"Erro de conexão com o banco: {e}")   
        return None

def close_bd(connection):
    """Fecha a conexão com o banco"""
    if connection and connection.is_connected():
        connection.close()