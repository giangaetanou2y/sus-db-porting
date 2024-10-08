#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct  3 11:31:39 2024

@author: up2you
"""

import mysql.connector
import streamlit as st
import pandas as pd
import json
from datetime import date
from datetime import datetime

#mysql-connector-python 
# pip uninstall mysql.connector


mappatura_tabelle = {
    "answers": {"order": 1, "key": ["uuid"]},
    "questions": {"order": 2, "key": ["tag"]},
    "survey": {"order": 3, "key": ["keyword"]},
    "survey_question": {"order": 4, "key": ["survey_keyword", "question_tag"]}
}
    
dict_of_changes={}
dict_of_changes['update']={}
dict_of_changes['insert']={}


def verify_sql(date_input):
    db_source = mysql.connector.connect(
        host="49.13.53.216",
        user="airbyte",
        password="DO184ani7ANu8e4uMucEyaWE3A576I",
        database="calcolatore_airbyte_1"
    )
    # Connessione al database di produzione
    db_dest = mysql.connector.connect(
        host="49.13.53.216",
        user="airbyte",
        password="DO184ani7ANu8e4uMucEyaWE3A576I",
        database="calcolatore_airbyte_2"
    )
    # Cursori per entrambi i database
    cursor_source = db_source.cursor(buffered=True,dictionary=True)
    cursor_dest= db_dest.cursor(buffered=True,dictionary=True)
    
    print ("Connection OK")
    
    # Convert datetime.date object to string in "YYYY-MM-DD" format
    date_input_str = date_input.strftime("%Y-%m-%d")    
    
    progress_text = "Operation in progress. Please wait."
    my_bar = st.progress(0, text=progress_text)
    
    count=0
    for tabella, attributi in mappatura_tabelle.items():
        
        
        my_bar.progress((count+1)/len(mappatura_tabelle) , text="Retrieving data from table: "+tabella) 
        count+=1
        keys = ", ".join([f"{key}" for key in attributi['key']])
        select_query = f"SELECT id, {keys}, transfered_at FROM {tabella} WHERE date(transfered_at) = '{date_input_str}'"
        
        try:
            cursor_source.execute(select_query)
        except:
            continue
        else:
            tabella_query_result = cursor_source.fetchall()
            
        # Query per verificare se l'ID esiste già nel database di produzione
        
        dict_of_changes['update'][tabella]={'order':attributi['order'],'data':[]}
        dict_of_changes['insert'][tabella]={'order':attributi['order'],'data':[]}


        # Ciclo per trasferire o aggiornare i dati
        for row in tabella_query_result:  

            
            # Costruzione delle condizioni WHERE in base alle chiavi
            condizioni = " AND ".join([f"{key}='{row[key]}'" for key in row if key != 'transfered_at' and key != 'id'])
            check_query = f"SELECT  transfered_at FROM {tabella} WHERE {condizioni}"

            cursor_dest.execute(check_query)
            result = cursor_dest.fetchone()
    
            if result:
                # Se la riga esiste, verifico se il campo transfered_at è diverso
                if result['transfered_at'] != row['transfered_at']:
                    # Aggiorno la riga se il transfered_at è diverso
                    dict_of_changes["update"][tabella]['data'].append(row)

            else:
                # Se la riga non esiste, la inserisco
                dict_of_changes["insert"][tabella]['data'].append(row)

    my_bar.progress((count)/len(mappatura_tabelle) , text="Retrieving finished.") 
    
    cursor_source.close()
    cursor_dest.close()
    db_source.close()
    db_dest.close()
    print("Connessione al server chiusa")
    st.write("Connection closed")
    
    return(dict_of_changes)
     






def update_sql(dict_of_changes):  
    
    db_soruce = mysql.connector.connect(
        host="49.13.53.216",
        user="airbyte",
        password="DO184ani7ANu8e4uMucEyaWE3A576I",
        database="calcolatore_airbyte_1"
    )
    # Connessione al database di produzione
    db_dest = mysql.connector.connect(
        host="49.13.53.216",
        user="airbyte",
        password="DO184ani7ANu8e4uMucEyaWE3A576I",
        database="calcolatore_airbyte_2"
    )
    # Cursori per entrambi i database
    cursor_source = db_soruce.cursor(dictionary=True)
    cursor_dest = db_dest.cursor(dictionary=True)
    
    progress_text = "Update operation in progress. Please wait."
    my_bar_update = st.progress(0, text=progress_text)
    #table_to_update = {sotto_lista[1] for sotto_lista in table_to_update if sotto_lista}
    
    #update
    count=0
    for tabella, attributi in sorted(dict_of_changes['update'].items(), key=lambda x: x[1]["order"]):
        print(tabella)
        print(attributi)
        
        my_bar_update.progress((count+1)/len(dict_of_changes['update']) , text="Updating data into table: "+tabella) 
        count+=1

        for row in attributi['data']:
            condizioni = " AND ".join([f"{key}='{row[key]}'" for key in row if key != 'transfered_at' and key != 'id'])
            select_query = f"SELECT * FROM {tabella} WHERE {condizioni}"
    
            cursor_source.execute(select_query)
            dati_full_source = cursor_source.fetchall()
   
            if len(dati_full_source)>1:
                error_message="Errore: la query ha risposto con più di un record con lo stesso ID"
                print(error_message)
                st.error(error_message, icon="🚨")

            update_fields = ', '.join([f"{key} = %s" for key in dati_full_source[0].keys() if key != 'id'])
            update_query = f"UPDATE {tabella} SET {update_fields} WHERE {condizioni}"
         
            values = [value for key, value in dati_full_source[0].items() if key != "id"]
         
            try:
                cursor_dest.execute(update_query, values)
                db_dest.commit()
            except mysql.connector.Error as err:
                error_message=f"Errore durante l'aggiornamento: {err}"
                print(error_message)
                st.error(error_message, icon="🚨")
            finally:
                cursor_dest.close()
                db_dest.close()
    print("Trasferimento completato.")
    
    
    #insert
    progress_text = "Insert operation in progress. Please wait."
    my_bar_insert = st.progress(0, text=progress_text)
    #table_to_update = {sotto_lista[1] for sotto_lista in table_to_update if sotto_lista}
    count=0
    for tabella, attributi in sorted(dict_of_changes['insert'].items(), key=lambda x: x[1]["order"]):
        print(tabella)
        print(attributi)
        
        my_bar_insert.progress((count+1)/len(dict_of_changes['insert']) , text="Insert data into table: "+tabella) 
        count+=1

        for row in attributi['data']:
            condizioni = " AND ".join([f"{key}='{row[key]}'" for key in row if key != 'transfered_at' and key != 'id'])
            select_query = f"SELECT * FROM {tabella} WHERE {condizioni}"
    
            cursor_source.execute(select_query)
            dati_full_source = cursor_source.fetchall()
   
            if len(dati_full_source)>1:
                print("Il record è già presente")
                print(row)
                
            else:
                
                
                
                insert_fields = ', '.join([f"{key}" for key in dati_full_source[0].keys() if key != 'id'])
                insert_values_placeholders = ', '.join([f"%s" for key in dati_full_source[0].keys() if key != 'id'])
                insert_query = f"INSERT INTO {tabella} ({insert_fields}) VALUES ({insert_values_placeholders})"
             
                values = [value for key, value in dati_full_source[0].items() if key != "id"]
             
                try:
                    cursor_dest.execute(insert_query, values)
                    db_dest.commit()
                except mysql.connector.Error as err:
                    error_message=f"Errore durante l'aggiornamento: {err}"
                    print(error_message)
                    st.error(error_message, icon="🚨")
                finally:
                    cursor_dest.close()
                    db_dest.close()

def is_data_filled(data_dict):
    for section in data_dict.values():
        for item in section.values():
            if item['data']:  # Se la lista non è vuota
                return True
    return False

# dict_of_changes=verify_sql(datetime.strptime('2024-10-07', '%Y-%m-%d'))
# update_sql(dict_of_changes)



#streamlit

st.title("🤖 SUS Transporter ")
st.write("A tool to update sus database to prod database")




# INITIALIZE BUTTON
# Inizializza il flag nel session state
if "verify_clicked" not in st.session_state:
    st.session_state.verify_clicked = False
if "date_insert" not in st.session_state:
    st.session_state.date_insert = False
if "update_clicked" not in st.session_state:
    st.session_state.update_clicked = False
    
    
#CLICKED BUTTON
#controllo se la data è stata inserita
st.session_state.date=st.date_input("Select date of transfer request", date.today(), format="DD/MM/YYYY",help="Seleziona il giorno in cui è stato richiesto il trasferimento a BackOffice")
if st.session_state.date:
    print("R206")
    st.session_state.date_insert = True
    print(st.session_state.date)
    
    
# Controlla se il bottone è stato premuto
if st.button("Verifica aggiornamenti", type="primary"):
    if st.session_state.verify_clicked:
        st.write("Inserisci una data")
    else:
        st.session_state.verify_clicked = True
        st.session_state.date_insert = True
       

        st.session_state.changes = verify_sql(st.session_state.date)
        print ("Connection closed")

        # Mostra il risultato solo se il bottone è stato premuto
if st.session_state.verify_clicked:
    if len(st.session_state.changes) > 0:
        st.write("Update result")

        edited_df_update = st.dataframe(st.session_state.changes['update'])
        st.write("Insert result")

        edited_df_insert = st.dataframe(st.session_state.changes['insert'])
            
        # Controlla se il bottone è stato premuto
        if st.button("Trasferisci dati", type="primary"):
            st.session_state.update_clicked = True
            update_sql(st.session_state.changes)
            st.success("Operazione terminata") 

        
        # Mostra il risultato solo se il bottone è stato premuto
        if st.session_state.update_clicked:
            if is_data_filled(st.session_state.changes):
                st.session_state.verify_clicked = False




    else:
        st.write("No data to update")
        st.session_state.verify_clicked = False

























