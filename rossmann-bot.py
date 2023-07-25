import requests
import pandas as pd
import json
from flask import Flask, request, Response
import os

# constant
TOKEN = '6614251653:AAFkpW47k1v9Ykd0wyjXzRncnpz5nIqNntk'
chat_id = 1027995026

# Info about the Bot
# https://api.telegram.org/bot6614251653:AAFkpW47k1v9Ykd0wyjXzRncnpz5nIqNntk/getMe

# Get updates
# https://api.telegram.org/bot6614251653:AAFkpW47k1v9Ykd0wyjXzRncnpz5nIqNntk/getUpdates

# Webhook
# https://api.telegram.org/bot6614251653:AAFkpW47k1v9Ykd0wyjXzRncnpz5nIqNntk/setWebhook?url=https://1ebdab1829f7cf.lhr.lif

# Webhook Render
# https://api.telegram.org/bot6614251653:AAFkpW47k1v9Ykd0wyjXzRncnpz5nIqNntk/setWebhook?url=https://rossmann-telegram-bot-oxyw.onrender.com

# Send message
# https://api.telegram.org/bot6614251653:AAFkpW47k1v9Ykd0wyjXzRncnpz5nIqNntk/sendMessage?chat_id=1027995026&text=Hello!

def send_message(chat_id, text):
    url = 'https://api.telegram.org/bot'+TOKEN+'/sendMessage?chat_id='+str(chat_id)
    
    r = requests.post(url, json={'text': text})
    print(f'Status Code {r.status_code}')

    return None

def load_dataset(store_id):

    # loading test dataset
    df_store_raw = pd.read_csv('store.csv')
    df = pd.read_csv('test.csv')

    # merge test dataset + store
    df_test = pd.merge(df, df_store_raw, how='left', on='Store')

    # choosse store for prediction
    df_test = df_test[df_test['Store'] == store_id]
    #df_test = df_test[df_test['Store'].isin([7, 25, 30])]
    
    if not df_test.empty:
        # remove closed days
        df_test = df_test[df_test['Open'] != 0]
        df_test = df_test[~df_test['Open'].isnull()]
        df_test = df_test.drop('Id', axis=1)

        data = df_test.to_json(orient='records', date_format='iso')
    else:
        data = 'error'

    return data

def predict(data):

    # API call
    url = 'https://rossmann-sales-predict-g0vz.onrender.com/rossmann/predict'
    #header = {'Content-type': 'application/jason'}
    data = data

    r = requests.post(url, json=data)
    #r = requests.post(url, data=data, headers=header)
    print(f'Status Code {r.status_code}')

    d1 = pd.json_normalize(r.json())

    return d1

def parse_message(message):
    chat_id = message['message']['chat']['id']
    store_id = message['message']['text']

    store_id = store_id.replace('/', '')

    try:
        store_id = int(store_id)
    except ValueError:
        store_id = 'error'

    return chat_id, store_id

# API initialize
app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])

def index():
    if request.method == 'POST':
        message = request.get_json()
        chat_id, store_id = parse_message(message)

        if store_id != 'error':
            # load data
            data = load_dataset(store_id)

            if data != 'error':
                # prediction
                d1 = predict(data)
                # calculation
                d2 = d1[['store', 'prediction']].groupby('store').sum().reset_index()
                # send message
                msg = f'Store number {d2["store"].values[0]} will sell {d2["prediction"].values[0]} in the next 6 weeks'
                send_message(chat_id, msg)
                return Response('OK', status=200)

            else:
                send_message(chat_id, 'Store not available')
                return Response('OK', status=200)
            
        else:
            send_message(chat_id, 'Store ID not supported')
            return Response('OK', status=200)
    
    else:
        return '<h1> Rossmann Telegram Bot </h1>'

if __name__ == '__main__':
    port = os.environ.get('PORT', 5000)
    app.run(host='0.0.0.0', port=port)
