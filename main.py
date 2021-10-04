import os
import discord
import requests
from datetime import timedelta,datetime
from keep_alive import keep_alive
from replit import db
import pandas as pd
import asyncio

#=获得bot的token和建立bot
my_secret = os.environ['TOKEN']
ETHERSCAN_API_KEY = os.environ['ETHERSCAN_API_KEY']
client = discord.Client()
MINUTES = 2 #回看的频率，每MINUTE分钟
#DC_CHANNEL_ID = 894187422290685963 #test server
DC_CHANNEL_ID = 894169478177390612 #production on MADNFTs

"""对于所有的交易，都保存在里面，key是poet名字，value是历史的成交记录"""
def storeTransactions(sales,db):
  # = 对每笔交易进行处理
  db_df = pd.DataFrame(db)
  print('Currently, the dabatbase has data:', db_df)
  print(db_df.shape)
  new_sales = [] #储存新的交易，不存在数据库中的
  for sale in sales:
    if 'payment_token' in sale.keys():
      #page或者诗人的名字
      poet = sale['asset']['name']
      print('Found new sale on opeansea:',poet)
      #售价方式和金额
      payment_symbol = sale['payment_token']['symbol']
      payment_symbol_amt = scanEtherTransaction(sale)
      #得到销售时间
      sale_time = pd.to_datetime(sale['created_date'])
      #获取图片url
      image_url = sale['asset']['image_url']
      #获取买卖方
      seller = sale['seller']['user']['username']  if sale['seller']['user']!= None else 'None'
      buyer = sale['winner_account']['user']['username'] if sale['winner_account']['user']!= None else 'None'
      #get Beijing time
      sale_time_bj = pd.to_datetime(sale_time) + timedelta(hours=8)
      #获得交易数量
      quantity = sale['quantity']
      #将以上信息整合放入数据库
      t_dict = {'name': poet,
                'price': str(payment_symbol_amt)+payment_symbol,
                'buyer':buyer,
                'seller':seller,
                'image_url':image_url,
                'sale_time_bj':sale_time_bj,
                'sale_time':sale_time,
                'quantity':quantity}

      #如果该poet第一次交易，则新建一个key,value pair
      if db_df.shape[0] != 0:
        if db_df[(db_df['name']==poet) & (db_df['sale_time']==sale_time)].shape[0] != 0:
          print('INFO: already found data in the database, stop inserting sales data')
          break
      #将交易数据保存
      print('one transaction added.')
      new_sales.append(t_dict)

      print('At time: {}, {} was sold! \n Sale Price: {} {}\n Buyer:{}, Seller:{}'.format(sale_time_bj,poet,payment_symbol,payment_symbol_amt,buyer,seller))

  new_sales_df = pd.DataFrame(new_sales)
  print('Add new sales.., with shape..', new_sales_df)
  print(new_sales_df.shape)
  updated_db = pd.concat([db_df, new_sales_df], axis=0).drop_duplicates()
  db = updated_db.to_dict()
  print('db is updated!')
  print(updated_db)
  return updated_db

"""从opeansea上获得最新的交易，并放入数据库中"""
def getTransactions(db,num=20): #todo:修改默认值
  now = datetime.now()
  #= 设置一个查询的范围
  interval_start = now - timedelta(minutes=MINUTES)
  url = "https://api.opensea.io/api/v1/events"
  # OPEANSEA: LOSTPOETS
  # ref:https://docs.opensea.io/reference/retrieving-asset-events
  querystring = {"asset_contract_address":"0xA7206d878c5c3871826DfdB42191c49B1D11F466",
                "only_opensea":"true",
                "offset":"0",
                "limit": num, #可以调整 最多300
                'event_type':'successful', #获取最新交易数据
                'occurred_after': interval_start,
                'occurred_before': now}
  headers = {"Accept": "application/json"}
  response = requests.request("GET", url, headers=headers, params=querystring)
  #=将数据转换成JSON格式
  sales = response.json()['asset_events']
  print('Found new sales on opensea:', len(sales))
  print('Storing transactions....')
  db_df = storeTransactions(sales,db)
  return db_df


"""对于一个sale transaction，从etherscan得到它的交易价格"""
def scanEtherTransaction(sale):
  txhash = sale['transaction']['transaction_hash']
  action = 'eth_getTransactionByHash'
  url = "https://api.etherscan.io/api?module=proxy&action={}&txhash={}&apikey={}".format(action,txhash,ETHERSCAN_API_KEY)

  response = requests.request("GET", url)
  etherscan = response.json()['result']

  start = 10+18*64 #10个字符作为method type，再加上18行(0开始算），每行64
  end = 10+18*64+64
  value = int(etherscan['input'][start:end],16)/1e18 #in Wei so convert
  return value

# =这是最重要的程序：自动从后台发消息
async def my_background_task():
    await client.wait_until_ready()
    #= 这个决定了发布消息的channel
    channel = client.get_channel(id=DC_CHANNEL_ID)
    await channel.send('I am a hard working bot.. Start working..')
    while not client.is_closed():
        #获取过去一段时间的交易，并存入数据库中 
        db_df = getTransactions(db)
        print('Transactions done!')
        #过去5分钟的交易
        deadline = datetime.now() - timedelta(minutes=MINUTES)
        #=查看db_df是否有新的交易
        #await channel.send('Smart bot found {} transactions during past {} minutes'.format(db_df.shape[0],MINUTES))
        if db_df.shape[0] != 0: 
          matched = db_df[db_df['sale_time']>=deadline].sort_values(by=['sale_time'])
          #= 如果有新的交易，则发布到channel上
          if matched.shape[0]!= 0:
            for idx, poet in matched.iterrows():
              print('sending poet: ', poet['name'])
              embedVar = discord.Embed(title=poet['name']+" sold!", 
                                      description="Price: "+poet['price'], 
                                      color=0x00ff00)
              embedVar.add_field(name="Quantity", value=poet['quantity'], inline=False)
              embedVar.add_field(name="Buyer", value=poet['buyer'], inline=True)
              embedVar.add_field(name="Seller", value=poet['seller'], inline=True)
              embedVar.add_field(name="Sale Time (Beijing Time)", value=poet['sale_time_bj'], inline=False)
              embedVar.set_image(url=poet['image_url'])
              await channel.send(embed=embedVar)
        await asyncio.sleep(MINUTES*60) # task runs every 60 seconds
client.loop.create_task(my_background_task())


keep_alive()
client.run(my_secret)