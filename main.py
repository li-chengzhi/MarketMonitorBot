import os
import discord
import requests
from datetime import timedelta,datetime
from keep_alive import keep_alive
from replit import db
import pandas as pd

#=获得bot的token和建立bot
my_secret = os.environ['TOKEN']
ETHERSCAN_API = os.environ['ETHERSCAN_API_KEY']
client = discord.Client()

#=删除database
"""for key in db.keys():
  del db[key]
print('DB cleaned!')"""

"""从opeansea上获得最新的10笔交易"""
def getTransactions(num=10):
  now = datetime.now() - timedelta(hours=8)
  one_day_ago = now - timedelta(hours=24)
  url = "https://api.opensea.io/api/v1/events"
  # OPEANSEA: LOSTPOETS
  # ref:https://docs.opensea.io/reference/retrieving-asset-events
  querystring = {"asset_contract_address":"0xA7206d878c5c3871826DfdB42191c49B1D11F466",
                "only_opensea":"true",
                "offset":"0",
                "limit": num, #可以调整 最多300
                'event_type':'successful', #获取最新交易数据
                'occurred_after': one_day_ago,
                'occurred_before': now}
  headers = {"Accept": "application/json"}
  response = requests.request("GET", url, headers=headers, params=querystring)
  #=将数据转换成JSON格式
  sales = response.json()['asset_events']
  return sales


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

"""对于所有的交易，都保存在里面，key是poet名字，value是历史的成交记录"""
def storeTransactions(sales):
  count = 0 #to delete, 记录新添加了多少记录
  for sale in sales:
    if 'payment_token' in sale.keys() and 'Poet' in sale['asset']['name']:
      poet = sale['asset']['name']
      print(poet)
      payment_symbol = sale['payment_token']['symbol']
      payment_symbol_amt = scanEtherTransaction(sale)
      #得到时间
      sale_time = (pd.to_datetime(sale['created_date'])).strftime('%Y-%m-%d %H:%M:%S')
      #获取图片url
      image_url = sale['asset']['image_url']
      #获取买卖方
      seller = sale['seller']['user']['username']  if sale['seller']['user']!= None else 'None'
      buyer = sale['winner_account']['user']['username'] if sale['winner_account']['user']!= None else 'None'
      #get Beijing time
      sale_time_bj = (pd.to_datetime(sale_time) + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
      #获得交易数量
      quantity = sale['quantity']
      #将以上信息整合放入数据库
      t_dict = {'name': poet,
                'price': str(payment_symbol_amt)+payment_symbol,
                'buyer':buyer,
                'seller':seller,
                'image_url':image_url,
                'sale_time_bj':sale_time_bj,
                'sale_time':sale_time}

      #如果该poet第一次交易，则新建一个key,value pair
      if poet not in db.keys():
        db[poet] = {}
      #将交易数据保存
      db[poet][sale_time] = t_dict
      count += 1

      print('At time: {}, {} was sold! \n Sale Price: {} {}\n Buyer:{}, Seller:{}'.format(sale_time_bj,poet,payment_symbol,payment_symbol_amt,buyer,seller))
      
  return count


@client.event
async def on_ready():
  print('We have logged in as {0.user}'.format(client))

@client.event
async def on_message(message):
  if message.author == client.user:
    return
  if message.content.startswith('$hello'):
    #=得到最近2小时内的交易
    deadline = datetime.now() - timedelta(hours=2)-timedelta(hours=8)
    print('Your time is: ', deadline)
    res = []
    for poet in db.keys():
      for sale_time in db[poet].keys():
        #if pd.to_datetime(sale_time)>deadline:
        res.append(db[poet][sale_time])
    for poet in res:
      print(poet['name'])
      embedVar = discord.Embed(title=poet['name']+" sold!", 
                               description="Price: "+poet['price'], 
                               color=0x00ff00)
      embedVar.add_field(name="Buyer", value=poet['buyer'], inline=False)
      embedVar.add_field(name="Seller", value=poet['seller'], inline=False)
      embedVar.add_field(name="Sale Time (Beijing Time)", value=poet['sale_time_bj'], inline=False)
      embedVar.set_image(url=poet['image_url'])
      await message.channel.send(embed=embedVar)

  if message.content.startswith("/update"):
    sales = getTransactions(20)
    count = storeTransactions(sales)
    await message.channel.send('Update is done! {} transactions added!'.format(count))

keep_alive()
client.run(my_secret)