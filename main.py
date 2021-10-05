import os
import discord
from keep_alive import keep_alive
import requests
from datetime import timedelta,datetime
import pandas as pd
import asyncio

client = discord.Client()
MINUTES = 1
TOKEN = os.environ['TOKEN']
ETHERSCAN_API_KEY = os.environ['ETHERSCAN_API_KEY']

#DC_CHANNEL_ID = 894187422290685963 #test server
DC_CHANNEL_ID = 894169478177390612 #production on MADNFTs

"""从opeansea上获得最新的交易，并放入数据库中"""
def getTransactions(start, end, num=300):
  url = "https://api.opensea.io/api/v1/events"
  # OPEANSEA: Address of LOSTPOETS
  querystring = {"asset_contract_address":"0xA7206d878c5c3871826DfdB42191c49B1D11F466",
                "only_opensea":"false",
                "offset":"0",
                "limit": num, #可以调整 最多300
                'event_type':'successful', #获取交易成功的数据
                'occurred_after': start,
                'occurred_before': end}
  headers = {"Accept": "application/json"}
  response = requests.request("GET", url, headers=headers, params=querystring)
  #=将数据转换成JSON格式
  sales = response.json()['asset_events']
  return sales

"""对于一个sale，从etherscan得到它的交易价格"""
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

"""给了sales list，获取其中每笔交易的NFT名字，交易金额和币种，买家和卖家，交易时间和图片url"""
def processSales(sales):
  # = 对每笔交易进行处理
  new_sales = [] #储存新的交易
  for sale in sales:
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
    
      #将交易数据保存
      print('One new transaction added.')
      new_sales.append(t_dict)
      print('At time: {}, {} was sold! \n Sale Price: {} {}\n Buyer:{}, Seller:{}'.format(sale_time_bj,poet,payment_symbol,payment_symbol_amt,buyer,seller))

  new_sales_df = pd.DataFrame(new_sales)
  return new_sales_df


"""这是最重要的程序：自动从后台发消息"""
async def my_background_task():
    await client.wait_until_ready()
    print('Program is started')
    #= 这个决定了发布消息的channel
    channel = client.get_channel(id=DC_CHANNEL_ID)
    print(channel)
    await channel.send('I am a hard working bot at next level ... Start working every {} minute ...'.format(MINUTES))
    while not client.is_closed():
        #以这个时间段为准，记为timestamp
        timestamp = datetime.now()
        start = (timestamp - timedelta(minutes=MINUTES)).replace(second=0,microsecond=0)
        end = start+timedelta(minutes=1)

        #获取过去一段时间的交易
        new_sales = getTransactions(start,end)
        processed_sales = processSales(new_sales)

        #=查看过去一段时间内是否有新的交易
        #await channel.send('Smart bot found {} transactions between {}  and {}'.format(processed_sales.shape[0],start,end))

        if processed_sales.shape[0] != 0:
          #= 如果有新的交易，则发布到channel上
            for idx, poet in processed_sales.iterrows():
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
        else:
          print('No new transaction found..')
        await asyncio.sleep(MINUTES*60) # task runs every 60 seconds
        
client.loop.create_task(my_background_task())

keep_alive()
client.run(TOKEN)