# 判断交易所修正数据的 是否缺少交易所官方地址



from  OklinkGlobal import *
import threading
import pickle
import json
import os
import queue

tron_exchange_offical = {}
bsc_exchange_offical = {}
eth_exchange_offical = {}

data_dict = {} #存储结构的字典
current_dir_path = ""
# 当前目录
def current_dir():

    current_dir = os.path.dirname(os.path.abspath(__file__))
    separator = os.path.sep
    current_dir = current_dir+separator
    return current_dir
current_dir_path = current_dir()
#初始化官方地址 到字典
def statr_offical():

    with open(current_dir_path+'交易所官方地址/交易所官方地址.csv','r') as f:
        # tron_list = f.readlines()
        for tron_list in f.readlines():

            split_content = tron_list.split(",")

            # print(split_content)
            if len(split_content[0]) > 10 :
                # print(split_content)

                chain = split_content[1].strip()

                if chain == 'TRON':
                    tron_exchange_offical[split_content[0]] = True
                elif chain == 'BSC':
                    bsc_exchange_offical[split_content[0]] = True
                elif chain == 'ETH':
                    eth_exchange_offical[split_content[0]] = True


statr_offical()

#判断交易所用户被修正的原因： 是否是因为缺少交易所官方地址导致的
class getAddressTransfer(OklinkGlobal):

    def __init__(self,  chain_name, address):
        self.address = address
        self.chain_name = chain_name
        self.timestamp = int(time.time() * 1000)
        self.data_dict = data_dict
        

    #存入本地文件
    def write_csv(self,content):
        pathwj = current_dir_path+"修正交易所官方地址结果/"
        with open(pathwj+self.chain_name+"1_csv.csv",'a+') as f:
            f.write(content+'\n')

    def write_json(self,content):
        pathwj = current_dir_path+"修正交易所官方地址结果/"
        json_data = json.dumps(content)
        with open(pathwj+self.chain_name+"_json.json",'w') as f:
            f.write(json_data)

    def write_pk(self,content):
        pathwj = current_dir_path+"修正交易所官方地址结果/"
        with open(pathwj+self.chain_name+'_序列化结果.pk', 'wb') as file_to_write:
            pickle.dump(content, file_to_write)

    #和本地官方地址对比是否存在缺失
    def filert_address(self,chain_name,address):
        if chain_name == 'tron':
            # value = tron_exchange_offical[address]
            value =  tron_exchange_offical.get(address)

        elif chain_name == 'eth':
            value =  eth_exchange_offical.get(address)
        elif chain_name == 'bsc':
            value =  bsc_exchange_offical.get(address)
        

        return value

    #过滤掉下面的地址
    def filet_token_list(self,token_address):
        if self.chain_name == 'eth':
            filet_token = {
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48':'USDC',
                '0xdac17f958d2ee523a2206206994597c13d831ec7':'USDT',
                '0x0000000000085d4780b73119b644ae5ecd22b376':'TUSD',
                '0xdf574c24545e5ffecb9a659c229253d4111d87e1':'HUSD',
                '0x6b175474e89094c44da98b954eedeac495271d0f':'DAI',
                '0x4fabb145d64652a948d72533023f6e7a623c7c53':'BUSD',
            }
        elif(self.chain_name == 'bsc'):
            filet_token = {
                '0xe9e7cea3dedca5984780bafc599bd69add087d56':'BUSD',
                '0x55d398326f99059ff775485246999027b3197955':'USDT',
                '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d':'USDC',
                '0x14016e85a25aeb13065688cafb43044c2ef86784':'TUSD',
                '0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3':'DAI',
                '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c':'WBNB'
            }
            
        else:
            filet_token =  {
                'TMz2SWatiAtZVVcH2ebpsbVtYwUPT9EdjH':'BUSD',
                'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t':'USDT',
                'TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8':'USDC',
                'TUpMhErZL2fhh4sVNULAbNKLokS4GjC1F4':'TUSD',
                'TL2FiXffdjG5Ep8eqPN6ouLyydvmgoR95h':'HUSD',
                'TPYmHEhy5n8TCEfYGqW2rPxsghSfzghPDn':'USDD',
                'TMwFHYXLJaRUPeW6421aqXL4ZEzPRFGkGT':'USDJ',
                'TSSMHYeV2uE9qYH95DqyoCuNCzEL1NvU3S':'Sun.io.SUN'

            }


        return filet_token.get(token_address)

# https://www.oklink.com/api/explorer/v1/tron/transactions?t=1697524691668&offset=0&address=TA2zpjMh8hjE1zTCn9Dtq8D9HMXcHGqM1k&limit=20&nonzeroValue=false
    
    #判断是否存在
    def address_manager(self):
        pass
    #处理列表数据
    def __hits_deal_with(self,response_data):

        
        to_address_list = {}
        to_address_list_subsidiary = {}
        if(response_data['code'] == 0):
            data = response_data['data']
            # print(data)
            if(data['hits']):
                

                print("结果长度：",len(data['hits']))
                for transfer_list in data['hits']:
                    tag_name = ''
                    isToContract = ''
                    # print(transfer_list)
                    from_address = transfer_list['from']
                    if isinstance(from_address,list) :
                        from_address = from_address[0]

                    
                    to_address = transfer_list.get('to')
                    if to_address :
                        if isinstance(to_address,list) :
                            to_address = to_address[0]




                    # to 地址不是合约地址
                    isToContract_value = transfer_list.get('isToContract')
                    if isToContract_value != None:


                        if isToContract_value == True:
                            isToContract = "to是合约"
                        else:
                            isToContract = "to不是合约"
                    


                    # print('1',isToContract_value,isToContract)
                   


                    #to地址的标签

                    toTagMap = transfer_list.get('toTagMap')
                    if toTagMap:
                        if toTagMap[to_address]: 
                         
                            tag_name = self.process_tag_content(toTagMap[to_address]) 


                    if self.chain_name != 'tron':
                        toEntityTag = transfer_list.get('toEntityTag')
                        if toEntityTag:
                            tag_name = self.process_tag_content(toEntityTag) 
                        

                    to_address_list[to_address] = tag_name

             

                    to_address_list_subsidiary[to_address] = {"contract":isToContract}

                # print(to_address_list_subsidiary)

                for to_address in to_address_list:
                    tag_name = to_address_list[to_address]
                    value = self.filert_address(self.chain_name,to_address)

                    if value == None :

                        if self.filet_token_list(to_address) == None:

                            to_address_subsidiary = to_address_list_subsidiary[to_address]

                            isToContract = to_address_subsidiary['contract']
                            # print(to_address_subsidiary['contract'])

                            print("不存在的from地址：",self.chain_name,from_address,to_address,tag_name,isToContract)

                            content = "{0},{1},{2},{3},{4}".format(self.chain_name,from_address,to_address,tag_name,isToContract)
                            self.write_csv(content)

                        #把from地址都加入到来源地址中
                            if self.data_dict.get(to_address):
                                self.data_dict[to_address]['fromAddresses'].append(from_address)
                            else:
                                address_list = [from_address]
                                # 将数据加入字典
                                self.data_dict[to_address] = {'name': tag_name, 'fromAddresses': address_list}



                # print(self.data_dict)


    #判断地址是否是合约地址
    # https://www.oklink.com/api/explorer/v1/eth/contract/getAddressStatus?t=1697595939832&address=0x47b9f01b16e9c9cb99191dca68c9cc5bf6403957
    


    # https://www.oklink.com/api/explorer/v2/eth/tokens/0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2?t=1697596381422

    #获取转出方向的主链交易记录
    def get_tron_transactions(self):
        api_url = "https://www.oklink.com/api/explorer/v1/tron/transactions"
        chain_name = self.chain_name

        pstring = {
            "t": self.timestamp,
            "offset": "0",
            "address": self.address,
            "limit": "20",
            "from": self.address,
            "nonzeroValue": "false"
        }

        while True:
            try:
                response_data = self.fetch_get_req(api_url,pstring)
            except Exception as e:
                # response_data['code'] = 1
                print("请求报错：",e)
                # print(response_data)
            else:

                self.__hits_deal_with(response_data)
                break

    #获取转出方向的交易记录
    def get_tron_20transfer(self):

        api_url = "https://www.oklink.com/api/explorer/v1/tron/transfers"
        chain_name = self.chain_name

        pstring = {
            "t": self.timestamp,
            "offset": "0",
            "address": self.address,
            "tokenType": "TRC20",
            "contractAddress": self.address,
            "limit": "20",
            "from": self.address,
            "nonzeroValue": "true"
        }

        while True:
            try:
                response_data = self.fetch_get_req(api_url,pstring)
            except Exception as e:
                # response_data['code'] = 1
                print("请求报错：",e)
                # print(response_data)
            else:

                self.__hits_deal_with(response_data)
                break



    #获取转出方向的主链交易记录
    def get_eth_transactions(self):
        api_url = f"https://www.oklink.com/api/explorer/v2/eth/addresses/{self.address}/transactionsByClassfy/condition"
        # addresses/0x0055342d2b77406819a99ac4509a60243553faaf/transactionsByClassfy/condition

        chain_name = self.chain_name

        pstring = {
            "t": self.timestamp,
            "offset": "0",
            "address": self.address,
            "limit": "20",
            "start": '',
            "end": '',
            "direction": "1",
            "otherAddress": self.address,
            "value": '',
            "valueUpperLimit":'',
            "method": '',
            "type": "2",
            "txType": "",
            "nonzeroValue": "false"
        }

        while True:
            try:
                response_data = self.fetch_get_req(api_url,pstring)
            except Exception as e:
                # response_data['code'] = 1
                print("请求报错：",e)
                # print(response_data)
            else:

                self.__hits_deal_with(response_data)
                break

    #获取转出方向的交易记录
    def get_eth_20transfer(self):

        api_url = f"https://www.oklink.com/api/explorer/v2/eth/addresses/{self.address}/transfers/condition/token"


        # https://www.oklink.com/api/explorer/v2/eth/addresses/0x00b3db15e6b03e30d776b4544edf85eeb7ad4908/transfers/condition/token?
        chain_name = self.chain_name

        pstring = {
            "t": self.timestamp,
            "offset": "0",
            "address": self.address,
            "tokenType": "ERC20",
            "limit": "20",
            "start": '',
            "end": '',
            "direction": "1",
            "otherAddress": self.address,
            "value": '',
            "valueUpperLimit":'',
            "nonzeroValue": "true"
        }

        while True:
            try:
                response_data = self.fetch_get_req(api_url,pstring)
            except Exception as e:
                # response_data['code'] = 1
                print("请求报错：",e)
                # print(response_data)
            else:

                self.__hits_deal_with(response_data)
                break




    #获取转出方向的主链交易记录
    def get_bsc_transactions(self):
        api_url = f"https://www.oklink.com/api/explorer/v2/bsc/addresses/{self.address}/transactionsByClassfy/condition"
        # addresses/0x0055342d2b77406819a99ac4509a60243553faaf/transactionsByClassfy/condition

        chain_name = self.chain_name

        pstring = {
            "t": self.timestamp,
            "offset": "0",
            "address": self.address,
            "limit": "20",
            "start": '',
            "end": '',
            "direction": "1",
            "otherAddress": self.address,
            "value": '',
            "valueUpperLimit":'',
            "method": '',
            "type": "2",
            "txType": "",
            "nonzeroValue": "false"
        }

        while True:
            try:
                response_data = self.fetch_get_req(api_url,pstring)
            except Exception as e:
                # response_data['code'] = 1
                print("请求报错：",e)
                # print(response_data)
            else:

                self.__hits_deal_with(response_data)
                break


  #获取转出方向的交易记录
    def get_bsc_20transfer(self):

        api_url = f"https://www.oklink.com/api/explorer/v2/bsc/addresses/{self.address}/transfers/condition/token"


        # https://www.oklink.com/api/explorer/v2/eth/addresses/0x00b3db15e6b03e30d776b4544edf85eeb7ad4908/transfers/condition/token?
        chain_name = self.chain_name

        pstring = {
            "t": self.timestamp,
            "offset": "0",
            "address": self.address,
            "tokenType": "BEP20",
            "limit": "20",
            "start": '',
            "end": '',
            "direction": "1",
            "otherAddress": self.address,
            "value": '',
            "valueUpperLimit":'',
            "nonzeroValue": "true"
        }

        while True:
            try:
                response_data = self.fetch_get_req(api_url,pstring)
            except Exception as e:
                # response_data['code'] = 1

                print("请求报错：",e)
                # print(response_data)
            else:
                self.__hits_deal_with(response_data)
                break



    def run_tron(self):
        self.get_tron_transactions()
        self.get_tron_20transfer()

    

    def run_eth(self):
        self.get_eth_transactions()
        self.get_eth_20transfer()


    def run_bsc(self):
        self.get_bsc_transactions()
        self.get_bsc_20transfer()


    def main(self):

        # 存入json文件
        if self.chain_name == 'tron':
            self.run_tron()
        elif self.chain_name == 'bsc':
            self.run_bsc()
        elif self.chain_name == 'eth':
            self.run_eth()


        # self.write_json(self.data_dict)
        # self.write_pk(self.data_dict)


def worker(chain_name, address_queue):
    while True:
        try:
            address = address_queue.get_nowait()  # Non-blocking get
        except queue.Empty:
            break  # No more addresses to process

        print("获取地址的交易数据:", address)
        getAddressTransfer(chain_name, address).main()
        address_queue.task_done()



def edit_exchange_user(chain_name,path_name,max_threads=10):
    address_queue = queue.Queue()
    threads = []
    count = 1
    with open(path_name,'r') as f:
        # content_list = f.readlines()
        for content_list in f.readlines():
            split_content = content_list.split(",")
            count += 1
            if  count < 241634:
                continue
            
            if len(split_content[0]) > 10 :
                address = split_content[0].strip()
                print("获取地址的交易数据:",address)
                address_queue.put(address)
                
                # t = threading.Thread(target=worker, args=(chain_name, address,semaphore))
                # threads.append(t)
                # t.start()

                # print(address)
    for _ in range(max_threads):  # You can specify the number of worker threads here
        t = threading.Thread(target=worker, args=(chain_name, address_queue))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

edit_exchange_user('tron',current_dir_path+"修正交易所官方地址/tron_修改的交易所用户.csv",10)
# edit_exchange_user('eth',"修正交易所官方地址/eth_修改的交易所用户.csv")
# edit_exchange_user('bsc',"修正交易所官方地址/bsc_修改的交易所用户.csv")
exit()

address = "0x002e27f716f8295d48fe79d46cc34fd630d82588"
chain_name = "bsc"
getAddressTransfer(chain_name,address).run_bsc()
# getAddressTransfer(chain_name,address).get_tron_transactions()
# getAddressTransfer(address,chain_name).get_tron_20transfer()

