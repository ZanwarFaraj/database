"""
This script populates a MySQL database hosted on AWS with data acquired using the IB API.
"""

import mysql.connector
from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.scanner import ScannerSubscription
from ibapi.contract import ContractDetails, Contract
from ibapi.common import TickerId, BarData
from ibapi.ticktype import TickType
from datetime import date, datetime
from threading import Thread
import time
import re

class IBWrapper(EWrapper):
    """Overrides methods of the inherited EWrapper class. Used to process the data returned by the IB API."""

    def __init__(self):
        EWrapper.__init__(self)
        self.is_connected = False

        # Stores the complete contract attributes of the contracts returned by a market scan
        self.contracts = []
        # Stores the fundamental ratios returned by a streaming market data subscription
        self.fundamentals = []
        # Stores the price volume data returned by IB
        self.pv_bars = []
        # Indicates whether or not all of the data for the current IB historical data request has been returned
        self.historical_data_returned = False

    def nextValidId(self, orderId:int):
        """Overridden method. This method is automatically called when the IB API connects to TWS."""

        self.is_connected = True

    def scannerData(self, reqId: int, rank: int, contractDetails: ContractDetails,
                    distance: str, benchmark: str, projection: str, legsStr: str):
        """Overridden method. Receives the incomplete contractDetails returned by a call to
        EClient.reqScannerSubscription()"""

        # The contractDetails objects returned by the scanner have limited information so request the complete
        # contractDetails object
        self.reqContractDetails(reqId, contractDetails.contract)

    def scannerDataEnd(self, reqId: int):
        """Overridden method. This method is automatically called when the scan associated with the reqId is
        finished."""

        self.cancelScannerSubscription(reqId)

    def contractDetails(self, reqId:int, contractDetails:ContractDetails):
        """Overridden method. Receives the complete contractDetails returned by a call to
        EClient.reqContractDetails()"""

        # Use the complete contractDetail object (with all information about the contract) to create a dictionary
        # containing key-value pairs for each attribute of the contract table in the MySQL database. Then, append this
        # dictionary to self.contracts
        contract = {'exchange_id': reqId, 'ib_conid': contractDetails.contract.conId,
                    'symbol': contractDetails.contract.symbol, 'security_type': contractDetails.contract.secType,
                    'currency': contractDetails.contract.currency, 'company': contractDetails.longName,
                    'industry': contractDetails.industry}
        self.contracts.append(contract)

    def historicalData(self, reqId: int, bar: BarData):
        """Overridden method. Receives and processes the historical price volume data returned by IB."""

        pv_bar = {'contract_id': reqId, 'date_time': datetime.fromtimestamp(float(bar.date)), 'open': bar.open,
                   'high': bar.high, 'low': bar.low, 'close': bar.close, 'volume': bar.volume}
        self.pv_bars.append(pv_bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        """Overridden method. Automatically called when all historical data for the specified reqId is returned."""
        self.historical_data_returned = True

    def tickString(self, reqId:TickerId, tickType:TickType, value:str):
        """Receives and processes the fundamental data returned by the IB API."""

        # Only process this tick if it contains the fundamental ratios (tickType is 47)
        if tickType == 47:
            # print(reqId, tickType, value)
            self.cancelMktData(reqId)

            # Create a dictionary containing all of the relevant fundamental data for the contract and append this
            # dictionary to self.fundamentals. Use a regular expression search to find the relevant fundamental data
            # fields in the "value" string returned by the API
            fundamental = {}
            fundamental['contract_id'] = reqId
            fundamental['date'] = date.today()
            fundamental['market_cap'] = re.search('MKTCAP=(.+?);', value).group(1)
            fundamental['pe_ratio'] = re.search('APENORM=(.+?);', value).group(1)
            fundamental['roe'] = re.search('TTMROEPCT=(.+?);', value).group(1)
            fundamental['free_cash_flow'] = re.search('TTMFCF=(.+?);', value).group(1)
            fundamental['pb_ratio'] = re.search('PRICE2BK=(.+?);', value).group(1)
            fundamental['debt_to_equity'] = re.search('QTOTD2EQ=(.+?);', value).group(1)
            fundamental['eps'] = re.search('AEPSNORM=(.+?);', value).group(1)
            fundamental['ps_ratio'] = re.search('TTMPR2REV=(.+?);', value).group(1)
            # Some contracts don't have dividends so the "YIELD" field may not be present in the "value" string
            dividend_yield = re.search('YIELD=(.+?);', value)
            if dividend_yield is not None:
                fundamental['dividend_yield'] = dividend_yield.group(1)
            else:
                fundamental['dividend_yield'] = None

            # If a fundamental ratio has the value -99999.99, it signifies that IB does not have data available for that
            # fundamental ratio. Replace the value with the None type.
            for key, value in fundamental.items():
                if value == '-99999.99':
                    fundamental[key] = None

            self.fundamentals.append(fundamental)

class IBClient(EClient):
    """Makes calls to the IB API. Don't override methods in EClient."""

    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)

class IBApp(IBWrapper, IBClient):
    """Defines custom helper functions for interacting with the API."""

    def __init__(self):
        IBWrapper.__init__(self)
        IBClient.__init__(self, wrapper=self)

        # Connects to the TWS application
        self.connect("127.0.0.1", 7497, 0)
        thread = Thread(target=self.run)
        thread.start()

        while (self.is_connected == False):
            time.sleep(0.1)
        print('Connected: ' + str(self.is_connected))

        # The cnx object is used to interface with the MySQL database using Python
        # Account sensitive information has been replaced with asterisks
        self.cnx = mysql.connector.connect(user='****', password='****',
                                      host='****',
                                      database='trading_database',
                                      use_pure=True)
        self.cursor = self.cnx.cursor()

        # Stores the IB Contract objects created based on the contract information in the MySQL database.
        # Each item in the list is a tuple, with index 0 of the tuple containing the contract id used in the MySQL
        # database and index 1 containing the corresponding IB contract object
        self.contract_objects = []

    def scan(self, req_id: int, exchange_abbrev: str):
        """Performs a market scan of contracts through the IB API"""

        # Creates the scanner object to be used in reqScannerSubscription()
        scanner = ScannerSubscription()
        scanner.instrument = 'STK'
        scanner.locationCode = 'STK.' + exchange_abbrev
        scanner.stockTypeFilter = 'CORP'
        scanner.scanCode = "MOST_ACTIVE_AVG_USD"
        scanner.abovePrice = 10.0
        scanner.belowPrice = 20.0

        # Third argument of reqScannerSubscription() should always be an empty list. It is used internally by IB API.
        # Fourth argument is an optional list of filters as TagValue objects.
        self.reqScannerSubscription(req_id, scanner, [], [])

    def query_contracts(self):
        """Gets the contracts from the contract table of the MySQL database"""

        # Queries the MySQL database to get the attributes of the contract table that are necessary to define each
        # contract in the IB API.
        self.cursor.execute('''SELECT id, exchange_id, symbol, security_type, currency FROM contract''')

        #  Appends self.contract_objects with tuples containing the contract ids and corresponding IB Contract objects.
        for contract_info in self.cursor:
            contract = Contract()
            # NASDAQ is called ISLAND in the API so define the contract's exchange based on the exchange_id
            if contract_info[1] == 1:
                contract.exchange = 'ISLAND'
            elif contract_info[1] == 2:
                contract.exchange = 'NYSE'
            contract.symbol = contract_info[2]
            contract.secType = contract_info[3]
            contract.currency = contract_info[4]
            self.contract_objects.append((contract_info[0], contract))

    def set_exchanges(self):
        """Manually populates the exchange table of the MySQL database"""

        insert_statement = ('INSERT INTO exchange '
                            '(abbrev, name, ib_name, state, city) '
                            'VALUES (%s, %s, %s, %s, %s)')
        exchanges = [('NASDAQ', 'Nasdaq', 'ISLAND', 'New York', 'New York'),
                     ('NYSE', 'New York Stock Exchange', 'NYSE', 'New York', 'New York')]

        for exchange in exchanges:
            self.cursor.execute(insert_statement, exchange)
            self.cnx.commit()

        self.cursor.close()
        self.cnx.close()

    def get_contract_data(self):
        """Populates the contract table of the MySQL database"""

        # Gets all of the exchange ids and abbreviations in the exchange table and performs a market scan on each
        # exchange using the exchange ids to identify the scans
        self.cursor.execute('''SELECT id, abbrev FROM exchange''')
        for exchange in self.cursor:
            self.scan(exchange[0], exchange[1])

        # Wait for the contract data to be returned, then add the data to the contract table in the MySQL database
        while len(self.contracts) != 100:
            time.sleep(0.1)
        insert_statement = ('INSERT INTO contract '
                            '(exchange_id, ib_conid, symbol, security_type, currency, company, industry) '
                            'VALUES (%s, %s, %s, %s, %s, %s, %s)')
        for contract in self.contracts:
            contract_data = (contract['exchange_id'], contract['ib_conid'], contract['symbol'],
                             contract['security_type'], contract['currency'], contract['company'],
                             contract['industry'])

            self.cursor.execute(insert_statement, contract_data)
            self.cnx.commit()

    def get_pv_data(self):
        """Populates the price_volume table of the MySQL database."""

        # Query the contracts from the MySQL database if they haven't already been queried.
        if len(self.contract_objects) == 0:
            self.query_contracts()

        # Requests historical 30min price volume data for each contract in the contract table of the MySQL database
        for contract in self.contract_objects:
            self.reqHistoricalData(contract[0], contract[1], '', '2 D', "30 mins", "TRADES", 1, 2, False, [])

            # Wait before making another request to stay within IB data restrictions
            while self.historical_data_returned == False:
                time.sleep(0.1)
            self.historical_data_returned = False
            time.sleep(15)

        insert_statement = ('INSERT INTO price_volume '
                            '(contract_id, date_time, open, high, low, close, volume) '
                            'VALUES (%s, %s, %s, %s, %s, %s, %s)')

        for pv_bar in self.pv_bars:
            pv_data = (pv_bar['contract_id'], pv_bar['date_time'], pv_bar['open'], pv_bar['high'], pv_bar['low'],
                        pv_bar['close'], pv_bar['volume'])

            self.cursor.execute(insert_statement, pv_data)
            self.cnx.commit()

    def get_fundamental_data(self):
        """Populates the fundamental table of the MySQL database."""

        # Value of 2 indicates Frozen data is being requested (data as of last market close) as opposed to Live data
        self.reqMarketDataType(2)

        # Query the contracts from the MySQL database if they haven't already been queried.
        if len(self.contract_objects) == 0:
            self.query_contracts()

        # Requests the fundamental data for each contract in the contract table of the MySQL database
        for contract in self.contract_objects:
            # Generic tick string 258 indicates that the fundamental ratios of the contract are being requested
            self.reqMktData(contract[0], contract[1], '258', False, False, [])
            # Wait before making another request to stay within IB data restrictions
            time.sleep(0.5)

        # Wait until fundamental date for all contracts has been acquired, then add the data to the fundamental table
        while len(self.fundamentals) != len(self.contract_objects):
            time.sleep(0.1)

        insert_statement = ('INSERT INTO fundamental '
                            '(contract_id, date, market_cap, pe_ratio, roe, free_cash_flow, pb_ratio, debt_to_equity, '
                            'eps, ps_ratio, dividend_yield) '
                            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)')

        for fundamental in self.fundamentals:
            fundamental_data = (fundamental['contract_id'], fundamental['date'], fundamental['market_cap'],
                                fundamental['pe_ratio'], fundamental['roe'], fundamental['free_cash_flow'],
                                fundamental['pb_ratio'], fundamental['debt_to_equity'], fundamental['eps'],
                                fundamental['ps_ratio'], fundamental['dividend_yield'])

            self.cursor.execute(insert_statement, fundamental_data)
            self.cnx.commit()


app = IBApp()
app.set_exchanges()
app.get_contract_data()
app.get_pv_data()
app.get_fundamental_data()
app.disconnect()
