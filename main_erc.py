# This is a sample Python script.
from GetDataFromExcel import Main as gd
from selenium import webdriver
from google_currency import convert
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time, zipfile, json
import xlrd, requests


class Automation_WILDBERRIES:

    def __init__(self):
        self.base_url = "https://suppliers-api.wildberries.ru/public/api/v1/info"
        CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
        CHROMEDRIVER_PATH = r"C:\WB_programm\chromedriver_111\chromedriver.exe"
        WINDOW_SIZE = "1920,1080"
        self.token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NJRCI6IjBhMjU4ZWVjLTIyZDgtNDBjOS04OWJlLTcwMDBmYWU1MTZhNCJ9.IgzWeX1zuEh_xlcRqZh7JL7I9kbi6mXV3NMfLjvGSiE"

        chrome_options = Options()
        chrome_options.add_argument("download.default_directory=C:/temp")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=%s" % WINDOW_SIZE)
        chrome_options.binary_location = CHROME_PATH
        self.driver = webdriver.Chrome(executable_path=CHROMEDRIVER_PATH, options=chrome_options)
        params = {'behavior': 'allow', 'downloadPath': r'C:\temp'}
        self.driver.execute_cdp_cmd('Page.setDownloadBehavior', params)
        # self.driver.get("https://pulser.kz/")

    def end_session(self):
        self.driver.close()
        self.driver.quit()

    def get_screen(self):
        S = lambda X: self.driver.execute_script('return document.body.parentNode.scroll' + X)
        self.driver.set_window_size(S('Width'), S('Height'))  # May need manual adjustment
        self.driver.find_element(By.TAG_NAME, 'body').screenshot('web_screenshot.png')

    @staticmethod
    def get_data_from_source(file):
        app = gd.GetDataFromExcel()
        app.open_xlsx(file)
        headers = app.find_headers(["Артикул товара", "Номенклатура"])
        data = app.gather_table_data(headers)
        return data

    def find_by_id(self, id):
        element = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.ID, id)))
        return element

    def find_by_xpath(self, xpath):
        element = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, xpath)))
        return element

    def unzip_file(self, path):
        with zipfile.ZipFile(path, 'r') as zip:
            zip.extract('pdprice.xls', 'C:/temp')
            return path.replace("zip", "xls")

    def authorization(self):
        self.driver.get("https://pulser.kz/")
        login = self.find_by_id('logInput')
        psw = self.find_by_id('pasInput')
        login.send_keys("reseller")
        psw.send_keys("PULSER")
        self.find_by_id("auGo").click()
        # self.driver.get_screenshot_as_file("entered.png")

    def highlight(self, element, effect_time, color, border):
        """Highlights (blinks) a Selenium Webdriver element"""
        driver = element._parent

        def apply_style(s):
            driver.execute_script("arguments[0].setAttribute('style', arguments[1]);",
                                  element, s)

        original_style = element.get_attribute('style')
        apply_style("border: {0}px solid {1};".format(border, color))
        time.sleep(effect_time)
        apply_style(original_style)

    def download_file(self):
        element = self.find_by_xpath("//a[@title='Скачать прайс-лист реселлера']")
        self.highlight(element, 3, "blue", 5)
        element.click()
        time.sleep(3)
        path = "C:/temp/"
        return path + element.get_attribute("href").split("/")[-1]

    def get_data_from_pulser(self):
        self.authorization()
        zip_file = self.download_file()
        xls_file = self.unzip_file(zip_file)
        self.end_session()
        return xls_file

    def read_xls_from_pulser(self, file):
        data = []
        book = xlrd.open_workbook(file)
        sheet = book.sheet_by_index(0)
        for rx in range(9, sheet.nrows):
            if not isinstance(sheet.cell_value(rowx=rx, colx=6), float):
                print(sheet.cell_value(rowx=rx, colx=1), "skipped, because null")
                continue
            elif int(sheet.cell_value(rowx=rx, colx=6)) < 5:
                print(sheet.cell_value(rowx=rx, colx=1), "skipped, because less than 5")
                continue
            data.append({"kod": sheet.cell_value(rowx=rx, colx=3), "price": int(sheet.cell_value(rowx=rx, colx=6))})
        return data



    def get_kt_data_from_wlbr(self, wlb_kt, lst_for_del=[]):

        i = 0
        while len(wlb_kt) > i:
            if wlb_kt[i]['vendorCode'] not in lst_for_del:
                del(wlb_kt[i]) # перепроверка
            i += 1
        return wlb_kt

    def get_all_cards_from_wb(self):
        data = {
          "sort": {
              "cursor": {
                  "limit": 1000
              },
              "filter": {
                  "withPhoto": -1
              }
          }
        }
        url = "https://suppliers-api.wildberries.ru/content/v1/cards/cursor/list"
        total = 1000
        cards = []
        while total == 1000:
            r = requests.post(url, headers={"Authorization": self.token, "content-type": "application/json"}, json=data)

            json_result = json.loads(r.content.decode())
            data["sort"]["cursor"]["updatedAt"] = json_result["data"]["cursor"]["updatedAt"]
            data["sort"]["cursor"]["nmID"] = json_result["data"]["cursor"]["nmID"]
            cards = cards + json_result["data"]["cards"]
            total = json_result["data"]["cursor"]["total"]
        return cards

    def get_prices(self):
        p = requests.get(self.base_url, headers={"Authorization": self.token}, params={"quantity": 0})
        return json.loads(p.content.decode())

    def collect_barcodes(self, body, xl_source):

        barcode_lst = []
        for i in range(len(body)):
            tmp = body[i]["sizes"][0]
            barcode_lst.append({"sku": tmp["skus"][0], "amount": 0,
                                "vendorCode": body[i]['vendorCode'], "nmID": body[i]["nmID"]})
        return barcode_lst

    def upd_wldb(self, data):
        url = "https://suppliers-api.wildberries.ru/content/v1/cards/update"
        r = requests.post(url, headers={"Authorization": self.token, "content-type": "application/json"}, json=data)
        if r.status_code == 200:
            return True

    def get_warehouses(self):
        url = "https://suppliers-api.wildberries.ru/api/v3/warehouses"
        r = requests.get(url, headers={"Authorization": self.token})
        if r.status_code == 200:
            return json.loads(r.content.decode())
        else:
            return []

    def get_stocks_by_warehouse(self, warehouse_id, data):
        url = "https://suppliers-api.wildberries.ru/api/v3/stocks/{0}".format(warehouse_id)
        r = requests.post(url, headers={"Authorization": self.token, "content-type": "application/json"},
                          json={"skus": data})
        return json.loads(r.content.decode())

    def upd_warehouse(self, data, warehouse_id):
        url = "https://suppliers-api.wildberries.ru/api/v3/stocks/{0}".format(warehouse_id)
        r = requests.put(url, headers={"Authorization": self.token, "content-type": "application/json"}, json=data)
        if r.status_code == 200:
            return True

    def main(self):
        pulser_file = r"C:\WB_programm\ERC Price.xls"
        pulser_data = self.read_xls_from_pulser(pulser_file)
        source_data = self.get_data_from_source(r"C:\WB_programm\source_erc.xlsx")
        wb_data = self.get_all_cards_from_wb()
        warehouses_lst = self.get_warehouses()
        barcode_json = self.collect_barcodes(wb_data, source_data)
        currency = json.loads(convert('kzt', 'rub', 1000))
        currency_ = 1000/float(currency["amount"])
        #currency_ = 5.45 # don't forget!!!!!
        for i in range(len(warehouses_lst)):
            self.synchronization_price(barcode_json, pulser_data, currency_)
            self.synchronization_stock(pulser_data, barcode_json, warehouses_lst[i]["id"])

    def get_vendor_code_by_sku(self, sku, barcode_json):

        vendoreCode = [a for a in barcode_json if a["sku"] == sku][0]['vendorCode']
        return vendoreCode

    def get_price_from_pulser_by_id(self, vendorcode, pulser_data):

        tempcode = [a["price"] for a in pulser_data if a["kod"] == vendorcode]
        if len(tempcode) > 0:
            return tempcode
        return []

    def calculate_sum(self, amount, currency_):
        # converted_sum = json.loads(convert('kzt', 'rub', amount))
        currency = currency_
        if amount < 2000 and amount > 1800:
            mn = 2.5
        elif amount < 1800 and amount > 1600:
            mn = 2.7
        elif amount < 1600 and amount > 1400:
            mn = 2.9
        elif amount < 1400 and amount > 1200:
            mn = 3.1
        elif amount < 1200 and amount > 1000:
            mn = 3.3
        elif amount < 1000 and amount > 900:
            mn = 3.5
        elif amount < 900 and amount > 800:
            mn = 4.8
        elif amount < 800 and amount > 700:
            mn = 6.1
        elif amount < 700 and amount > 600:
            mn = 7.4
        elif amount < 600 and amount > 500:
            mn = 8.7
        elif amount < 500 and amount >1:
            mn = 10
        else:
            mn = 2
        return amount*mn/currency

    def synchronization_price(self, barcode_json, pulser_data, currency_):
        all_prices = self.get_prices()
        prices = []
        for i in range(len(all_prices)):
            vendorcode = [a["vendorCode"] for a in barcode_json if a["nmID"] == all_prices[i]['nmId']]
            if len(vendorcode) < 1:
                continue
            else:

                vendorcode = vendorcode[0]
                    #if int(vendorcode) != 161539:
                        #continue
            pulser_price = self.get_price_from_pulser_by_id(vendorcode, pulser_data)
            if not pulser_price:
                continue
            else:
                converted_sum = self.calculate_sum(pulser_price[0], currency_)
                if converted_sum != all_prices[i]['price']:
                    print("new_summ=>", converted_sum, "current_summ",
                          all_prices[i]['price'], "vendorCode", vendorcode,
                          "summ in kzt", pulser_price[0])
                    prices.append({"nmId": all_prices[i]['nmId'], "price": int(round(converted_sum, 0))})
        if len(prices) > 0:
            if self.update_prices(prices):
                print("цены обновлены**********************************************")


    def update_prices(self, data):
        url = "https://suppliers-api.wildberries.ru/public/api/v1/prices"
        r = requests.post(url, headers={"Authorization": self.token, "content-type": "application/json"},
                          json=data)
        if r.status_code == 200:
            return True


    def synchronization_stock(self, pulser_data, barcode_json, lst_warehouses_id):

        barcode_lst = [a["sku"] for a in barcode_json]
        for j in range(0, len(barcode_lst), 1000):
            tmp_lst = barcode_lst[j:j+1000]
            stocks = self.get_stocks_by_warehouse(lst_warehouses_id, tmp_lst)["stocks"]
            new_stock = {"stocks": []}
            for k in stocks:
                tmp_vendorcode = self.get_vendor_code_by_sku(k["sku"], barcode_json)
                if not tmp_vendorcode:
                    continue
                if len([a for a in pulser_data if a["kod"] == tmp_vendorcode]) > 0 and k["amount"] < 1:
                    tmp_stock = {"sku": k["sku"], "amount": 5}
                    print("обновляю остаток у карточки =>", tmp_stock)
                    new_stock["stocks"].append(tmp_stock)
                elif len([a for a in pulser_data if a["kod"] == tmp_vendorcode]) < 1 and k["amount"] > 0:
                    tmp_stock = {"sku": k["sku"], "amount": 0}
                    print("обновляю остаток у карточки =>", tmp_stock)
                    new_stock["stocks"].append(tmp_stock)
            self.upd_warehouse(new_stock, lst_warehouses_id)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    cls = Automation_WILDBERRIES()
    cls.main()
    # prices = cls.get_prices()
    # lst = cls.get_all_list()
    print()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
