# This is a sample Python script.
from GetDataFromExcel import Main as gd
from selenium import webdriver
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
        CHROMEDRIVER_PATH = r"C:\WB programm\chromedriver_111\chromedriver.exe"
        WINDOW_SIZE = "1920,1080"
        self.token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NJRCI6IjhjMGNmZjZhLWRiYjgtNDNkMS05MGRkLTQwYjhlZmE4MmUyYSJ9.riEWh1RffqvqEPcfbdrA3QqVBVb0IX8FoV8PdELoeos"

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
        headers = app.find_headers(["Артикул товара", "Количество"])
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
        for rx in range(sheet.nrows):
            if not isinstance(sheet.cell_value(rowx=rx, colx=1),  float):
                continue
            elif sheet.cell_value(rowx=rx, colx=7) == "resrv":
                print("skipped cause resrv")
                continue
            data.append({"kod": sheet.cell_value(rowx=rx, colx=1), "price": int(sheet.cell_value(rowx=rx, colx=5))})
        return data

    def matching_datas(self, pls, src):
        upd_data = []
        for i in range(len(src)):
            try:
                tmp_kod = int(src[i]['Артикул товара'][0:6])
            except:
                print(src[i]['Артикул товара'], "skipped")
                continue
            if len([a for a in pls if a["kod"] == tmp_kod]) < 1:
                upd_data.append(str(tmp_kod))

        return upd_data

    def get_kt_data_from_wlbr(self, lst=[]):
        url = "https://suppliers-api.wildberries.ru/content/v1/cards/filter"
        data = {"vendorCodes": lst}
        r = requests.post(url, headers={"Authorization": self.token, "content-type": "application/json"}, json=data)
        resp = json.loads(r.content.decode())
        i = 0
        while len(resp["data"]) > i:
            if resp["data"][i]['vendorCode'] not in lst:
                del(resp["data"])[i]
            i += 1
        return resp

    def get_price(self):
        p = requests.get(self.base_url, headers={"Authorization": self.token}, params={"quantity":1})
        return json.loads(p.content.decode())

    def collect_barcodes(self, body):
        if not body["error"]:
            barcode_lst = []
            for i in range(len(body["data"])):
                tmp = body["data"][i]["sizes"][0]
                barcode_lst.append({"sku": tmp["skus"][0], "amount": 0})
            return barcode_lst

    def upd_wldb(self, data):
        url = "https://suppliers-api.wildberries.ru/content/v1/cards/update"
        r = requests.post(url, headers={"Authorization": self.token, "content-type": "application/json"}, json=data)
        if r.status_code == 200:
            return True

    def get_warehouses(self):
        url = "https://suppliers-api.wildberries.ru/api/v2/warehouses"
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

    def zeroing_amounts(self, stock_lst):
        new_body = {"stocks":[]}
        for i in range(len(stock_lst["stocks"])):
            if stock_lst["stocks"][i]["amount"] != 0:
                print("обнуляю остаток по карточке = >",stock_lst["stocks"][i])
                stock_lst["stocks"][i]["amount"] = 0
                new_body["stocks"].append(stock_lst["stocks"][i])
        return new_body

    def upd_warehouse(self, data, warehouse_id):

        url = "https://suppliers-api.wildberries.ru/api/v3/stocks/{0}".format(warehouse_id)
        r = requests.put(url, headers={"Authorization": self.token, "content-type": "application/json"}, json=data)
        if r.status_code == 200:
            return True

    def process_data(self, lst):
        body = self.get_kt_data_from_wlbr(lst)
        barcode_json = self.collect_barcodes(body)
        barcode_lst = [a["sku"] for a in barcode_json]
        lst_warehouses = self.get_warehouses()
        for i in range(len(lst_warehouses)):
            stocks = self.get_stocks_by_warehouse(lst_warehouses[i]["id"], barcode_lst)
            new_stocks = self.zeroing_amounts(stocks)
            self.upd_warehouse(new_stocks, lst_warehouses[i]["id"])

    def main(self):
        pulser_file = self.get_data_from_pulser()
        pulser_data = self.read_xls_from_pulser(pulser_file)
        source_data = self.get_data_from_source(r"C:\WB_programm\source.xlsx")
        data_for_upd = self.matching_datas(pulser_data, source_data)
        len_lst = len(data_for_upd)
        for i in range(0, len_lst, 100):
            tmp = data_for_upd[i:i+100]
            self.process_data(tmp)
            print(i,"set is complete", "count=",len(data_for_upd[i:i+100]))


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    cls = Automation_WILDBERRIES()
    cls.main()
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
