# This is a sample Python script.
import sys
import traceback

from selenium import webdriver
from google_currency import convert
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time, zipfile, json, re, psycopg2
import xlrd, requests, openpyxl, uuid
import datetime, os
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains


class Monitoring:

    def send_alert(self, text):
        """ Метод для отправки уведомлений на телегу
        данные собирает из вьюшки [dbo].[vFaultedJobsToday2]
        со статусом Faulted, то что обработал, записывает в таблицу [dbo].[faulted_id2] """

        process = "update WB"
        server = "PC KEML"
        try:
            info = text
            createtime = datetime.datetime.now()
            head = "---------------------------------------------------------------------------\n"
            row1 = " <em> {} </em>\n".format(str(createtime))
            row3 = "<strong> {} </strong>".format(info)
            footer = "---------------------------------------------------------------------------\n"

            msg = row1 + row3

            self.send_message_bot(msg)
        except:
            pass
    def send_message_bot(self, text):
        endPoint = "https://api.telegram.org/bot"
        # id = "-579581470"
        id = "-4011969729"  # supergroup id
        Token = "5681293588:AAEWMxiSSAdVkc0HRW1yTEAzDqOGVKzokWk/"
        cnt = 0
        while cnt < 10:
            try:
                r = requests.get(url=endPoint + Token + "sendMessage?chat_id=" + id + "&text=" + text + "&parse_mode=html",
                         verify=False)
                resp = r.json()
                if resp["ok"]:
                    return 'successfull'
            except:
                self.logging_('_', '_', 'ошибка при отправке сообщения в телегу', type='system')


class Config:

    sql_update = """
                begin;
                    update public.data_from_wb
                    set "source" = '{0}', "vendorPrice" = '{1}', "is_active" = True
                    where "id" = '{2}';
                    commit;
                end;
    """
    sql_update_active = """
            begin;
                update public.data_from_wb
                set "is_active" = {0}
                where "id" = '{1}';
                commit;
            end;
    """
    sql_count = """
                SELECT count(*)
                FROM public.data_from_wb
                where "vendorCode" = '{0}' """
    sql_insert = """Begin; 
                INSERT INTO public.data_from_wb(
                id, "vendorCode", "barCode", source, "userName", is_active)
                VALUES (nextval('data_from_wb_id_seq'), '{0}', '{1}', '{2}', '{3}', '{4}');
                commit;
                end;
        """
    sql_insert_logs = """
        Begin;
            INSERT INTO public.logs(
            id, barcode, source, action, action_date, type)
            VALUES (nextval('logs_id_seq'), '{0}', '{1}', '{2}' , now(), '{3}');
        commit;
        end;
    """
    sql_insert_currency = """
        Begin;
            INSERT INTO public.currency_history(
                id, currency, insert_date)
                VALUES (nextval('currency_history_id_seq'), '{0}', now());
        commit;
        end;
    """
    sql_get_last_currency = """
                SELECT currency
                FROM public.currency_history
                ORDER BY insert_date DESC
                LIMIT 1"""

    percent_diff = 20


class PostGreSql:

    def __init__(self):
        print('initialization connect to postgres')
        self.conn = self.connect_to_db()
        print('success')

    @staticmethod
    def connect_to_db():
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="password")
        return conn

    @staticmethod
    def execute_sql(conn, sql, param=0):
        try:
            cur = conn.cursor()
            cur.execute(sql)
            if param == 1:
                return cur.fetchall()
        except Exception as ex:
            print(ex)
            return False
        return True

    def get_data_from_db(self, sql="select * from public.data_from_wb"):
        data = self.execute_sql(self.conn, sql, 1)
        return data


class AutomationWILDBERRIES(PostGreSql, Monitoring):

    def __init__(self):
        super().__init__()
        self.base_url = "https://suppliers-api.wildberries.ru/public/api/v1/info"
        # self.token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NJRCI6IjIzZGFjZTgzLTI5YWYtNGExYS1hZmNmLTI4NmQ3MmE0NWNjMyJ9.y5-UAnKoezTMjA6X7rERZuv8B89fozD2rSwthnbHhE8"
        self.token = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxODM5MTkxNCwiaWQiOiJjMmQ1MjllZi1jYTc4LTQ1ODEtYTYwMC0xOGRhYmQ1NmY5ZmYiLCJpaWQiOjYwNjA2MzAyLCJvaWQiOjc5NTIzNCwicyI6NTEwLCJzYW5kYm94IjpmYWxzZSwic2lkIjoiNWZlMzVmY2MtZDA0MC00NjhjLThjODctZDBjNmYwNTI0MjBjIiwidWlkIjo2MDYwNjMwMn0.wli1iIa1e3fx7SRKUznOdfQf7rCd1wsmmwWUNzpGpBxmXV5LNzFqvSgoplL-i0EO-ShZRnxu-p38x7uFk0lXjw"
    def start_browser(self):
        CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
        CHROMEDRIVER_PATH = r"C:\Projects\Keml\chromedriver-win64\chromedriver-win64\chromedriver.exe"
        WINDOW_SIZE = "1920,1080"
        options = Options()
        service = Service(executable_path=CHROMEDRIVER_PATH)
        options.add_argument("download.default_directory=C:/temp")
        options.add_argument("--disable-extensions")
        options.add_argument("--headless")
        options.add_argument("--no-sandboxw")
        options.add_argument("--disable-gpu")
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--disable-dev-shm-usage')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_argument('--disable-blink-features=AutomationControlled')

        # options.add_argument("--headless")
        options.add_argument("--window-size=%s" % WINDOW_SIZE)
        options.binary_location = CHROME_PATH
        self.driver = webdriver.Chrome(options=options, service=service)
        params = {'behavior': 'allow', 'downloadPath': r'C:\temp'}
        self.driver.execute_cdp_cmd('Page.setDownloadBehavior', params)
        # self.driver.get("https://pulser.kz/")

    def logging_(self, barcode, source, action, type='compare'):
        sql = Config.sql_insert_logs.format(barcode, source, action, type)
        self.execute_sql(self.conn, sql)

    def end_session(self):
        try:
            self.driver.close()
            self.driver.quit()
        except:
            pass

    def get_screen(self):
        S = lambda X: self.driver.execute_script('return document.body.parentNode.scroll' + X)
        self.driver.set_window_size(S('Width'), S('Height'))  # May need manual adjustment
        self.driver.find_element(By.TAG_NAME, 'body').screenshot('web_screenshot.png')

    def find_by_id(self, id):
        element = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.ID, id)))
        return element

    def find_by_xpath(self, xpath):
        element = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, xpath)))
        return element


    def find_by_xpath_txt(self, txt):
        element = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, f"//*[contains(text(),'{txt}')]")))
        return element

    def unzip_file(self, path):
        with zipfile.ZipFile(path, 'r') as zip:
            zip.extract('pdprice.xls', 'C:/temp')
            return path.replace("zip", "xls")

    def authorization_pulser(self):
        self.driver.get("http://old.pulser.kz/")
        login = self.find_by_id('logInput')
        psw = self.find_by_id('pasInput')
        login.send_keys("reseller")
        psw.send_keys("PULSER")
        self.find_by_id("auGo").click()
        # self.driver.get_screenshot_as_file("entered.png")

    def authorization_al_style(self):
        self.driver.get("http://b2bportal.al-style.kz/site/login")
        login = self.find_by_id('loginform-username')
        psw = self.find_by_id('loginform-password')
        login.send_keys("keml08@mail.ru")
        psw.send_keys("Yaebuipla4U")
        self.find_by_xpath("//button[@type='submit']").click()

    def authorization_azerti(self):
        self.driver.get("http://shop.azerti.kz/")
        login = self.find_by_xpath("//input[@name='USER_LOGIN']")
        psw = self.find_by_xpath("//input[@name='USER_PASSWORD']")
        login.send_keys("ZUFAR.KHUSAINOV888@MAIL.RU")
        psw.send_keys("xlz08i0g")
        self.find_by_xpath("//input[@type='submit']").click()

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

    def download_file_pulser(self):
        element = self.find_by_xpath("//a[@title='Скачать прайс-лист реселлера']")
        self.highlight(element, 3, "blue", 5)
        element.click()
        time.sleep(3)
        path = "C:/temp/"
        return path + element.get_attribute("href").split("/")[-1]

    def close_notice(self):
        try:
            WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.ID, "notification_deny"))).click()
        except Exception as ex:
            print(ex)


    def download_file_al_style(self):

        element = self.find_by_xpath("//a[@href='https://b2bportal.al-style.kz/export/Al-Style_price.xlsx']")
        self.close_notice()
        self.highlight(element, 3, "blue", 5)
        element.click()
        time.sleep(3)
        path = "C:/temp/"
        return path + element.get_attribute("href").split("/")[-1]

    def download_file_azerti(self):
        element = self.find_by_xpath("//span[@class='i_hed_blp_h']")
        self.highlight(element, 3, "blue", 5)
        element.click()
        time.sleep(3)
        path = "C:/temp/"
        return path + 'price-list.xls'

    def get_data_from_pulser(self):
        self.authorization_pulser()
        zip_file = self.download_file_pulser()
        if os.stat(zip_file).st_size != 0:
            xls_file = self.unzip_file(zip_file)

            return xls_file

    def get_data_from_al_style(self):
        self.authorization_al_style()
        xls_file = self.download_file_al_style()

        return xls_file

    def get_data_from_azerti(self):
        self.authorization_azerti()
        xls_file = self.download_file_azerti()

        return xls_file

    def read_xls_from_pulser(self, file):
        data = []
        if not file:
            return data
        book = xlrd.open_workbook(file)
        sheet = book.sheet_by_index(0)
        for rx in range(sheet.nrows):
            if not isinstance(sheet.cell_value(rowx=rx, colx=1), float):
                continue
            elif sheet.cell_value(rowx=rx, colx=7) == "resrv":
                continue
            data.append(
                {"kod": str(int(sheet.cell_value(rowx=rx, colx=1))), "price": int(sheet.cell_value(rowx=rx, colx=5))})
        return data

    def read_xls_from_al_style(self, file):
        data = []
        wookbook = openpyxl.load_workbook(file)
        sheet = wookbook.active
        for rx in range(1, sheet.max_row):
            if not isinstance(sheet.cell(row=rx, column=1).value, int):
                continue
            elif int(self.s(sheet.cell(row=rx, column=8).value)) < 5:
                self.logging_('_', '_',
                              'пропукаю экспорт товара из файла, поскольку кол-во меньше 5 =>'
                              + str(sheet.cell(row=rx, column=1).value) + 'AL', type="download")
                continue

            data.append({"kod": str(sheet.cell(row=rx, column=1).value) + 'AL',
                         "price": int(sheet.cell(row=rx, column=5).value)})
        return data

    @staticmethod
    def ifnul(s, s2):
        if s == None or s == "":
            return s2
        else:
            return s

    def s(self, s):
        return float(self.ifnul(str(s).replace('<', "").replace(">", ""), 0))

    def read_xls_from_azerti(self, file):
        data = []
        book = xlrd.open_workbook(file)
        sheet = book.sheet_by_index(0)
        for rx in range(sheet.nrows):
            if not isinstance(sheet.cell_value(rowx=rx, colx=0), float):
                continue
            elif int(self.s(sheet.cell_value(rowx=rx, colx=3))) < 5:
                self.logging_('_', '_',
                              'пропукаю экспорт товара из файла, поскольку кол-во меньше 5 =>'
                              + str(int(sheet.cell_value(rowx=rx, colx=0))), type="download")
                continue
            data.append(
                {"kod": str(int(sheet.cell_value(rowx=rx, colx=0))), "price": int(sheet.cell_value(rowx=rx, colx=4))})
        return data

    def get_kt_data_from_wlbr(self, wlb_kt, lst_for_del=[]):

        i = 0
        while len(wlb_kt) > i:
            if wlb_kt[i]['vendorCode'] not in lst_for_del:
                del (wlb_kt[i])  # перепроверка
            i += 1
        return wlb_kt

    def get_all_cards_from_wb(self):
        self.token = "Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxODQzMTQxMiwiaWQiOiJlMTk3ODFiNy0xNmFmLTQ3NTktYTYyMS03ZDVlOGIzMWZiOTAiLCJpaWQiOjYwNjA2MzAyLCJvaWQiOjc5NTIzNCwicyI6NTEwLCJzYW5kYm94IjpmYWxzZSwic2lkIjoiNWZlMzVmY2MtZDA0MC00NjhjLThjODctZDBjNmYwNTI0MjBjIiwidWlkIjo2MDYwNjMwMn0.FgMpsnwF7EBLbwSy5hLYTqqlxTHk_HwNKvUGi47ZxReKSlpxHwFz8A7_it68Zq04cZ6nPJ1gKlGyDDTd17ab6A"
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
            r = requests.post(url, headers={"Authorization": self.token, "content-type": "application/json"},
                              json=data, timeout=10)
            if r.status_code == 200:
                self.logging_('_', '_', 'данные с wb успешно получены')
            else:
                self.logging_('_', '_', 'ошибка получения данных с wb')

            json_result = json.loads(r.content.decode())
            data["sort"]["cursor"]["updatedAt"] = json_result["data"]["cursor"]["updatedAt"]
            data["sort"]["cursor"]["nmID"] = json_result["data"]["cursor"]["nmID"]
            cards = cards + json_result["data"]["cards"]
            total = json_result["data"]["cursor"]["total"]
        return cards

    def get_prices(self):
        p = requests.get(self.base_url, headers={"Authorization": self.token}, params={"quantity": 0})
        return json.loads(p.content.decode())

    def collect_barcodes(self, db_data, wb_data):
        actual_data = [a
                       for a in db_data
                       if a[3] != "" and a[5] and not a[6]
                       ]
        barcode_lst = []

        for row in actual_data:
            match_data = [i for i in wb_data if i['vendorCode'].strip() == row[1].strip()]
            if match_data:
                match_data = match_data[0]
                vendorCode = match_data['vendorCode']
                sku = match_data["sizes"][0]
                barcode_lst.append({"sku": sku["skus"][0], "amount": 0,
                                "vendorCode": vendorCode, "nmId": match_data["nmID"],
                                "price": row[7], "source": row[3]})
            else:
                self.logging_('_', '_', 'ошибка при сверке ' + row[1] + ', отсутствует в списке карточек wb')

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
        if r.status_code == 204:
            self.logging_('', '', 'успешно обновил остатки', type='stock')
            return True
        else:
            self.logging_('', '', 'ошибка обновления остатков', type='stock')

    def get_data_from_all_files(self):
        self.logging_('_', '_', 'начинаю загружать файлы поставщиков')
        data = {}
        al_style_file = self.get_data_from_al_style()
        azerti_file = self.get_data_from_azerti()
        pulser_file = self.get_data_from_pulser()
        self.end_session()
        data["al_style_data"] = self.read_xls_from_al_style(al_style_file)
        data["azerti_data"] = self.read_xls_from_azerti(azerti_file)
        data["pulser_data"] = self.read_xls_from_pulser(pulser_file)
        self.logging_('_', '_', 'завершил загрузку файлов поставщиков')
        return data

    def get_vendor_code_by_sku(self, sku, barcode_json):

        vendoreCode = [a for a in barcode_json if a["sku"] == sku][0]['vendorCode']
        return vendoreCode
        # field_match = re.findall(r'\d+AL', vendoreCode)
        # if field_match:
        #     return field_match[0]
        # elif vendoreCode[0:6].isdigit():
        #     return vendoreCode[0:6]
        # elif vendoreCode[0:5].isdigit():
        #     return vendoreCode[0:5]
        # else:
        #     return

    def get_price_from_wb(self, vendorcode, wb_data):

        tempcode = [a["price"] for a in wb_data if a["kod"] == vendorcode]
        if len(tempcode) > 0:
            return tempcode[0]
        return

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
        elif amount <= 500 and amount > 1:
            mn = 10
        else:
            mn = 2
        return amount * mn / currency

    def synchronization_price(self, barcode_json, currency_):
        updated_cnt = 0
        all_prices = self.get_prices()
        prices = []
        for row in barcode_json:

            vendor_price = float(row['price'])
            converted_sum = self.calculate_sum(vendor_price, currency_)
            wb_price_ = [a['price'] for a in all_prices if a['nmId'] == row['nmId']]
            wb_price = wb_price_[0] if wb_price_ else 0
            if wb_price == 0:
                self.logging_(row['sku'], row['source'], f'надо проверить = {row}', type='to_check')
            if round(converted_sum) != wb_price and wb_price != 0:
                prices.append({"nmId": row['nmId'], "price": int(round(converted_sum, 0))})
                self.logging_(row['sku'], row['source'], 'новая цена = ' + str(converted_sum) + ',  '
                                                   'старая цена = '+ str(wb_price), type='price')
        if len(prices) > 0:
            updated_cnt += len(prices)
            self.update_prices(prices)

        return updated_cnt

    def update_prices(self, data):

        start = 0
        step = 1000
        stop = 1000
        temp = 1000
        while temp > 0:
            temp_data = data[start:stop]
            url = "https://suppliers-api.wildberries.ru/public/api/v1/prices"
            r = requests.post(url, headers={"Authorization": self.token, "content-type": "application/json"},
                              json=temp_data)
            if r.status_code == 200:
                self.logging_('_', '_', 'обновление цен завершено', type='price')
                start += step
                stop += step
                temp = len(data[start:stop])

            else:
                self.logging_('_', '_', 'ошибка обновления цен', type='price')
                return

    def synchronization_stock(self, db_data, wb_data, lst_warehouses_id):
        updated_cnt = 0
        barcode_json = [{'sku': a['sizes'][0]['skus'][0], 'vendorCode': a['vendorCode']} for a in wb_data]
        barcode_lst = [a["sku"] for a in barcode_json]
        for j in range(0, len(barcode_lst), 1000):
            tmp_lst = barcode_lst[j:j + 1000]
            stocks = self.get_stocks_by_warehouse(lst_warehouses_id, tmp_lst)["stocks"]
            new_stock = {"stocks": []}
            for k in stocks:
                tmp_vendorcode = self.get_vendor_code_by_sku(k["sku"], barcode_json)
                if not tmp_vendorcode:
                    continue
                elif len([a for a in db_data if a[1] == tmp_vendorcode and a[6]]) > 0:
                    continue
                if len([a for a in db_data if a[1] == tmp_vendorcode and a[5]]) > 0 and k["amount"] < 1:
                    tmp_stock = {"sku": k["sku"], "amount": 5}
                    self.logging_(k["sku"], '', 'обновляю остаток у карточки, +5 =>' + str(k["sku"]), type='stock')
                    new_stock["stocks"].append(tmp_stock)
                elif len([a for a in db_data if a[1] == tmp_vendorcode and not a[5]]) > 0 and k["amount"] > 0:
                    tmp_stock = {"sku": k["sku"], "amount": 0}
                    self.logging_(k["sku"], '', 'обнуляю остаток у карточки, 0 =>' + str(k["sku"]), type='stock')
                    new_stock["stocks"].append(tmp_stock)
            if new_stock['stocks']:
                updated_cnt += len(new_stock['stocks'])
                self.upd_warehouse(new_stock, lst_warehouses_id)
        return updated_cnt


class Main(AutomationWILDBERRIES, Config):
    def __init__(self):
        super().__init__()

    def filling_db_from_wb(self, wb_data):

        for i in range(len(wb_data)):
            barCode = wb_data[i]["sizes"][0]['skus'][0].strip()
            vendorCode = wb_data[i]['vendorCode'].strip()
            sql_cnt = Config.sql_count.format(vendorCode)
            data_count = self.execute_sql(self.conn, sql_cnt, 1)
            if data_count[0][0] > 0:
                continue
            sql = Config.sql_insert
            sql = sql.format(vendorCode, barCode, '', 'Keml', True)
            if not self.execute_sql(self.conn, sql):
                self.logging_('barcode', 'source', 'ошибка записи по вендоркоду ' + vendorCode)

    def update_column_source_in_db(self, files_data, db_data, wb_data):
        self.logging_('_', '_', 'обновляю БД исходя от данных в файлах')
        temp_list = []
        new_data = []
        for file, rows in files_data.items():
            for row in rows:
                wb_exists = [wb for wb in wb_data if wb['vendorCode'].strip() in row['kod'].strip()]
                if not wb_exists:
                    continue
                temp_row = [(row['price'], data[0]) for data in db_data if data[1].strip() == row['kod'].strip()]
                if len(temp_row) == 1:
                    temp_list.append(row['kod'])
                    id_ = temp_row[0][1]
                    sql_update = Config.sql_update.format(file, temp_row[0][0], id_)
                    if not self.execute_sql(self.conn, sql_update):
                        self.logging_('_', '_', 'ошибка обновления в БД ' + temp_row[0][0].strip())
                elif len(temp_row) > 1:
                    self.logging_('_', '_', 'нашел больше 1 карточки по вендоркоду ' + temp_row[0][0].strip(), type='exclude')
                elif len(temp_row) == 0:
                    new_data.append(row)
        print('check new data')

        for data in db_data:
            for file, rows in files_data.items():
                temp_row = [row['price'] for row in rows if data[1].strip() == row['kod'].strip()]
                if len(temp_row) == 1:
                    temp_list.append(data[1].strip())
            if data[1] not in temp_list:
                # self.logging_('_', '_', 'не нашел товар по вендоркоду ' + data[1].strip(), type='is_active')
                sql_active = Config.sql_update_active.format(False, data[0])
                self.execute_sql(self.conn, sql_active)

    def get_currency(self):
        currency_json = json.loads(convert('kzt', 'rub', 1000))
        currency_value = 1000 / float(currency_json["amount"])
        previuos_currency = self.execute_sql(self.conn, self.sql_get_last_currency, param=1)
        difference = (100/float(previuos_currency[0][0]))*abs(float(previuos_currency[0][0])-currency_value)
        currncy_text = f'текущий курс валют {round(currency_value, 2)}\n' + \
                       f'предыдущий курс валют {round(float(previuos_currency[0][0]), 2)}\n' + \
                       f'Разница курс валют составляет {difference} %'
        self.send_alert(currncy_text)
        self.logging_('_', '_', f'текущий курс валют {currency_value}', 'currency')
        self.logging_('_', '_', f'предыдущий курс валют {previuos_currency[0][0]}', 'currency')
        self.logging_('_', '_', f'Разница курс валют составляет {difference} %', 'currency')
        self.execute_sql(self.conn, self.sql_insert_currency.format(currency_value))
        if difference > self.percent_diff:
            self.send_alert(f'Пропускаю данную сессию {difference} %')
            return 0
        else:
            return currency_value

    def run_robot(self):
        currency = self.get_currency()
        if currency == 0:
            return
        # elif currency < 5.1:
        #     currency = 5.1
        updated_data = {'updated_prices': 0, 'updated_stock': 0}
        wb_data = self.get_all_cards_from_wb()
        files_data = self.get_data_from_all_files()
        self.logging_('_', '_', 'получил данные с wb, db, files')
        self.filling_db_from_wb(wb_data)
        db_data = self.get_data_from_db()
        self.update_column_source_in_db(files_data, db_data, wb_data)
        warehouses_lst = self.get_warehouses()
        db_data = self.get_data_from_db(sql="""select * from public.data_from_wb where "source" != ''""")
        barcode_json = self.collect_barcodes(db_data, wb_data)
        for i in range(len(warehouses_lst)):
            updated_data['updated_prices'] = self.synchronization_price(barcode_json, currency)
            updated_data['updated_stock'] = self.synchronization_stock(db_data, wb_data, warehouses_lst[i]["id"])
        return updated_data

    def main(self):

        self.send_alert('инициализация робота')
        while True:

            now = datetime.datetime.now()
            begin_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = now.replace(hour=23, minute=59, second=0, microsecond=0)
            wb_error = False
            if end_time > now > begin_time:
                try:
                    self.logging_('_', '_', 'запускаю робота!!!')
                    self.start_browser()
                    result = self.run_robot()
                    self.send_alert(f'обновлено цен {result["updated_prices"]}')
                    self.send_alert(f'обновлено остатков {result["updated_stock"]}')
                    self.logging_('_', '_', 'завершаю работу!!!')
                except TypeError:
                    wb_error = True
                    self.logging_('_', '_', 'ошибка wb, повторю запрос через 5 мин')
                    time.sleep(300)
                except Exception as ex:
                    print(traceback.print_exc())
                    self.logging_('_', '_', 'ошибка в работе робота!!!')
                    self.send_alert('ошибка в работе робота!!!')
                    self.send_alert(f'{ex.msg}')
                    self.send_alert(str(ex.args))

                finally:
                    if not wb_error:
                        time.sleep(7200)
                    self.end_session()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    cls = Main()
    cls.main()
    # cls.stay_active()
