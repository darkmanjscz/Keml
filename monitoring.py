import datetime
import requests, pyodbc, os
import logging
os.environ["NLS_LANG"] = ".UTF8"
pyodbc.pooling = False

class Monitoring():

     def __init__(self):
         logging.basicConfig(filename='myapp.log', level=logging.INFO)

     def send_alert(self, text):
          """ Метод для отправки уведомлений на телегу
          данные собирает из вьюшки [dbo].[vFaultedJobsToday2]
          со статусом Faulted, то что обработал, записывает в таблицу [dbo].[faulted_id2] """

          process = "update WB"
          server = "PC KEML"
          info = text
          createtime = datetime.datetime.now()
          head = "---------------------------------------------------------------------------\n"
          row1 = "<b> {} </b> \n".format("🔥🔥🔥 " + process)
          row2 = "<i> {} </i> \n".format("🖥️ " + server)
          row3 = "<strong> {} </strong>\n".format("💡 " + info.replace("\n", ""))
          row4 = " <em> {} </em>\n".format("⌚ " + str(createtime))
          footer = "---------------------------------------------------------------------------\n"

          msg = row1 + row2 + row3 + row4
          self.send_message_bot(msg)
          logging.info('row with id ' + str(id) + "processed")


     def send_message_bot(self, text):
          endPoint = "https://api.telegram.org/bot"
          # id = "-579581470"
          id = "-914261239" # supergroup id
          Token = "5681293588:AAEWMxiSSAdVkc0HRW1yTEAzDqOGVKzokWk/"
          r = requests.get(url=endPoint+Token+"sendMessage?chat_id="+id+"&text="+text+"&parse_mode=html", verify=False)

          resp = r.json()
          if resp["ok"]:
               return 'successfull'
          else:
               raise RuntimeError(r.content)


cls = Monitoring()
cls.send_alert("I have some updates for you")




