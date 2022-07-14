from datetime import date
import requests
import logging
import xmltodict
import sqlite3
import re


# Логгирование
class Logger:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        logger_handler = logging.FileHandler('main.log')
        logger_handler.setLevel(logging.INFO)

        logger_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        logger_handler.setFormatter(logger_formatter)
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
        self.logger.addHandler(logger_handler)


# Валюта (код, масштаб, курс)
class Value:
    def __init__(self, code, scale, rate):
        self.code = code
        self.scale = scale
        self.rate = rate

    def __str__(self):
        return self.scale + " " + self.code + " = " + self.rate + " RUB"


# Сервис DailyInfo
class DailyInfoClient:
    def __init__(self, date_list):
        # Логгирование
        self.log = Logger()
        try:
            self.date_currency = date(int(date_list[2]), int(date_list[1]), int(date_list[0]))
        except ValueError:
            self.log.logger.warning('Неверный формат даты')

    def get_xml(self):
        url = 'https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?op=GetCursOnDateXML'
        body = """
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <GetCursOnDateXML xmlns="http://web.cbr.ru/">
      <On_date>{0}</On_date>
    </GetCursOnDateXML>
  </soap12:Body>
</soap12:Envelope>
""".format(self.date_currency)
        headers = {
            "POST": "/DailyInfoWebServ/DailyInfo.asmx HTTP/1.1",
            "Host": "www.cbr.ru",
            "Content-Type": "application/soap+xml; charset=utf-8",
            "Content-Length": str(len(body))
        }
        body = body.encode('utf-8')
        response = requests.post(url, headers=headers, data=body)
        if response.status_code == 200:
            return response.text
        else:
            self.log.logger.warning('Подключение не удалось')


# Парсер XML
class XMLParser:
    def __init__(self, file):
        self.file = file

    # XML -> Values List
    def get_values(self):
        dicts = xmltodict.parse(self.file)
        list_of_dicts = dicts['soap:Envelope']['soap:Body']['GetCursOnDateXMLResponse']['GetCursOnDateXMLResult']
        list_of_dicts = list_of_dicts['ValuteData']['ValuteCursOnDate']
        list_of_values = []
        for i in list_of_dicts:
            list_of_values.append(Value(i['VchCode'], i['Vnom'], i['Vcurs']))
        return list_of_values


# SQLite БД
class DB:
    def __init__(self):
        # Логгирование
        self.log = Logger()
        try:
            self.sql = sqlite3.connect('main.db')
            self.cursor = self.sql.cursor()
        except sqlite3.Error:
            self.log.logger.error('Ошибка при подключении к БД')

    def close(self):
        self.cursor.close()
        self.sql.close()

    # Создаем БД
    def create(self):
        # Таблица подразделений
        self.cursor.execute("PRAGMA foreign_keys = 1")
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS CURRENCY_SCOPE (
        division_id TEXT PRIMARY KEY ON CONFLICT IGNORE NOT NULL,
        branch_id TEXT NOT NULL,
        cashdepart_id INTEGER NOT NULL);''')

        # Таблица пользователей-авторизаторов
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS USER_AUTHORIZATOR (
        id TEXT PRIMARY KEY NOT NULL,
        password TEXT NOT NULL,
        division_id TEXT NOT NULL,
        FOREIGN KEY (division_id) REFERENCES CURRENCY_SCOPE (division_id)
        );''')
        # Таблица пользователей
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS USER (
        id TEXT PRIMARY KEY NOT NULL,
        password TEXT NOT NULL,
        division_id INTEGER NOT NULL,
        FOREIGN KEY (division_id) REFERENCES CURRENCY_SCOPE (division_id)
        );''')

        # Таблица распоряжений
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS CURRENCY_ORDER (
        order_no INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        created TEXT DEFAULT (datetime('now', 'localtime')),
        created_by TEXT DEFAULT NULL,
        branch_id TEXT DEFAULT NULL,
        updated TEXT DEFAULT NULL,
        updated_by TEXT DEFAULT NULL,
        remarks TEXT DEFAULT ('Официальный курс ЦБ РФ')
        );""")
        # Таблица архива валют
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS CURRENCY_COURSES (
        course_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        order_no INTEGER NOT NULL,
        currency_no_1 TEXT DEFAULT NULL,
        currency_no_2 TEXT DEFAULT NULL,
        currency_date TEXT DEFAULT NULL,
        scale INTEGER DEFAULT NULL,
        amount REAL DEFAULT NULL,
        created TEXT DEFAULT NULL,
        created_by TEXT DEFAULT NULL,
        branch_id TEXT DEFAULT NULL,
        updated TEXT DEFAULT NULL,
        updated_by TEXT DEFAULT NULL,
        remarks TEXT DEFAULT ('Официальный курс ЦБ РФ'),
        FOREIGN KEY (order_no) REFERENCES CURRENCY_ORDER (order_no) ON DELETE CASCADE
        );''')

        # Триггер: при обновлении архивной записи обновляем распоряжение
        self.cursor.execute('''CREATE TRIGGER IF NOT EXISTS update_order AFTER UPDATE on CURRENCY_COURSES
        BEGIN
           UPDATE CURRENCY_ORDER SET updated = NEW.updated, updated_by = NEW.updated_by WHERE order_no = NEW.order_no;
        END;
        ''')
        self.sql.commit()
        self.log.logger.info('БД создана')

    # Select DB -> Values List
    @staticmethod
    def select_to_value(select):
        result = []
        for i in range(len(select)):
            tmp = Value(str(select[i][2]), str(select[i][5]), str(select[i][6]))
            result.append(tmp)
        return result

    def test(self):
        self.cursor.execute('''INSERT INTO CURRENCY_SCOPE (division_id, branch_id, cashdepart_id) 
        VALUES ('KIROV1', 'HLN', 909891069)''')
        self.cursor.execute('''INSERT INTO USER_AUTHORIZATOR (id, password, division_id) 
        VALUES ('a01', 'qwerty', 'KIROV1');''')
        self.cursor.execute('''INSERT INTO USER (id, password, division_id) 
        VALUES ('u01', 'qwerty', 'KIROV1');
        ''')
        self.sql.commit()


# Класс авторизации
class Authorization:
    def __init__(self):
        self.login = ''
        self.password = ''
        self.db = DB()
        # Логгирование
        self.log = Logger()

    def is_logged_in(self):
        logged = self.db.cursor.execute('''
        SELECT id, password FROM USER WHERE id = '{0}' AND password = '{1}' 
        UNION SELECT id, password FROM USER_AUTHORIZATOR 
        WHERE id = '{0}' AND password = '{1}';'''.format(self.login, self.password)).fetchone()
        self.db.sql.commit()
        if logged is not None:
            self.login = logged[0]
            self.password = logged[1]
            self.log.logger.info('Успешная авторизация {0}'.format(self.login))
            return True
        else:
            self.log.logger.info('Неудачная попытка авторизации')
            return False

    def is_authorizator(self):
        authorizators = self.db.cursor.execute('''
        SELECT id, password FROM USER_AUTHORIZATOR 
        WHERE id = '{0}' AND password = '{1}';'''.format(self.login, self.password)).fetchone()
        self.db.sql.commit()
        if authorizators is None or len(authorizators) == 0:
            return False
        else:
            return True

    def try_logging(self):
        self.login = input()
        self.password = input()
        self.log.logger.info('Попытка входа')


# Обработка запроса скрипта
class ScriptRequest:
    def __init__(self, text=''):
        self.text = text
        # Логгирование
        self.log = Logger()

    def parse_command(self):
        result = []
        if re.fullmatch(r'\d{2}\.\d{2}\.\d{4}$', self.text):
            curs_date = re.fullmatch(r'\d{2}\.\d{2}\.\d{4}$', self.text).group(0).split('.')
            result.append(curs_date)
        elif re.fullmatch(r'\d{2}\.\d{2}\.\d{4} (?:[A-Z]{3} )*[A-Z]{3}$', self.text):
            request = re.fullmatch(r'\d{2}\.\d{2}\.\d{4} (?:[A-Z]{3} )*[A-Z]{3}$', self.text).group(0).split()
            curs_date = request[0].split('.')
            codes = request[1::]
            result.append(curs_date)
            result.append(codes)
        else:
            self.log.logger.info('Неизвестная комманда')
        return result


# Класс пользователя
class User:
    def __init__(self, login):
        self.login = login
        self.db = DB()
        # Логгирование
        self.log = Logger()

    def get_currency(self, text):
        sr = ScriptRequest(text)
        req = sr.parse_command()
        date_str = sr.text.split()[0]
        date_now = self.db.cursor.execute('''SELECT datetime('now', 'localtime')''').fetchone()[0]
        order_no = self.db.cursor.execute(
            '''SELECT order_no FROM CURRENCY_COURSES WHERE currency_date = '{0}';'''.format(date_str)).fetchone()
        if order_no is None:
            self.log.logger.info('Курсы валют не найдены')
        else:
            order_no = order_no[0]
            if len(req) == 1:
                # Обновляем архив
                self.db.cursor.execute('BEGIN TRANSACTION;')
                self.db.cursor.execute('''UPDATE CURRENCY_COURSES SET 
                                                updated = '{0}', updated_by = '{1}' WHERE order_no = {2};'''.format(
                    date_now, self.login, order_no))
                self.db.cursor.execute('END;')
                self.db.sql.commit()
                # Вывод в лог файл
                archive = self.db.cursor.execute(
                    '''SELECT * FROM CURRENCY_COURSES WHERE order_no = '{0}';'''.format(order_no)).fetchall()
                for i in self.db.select_to_value(archive):
                    self.log.logger.info(i)
            elif len(req) == 2:
                codes = req[1]
                # Обновляем архив по конкретным кодам
                self.db.cursor.execute('BEGIN TRANSACTION;')
                for i in codes:
                    self.db.cursor.execute('''
                    UPDATE CURRENCY_COURSES SET updated = '{0}', updated_by = '{1}' 
                    WHERE currency_no_1 = '{2}' AND order_no = {3};'''.format(date_now, self.login, i, order_no))
                self.db.cursor.execute('END;')
                self.db.sql.commit()
                # Выводим в лог файл
                results = []
                for i in range(len(codes)):
                    tmp = self.db.cursor.execute(
                        '''SELECT * FROM CURRENCY_COURSES WHERE order_no = {0} AND currency_no_1 = '{1}';'''.format(
                            order_no, codes[i])).fetchone()
                    if tmp is not None:
                        results.append(tmp)
                values = self.db.select_to_value(results)
                for i in values:
                    self.log.logger.info(i)


class UserAuthorizator(User):
    def get_currency(self, text):
        sr = ScriptRequest(text)
        req = sr.parse_command()
        date_str = sr.text.split()[0]

        # Подключение к DailyInfo, парсинг XML и получение курсов

        dic = DailyInfoClient(req[0])
        xml_file = dic.get_xml()
        parser = XMLParser(xml_file)
        values = parser.get_values()

        # Получение данных из БД (тут как-то можно использовать join?)
        division = self.db.cursor.execute('''
        SELECT division_id FROM USER_AUTHORIZATOR WHERE id = '{0}';'''.format(self.login)).fetchone()[0]
        branch = self.db.cursor.execute(
            '''SELECT branch_id FROM CURRENCY_SCOPE WHERE division_id = '{0}';'''.format(division)).fetchone()[0]
        date_now = self.db.cursor.execute('''SELECT datetime('now', 'localtime')''').fetchone()[0]

        order_no = self.db.cursor.execute(
            '''SELECT order_no FROM CURRENCY_COURSES WHERE currency_date = '{0}';'''.format(date_str)).fetchone()
        self.db.sql.commit()
        if order_no is None:
            # В архиве - пусто создаем ордер
            self.db.cursor.execute('''INSERT INTO CURRENCY_ORDER (created ,created_by, branch_id) 
            VALUES ('{0}', '{1}','{2}');'''.format(date_now, self.login, branch))
            order_no = self.db.cursor.execute('''SELECT order_no FROM CURRENCY_ORDER 
            WHERE created = '{0}' AND created_by = '{1}';'''.format(date_now, self.login)).fetchone()[0]
            self.db.sql.commit()
            if len(req) == 1:
                self.db.cursor.execute('BEGIN TRANSACTION;')
                # Создаем записи в архиве и выводим курсы валют
                for i in values:
                    self.db.cursor.execute('''INSERT INTO CURRENCY_COURSES (order_no, currency_no_1, currency_no_2, 
                    currency_date, scale, amount, created, created_by, branch_id) 
                    VALUES ({0}, '{1}', '{2}', '{3}', {4}, {5}, '{6}', '{7}', '{8}')
                    '''.format(order_no, i.code, 'RUB', date_str, i.scale, i.rate, date_now, self.login, branch))
                self.db.cursor.execute('END;')
                self.db.sql.commit()
                # Выводим в лог курсы валют из таблицы
                select = self.db.cursor.execute('''SELECT * FROM CURRENCY_COURSES 
                WHERE order_no = '{0}';'''.format(order_no)).fetchall()

                for i in self.db.select_to_value(select):
                    self.log.logger.info(i)
            elif len(req) == 2:
                self.db.sql.commit()
                request_values = []
                # Получаем нужные курсы валют
                for i in range(len(values)):
                    if values[i].code in req[1]:
                        request_values.append(values[i])
                        self.log.logger.info(values[i])
                # Тут была вставка ордера
                self.db.cursor.execute('BEGIN TRANSACTION;')
                # Создаем записи в архиве и выводим курсы валют
                for i in request_values:
                    self.db.cursor.execute('''INSERT INTO CURRENCY_COURSES (order_no, currency_no_1, currency_no_2, 
                                                        currency_date, scale, amount, created, created_by, branch_id) 
                                                        VALUES ({0}, '{1}', '{2}', '{3}', {4}, {5}, '{6}', '{7}', '{8}')
                                                        '''.format(order_no, i.code, 'RUB', date_str, i.scale, i.rate,
                                                                   date_now, self.login, branch))
                self.db.cursor.execute('END;')
                selects = []
                # Выводим в лог курсы валют из таблицы
                for i in range(len(req[1])):
                    tmp = self.db.cursor.execute(
                        '''SELECT * FROM CURRENCY_COURSES WHERE order_no = {0} AND currency_no_1 = '{1}';'''.format(
                            order_no, req[1][i])).fetchone()
                    selects.append(tmp)
                values = self.db.select_to_value(selects)
                for i in values:
                    self.log.logger.info(i)
        # НАДО ПЕРЕПИСАТЬ ТО ЧТО НИЖЕ
        else:
            # В архиве есть записи
            order_no = order_no[0]
            if len(req) == 1:
                # Существующие коды
                codes_exist = self.db.cursor.execute(
                    '''SELECT currency_no_1 FROM CURRENCY_COURSES WHERE currency_date = '{0}';'''.format(
                        date_str)).fetchall()
                self.db.sql.commit()
                # Обновляем архив
                self.db.cursor.execute('''UPDATE CURRENCY_COURSES SET 
                updated = '{0}', updated_by = '{1}' WHERE order_no = {2};'''.format(date_now, self.login, order_no))
                self.db.sql.commit()
                # Находим еще несуществующие курсы
                codes_dont_exist = []
                list_codes_exist = []
                for i in range(len(codes_exist)):
                    list_codes_exist.append(codes_exist[i][0])

                for i in range(len(values)):
                    if values[i].code not in list_codes_exist:
                        codes_dont_exist.append(values[i])
                # Добавляем несуществующие курсы в архив
                for i in codes_dont_exist:
                    self.db.cursor.execute('''INSERT INTO CURRENCY_COURSES (order_no, currency_no_1, currency_no_2, 
                                                    currency_date, scale, amount, created, created_by, branch_id) 
                                                    VALUES ({0}, '{1}', '{2}', '{3}', {4}, {5}, '{6}', '{7}', '{8}')
                                                    '''.format(order_no, i.code, 'RUB', date_str, i.scale, i.rate,
                                                               date_now, self.login, branch))
                self.db.sql.commit()
                # Выводим в консоль курсы валют из таблицы
                selects = self.db.cursor.execute(
                    '''SELECT * FROM CURRENCY_COURSES WHERE order_no = '{0}';'''.format(order_no)).fetchall()
                for i in self.db.select_to_value(selects):
                    self.log.logger.info(i)
                self.db.sql.commit()
            elif len(req) == 2:
                # Находим уже существующие коды, обновляем ордер и архив
                codes_exist = []
                for i in req[1]:
                    tmp_codes_exist = self.db.cursor.execute('''SELECT currency_no_1 FROM CURRENCY_COURSES 
                    WHERE currency_date = '{0}' AND currency_no_1 = '{1}';'''.format(date_str, i)).fetchone()
                    if tmp_codes_exist is not None:
                        codes_exist.append(tmp_codes_exist)

                # Обновляем архив
                self.db.cursor.execute('BEGIN TRANSACTION;')
                for i in codes_exist:
                    self.db.cursor.execute('''UPDATE CURRENCY_COURSES SET updated = '{0}', updated_by = '{1}' 
                    WHERE order_no = {2} AND currency_date = '{3}' AND currency_no_1 = '{4}';
                    '''.format(date_now, self.login, order_no, date_str, i[0]))
                self.db.cursor.execute('END;')
                self.db.sql.commit()

                # Находим еще несуществующие курсы
                codes_dont_exist = []
                list_codes_exist = []
                values_dont_exist = []
                for i in range(len(codes_exist)):
                    list_codes_exist.append(codes_exist[i][0])

                for i in range(len(req[1])):
                    if req[1][i] not in list_codes_exist:
                        codes_dont_exist.append(req[1][i])
                for i in range(len(values)):
                    if values[i].code in codes_dont_exist:
                        values_dont_exist.append(values[i])

                # Добавляем несуществующие курсы в архив
                self.db.cursor.execute('BEGIN TRANSACTION;')
                for i in values_dont_exist:
                    self.db.cursor.execute('''INSERT INTO CURRENCY_COURSES (order_no, currency_no_1, currency_no_2, 
                    currency_date, scale, amount, created, created_by, branch_id) 
                    VALUES ({0}, '{1}', '{2}', '{3}', {4}, {5}, '{6}', '{7}', '{8}')
                    '''.format(order_no, i.code, 'RUB', date_str, i.scale, i.rate, date_now, self.login, branch))
                self.db.cursor.execute('END;')
                self.db.sql.commit()

                # Выводим в консоль курсы валют из таблицы
                selects = []
                for i in range(len(req[1])):
                    tmp = self.db.cursor.execute(
                        '''SELECT * FROM CURRENCY_COURSES WHERE order_no = {0} AND currency_no_1 = '{1}';'''.format(
                            order_no, req[1][i])).fetchone()
                    selects.append(tmp)
                values = self.db.select_to_value(selects)
                for i in values:
                    self.log.logger.info(i)


if __name__ == '__main__':
    db = DB()
    db.create()
    # db.test()
    au = Authorization()
    login_bool = False
    while login_bool is False:
        au.try_logging()
        login_bool = au.is_logged_in()
    u = UserAuthorizator(au.login) if au.is_authorizator() is True else User(au.login)

    command = ''
    while command.lower() != 'q':
        command = input()
        if command.lower() != 'q':
            u.get_currency(command)
    db.close()
