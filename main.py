from datetime import datetime
from zeep import Client
import xml.etree.ElementTree as et
import lxml
import re
import sqlite3
import os

# Класс валюты (код, масштаб, курс)
class Value:
    def __init__(self, vchcode, scale, rate):
        self.vchcode = vchcode
        self.scale = scale
        self.rate = rate

    def __str__(self):
        return (self.scale + " " + self.vchcode + " = " + self.rate + " RUB")

# Сервис DailyInfo
class DailyInfoClient:
    def __init__(self, date):
        try:
            self.date = datetime(int(date[2]), int(date[1]), int(date[0]))
        except ValueError:
            print('Неверный формат даты')

    def getXML(self):
        try:
            url = 'http://www.cbr.ru/dailyinfowebserv/dailyinfo.asmx?WSDL'
            client = Client(url)
            xml = client.service.GetCursOnDateXML(self.date)
            return xml
        except:
            print('Подключение не удалось')


# Парсим XML
class XMLParser:
    def __init__(self, file):
        self.file = file
    def __str__(self):
        return lxml.etree.tounicode(self.file, pretty_print=True)
    # XML -> Values List
    def getValues(self):
        listOfValues = []
        tree = et.ElementTree(self.file)
        try:
            vchCodes = tree.findall("ValuteCursOnDate/VchCode")
            scales = tree.findall("ValuteCursOnDate/Vnom")
            rates = tree.findall("ValuteCursOnDate/Vcurs")

            for i in range(len(vchCodes)):
                tmp = Value(vchCodes[i].text, scales[i].text, rates[i].text)
                listOfValues.append(tmp)
        except AttributeError:
            print('Парсинг невозможен')

        return listOfValues

class DB:
    def __init__(self):
        try:
            self.sql = sqlite3.connect('main.db')
            self.cursor = self.sql.cursor()
        except sqlite3.Error as error:
            print(error)
    def close(self):
        self.cursor.close()
        self.sql.close()
    #Создаем БД
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
        created TEXT DEFAULT (datetime('now')),
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


        # Триггер при обновлении распоряжения обновляем записи архива
        # не будет работать при обновлении курсов конкретных валют
        #self.cursor.execute('''CREATE TRIGGER IF NOT EXISTS update_archive AFTER UPDATE on CURRENCY_ORDER
        #BEGIN
        #    UPDATE CURRENCY_COURSES SET updated = NEW.updated, updated_by = NEW.updated_by WHERE order_no = NEW.order_no;
        #END;
        #''')
        self.sql.commit()
        print('БД загружена')
    # Тестирование
    def test(self):
        self.cursor.execute('''INSERT INTO CURRENCY_SCOPE (division_id, branch_id, cashdepart_id) 
        VALUES ('KIROV1', 'HLN', 909891069)''')
        self.sql.commit()
        self.cursor.execute('''INSERT INTO USER_AUTHORIZATOR (id, password, division_id) 
        VALUES ('a01', 'qwerty', 'KIROV1');''')
        self.cursor.execute('''INSERT INTO USER (id, password, division_id) 
        VALUES ('u01', 'qwerty', 'KIROV1');
        ''')
        self.sql.commit()

    # Функция авторизации, возвращает id и является ли пользователь авторизатором
    def login(self):
        res = []
        print('login: ')
        login = input()
        print('password: ')
        password = input()
        os.system('cls')
        ul = self.cursor.execute("SELECT * FROM USER WHERE id = '{0}'".format(login)).fetchall()
        al = self.cursor.execute("SELECT * FROM USER_AUTHORIZATOR WHERE id = '{0}'".format(login)).fetchall()
        if len(al) != 0 or len(ul) !=0:
            user = al if len(al) != 0 else ul
            if user[0][1] == password:
                print('Вход выполнен')
                res.append(user[0][0])
                isAuthorizator = True if len(al) else False
                res.append(isAuthorizator)
            else:
                print('Неверный логин или пароль')
        else:
            print('Неверный логин или пароль')

        return res
    # Select DB -> Values List
    def parseSelectToValue(self, list):
        res = []
        for i in range(len(list)):
            tmp = Value(str(list[i][2]), str(list[i][5]), str(list[i][6]))
            res.append(tmp)
        return res
if __name__ == '__main__':
    db = DB()
    db.create()
    #db.test()

    # Авторизация пользователя
    user = []
    while(len(user) == 0):
        user = db.login()

    com = ""
    #q - завершение работы
    #dd.mm.yyyy VCH_CODES - курсы валют по дате
    #dd.mm.yyyy - все курсы валют для даты
    while com.lower() != "q":
        com = input()
        allValues = []

        # Запрос в БД -> если курса нет, то парсим с DailyInfo
        if re.fullmatch('\d{2}\.\d{2}\.\d{4}$', com):
            dateStr = re.fullmatch('\d{2}\.\d{2}\.\d{4}$', com).group(0)
            date = dateStr.split('.')

            #Условия является ли пользователь авторизатором и есть ли уже записи в архиве
            checkArcive = db.cursor.execute('''SELECT * FROM CURRENCY_COURSES WHERE currency_date = '{0}';'''.format(dateStr)).fetchall()
            #Пользователь - авторизатор, курсов по дате нет -> парсим с сайта и добавляем в архив
            if user[1] == True and len(checkArcive) == 0:
                # Подключаемся к DailyInfo
                dc = DailyInfoClient(date)
                xml = dc.getXML()
                allValues = XMLParser(xml).getValues()

                #Создаем распоряжение
                division = db.cursor.execute('''SELECT division_id FROM USER_AUTHORIZATOR WHERE id = '{0}';'''.format(user[0])).fetchone()[0]
                branch = db.cursor.execute('''SELECT branch_id FROM CURRENCY_SCOPE WHERE division_id = '{0}';'''.format(division)).fetchone()[0]
                dateNow = db.cursor.execute('''SELECT datetime('now')''').fetchone()[0]

                db.cursor.execute('''INSERT INTO CURRENCY_ORDER (created ,created_by, branch_id)
                VALUES ('{0}', '{1}','{2}');'''.format(dateNow, user[0], branch))
                db.sql.commit()

                order_no = db.cursor.execute('''SELECT order_no FROM CURRENCY_ORDER WHERE created = '{0}' AND created_by = '{1}';'''.format(dateNow, user[0])).fetchone()[0]

                db.cursor.execute('BEGIN TRANSACTION;')
                #Создаем записи в архиве и выводим курсы валют
                for i in allValues:
                    db.cursor.execute('''INSERT INTO CURRENCY_COURSES (order_no, currency_no_1, currency_no_2, 
                    currency_date, scale, amount, created, created_by, branch_id) 
                    VALUES ({0}, '{1}', '{2}', '{3}', {4}, {5}, '{6}', '{7}', '{8}')
                    '''.format(order_no, i.vchcode, 'RUB', dateStr, i.scale, i.rate, dateNow, user[0], branch))
                db.cursor.execute('END;')
                db.sql.commit()

                #Выводим в консоль курсы валют из таблицы
                listFromDB = db.cursor.execute('''SELECT * FROM CURRENCY_COURSES WHERE order_no = '{0}';'''.format(order_no)).fetchall()
                for i in db.parseSelectToValue(listFromDB):
                    print(i)

            #Пользователь авторизатор, но в архиве не все записи
            elif user[1] == True and len(checkArcive) != len(XMLParser(DailyInfoClient(date).getXML()).getValues()):
                # Подключаемся к DailyInfo
                dc = DailyInfoClient(date)
                xml = dc.getXML()
                allValues = XMLParser(xml).getValues()

                #Находим уже существующие курсы, обновляем ордер и архив
                codesExist = db.cursor.execute('''SELECT currency_no_1 FROM CURRENCY_COURSES WHERE currency_date = '{0}';'''.format(dateStr)).fetchall()
                order_no = db.cursor.execute('''SELECT order_no FROM CURRENCY_COURSES WHERE currency_date = '{0}';'''.format(dateStr)).fetchone()[0]
                dateNow = db.cursor.execute('''SELECT datetime('now')''').fetchone()[0]
                db.cursor.execute('''UPDATE CURRENCY_ORDER SET updated = '{0}', updated_by = '{1}' WHERE order_no = {2};'''.format(dateNow, user[0], order_no))

                division = db.cursor.execute('''SELECT division_id FROM USER_AUTHORIZATOR WHERE id = '{0}';'''.format(user[0])).fetchone()[0]
                branch = db.cursor.execute('''SELECT branch_id FROM CURRENCY_SCOPE WHERE division_id = '{0}';'''.format(division)).fetchone()[0]

                # Обновляем архив
                db.sql.commit()
                db.cursor.execute('BEGIN TRANSACTION;')
                db.cursor.execute('''UPDATE CURRENCY_COURSES SET 
                                                    updated = '{0}', updated_by = '{1}' WHERE order_no = {2};'''.format(
                    dateNow, user[0], order_no))
                db.cursor.execute('END;')
                db.sql.commit()

                #Находим еще несуществующие курсы
                codesDontExist = []
                listCodesExist = []
                for i in range(len(codesExist)):
                    listCodesExist.append(codesExist[i][0])

                for i in range(len(allValues)):
                        if allValues[i].vchcode not in listCodesExist:
                            codesDontExist.append(allValues[i])

                #Добавляем несуществующие курсы в архив
                db.cursor.execute('BEGIN TRANSACTION;')
                for i in codesDontExist:
                    db.cursor.execute('''INSERT INTO CURRENCY_COURSES (order_no, currency_no_1, currency_no_2, 
                                    currency_date, scale, amount, created, created_by, branch_id) 
                                    VALUES ({0}, '{1}', '{2}', '{3}', {4}, {5}, '{6}', '{7}', '{8}')
                                    '''.format(order_no, i.vchcode, 'RUB', dateStr, i.scale, i.rate, dateNow, user[0],
                                               branch))
                db.cursor.execute('END;')
                db.sql.commit()

                # Выводим в консоль курсы валют из таблицы
                listFromDB = db.cursor.execute(
                    '''SELECT * FROM CURRENCY_COURSES WHERE order_no = '{0}';'''.format(order_no)).fetchall()
                for i in db.parseSelectToValue(listFromDB):
                    print(i)

            #Пользователь НЕавторизатор, в архиве - пусто
            elif user[1] == False and len(checkArcive) == 0:
                print('Курсы не найдены')

            #Пользователь НЕавторизатор или пользователь-авторизатор показываем все доступные курсы, обновляем данные в распоряжении и архиве
            elif (user[1] == False and len(checkArcive) != 0) or (user[1] == True and len(checkArcive) == len(XMLParser(DailyInfoClient(date).getXML()).getValues())):
                # Обновляем записи в распоряжениях
                order_no = db.cursor.execute('''SELECT order_no FROM CURRENCY_COURSES WHERE currency_date = '{0}';'''.format(dateStr)).fetchone()[0]
                dateNow = db.cursor.execute('''SELECT datetime('now')''').fetchone()[0]
                db.cursor.execute('''UPDATE CURRENCY_ORDER SET updated = '{0}', updated_by = '{1}' WHERE order_no = {2};'''.format(dateNow, user[0], order_no))
                db.sql.commit()

                # Обновляем архив
                db.cursor.execute('BEGIN TRANSACTION;')
                db.cursor.execute('''UPDATE CURRENCY_COURSES SET 
                                    updated = '{0}', updated_by = '{1}' WHERE order_no = {2};'''.format(
                    dateNow, user[0], order_no))
                db.cursor.execute('END;')
                db.sql.commit()

                # Выводим в консоль курсы валют из таблицы
                listFromDB = db.cursor.execute(
                    '''SELECT * FROM CURRENCY_COURSES WHERE order_no = '{0}';'''.format(order_no)).fetchall()
                for i in db.parseSelectToValue(listFromDB):
                    print(i)

        #DD.MM.YYYY VCH_CODE1 VCH_CODE 2 ...
        elif re.fullmatch('\d{2}\.\d{2}\.\d{4} (?:[A-Z]{3} )*[A-Z]{3}$', com):
            #Получаем дату и коды из запроса
            request = re.fullmatch('\d{2}\.\d{2}\.\d{4} (?:[A-Z]{3} )*[A-Z]{3}$', com).group(0).split()
            dateStr = request[0]
            date = request[0].split('.')
            codes = request[1::]

            resValues = []

            # Подключаемся к DailyInfo
            dc = DailyInfoClient(date)
            xml = dc.getXML()
            allValues = XMLParser(xml).getValues()

            #Проверяем корректность кодов из запроса
            validCodes = 0
            for i in range(len(allValues)):
                if allValues[i].vchcode in codes:
                    validCodes += 1
            isCorrect = True if validCodes == len(codes) else False

            if isCorrect:
                # Обращаемся к архиву по заданным кодам
                checkArcive = []
                for i in range(len(codes)):
                    tmpArcive = db.cursor.execute('''
                    SELECT * FROM CURRENCY_COURSES WHERE currency_date = '{0}' AND currency_no_1 = '{1}';'''.format(
                        dateStr, codes[i])).fetchone()
                    if tmpArcive != None:
                        checkArcive.append(tmpArcive)
                # Пользователь - авторизатор, курсов по дате нет -> парсим с сайта и добавляем в архив
                if user[1] == True and len(checkArcive) == 0:

                    #Получаем нужные курсы валют
                    for i in range(len(allValues)):
                        if allValues[i].vchcode in codes:
                            resValues.append(allValues[i])
                            print(allValues[i])

                    # Создаем распоряжение
                    division = db.cursor.execute(
                        '''SELECT division_id FROM USER_AUTHORIZATOR WHERE id = '{0}';'''.format(user[0])).fetchone()[0]
                    branch = db.cursor.execute(
                        '''SELECT branch_id FROM CURRENCY_SCOPE WHERE division_id = '{0}';'''.format(division)).fetchone()[0]
                    dateNow = db.cursor.execute(
                        '''SELECT datetime('now')''').fetchone()[0]

                    db.cursor.execute('''INSERT INTO CURRENCY_ORDER (created ,created_by, branch_id) 
                    VALUES ('{0}', '{1}','{2}');'''.format(dateNow, user[0], branch))
                    db.sql.commit()

                    order_no = db.cursor.execute(
                        '''SELECT order_no FROM CURRENCY_ORDER WHERE created = '{0}' AND created_by = '{1}';'''.format(
                            dateNow, user[0])).fetchone()[0]

                    db.cursor.execute('BEGIN TRANSACTION;')
                    # Создаем записи в архиве и выводим курсы валют
                    for i in resValues:
                        db.cursor.execute('''INSERT INTO CURRENCY_COURSES (order_no, currency_no_1, currency_no_2, 
                                        currency_date, scale, amount, created, created_by, branch_id) 
                                        VALUES ({0}, '{1}', '{2}', '{3}', {4}, {5}, '{6}', '{7}', '{8}')
                                        '''.format(order_no, i.vchcode, 'RUB', dateStr, i.scale, i.rate, dateNow,
                                                   user[0], branch))
                    db.cursor.execute('END;')
                    db.sql.commit()

                    # Выводим в консоль курсы валют из таблицы
                    results = []
                    for i in range(len(codes)):
                        tmp = db.cursor.execute(
                            '''SELECT * FROM CURRENCY_COURSES WHERE order_no = {0} AND currency_no_1 = '{1}';'''.format(
                                order_no, codes[i])).fetchone()
                        results.append(tmp)
                    values = db.parseSelectToValue(results)
                    for i in values:
                        print(i)
                # Пользователь-авторизатор в архиве не все записи
                elif user[1] == True and len(checkArcive) != validCodes:
                    # Обновляем записи в распоряжениях
                    order_no = db.cursor.execute(
                        '''SELECT order_no FROM CURRENCY_COURSES WHERE currency_date = '{0}';'''.format(
                            dateStr)).fetchone()[0]
                    dateNow = db.cursor.execute('''SELECT datetime('now')''').fetchone()[0]
                    db.cursor.execute(
                        '''UPDATE CURRENCY_ORDER SET updated = '{0}', updated_by = '{1}' WHERE order_no = {2};'''.format(
                            dateNow, user[0], order_no))
                    db.sql.commit()

                    division = db.cursor.execute('''SELECT division_id FROM USER_AUTHORIZATOR WHERE id = '{0}';'''.format(user[0])).fetchone()[0]
                    branch = db.cursor.execute('''SELECT branch_id FROM CURRENCY_SCOPE WHERE division_id = '{0}';'''.format(division)).fetchone()[0]
                    # Находим уже существующие коды, обновляем ордер и архив
                    codesFromArchive = []
                    for i in codes:
                        tmpFromArchive = db.cursor.execute('''SELECT currency_no_1 FROM CURRENCY_COURSES 
                        WHERE currency_date = '{0}' AND currency_no_1 = '{1}';'''.format(dateStr, i)).fetchone()
                        if tmpFromArchive != None:
                            codesFromArchive.append(tmpFromArchive)

                    # Обновляем архив
                    db.cursor.execute('BEGIN TRANSACTION;')
                    for i in codesFromArchive:
                        db.cursor.execute('''UPDATE CURRENCY_COURSES SET updated = '{0}', updated_by = '{1}' 
                        WHERE order_no = {2} AND currency_date = '{3}' AND currency_no_1 = '{4}';'''.format(dateNow, user[0], order_no, dateStr, i[0]))
                    db.cursor.execute('END;')
                    db.sql.commit()

                    # Находим еще несуществующие курсы
                    codesDontExist = []
                    listCodesExist = []
                    valuesDontExist = []
                    for i in range(len(codesFromArchive)):
                        listCodesExist.append(codesFromArchive[i][0])

                    for i in range(len(codes)):
                        if codes[i] not in listCodesExist:
                            codesDontExist.append(codes[i])
                    for i in range(len(allValues)):
                        if allValues[i].vchcode in codesDontExist:
                            valuesDontExist.append(allValues[i])

                    # Добавляем несуществующие курсы в архив
                    db.cursor.execute('BEGIN TRANSACTION;')
                    for i in valuesDontExist:
                        db.cursor.execute('''INSERT INTO CURRENCY_COURSES (order_no, currency_no_1, currency_no_2, 
                        currency_date, scale, amount, created, created_by, branch_id) 
                        VALUES ({0}, '{1}', '{2}', '{3}', {4}, {5}, '{6}', '{7}', '{8}')
                        '''.format(order_no, i.vchcode, 'RUB', dateStr, i.scale, i.rate, dateNow, user[0], branch))
                    db.cursor.execute('END;')
                    db.sql.commit()

                    # Выводим в консоль курсы валют из таблицы
                    results = []
                    for i in range(len(codes)):
                        tmp = db.cursor.execute(
                            '''SELECT * FROM CURRENCY_COURSES WHERE order_no = {0} AND currency_no_1 = '{1}';'''.format(
                                order_no, codes[i])).fetchone()
                        results.append(tmp)
                    values = db.parseSelectToValue(results)
                    for i in values:
                        print(i)

                # Пользователь НЕавторизатор, в архиве - пусто
                elif user[1] == False and len(checkArcive) == 0:
                    print('Курсы не найдены')
                # Пользователь НЕавторизатор или пользователь-авторизатор показываем все доступные курсы, обновляем данные в распоряжении и архиве
                elif (user[1] == False and len(checkArcive) != 0) or (user[1] == True and len(checkArcive) == validCodes):
                    #Обновляем распоряжение
                    order_no = db.cursor.execute('''SELECT order_no FROM CURRENCY_COURSES WHERE currency_date = '{0}';'''.format(dateStr)).fetchone()[0]
                    dateNow = db.cursor.execute('''SELECT datetime('now')''').fetchone()[0]

                    db.cursor.execute(
                        '''UPDATE CURRENCY_ORDER SET updated = '{0}', updated_by = '{1}' 
                        WHERE order_no = {2};'''.format(dateNow, user[0], order_no))
                    db.sql.commit()

                    #Обновляем архив
                    db.cursor.execute('BEGIN TRANSACTION;')
                    for i in codes:
                        db.cursor.execute('''UPDATE CURRENCY_COURSES SET updated = '{0}', updated_by = '{1}' WHERE currency_no_1 = '{2}';'''.format(dateNow, user[0], i))
                    db.cursor.execute('END;')
                    db.sql.commit()

                    # Выводим в консоль курсы валют из таблицы
                    results = []
                    for i in range(len(codes)):
                        tmp = db.cursor.execute('''SELECT * FROM CURRENCY_COURSES WHERE order_no = {0} AND currency_no_1 = '{1}';'''.format(order_no, codes[i])).fetchone()
                        results.append(tmp)
                    values = db.parseSelectToValue(results)
                    for i in values:
                        print(i)
            else:
                print('Указаны неверные коды валют')
    db.close()