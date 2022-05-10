import pymysql  # импортируем библиотеку по работе с mysql
import smtplib  # Импортируем библиотеку по работе с SMTP
import os  # библитека для лог файла
import datetime  # библиотека для записи времени в лог файл
from config import mysql_param  # словарь для подключений к mysql
from config import organization_name  # название организации для отправки письма
from config import mail_information  # информация для подключения почты
# Добавляем необходимые подклассы - MIME-типы
from email.mime.multipart import MIMEMultipart  # Многокомпонентный объект
from email.mime.text import MIMEText  # Текст/HTML
from email.mime.image import MIMEImage  # Изображения

if os.path.exists('send_to_mail.log') is None:  # проверяем есть ли лог файл
    log = open('send_to_mail.log', 'w')  # если лог файла нет то создаем
    log.close()  # закрываем лог файл


# функция для отправки почты автору заявки и исполнителю заявки
def send_to_mail(addr_to, subject, number, addr_to_assigned):
    try:
        addr_from = mail_information['addr_from']  # Адресат
        addr_to = addr_to  # Получатель
        password = mail_information['password']  # Пароль

        msg = MIMEMultipart()  # Создаем сообщение
        msg['From'] = addr_from  # Адресат
        msg['To'] = addr_to  # Получатель
        msg['CC'] = addr_to_assigned  # копия письма исполнителю
        msg['Subject'] = f'❗ {organization_name} Helpdesk ❗️ #' + number + ' ' + subject  # Тема сообщения

        body = """Уважаемый клиент!
        Ваша заявка была автоматически закрыта. 
        Причина закрытия: заявка передана на проверку автору более 7 дней назад.
        Для изменения статуса заявки, перейдите по ссылке https://helpdesk.ps-consult.info/issues/""" + number + """
        Это письмо сформировано автоматически. Пожалуйста, не отвечайте на него."""
        msg.attach(MIMEText(body, 'plain'))  # Добавляем в сообщение текст

        server = smtplib.SMTP('smtp.gmail.com', 587)  # Создаем объект SMTP
        # server.set_debuglevel(True)  # Включаем режим отладки - если отчет не нужен, строку можно закомментировать
        server.starttls()  # Начинаем шифрованный обмен по TLS
        server.login(addr_from, password)  # Получаем доступ
        server.send_message(msg)  # Отправляем сообщение
        server.quit()  # Выходим
        return True
    # обрабатываем ошибки
    except smtplib.SMTPAuthenticationError as exc:
        log = open('send_to_mail.log', 'a')  # открываем лог файл
        log.write(
            f'{datetime.datetime.now()} \n  Проверьте логин и пароль в почте \n {exc} \n')  # записываем в лог файл
        log.close()
        return False
    # except socket.gaierror as exc:
    #     log = open('send_to_mail.log', 'a')  # открываем лог файл
    #     log.write(
    #         f'{datetime.datetime.now()} \n  Проверьте адрес smtp сервера \n {exc} \n')  # записываем в лог файл
    #     log.close()
    #     return 'Error'
    except TimeoutError as exc:
        log = open('send_to_mail.log', 'a')  # открываем лог файл
        log.write(
            f'{datetime.datetime.now()} \n  Тайм-аут операции \n {exc} \n')  # записываем в лог файл
        log.close()
        return False
    except ConnectionRefusedError as exc:
        log = open('send_to_mail.log', 'a')  # открываем лог файл
        log.write(
            f'{datetime.datetime.now()} \n  Соединение отклонено проверьте адрес smtp сервера \n {exc} \n')  # записываем в лог файл
        log.close()
        return False
    except:
        log = open('send_to_mail.log', 'a')  # открываем лог файл
        log.write(
            f'{datetime.datetime.now()} \n  Что то пошло не так в функции отправки почты \n')  # записываем в лог файл
        log.close()
        return False


# функция для изменения статуса в БД
def update_mysql(number, id_assigned):
    try:
        con = pymysql.connect(mysql_param['ip_address'], mysql_param['user'],  # подключаемся к mysql
                              mysql_param['password'], mysql_param['db'])
        with con:  # Вывод заявок которые старше 7 дней  и переданы на проверку + email авторов заявки
            cur = con.cursor()
            cur.execute("""UPDATE issues
    SET issues.status_id = 5, issues.closed_on = NOW()
    WHERE issues.id = %s AND issues.status_id = 3 AND 
    issues.updated_on <= DATE_SUB(NOW(), INTERVAL 7 DAY)""", number)
            cur2 = con.cursor()
            cur2.execute('SELECT @id_jurnals := max(journals.id) FROM journals;')  # , number)#,id_assigned)
            max_journals_id = cur2.fetchone()
            max_journals_id = max_journals_id[0] + 1
            cur3 = con.cursor()
            cur3.execute(
                "INSERT INTO journals VALUES ({}, {}, 'Issue',{},'Автоматическое закрытие заявки',NOW(),0)".format(
                    max_journals_id, number, id_assigned))
            return True
    # обрабатываем ошибки
    except pymysql.err.OperationalError as exc:
        log = open('send_to_mail.log', 'a')  # открываем лог файл
        log.write(
            f'{datetime.datetime.now()} \n  Ошибка  при подключение к серверу \n {exc} \n')  # записываем в лог файл
        log.close()
        return False
    except RuntimeError as exc:
        log = open('send_to_mail.log', 'a')  # открываем лог файл
        log.write(
            f'{datetime.datetime.now()} \n  Ошибка  при подключение к серверу \n проверьте логин и пароль \n {exc} \n')  # записываем в лог файл
        log.close()
        return False
    except pymysql.err.InternalError as exc:
        log = open('send_to_mail.log', 'a')  # открываем лог файл
        log.write(
            f'{datetime.datetime.now()} \n  Неизвестная база данных \n {exc} \n')  # записываем в лог файл
        log.close()
        return False
    except pymysql.err.ProgrammingError as exc:
        log = open('send_to_mail.log', 'a')  # открываем лог файл
        log.write(
            f'{datetime.datetime.now()} \n  Неправильный синтаксис \n {exc} \n')  # записываем в лог файл
        log.close()
        return False
    except pymysql.err.IntegrityError as exc:
        log = open('send_to_mail.log', 'a')  # открываем лог файл
        log.write(
            f'{datetime.datetime.now()} \n  Двойная запись в таблице journals.id \n {exc} \n')  # записываем в лог файл
        log.close()
        return False
    except:
        log = open('send_to_mail.log', 'a')  # открываем лог файл
        log.write(
            f'{datetime.datetime.now()} \n  Что то пошло не так в функции update_mysql \n')  # записываем в лог файл
        log.close()
        return False


# подключаемся к БД и создаем наш словарь
try:
    con = pymysql.connect(mysql_param['ip_address'], mysql_param['user'],  # подключаемся к mysql
                          mysql_param['password'], mysql_param['db'])
    zapros_dict = {}  # словарь для наших заявок в формате {номер заявки :[тема, email]}
    with con:  # Вывод заявок которые старше 7 дней  и переданы на проверку + email авторов заявки
        cur = con.cursor()
        cur.execute("""SELECT issues.id AS 'Номер заявки',
    issues.subject AS 'Тема заявки',
    issues.status_id AS 'Статус заявки', 
    issues.updated_on AS 'Дата последнего обновления заявки',
    issues.assigned_to_id AS 'id назначенного исполнителя',
    (select email_addresses.address from  email_addresses WHERE email_addresses.user_id = issues.author_id) AS 'Email Автора',
    (select email_addresses.address from  email_addresses WHERE email_addresses.user_id = issues.assigned_to_id) AS 'Email Исполнителя' 
    FROM issues
    WHERE issues.status_id = 3 AND issues.updated_on <= DATE_SUB(NOW(), INTERVAL 7 DAY)""")

        rows = cur.fetchall()

        print('Готовим список заявок которые страше 7 дней')
        for row in rows:  # заполняем наш словарь ключ - номер заявки,[тема обращения, email автора, id исполнителя,
            # email исполнителя]
            cur_dict = {row[0]: [row[1], row[5], row[4], row[6]]}
            zapros_dict.update(cur_dict)
        print('Будут обработаны заявки: ' + str(zapros_dict.keys()))

    # Цикл для отправки почты
    for key in zapros_dict:
        addr_to = list(zapros_dict[key])[1]  # переменная email автора
        addr_to_assigned = list(zapros_dict[key])[3]  # переменная email исполнителя
        subject = list(zapros_dict[key])[0]  # тема обращения
        number = str(key)  # номер заявки
        id_assigned = list(zapros_dict[key])[2]  # id исполнителя
        status = send_to_mail(addr_to, subject, number, addr_to_assigned)  # переменная для статуса отправки почты
        if status == False:  # Если вернулись False то ошибка то прерываем цикл
            break
        elif status == True:
            print(
                f'{datetime.datetime.now()} \n Сообщение успешно отправлено {addr_to} и {addr_to_assigned} номер заявки {number}  \n ')
            log = open('send_to_mail.log', 'a')  # открываем лог файл
            log.write(
                f'{datetime.datetime.now()} \n  Сообщение успешно отправлено {addr_to} и {addr_to_assigned} номер заявки {number}  \n ')  # записываем в лог файл
            log.close()
            number = int(number)
            update_mysql_status = update_mysql(number, id_assigned)  # переменная для статуса
            if update_mysql_status == False:  # Если вернулись False то ошибка то прерываем цикл
                break
            elif status == True:  # инчае записываем в файл что все успешно
                print(f'{datetime.datetime.now()} \n  изменен статус у заявки {number} на "Принято заказчиком"  \n ')
                log = open('send_to_mail.log', 'a')  # открываем лог файл
                log.write(
                    f'{datetime.datetime.now()} \n  изменен статус у заявки {number} на "Принято заказчиком"  \n ')  # записываем в лог файл
                log.close()
            # вызываем функцию по внесению изменений в бд
# обрабатываем ошибки при подключение к mysql формируем данные для подготовки словаря
except pymysql.err.OperationalError as exc:
    log = open('send_to_mail.log', 'a')  # открываем лог файл
    log.write(f'{datetime.datetime.now()} \n  Ошибка  при подключение к серверу \n {exc} \n')  # записываем в лог файл
    log.close()
except RuntimeError as exc:
    log = open('send_to_mail.log', 'a')  # открываем лог файл
    log.write(
        f'{datetime.datetime.now()} \n  Ошибка  при подключение к серверу \n проверьте логин и пароль \n {exc} \n')  # записываем в лог файл
    log.close()
except pymysql.err.InternalError as exc:
    log = open('send_to_mail.log', 'a')  # открываем лог файл
    log.write(
        f'{datetime.datetime.now()} \n  Неизвестная база данных \n {exc} \n')  # записываем в лог файл
    log.close()
except:
    log = open('send_to_mail.log', 'a')  # открываем лог файл
    log.write(
        f'{datetime.datetime.now()} \n  Что то пошло не так в при сборке данных \n')  # записываем в лог файл
    log.close()
# :TODO сделать транзакции
