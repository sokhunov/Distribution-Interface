import pandas as pd
import datetime
from connectors.connection import create_rarus_connection, create_sql02_connection
import logging

logging.basicConfig(filename=f"./logs/log_{datetime.date.today().strftime('%d-%m-%Y')}",
                    format='%(asctime)s: %(message)s', level=logging.DEBUG)


class DistributionGoods:
    """

    Товары поставщиков дистрибуции.
    Сами товары изначально заносятся в 1С. По согласованным ценам можно получить товары и поставщиков.
    Данный класс получает товары из 1С и дополняет их в БД таблицу DistributionGoods.

    ! DISTRIBTUION_SUPPLIER_BRANDS - хранит информаию 1С кодах поставщика и бренда поставщика. Важно, при появлении
    новых поставщиков дополнять список, иначе товары новых поставщиков не попадут в запрос из 1С.

    1. В целях оптимизации запроса из 1С, сначала получим уже существующие 1С кода товаров из БД таблица DistributionGoods
    2. Получим товары, которые принадлежат поставщикам дистрибуции из DISTRIBUTION_SUPPLIERS_BRANDS из 1С и исключим
    товары полученные из шага 1.
    3. Запишем результат в БД.

    """
    DB_TABLE_NAME = 'ANALITYCS.dbo.DistributionGoods'
    # Список поставщиков дистрибуции и их бренды
    DISTRIBUTION_SUPPLIERS_BRANDS = {
        'DR002218': 'ROZMETOV',
        'M1000015': 'DANONE',
        'DR001246': 'MIRATORG',
        'DR001365': 'MIRATORG',
        'DR001991': 'JAHIDA',
        'DR002128': 'JAHIDA',
        'DR002284': 'VALIO',
        'DR002220': 'ULTRAFISH',
        'DR001488': 'POLAR',
        'DR001643': 'POLAR',
        'DR002292': 'CHERKIZOVO',
        'DR002219': 'SVITLOGORIE'
    }

    def __call__(self):
        added_skus = 0
        res = self.get_brands_skus_from_rarus()
        if not res.empty:
            added_skus = self._add_goods_to_db(res)

        logging.info('**** ADDED SKUS: ', added_skus)

    @staticmethod
    def convert_to_string(codes) -> str:
        """
        Конвертация списка кодов ["code1", "code2", "code3" ..] в вид -> "code1", "code2", "code3" ..
        """
        logging.info(f'Converting list of codes to rarus format..')
        return ','.join([f'"{code}"' for code in codes])

    @staticmethod
    def convert_db_codes_to_string(codes: list) -> str:
        """
        Конвертация [("code1", ), ("code2", ), ("code3", )..]  в вид -> "code1", "code2", "code3" ...
        """
        logging.info(f'Converting list of db codes to rarus format')
        return ','.join([f'"{code[0]}"' for code in codes])

    @classmethod
    def get_goods_codes_from_db(cls):
        """
        Запрос получает все 1С кода товаров, которые есть в БД.
        Все кода возращенные из запроса, не будут включаться в дальнейший запрос из 1С get_brands_rarus_skus.
        Поэтому важно возвращать любой 1С код как резульат запроса, если не возвратить, то запрос get_brands_rarus_skus
        "поломается"

        :return result -> [()]: Return list of tuples
        """
        logging.info('Getting goods code from db..')
        logging.info('loading session..')
        db_session = create_sql02_connection()
        logging.info('Executing query..')
        result = db_session.execute(f"Select code from {cls.DB_TABLE_NAME}").fetchall()
        db_session.close()

        return result

    @classmethod
    def get_db_goods_codes_in_rarus_format(cls) -> str:
        """
        Функция возвращает коды товаров в понятный для фильтра формат.
        Данный формат понятен для РАРУС-а и для sql
        :return string of codes: "code1", "code2" .. "codeN"
        """
        codes_str = ''
        db_goods_codes = cls.get_goods_codes_from_db()
        if db_goods_codes:
            codes_str = cls.convert_db_codes_to_string(db_goods_codes)

        return codes_str

    def get_brands_skus_from_rarus(self):
        """
        Получим из 1С товары, которые принадлежат постащвикам дистрибуции, но которых еще нет в БД.
        Установим бренд из DISTRIBUTION_SUPPLIERS_BRANDS по SupplierCode-у из результата запроса
        :return df {DataFrame}: Данные по 1С коду товара, названию товара,  названиям поставщика из 1С и Бренду
        """
        logging.info('Getting brands suppliers codes')
        r_supplier_codes = self.convert_to_string(self.DISTRIBUTION_SUPPLIERS_BRANDS.keys())
        r_codes = self.get_db_goods_codes_in_rarus_format()
        if not r_codes:
            logging.warning('Exclude codes is empty. Get all brands SKUs')
            r_codes = "00000001"  # Предоплата код

        qry_suppl_skus = f"""
            ВЫБРАТЬ
                СогласованиеЦенСрезПоследних.Номенклатура.Код КАК Code,
                СогласованиеЦенСрезПоследних.Номенклатура.Наименование КАК Name,
                СогласованиеЦенСрезПоследних.Контрагент.Код КАК SupplierCode,
                СогласованиеЦенСрезПоследних.Контрагент.Наименование КАК Supplier
            ИЗ
                РегистрСведений.СогласованиеЦен.СрезПоследних КАК СогласованиеЦенСрезПоследних
                    ВНУТРЕННЕЕ СОЕДИНЕНИЕ (ВЫБРАТЬ
                        МАКСИМУМ(СогласованиеЦенСрезПоследних.Период) КАК Период,
                        СогласованиеЦенСрезПоследних.Номенклатура КАК Номенклатура
                    ИЗ
                        РегистрСведений.СогласованиеЦен.СрезПоследних КАК СогласованиеЦенСрезПоследних
                    ГДЕ
                        НЕ СогласованиеЦенСрезПоследних.Номенклатура.Код В ({r_codes})
                        И  СогласованиеЦенСрезПоследних.Контрагент.Код В({r_supplier_codes})

                    СГРУППИРОВАТЬ ПО
                        СогласованиеЦенСрезПоследних.Номенклатура) КАК МаксДаты
                    ПО СогласованиеЦенСрезПоследних.Номенклатура = МаксДаты.Номенклатура
                        И СогласованиеЦенСрезПоследних.Период = МаксДаты.Период
            ГДЕ
                НЕ СогласованиеЦенСрезПоследних.Номенклатура.Код В ({r_codes})
                И СогласованиеЦенСрезПоследних.Контрагент.Код В({r_supplier_codes})

            СГРУППИРОВАТЬ ПО
                СогласованиеЦенСрезПоследних.Номенклатура.Наименование,
                СогласованиеЦенСрезПоследних.Номенклатура.Код,
                СогласованиеЦенСрезПоследних.Контрагент.Код,
                СогласованиеЦенСрезПоследних.Контрагент.Наименование
            """

        logging.info('Getting rarus connector')
        rarus_connector = create_rarus_connection()
        logging.info('Getting data from RARUS')
        query = rarus_connector.NewObject("Query", qry_suppl_skus)
        sel = query.Execute().Choose()  # Get result of RARUS query

        del rarus_connector

        data = []
        while sel.next():
            data.append((sel.Code, sel.Name, sel.Supplier, self.DISTRIBUTION_SUPPLIERS_BRANDS.get(sel.SupplierCode)))

        # ! Важно не менять название колонок, т.к по ним в дальнейшем будет выгрузка данных в БД.
        # Данные названия - это названия соотвествующих столбцов в таблице DistributionGoods в БД
        logging.info('Converting RARUS data to pd.Dataframe')
        df = pd.DataFrame(data, columns=['code', 'name', 'supplier_name', 'brand']).set_index('code')
        df['log_date'] = datetime.datetime.now()
        logging.info('Finished. Returning rarus brands skus')
        return df

    @classmethod
    def _add_goods_to_db(cls, df):
        """
        Сохранение данных в таблицу БД.
        :param df: Данные по товарам поставщиков дистрибуции и брендам
        :return: Number of added goods to DB table
        """
        logging.info('Adding goods to db table')
        db_session = create_sql02_connection()
        df.to_sql(cls.DB_TABLE_NAME, db_session.cursor(), if_exists='append')
        db_session.commit()
        db_session.close()

        logging.info(f'Finished. Added {len(df)} goods')
        return len(df)


class DistributionSales:
    DB_TABLE_NAME = 'Analitycs.dbo.DistributionSales'

    def __init__(self, append=True):
        """
        Если append == False, тогда
            1. У Пользователя запращивается периода анализа.
            2. Данные по данному периоду удаляется из БД
            3. Получаются данные по продажам из 1С по данному периоду
            4. Сохранение в БД данных о продажах
        Если append == True, тогда
            1. Дата начала анализа получаеми максимальную дату БД таблицы.
            2. Если даты нет, то пеориод анализа запращиваются у пользователя
            2.1 Если дата есть, то дата начала анализа = максимальная дата + 1 день,
                дата конца анализа = текущая дата - 2 дня
            3. Получаются данные по продажам из 1С по данному периоду
            4. Сохранение в БД данных о продажах

        :param append: Дополнять продажи или получить продажи за определенный период
        """
        self.append = append

    def __call__(self):
        if not self.append:
            self._get_dates_from_user()
            self._delete_from_db_table()
        else:
            max_date_from_db = self._get_max_sales_date_()
            if not max_date_from_db:
                self._get_dates_from_user()
            else:
                self.start_date = max_date_from_db + datetime.timedelta(days=1)
                self.end_date = datetime.date.today() - datetime.timedelta(days=2)

                if self.start_date > self.end_date:
                    print(f'{self.start_date} > {self.end_date}')
                    return

        df = self._get_sales_()
        self.save_to_db(df)
        print('Done!..')

    def _get_dates_from_user(self):
        """
        У пользователя запращиваются дата начала анализа и дата конца анализа
        Затем идет проверка на корректность пользовательских дат
        :return:
        """
        logging.info('Getting dates from the user')
        start_date = input('Please input start date in YYYY-MM-DD format: ')
        end_date = input('Please input end date in YYYY-MM-DD format: ')
        assert len(start_date) == 10 and '-' in start_date, 'Wrong start date. Input date in YYYY-MM-DD format.'
        assert len(end_date) == 10 and '-' in end_date, 'Wrong end date. Input date in YYYY-MM-DD format.'

        try:
            self.start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
            self.end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        except Exception as err:
            logging.error(err)
            raise err

    def _delete_from_db_table(self):
        """
        Удаление данных о продажах за периода анализа из БД
        :return:
        """
        logging.info(f'Delete sales data for the period {self.start_date} - {self.end_date}')
        db_session = create_sql02_connection()
        qry = f"Delete from {self.DB_TABLE_NAME} where date_ between '{self.start_date}' and '{self.end_date}'"
        db_session.execute(qry)
        db_session.commit()
        db_session.close()
        logging.info('Finished!')

    def _get_max_sales_date_(self):
        """
        Получить максимальную дату продаж из БД таблицы
        :return max_date: Самая последняя дата в БД таблице
        """
        logging.info('Getting last date from sales table')
        db_session = create_sql02_connection()
        res = db_session.execute(f"select max(date_) from {self.DB_TABLE_NAME}").fetchone()
        db_session.close()

        logging.info(f'Max sales date in the sales table {res}')
        return res

    def _check_sales_data_(self):
        """
        Проверим есть ли вообще данные в таблице продаж
        :return bool: True если есть иначе False
        """
        logging.info('Checking if table contains any sales data ')
        db_session = create_sql02_connection()
        res = db_session.execute(f"select max(id) from {self.DB_TABLE_NAME}").fetchone()
        db_session.close()

        logging.info(f'Finished. Result: {bool(res)}')
        return bool(res)

    def _get_sales_(self):
        """

        По периоду аналиаза получаем данные о продаж из 1С.
        Сначала получим все 1С кода товаров дистрибуции из БД таблицы DistributionGoods и затем отфильтруем продажи
        по полученным 1С кодам.

        :return df {DataFrame}: данные по продажам за периоджд
        """
        logging.info(f'Getting sales from RARUS for the period {self.start_date} - {self.end_date}')
        da1 = self.start_date.strftime('%Y, %m, %d')
        date_begin = f'ДАТАВРЕМЯ({da1}, 00, 00, 01)'
        da2 = self.end_date.strftime('%Y, %m, %d')
        date_end = f'ДАТАВРЕМЯ({da2}, 23, 59, 05)'

        r_sku_codes = DistributionGoods.get_db_goods_codes_in_rarus_format()

        try:
            assert r_sku_codes, 'Goods db table is empty'
        except AssertionError as err:
            logging.error('Goods db table is empty')
            raise err

        qry_sales = f"""
        ВЫБРАТЬ
            ПродажиОбороты.ПодразделениеКомпании.Наименование КАК Shop,
            НАЧАЛОПЕРИОДА(ПродажиОбороты.Период, ДЕНЬ) КАК Date_,
            ПродажиОбороты.Номенклатура.Код КАК Code,
            ПродажиОбороты.Номенклатура.Наименование КАК Name,
            СУММА(ПродажиОбороты.КоличествоОборот) КАК Qty,
            СУММА(ПродажиОбороты.СуммаОборот) КАК Turnover,
            СУММА(ПродажиОбороты.СуммаОборот - ПродажиОбороты.СуммаНДСОборот) КАК Turnover_wo_vat,
            СУММА(ПродажиОбороты.СебестоимостьУпрОборот - ПродажиОбороты.СуммаНДСВходящийОборот) КАК COGS,
            ВЫРАЗИТЬ(ВЫБОР
                    КОГДА СУММА(ПродажиОбороты.СуммаОборот) - СУММА(ПродажиОбороты.СуммаНДСОборот) = 0
                        ТОГДА СУММА(0)
                    ИНАЧЕ СУММА(ПродажиОбороты.СуммаОборот) - СУММА(ПродажиОбороты.СуммаНДСОборот) - 
                    (СУММА(ПродажиОбороты.СебестоимостьУпрОборот) - СУММА(ПродажиОбороты.СуммаНДСВходящийОборот))
                КОНЕЦ КАК ЧИСЛО(10, 2)) КАК Margin,
            ВЫРАЗИТЬ(ВЫБОР
                    КОГДА СУММА(ПродажиОбороты.СуммаОборот) - СУММА(ПродажиОбороты.СуммаНДСОборот) = 0
                        ТОГДА СУММА(0)
                    ИНАЧЕ (СУММА(ПродажиОбороты.СуммаОборот) - СУММА(ПродажиОбороты.СуммаНДСОборот) - 
                    (СУММА(ПродажиОбороты.СебестоимостьУпрОборот) - СУММА(ПродажиОбороты.СуммаНДСВходящийОборот))) / 
                    (СУММА(ПродажиОбороты.СуммаОборот) - СУММА(ПродажиОбороты.СуммаНДСОборот)) * 100
                КОНЕЦ КАК ЧИСЛО(10, 2)) КАК Margin_percent,
            ПродажиОбороты.Покупатель.Наименование КАК Customer,
            ПродажиОбороты.Покупатель.Код КАК CustomerCode

        ИЗ
            РегистрНакопления.Продажи.Обороты({date_begin}, {date_end}, День, Номенклатура.Код В ({r_sku_codes})) 
            КАК ПродажиОбороты

        СГРУППИРОВАТЬ ПО
            ПродажиОбороты.Номенклатура.Код,
            ПродажиОбороты.Номенклатура.Наименование,
            ПродажиОбороты.Период,
            ПродажиОбороты.Покупатель.Наименование,
            НАЧАЛОПЕРИОДА(ПродажиОбороты.Период, ДЕНЬ),
            ПродажиОбороты.ПодразделениеКомпании.Наименование,
            ПродажиОбороты.Покупатель.Код

        УПОРЯДОЧИТЬ ПО
            Date_
        """

        logging.info('Creating connection to RARUS')
        rarus_connector = create_rarus_connection()

        logging.info('Quering data from RARUS')
        query = rarus_connector.NewObject("Query", qry_sales)
        sel = query.Execute().Choose()  # Get result of RARUS query

        del rarus_connector
        data = []
        while sel.next():
            data.append((sel.Shop, sel.Date_.date(), sel.Code, sel.Name, sel.Qty, sel.Turnover, sel.Turnover_wo_vat,
                         sel.COGS, sel.Margin, sel.Margin_percent, sel.CustomerCode, sel.Customer))

        logging.info('Formating RARUS sales to pd.Dataframe')
        df = pd.DataFrame(data,
                          columns=['branch', 'date_', 'code', 'name', 'quantity_sold', 'turnover', 'turnover_wo_vat',
                                   'cogs', 'margin', 'margin_percent', 'client_code', 'client'])
        df.set_index('code', inplace=True)
        df['log_date'] = datetime.datetime.now()
        df_sales = self._set_customers_(df)

        return df_sales

    @staticmethod
    def _get_customer_(x):
        """
        Продажи в 1С могут идти на покупателей:
            1. Частное лицо (код 0000003) = покупатели из Z-отчетов
            2. Покупатели из справочника Контрагентов = покупатели из документов реализации

        Если покупатель в это частное лицо, то для отдела дистрибуции клиентом является самое подразделение, которое
        продала товар дистрибуции.
        Если покупатель не частное лицо и продавцом является ДМ АШАН, то это отдел Б2Б

        :param x: pd.Series. Строка из pd.Dataframe
        :return:
        """
        if x['client_code'] == '00000003':  # частное лицо
            return x['branch']
        elif x['branch'] == 'ДМ АШАН' and x['client_code'] != '00000003':
            return 'B2B'
        else:
            return x['client']

    def _set_customers_(self, df):
        """
        Установим правильных покупателей
        :param df -> pd.Dataframe: Данные по продажам
        :return:
        """
        logging.info('Setting customers..')
        df['client'] = df.apply(lambda x: self._get_customer_(x), axis=1)
        df.drop('client_code', axis=1, inplace=True)

        logging.info('Finished!')
        return df

    def save_to_db(self, df):
        logging.info('Saving data to database..')
        db_session = create_sql02_connection()
        df.to_sql(self.DB_TABLE_NAME, db_session.cursor(), if_exists='append')
        db_session.commit()
        db_session.close()
        logging.info('Finished!')


if __name__ == '__main__':
    DistributionGoods().__call__()
    sales = DistributionSales(append=True)
    sales()
