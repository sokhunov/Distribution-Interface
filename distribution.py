from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import column_property
import pandas as pd
from sqlalchemy import Column, Integer, String, Date, DateTime, Float, Boolean
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.sql.expression import func
import datetime
from connectors.connection import  load_session, create_rarus_connection

Base = automap_base()


class DistributionGoods(Base):
    """

    Товары поставщиков дистрибуции.
    Сами товары изначально заносятся в 1С. По согласованным ценам можно получить товары и поставщиков.
    Данный запрос получает товары из 1С и дополняет их в БД таблицу DistributionGoods.

    ! DISTRIBTUION_SUPPLIER_BRANDS - хранит информаию 1С кодах поставщика и бренда поставщика. Важно, при появлении новых
    поставщиков дополнять список, иначе товары не попадут в запрос из 1С.

    1. В целях оптимизации запроса из 1С, сначала получим уже существующие 1С кода товаров из БД таблица DistributionGoods
    2. Получим товары, которые принадлежат поставщикам дистрибуции из DISTRIBUTION_SUPPLIERS_BRANDS из 1С
    3. Запишем результат в БД.

    """
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
        'DR002292': 'CHERKIZOVO'
    }

    __tablename__ = 'DistributionGoods'

    __table_args__ = {'extend_existing': True}

    code = Column(String, primary_key=True)  # код 1С товара
    name = Column(String)  # название товара
    supplier_name = Column(Boolean)
    brand = Column(String)  # название бренда
    log_date = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)

    def __call__(self):
        codes = self._get_goods_codes_from_db_()
        added_skus = 0
        res = self.get_brands_rarus_skus(codes)
        if not res.empty:
            added_skus = self._add_goods_to_db(res)

        print(f'Done! Added {added_skus} skus.')

    @hybrid_method
    def _convert_to_rarus_format(cls, codes):
        """
        Конвертация списка кодов ["code1", "code2", "code3" ..] в вид -> "code1", "code2", "code3" ..
        """
        return ','.join([f'"{code}"' for code in codes])

    @hybrid_method
    def _convert_codes_to_rarus_format(cls, codes: list) -> str:
        """
        Конвертация [("code1"), ("code2"), ("code3")..]  в вид -> "code1", "code2", "code3" ...
        """
        return ','.join([f'"{code[0]}"' for code in codes])

    @hybrid_method
    def get_brands_rarus_skus(cls, codes):
        """
        Получим из 1С товары, которые принадлежат постащвикам дистрибуции, но которых еще нет в БД.
        По SupplierCode-у из запроса, мы получим его бренд из DISTRIBTUION_SUPPLIERS_BRANDS

        :param codes: 1С код(а) товаров, которые будут исключены из результата запроса
        :return df {DataFrame}: Данные по 1С коду товара, названию товара,  названиям поставщика из 1С и Бренду
        """
        r_supplier_codes = cls._convert_to_rarus_format(cls.DISTRIBUTION_SUPPLIERS_BRANDS.keys())
        r_codes = cls._convert_codes_to_rarus_format(codes)
        rarus_connector = create_rarus_connection()

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

        rarus_connector = create_rarus_connection()
        query = rarus_connector.NewObject("Query", qry_suppl_skus)
        sel = query.Execute().Choose()  # Get result of RARUS query

        del rarus_connector

        data = []

        while sel.next():
            data.append((sel.Code, sel.Name, sel.Supplier, cls.DISTRIBUTION_SUPPLIERS_BRANDS.get(sel.SupplierCode)))

        # ! Важно не менять название колонок, т.к по ним в дальнейшем будет выгрузка данных в БД.
        # Данне названия - это названия соотвествующих столбцов в таблице DistributionGoods в БД
        df = pd.DataFrame(data, columns=['code', 'name', 'supplier_name', 'brand']).set_index('code')
        df['log_date'] = datetime.datetime.now()
        return df

    @hybrid_method
    def _get_goods_codes_from_db_(cls):
        """
        Запрос получает все 1С кода товаров, которые есть в БД.
        Все кода возращенные из запроса, не будут включаться в дальнейший запрос из 1С get_brands_rarus_skus.
        Поэтому важно возвращать любой 1С код как резульат запроса, если не возвратить, то запрос get_brands_rarus_skus
        "поламается"
        """
        db_session = load_session()
        result = db_session.query(DistributionGoods.code).all()
        db_session.close()
        if not result:
            return [("00000001")]  # Предоплата код

        return result

    @hybrid_method
    def _add_goods_to_db(cls, df):
        """
        Сохранение данных в таблицу Бд.
        :param df: Данные по товарам поставщиков дистрибуции и брендам
        :return:
        """
        db_session = load_session()
        df.to_sql(cls.__tablename__, db_session.connection(), if_exists='append')
        db_session.commit()
        db_session.close()

        return len(df)


class DistributionSales(Base):
    __tablename__ = 'DistributionSales'

    __table_args__ = {'extend_existing': True}

    id_ = column_property(Column('id', Integer, primary_key=True, autoincrement=True))
    date_ = Column(Date)   # дата продаж
    branch = Column(String)  # подраздление компании
    code = Column(String)  # код товара 1С
    name = Column(String)  # название товара
    quantity_sold = Column(Float)
    turnover = Column(Float)
    turnover_wo_vat = Column(Float)
    cogs = Column(Float)  # себестоимость товара
    margin = Column(Float)
    margin_percent = Column(Float)
    client = Column(String)  # название покупателя

    def __init__(self, append=True):
        """
        Если append == True, тогда
            1. У Пользователя запращивается периода анализа.
            2. Данные по данному периоду удаляется из БД
            3. Получаются данные по продажам из 1С по данному периоду
            4. Сохранение в БД данных о продажах
        Если append == False, тогда
            1. Дата начала анализа получаеми максимальную дату БД таблицы.
            2. Если даты, нет то пеориод анализа запращиваются у пользователя
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
            print('Dates is set.')
            self._delete_from_db_table()
            print(f'Data for the period {self.start_date} - {self.end_date} deleted from the table')
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

    @hybrid_method
    def _get_dates_from_user(self):
        """
        У пользователя запращиваются дата начала анализа и дата конца анализа
        Затем идет проверка на корректность пользовательских дат
        :return:
        """
        start_date = input('Please input start date in YYYY-MM-DD format: ')
        end_date = input('Please input end date in YYYY-MM-DD format: ')
        self._validate_dates_(start_date, end_date)  # Проверка корректности дат

        self.start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        self.end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        assert self.start_date < self.end_date, f'User {self.start_date} < {self.end_date}'

    @hybrid_method
    def _validate_dates_(self, start_date, end_date):
        """
        Проверка корректности введенных дат.
        :param start_date: Дата начала анализа
        :param end_date: Дата конца анализа
        :return:
        """
        assert len(start_date) == 10 and '-' in start_date, 'Wrong start date. Input date in YYYY-MM-DD format.'
        assert len(end_date) == 10 and '-' in end_date, 'Wrong end date. Input date in YYYY-MM-DD format.'

        for date_ in [start_date, end_date]:
            year, month, day = date_.strip().split('-')
            assert len(year) == 4 and int(
                year) >= 2016, f'Wrong year format in {date_}. Year should be 4 digits (YYYY) and >= 2016'
            assert len(month) == 2 and int(month) in range(1,
                                                           12), f'Wrong month format in {date_}. Month should be 2 digits (MM) and in range 1-12'
            assert len(day) == 2 and int(day) in range(1,
                                                       31), f'Wrong month format in {date_}. Day should be 2 digits (DD) and in range 1-31'

    @hybrid_method
    def _delete_from_db_table(self):
        """
        Удаление данных о продажах за периода анализа из БД
        :return:
        """
        db_session = load_session()
        db_session.query(DistributionSales).filter(
            DistributionSales.date_.between(self.start_date, self.end_date)).delete(synchronize_session=False)
        db_session.commit()
        db_session.close()

    @hybrid_method
    def _get_max_sales_date_(cls):
        """
        Получить максимальную дату продаж из БД таблицы
        :return max_date: Самая последняя дата в БД таблице
        """
        db_session = load_session()
        max_date = db_session.query(func.max(DistributionSales.date_)).scalar()
        db_session.close()

        return max_date

    @hybrid_method
    def _check_sales_for_period_(self):
        db_session = load_session()
        data = db_session.query(DistributionSales.id).scalar()

        return bool(data)

    @hybrid_method
    def _get_sales_(self):
        """

        По периоду аналиаза получаем данные о продаж из 1С.
        Сначала получим все 1С кода товаров дистрибуции из БД таблицы DistributionGoods и затем отфильтруем продажи
        по полученным 1С кодам.

        :return df {DataFrame}: данные по продажам за периоджд
        """
        da1 = self.start_date.strftime('%Y, %m, %d')
        date_begin = f'ДАТАВРЕМЯ({da1}, 00, 00, 01)'
        da2 = self.end_date.strftime('%Y, %m, %d')
        date_end = f'ДАТАВРЕМЯ({da2}, 23, 59, 05)'

        sku_codes = DistributionGoods._get_goods_codes_from_db_()

        assert len(sku_codes) > 1, 'DistribtuionGoods table is empty ..'

        r_codes = DistributionGoods._convert_codes_to_rarus_format(sku_codes)

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
            РегистрНакопления.Продажи.Обороты({date_begin}, {date_end}, День, Номенклатура.Код В ({r_codes})) 
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

        rarus_connector = create_rarus_connection()

        query = rarus_connector.NewObject("Query", qry_sales)
        sel = query.Execute().Choose()  # Get result of RARUS query

        del rarus_connector
        data = []
        while sel.next():
            data.append((sel.Shop, sel.Date_.date(), sel.Code, sel.Name, sel.Qty, sel.Turnover, sel.Turnover_wo_vat,
                         sel.COGS, sel.Margin, sel.Margin_percent, sel.CustomerCode, sel.Customer))

        df = pd.DataFrame(data,
                          columns=['branch', 'date_', 'code', 'name', 'quantity_sold', 'turnover', 'turnover_wo_vat',
                                   'cogs', 'margin', 'margin_percent', 'client_code', 'client'])
        df.set_index('code', inplace=True)
        df['log_date'] = datetime.datetime.now()

        df_sales = self._set_customers_(df)

        return df_sales

    @hybrid_method
    def _get_customer_(self, x):
        """
        Продажи в 1С могут идти на покупателей:
            1. Частное лицо (код 0000003) = покупатели из Z-отчетов
            2. Покупатели из справочника Контрагентов = покупатели из документов реализации

        Если покупатель в это частное лицо, то для отдела дистрибуции клиентом является самое подразделение, которое
        продала товар дистрибуции.
        Если покупатель не частное лицо и продавцом является ДМ АШАН, то это отдел Б2Б

        :param x:
        :return:
        """
        if x['client_code'] == '00000003':
            return x['branch']
        elif x['branch'] == 'ДМ АШАН' and x['client_code'] != '00000003':
            return 'B2B'
        else:
            return x['client']

    @hybrid_method
    def _set_customers_(self, df):
        df['client'] = df.apply(lambda x: self._get_customer_(x), axis=1)
        df.drop('client_code', axis=1, inplace=True)

        return df

    @hybrid_method
    def save_to_db(self, df):
        db_session = load_session()
        df.to_sql(self.__tablename__, db_session.connection(), if_exists='append')
        db_session.commit()
        db_session.close()


Base.prepare()

if __name__ == '__main__':
    DistributionGoods().__call__()
    sales = DistributionSales(append=True)
    sales()
