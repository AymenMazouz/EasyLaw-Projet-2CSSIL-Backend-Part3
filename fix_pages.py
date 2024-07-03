import scrapy
from scrapy import signals
from datetime import date as dt
from datetime import datetime
from sqlalchemy import and_, create_engine, Column, Integer, String, Date, Boolean, Text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from bs4 import BeautifulSoup
import logging
import os
from dotenv import load_dotenv

load_dotenv()

def setup_logger(name, log_file, level=logging.INFO):
    """Function to setup as many loggers as you want"""
    
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s : \n %(message)s \n"
    )
    handler = logging.FileHandler(
        log_file, encoding="utf-8", mode="w"
    )  # use 'a' if you want to keep history or 'w' if you want to override file content
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    # Prevent the logger from propagating messages to the root logger
    logger.propagate = False

    return logger


Base = declarative_base()


class LawText(Base):
    __tablename__ = "laws"
    id = Column(Integer, primary_key=True, autoincrement=False)
    text_type = Column(String)
    text_number = Column(String)
    journal_date = Column(Date)
    journal_num = Column(Integer)
    journal_page = Column(Integer)
    signature_date = Column(Date)
    ministry = Column(String)
    content = Column(String)
    field = Column(String, default="")
    long_content = Column(Text, default="")
    page_fixed = Column(Boolean, default=False)

 
DB_URL = os.getenv("PG_URL")
engine = create_engine(DB_URL)

Session = sessionmaker(bind=engine)
session = Session()


class JoradpSpider(scrapy.Spider):
    main_logger = setup_logger(
        f"pdf_page_fixing_logs",
        f"./pdf_page_fixing_logs.log",
    )
    data = {}
    name = 'joradp'
    currentYear = 0
    start_urls = ['https://www.joradp.dz/HAR/Index.htm']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(JoradpSpider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed,
                                signal=signals.spider_closed)
        return spider

    def parse(self, response):

        start_date_str = input("Enter the last scraping year : ")
        start_date = int(start_date_str)

        Base.metadata.create_all(engine)

        href = "https://www.joradp.dz/JRN/ZA2024.htm"
        if href:

            self.currentYear = int(href.split('ZA')[1].split('.')[0])

            # CHANGE HERE FOR DATE
            for year in range(self.currentYear, start_date - 1, -1):
                self.main_logger.info(f"Processing year {year}")
                url = f'https://www.joradp.dz/JRN/ZA{year}.htm'
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Referer': 'https://www.joradp.dz/HAR/ATitre.htm',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'frame',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'same-origin',
                    'Sec-Fetch-User': '?1',
                    'TE': 'trailers',
                }
                yield scrapy.Request(url, headers=headers, callback=self.parse_year, meta={'year': year})

    def parse_year(self, response):
        options = response.css(
            'form[name="zFrm2"] select[name="znjo"] option[value]:not(:empty)')
        year = response.meta['year']
        year_data = {year: [option.attrib['value'] for option in options]}
        self.data.update(year_data)

        if len(self.data) == (self.currentYear - 1963):
            yield scrapy.Request(url="https://www.joradp.dz", callback=self.process_laws)

    def process_laws(self, response):
        self.data = dict(sorted(self.data.items()))

        for year, numbers in self.data.items():
            if year < 2000:
                for number in numbers:
                    # fixing the laws for every newspaper
                    if 1961 < year <= 1983:
                        Lien = "Jo6283"
                    elif 1983 < year:
                        Lien = "Jo8499"

                    if int(number) < 10:
                        processed_number = f"00{int(number)}"
                    elif 10 <= int(number) < 100:
                        processed_number = f"0{int(number)}"
                    else:
                        processed_number = f"{int(number)}"

                    base_url = f"https://www.joradp.dz/{Lien}/{year}/{processed_number}/A_Pag1.htm"

                    yield scrapy.Request(base_url, callback=self.parse_law_text, meta={'year': year, 'number': processed_number})
            else:
                for number in numbers:
                    start_date = dt(int(year), 1, 1)
                    end_date = dt(int(year), 12, 31)

                    rowsToCorrect = session.query(LawText).filter(
                        and_(
                            LawText.journal_date >= start_date,
                            LawText.journal_date <= end_date,
                            LawText.journal_num == int(number)
                        )
                    ).all()
                    if (rowsToCorrect):
                        for row in rowsToCorrect:
                            row.page_fixed = True
                        session.commit()
                    self.main_logger.info(
                        f"Fixed journal number {number} for the year {year}")

    def parse_law_text(self, response):
        soup = BeautifulSoup(response.body, 'html.parser')
        table_rows = soup.find_all('tr')
        incorrect_numbers = [row.text.strip() for row in table_rows]

        incorrect_numbers = incorrect_numbers[1:]
        if (incorrect_numbers[0] != '1'):
            # here we performe our correction in the db
            correctPage = 1

            start_date = dt(int(response.meta['year']), 1, 1)
            end_date = dt(int(response.meta['year']), 12, 31)

            for incorrect_number in incorrect_numbers:
                rowsToCorrect = session.query(LawText).filter(
                    and_(
                        LawText.journal_date >= start_date,
                        LawText.journal_date <= end_date,
                        LawText.journal_page == incorrect_number,
                        LawText.journal_num == int(response.meta['number'])
                    )
                ).all()

                if (rowsToCorrect):
                    for row in rowsToCorrect:
                        row.journal_page = correctPage
                        row.page_fixed = True
                    session.commit()
                correctPage += 1

            self.main_logger.info(
                f"Fixed journal number {response.meta['number']} for the year {response.meta['year']}")
        else:
            start_date = dt(int(response.meta['year']), 1, 1)
            end_date = dt(int(response.meta['year']), 12, 31)

            rowsToCorrect = session.query(LawText).filter(
                and_(
                    LawText.journal_date >= start_date,
                    LawText.journal_date <= end_date,
                    LawText.journal_num == int(response.meta['number'])
                )
            ).all()
            if (rowsToCorrect):
                for row in rowsToCorrect:
                    row.page_fixed = True
                session.commit()
            self.main_logger.info(
                f"Fixed journal number {response.meta['number']} for the year {response.meta['year']}")

    def spider_closed(self, spider):
        pass
