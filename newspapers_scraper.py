import scrapy
from scrapy import signals
import requests
from tqdm import tqdm
import os
from sqlalchemy import create_engine, Column, String,Integer, Date
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
import logging
from datetime import date
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


main_logger = setup_logger(
    f"pdf_scraping_logs",
    f"./pdf_scraping_logs.log",
)

Base = declarative_base()

class LastScrapingDate(Base):
    __tablename__ = "last_scraping_date"
    id = Column(Integer, primary_key=True)
    newspapers_scraper = Column(Date)
    laws_metadata_scraper = Column(Date)
    kita3 = Column(Date)
    fix_pages = Column(Date)
    ocr_images = Column(Date)
    pdfs_to_images_conversion_journal_year = Column(Integer)
    pdfs_to_images_conversion_journal_number = Column(Integer)
    text_extraction = Column(Date)
    fix_law_texts = Column(Date)
   

class Newspaper(Base):
    __tablename__ = "official_newspaper"
    id = Column(String, primary_key=True)
    year = Column(String)
    number = Column(String)
    link = Column(String)


DB_URL = os.getenv("PG_URL")
engine = create_engine(DB_URL)

Session = sessionmaker(bind=engine)
session = Session()

def storeOfficialNewspaper(newsPaper):
    try:
        # Check if the law text already exists
        existing_news_paper = session.query(Newspaper).get(newsPaper["id"])
        if existing_news_paper:
            # Update existing record
            existing_news_paper.year = newsPaper["year"]
            existing_news_paper.number = newsPaper["number"]
            existing_news_paper.link = newsPaper["link"]
        else:
            # Insert new record
            new_news_paper = Newspaper(
                id=newsPaper["id"],
                year=newsPaper["year"],
                number=newsPaper["number"],
                link=newsPaper["link"]
            )
            session.add(new_news_paper)
        session.commit()
    except Exception as e:
        session.rollback()
        main_logger.info(f"Error inserting/updating newspaper: {e}")
    finally:
        session.close()


class JoradpSpider(scrapy.Spider):
    data = {}
    name = "joradp"
    start_urls = ["https://www.joradp.dz/HAR/Index.htm"]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(JoradpSpider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed,
                                signal=signals.spider_closed)
        return spider

    def parse(self, response):
        Base.metadata.create_all(engine)
        # Step 2: Extract href attribute from the specified element
        href = "https://www.joradp.dz/JRN/ZA2024.htm"
        if href:

            currentYear = int(href.split("ZA")[1].split(".")[0])
           
            start_date = session.query(LastScrapingDate).first().newspapers_scraper.year

            for year in range(currentYear, start_date - 1 , -1):
                main_logger.info(f"Processing year {year}")

                url = f"https://www.joradp.dz/JRN/ZA{year}.htm"
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Referer": "https://www.joradp.dz/HAR/ATitre.htm",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest": "frame",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "same-origin",
                    "Sec-Fetch-User": "?1",
                    "TE": "trailers",
                }
                yield scrapy.Request(
                    url, headers=headers, callback=self.parse_year, meta={
                        "year": year}
                )

    def parse_year(self, response):
        options = response.css(
            'form[name="zFrm2"] select[name="znjo"] option[value]:not(:empty)'
        )
        year = response.meta["year"]
        year_data = {year: [option.attrib["value"] for option in options]}
        self.data.update(year_data)

    def spider_closed(self, spider):
        base_url = "https://www.joradp.dz/FTP/JO-ARABE/"
        self.data = dict(sorted(self.data.items()))
        with open("pdf_numbers.txt", "w") as f:
            f.write(f"{self.data}\n")

        last_scraping_date = session.query(LastScrapingDate).first()
        last_scraping_date.newspapers_scraper = date.today()
        session.commit()

        for year, numbers in self.data.items():
            max = numbers[0]
            for number in tqdm(numbers, desc=f"Downloading PDFs for {year}"):
                if int(max) > 99:
                    if int(year) == 2021:
                        pdf_url = f"{base_url}{year}/A{year}0{number}.pdf"
                    else:
                        pdf_url = f"{base_url}{year}/A{year}{number}.pdf"
                else:
                    pdf_url = f"{base_url}{year}/A{year}0{number}.pdf"

                response = requests.get(pdf_url, stream=True,verify=False)

                if response.status_code == 200:
                    local_directory = f"joradp_pdfs/{year}"
                    local_file_path = f"{local_directory}/{year}_{int(number)}.pdf"

                    # Create directory if it doesn't exist
                    os.makedirs(local_directory, exist_ok=True)

                    with open(local_file_path, "wb") as pdf_file:
                        for chunk in response.iter_content(chunk_size=128):
                            pdf_file.write(chunk)

                    main_logger.info(f"Downloaded: {year}_{number}.pdf")

                    # inserting into the database
                    newspaper = {
                        "id": f"{year}{int(number)}",
                        "year": f"{year}",
                        "number": f"{int(number)}",
                        "link": f"{pdf_url}"
                    }

                    storeOfficialNewspaper(newsPaper=newspaper)

                    main_logger.info(
                        f"{newspaper} has been inserted in the db")

                else:
                    main_logger.info(
                        f"Failed to download {year}_{number}.pdf. Status Code: {response.status_code}"
                    )
