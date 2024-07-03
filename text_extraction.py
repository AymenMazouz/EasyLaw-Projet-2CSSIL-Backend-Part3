import logging
from sqlalchemy.orm import declarative_base
from sqlalchemy import (
    and_,
    create_engine,
    Column,
    Integer,
    String,
    Date,
    Boolean,
    Text,
    cast,
)
from sqlalchemy.orm import sessionmaker
import os
from datetime import date as dt
from fuzzywuzzy import fuzz
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
    f"./pdf_text_extraction_logs",
    f"./pdf_text_extraction_logs.log",
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


class Newspaper(Base):
    __tablename__ = "official_newspaper"
    id = Column(String, primary_key=True)
    year = Column(String)
    number = Column(String)
    link = Column(String)


class RecentlyScrapedLaws(Base):
    __tablename__ = "recently_scraped_laws"
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
    page_fixed = Column(Boolean, default=True)


DB_URL = os.getenv("PG_URL")
engine = create_engine(DB_URL)

Session = sessionmaker(bind=engine)
session = Session()

keywords = [
    "أمر",
    "منشور",
    "منشور وزاري مشترك",
    "لائحة",
    "مداولة",
    "مداولة م-أ-للدولة",
    "مرسوم",
    "مرسوم تنفيذي",
    "مرسوم تشريعي",
    "مرسوم رئاسي",
    "مقرر",
    "مقرر وزاري مشترك",
    "إعلان",
    "نظام",
    "اتفاقية",
    "تصريح",
    "تقرير",
    "تعليمة",
    "تعليمة وزارية مشتركة",
    "جدول",
    "رأي",
    "قانون",
    "قانون عضوي",
    "قرار",
    "قرار ولائي",
    "قرار وزاري مشترك",
    "أوامر",
    "مناشير",
    "مناشير وزارية مشتركة",
    "لوائح",
    "مداولات",
    "مداولات م-أ-للدولة",
    "مراسيم",
    "مراسيم تنفيذية",
    "مراسيم تشريعية",
    "مراسيم رئاسية",
    "مقررات",
    "مقررات وزارية مشتركة",
    "إعلانات",
    "نظم",
    "اتفاقيات",
    "تصاريح",
    "تقارير",
    "تعليمات",
    "تعليمات وزارية مشتركة",
    "جداول",
    "آراء",
    "قوانين",
    "قوانين عضوية",
    "قرارات",
    "قرارات ولائية",
    "قرارات وزارية مشتركة",
]


def iterate_law_texts():
    start_date = session.query(LastScrapingDate).first().newspapers_scraper.year

    # for newspapers out after the start date
    for news_paper in (
        session.query(Newspaper)
        .filter(and_(cast(Newspaper.year, Integer) >= start_date))
        .all()
    ):
        try:

            directory = (
                f"joradp_pdfs/{news_paper.year}/{news_paper.year}_{news_paper.number}"
            )

            if not os.path.exists(directory):
                continue

            txt_files = [
                file
                for file in os.listdir(directory)
                if (file.endswith(".txt") and int(file.split(".")[0]) >= 1)
            ]
            txt_files = sorted(txt_files, key=lambda x: int(x.split(".")[0]))

            lawsStartingPage = []
            for law in (
                session.query(LawText)
                .filter(
                    and_(
                        LawText.journal_date >= dt(int(news_paper.year), 1, 1),
                        LawText.journal_date <= dt(int(news_paper.year), 12, 31),
                        LawText.journal_num == int(news_paper.number),
                        LawText.page_fixed == True,
                        LawText.journal_page <= int(txt_files[-1].split(".")[0]),
                    )
                )
                .all()
            ):

                lawsStartingPage.append({"id": law.id, "pages": law.journal_page})
                lawsStartingPage = sorted(lawsStartingPage, key=lambda x: x["pages"])
            lawsPagesRanges = transform_to_page_ranges(lawsStartingPage)
            main_logger.info(f"lawsPagesRanges : {lawsPagesRanges}")

            for object in lawsPagesRanges:
                law = session.query(LawText).filter(LawText.id == object["id"]).first()
                text_number = law.text_number
                if text_number != "":
                    law_title = f"{law.text_type} رقم {law.text_number}"
                else:
                    law_title = f"{law.text_type}"

                long_text = ""

                for page in object["pages"]:
                    # if not last element in the list
                    if object != lawsPagesRanges[-1]:
                        with open(
                            f"{directory}/{page}.txt", "r", encoding="utf-8"
                        ) as f:
                            long_text += f.read()
                    else:
                        for file in txt_files:
                            file_page = int(file.split(".")[0])
                            if int(file_page) >= int(page):
                                with open(
                                    f"{directory}/{file}", "r", encoding="utf-8"
                                ) as f:
                                    long_text += f.read()

                trimed_long_text = trim_before_desired_name(
                    long_text, law_title, text_number
                )

                # main_logger.info(f"title : {law_title}")
                # main_logger.info(f"object : {object}")
                # main_logger.info(f"text : {trimed_long_text}")

                law.long_content = trimed_long_text
                session.commit()

                recently_scraped_law = RecentlyScrapedLaws(
                    id=law.id,
                    text_type=law.text_type,
                    text_number=law.text_number,
                    journal_date=law.journal_date,
                    journal_num=law.journal_num,
                    journal_page=law.journal_page,
                    signature_date=law.signature_date,
                    ministry=law.ministry,
                    content=law.content,
                    field=law.field,
                    long_content=law.long_content,
                    page_fixed=law.page_fixed,
                )
                session.add(recently_scraped_law)
                session.commit()

            last_scraping_date = session.query(LastScrapingDate).first()
            last_scraping_date.text_extraction = dt.today()
            session.commit()

        except Exception as e:
            print(f"An error occurred: {e}")


def transform_to_page_ranges(data):
    page_ranges = []
    for i in range(len(data)):
        law_title = data[i]["id"]
        starting_page = data[i]["pages"]
        if i < len(data) - 1:
            next_starting_page = data[i + 1]["pages"]
            page_range = list(range(starting_page, next_starting_page))
        else:
            page_range = list(range(starting_page, starting_page + 1))
        if not page_range:
            page_range = [starting_page]

        page_ranges.append({"id": law_title, "pages": page_range})
    return page_ranges


def trim_before_desired_name(long_text, desired_name, text_number):
    lines = long_text.split("\n")
    desired_line_index = None

    for i, line in enumerate(lines):
        words = line.split()
        num_words_to_compare = len(desired_name.split())
        if len(words) >= num_words_to_compare:
            initial_words = " ".join(words[:num_words_to_compare])

            if fuzz.ratio(desired_name, initial_words) >= 60:

                if text_number != None:
                    numbers = text_number.split("-")
                    try:
                        if numbers[1] in line:
                            desired_line_index = i
                            break
                    except:
                        desired_line_index = i
                        break
                else:
                    desired_line_index = i
                    break

    if desired_line_index is not None:
        lines = lines[desired_line_index:]
        return "\n".join(lines)
    else:
        return long_text


if __name__ == "__main__":
    Base.metadata.create_all(engine)
    iterate_law_texts()
