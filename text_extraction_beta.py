import logging
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Date, Integer, String, create_engine, Boolean, Text
from sqlalchemy.orm import sessionmaker
import os
from datetime import date as dt
from fuzzywuzzy import fuzz
from dotenv import load_dotenv


load_dotenv()


def setup_logger(name, log_file, level=logging.INFO):
    """Function to setup as many loggers as you want"""

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


before_logger = setup_logger(
    f".\pdf_text_extraction_logs_before",
    f".\pdf_text_extraction_logs_before.log",
)
after_logger = setup_logger(
    f".\pdf_text_extraction_logs_after",
    f".\pdf_text_extraction_logs_after.log",
)

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

keywords = ['أمر', 'منشور', 'منشور وزاري مشترك',
            'لائحة', 'مداولة', 'مداولة م-أ-للدولة',
            'مرسوم', 'مرسوم تنفيذي', 'مرسوم تشريعي',
            'مرسوم رئاسي', 'مقرر', 'مقرر وزاري مشترك',
            'إعلان', 'نظام', 'اتفاقية', 'تصريح', 'تقرير',
            'تعليمة', 'تعليمة وزارية مشتركة', 'جدول', 'رأي',
            'قانون', 'قانون عضوي', 'قرار', 'قرار ولائي', 'قرار وزاري مشترك',
            'أوامر', 'مناشير', 'مناشير وزارية مشتركة',
            'لوائح', 'مداولات', 'مداولات م-أ-للدولة',
            'مراسيم', 'مراسيم تنفيذية', 'مراسيم تشريعية',
            'مراسيم رئاسية', 'مقررات', 'مقررات وزارية مشتركة',
            'إعلانات', 'نظم', 'اتفاقيات',
            'تصاريح', 'تقارير', 'تعليمات', 'تعليمات وزارية مشتركة',
            'جداول', 'آراء', 'قوانين', 'قوانين عضوية',
            'قرارات', 'قرارات ولائية', 'قرارات وزارية مشتركة']


def iterate_law_texts():
    try:
        # Query all rows from the LawText table
        year = 2009
        start_date = dt(year, 1, 1)
        end_date = dt(year, 12, 31)

        law_texts = session.query(LawText).filter(
            LawText.journal_date.between(start_date, end_date)).all()

        # Iterate through each LawText object
        for law_text in law_texts:
            # Retrieve the desired fields
            journal_page = law_text.journal_page
            journal_num = law_text.journal_num
            journal_date = law_text.journal_date
            text_number = law_text.text_number
            if text_number != None:
                law_title = f"{law_text.text_type} رقم {law_text.text_number}"
            else:
                law_title = f"{law_text.text_type}"

            # Extract the year from journal_date
            journal_year = journal_date.year

            # Construct the text of the rest of the journal starting from the related page
            directory = f'joradp_pdfs\{journal_year}\{journal_year}_{journal_num}'
            txt_files = [file for file in os.listdir(directory) if (
                file.endswith('.txt') and int(file.split('.')[0]) >= int(journal_page))]

            txt_files = sorted(txt_files, key=lambda x: int(x.split('.')[0]))
            all_next_text = ""
            finalText = ""
            first_page = True
            stop = False

            for text_file in txt_files:
                text_path = f"{directory}\{text_file}"

                # Read the content of the .txt file
                with open(text_path, 'r', encoding='utf-8') as file:

                    # before we append the text, we need to process it, see if we have to stop or not ect...
                    # we can use the trim_before_desired_name function for that
                    if (first_page):
                        all_next_text = trim_before_desired_name(
                            file.read(), law_title, text_number)

                        all_next_text, stop = trim_after_desired_name(
                            all_next_text, first_page)
                        if (stop):
                            finalText += all_next_text
                            break
                    else:
                        all_next_text, stop = trim_after_desired_name(
                            file.read(), first_page)
                        if (stop):
                            finalText += all_next_text
                            break

                first_page = False

            # Print the retrieved data
            before_logger.info(
                f"law_title: {law_title}, Journal Num: {journal_num}, Journal Date: {journal_date}")
            before_logger.info(f"{finalText}")

            # insert the long_text in the related row
            law_text.long_content = finalText
            session.commit()

    except Exception as e:
        print(f"An error occurred: {e}")


def trim_before_desired_name(long_text, desired_name, text_number):
    lines = long_text.split('\n')
    desired_line_index = None

    # Find the line index where the desired name string is present
    for i, line in enumerate(lines):
        words = line.split()
        num_words_to_compare = len(desired_name.split())
        if len(words) >= num_words_to_compare:
            initial_words = ' '.join(words[:num_words_to_compare])

            if fuzz.partial_ratio(desired_name, initial_words) >= 60:
                # if there is an exact match with text number
                if text_number != None:
                    numbers = text_number.split('-')
                    try:
                        if numbers[1] in line:
                            desired_line_index = i
                            break
                    except:
                        desired_line_index = i
                        break

    # Remove lines before the desired line
    if desired_line_index is not None:
        lines = lines[desired_line_index:]
        return '\n'.join(lines)
    else:
        return long_text


def trim_after_desired_name(long_text, firstPage):
    lines = long_text.split('\n')
    desired_line_index = None
    count = 0
    stop = False

    # Find the line index where the desired name string is present
    for i, line in enumerate(lines):
        words = line.split()
        for keyword in keywords:
            num_words_to_compare = len(keyword.split())
            if len(words) >= num_words_to_compare:
                initial_words = ' '.join(words[:num_words_to_compare])

                if fuzz.partial_ratio(keyword, initial_words) >= 90:
                    count += 1
                    if (firstPage) and count == 2:
                        desired_line_index = i
                        stop = True
                        break
                    elif (not firstPage):
                        desired_line_index = i
                        stop = True
                    break

    # Remove lines before the desired line
    if desired_line_index is not None:
        lines = lines[:desired_line_index]
        return ('\n'.join(lines), stop)
    else:
        return (long_text, stop)


if __name__ == "__main__":
    iterate_law_texts()
