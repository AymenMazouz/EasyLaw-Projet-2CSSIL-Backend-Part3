import multiprocessing
import os
import time
from dotenv import load_dotenv
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from datetime import date as dt
from sqlalchemy import create_engine, Column, Integer, String, Date, Boolean, Text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import ARRAY

import logging
import random

import itertools

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
    f"main_program_logs",
    f"./kita3_scraping_logs/main_program_logs.log",
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


DB_URL = os.getenv("PG_URL")
engine = create_engine(DB_URL)


def scrape_kita3_law_data(kita3, start_date):
    number_of_pages = 0
    i = 0
    j = 0

    while i <= number_of_pages:
        i = 0
        log_line = f"TRY NUMBER {j + 1} FOR {kita3}!!!"
        main_logger.info(log_line)
        print(log_line)

        try:
            page_logger = setup_logger(
                f"page_{i}_{kita3}",
                f"./kita3_scraping_logs/{kita3}/page{i}.log",
            )

            random_duration = random.randint(3, 10)
            # Wait for the random duration
            time.sleep(random_duration)

            options = Options()
            options.add_argument("--disable-gpu")
            options.add_argument("--headless=new")
            driver = webdriver.Chrome(options=options)

            # Open the website
            driver.get("https://www.joradp.dz/HAR/Index.htm")

            random_duration = random.randint(3, 10)
            # Wait for the random duration
            time.sleep(random_duration)

            wait = WebDriverWait(driver, 10)  # Timeout after 10 seconds
            frame = wait.until(EC.presence_of_element_located((By.XPATH, '//frame[@src="ATitre.htm"]')))
            # Switch to the frame with src="ATitre.htm"
            driver.switch_to.frame(frame)

            # Wait for an element on the page to indicate that it's fully loaded
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located(
                    (By.XPATH, "/html/body/div/table[2]/tbody/tr/td[3]/a")
                )
            )

            # Now you can interact with elements inside this frame
            search_link = WebDriverWait(driver, 60).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "/html/body/div/table[2]/tbody/tr/td[3]/a")
                )
            )
            search_link.click()

            # Switch back to the default content before switching to another frame
            driver.switch_to.default_content()
            # Switch to the frame with name="FnCli"
            driver.switch_to.frame(
                driver.find_element(By.XPATH, '//frame[@name="FnCli"]')
            )

            # select category
            select_input = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.NAME, "zsec"))
            )

            select_object = Select(select_input)
            select_object.select_by_visible_text(kita3)

            # Find the input field and enter '01/01/1964'
            date_input = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.NAME, "znjd"))
            )
            date_input.clear()
            date_input.send_keys(start_date.strftime("%d/%m/%Y"))

            # Click on the "بــحـــث" button
            search_button = WebDriverWait(driver, 60).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '//a[contains(@title, "تشغيل البحث")]')
                )
            )
            search_button.click()
            
            def page_ready(driver):
                no_found_elements = driver.find_elements(
                    By.XPATH, '//*[@id="tit"]'
                )
                # No laws found
                if (len(no_found_elements)>0):
                    return True                
                display_settings_link = driver.find_elements(
                    By.XPATH, '/html/body/div/table[1]/tbody/tr/td[1]/a'
                )                
                if (len(display_settings_link)>0):
                    return True
                return False

            WebDriverWait(driver, 180, 2).until(page_ready)
            log_line = f"Page {i} ready"
            page_logger.info(log_line)
            main_logger.info(log_line)
            print(log_line)
            
            no_found_elements = driver.find_elements(
                By.XPATH, '//*[@id="tit"]'
            )
            # No laws found
            if (len(no_found_elements)>0):
                log_line = f"Page {i} of {kita3}: No laws found!"
                page_logger.info(log_line)
                main_logger.info(log_line)
                print(log_line)
                break
            
            display_settings_link_elements = driver.find_elements(
                By.XPATH, '/html/body/div/table[1]/tbody/tr/td[1]/a'
            )                

            display_settings_link = display_settings_link_elements[0]

            display_settings_link.click()

            pages_input = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.NAME, "daff"))
            )
            pages_input.clear()
            pages_input.send_keys("200")

            irsal_link = WebDriverWait(driver, 60).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "/html/body/div/form/table[2]/tbody/tr[1]/td/a")
                )
            )
            irsal_link.click()

            numberOfPages = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="tex"]'))
            )
            number_of_pages_text = numberOfPages.text
            pattern = r"العدد (\d+)"
            match = re.search(pattern, number_of_pages_text)

            number_of_laws = 0
            if match:
                number_of_laws = match.group(1)

            number_of_pages = int(int(number_of_laws) / 200)

            directory = f"./kita3_scraping_logs/{kita3}"
            os.makedirs(directory, exist_ok=True)

            allLawsIds = []

            while i <= number_of_pages:

                allLawsIds.clear()
                matching_rows = driver.find_elements(
                    By.XPATH, '//tr[@bgcolor="#78a7b9"]'
                )
                # Iterate through the matching rows
                page_logger.info(f"Starting kita3 scrape for {kita3}, page {i}")
                row_number = 0
                for row in matching_rows:
                    row_number += 1
                    log_line = f"----------------- \n row: {row_number}\n"
                    page_logger.info(log_line)

                    id_element = row.find_element(By.XPATH, ".//td[1]/a")
                    id_element_href = id_element.get_attribute("href")
                    match = re.search(r"#(\d+)", id_element_href)
                    id_number = match.group(1)
                    allLawsIds.append(int(id_number))

                log_line = (
                    f" ----------------- \n Storing kita3 for allLawsIds in db...\n"
                )
                page_logger.info(log_line)

                storeLawkita3(kita3, allLawsIds, page_logger)

                log_line = f" ----------------- \n Stored the laws in db...\n"
                page_logger.info(log_line)
                print(log_line)

                log_line = f" \n allLawsIds: {allLawsIds}  \n"
                page_logger.info(log_line)
                print(log_line)

                log_line = f" \n Finished scraping page {i} of {kita3} with {len(allLawsIds)} allLawsIds \n"
                page_logger.info(log_line)
                print(log_line)

                if i != number_of_pages:
                    next_page_button = WebDriverWait(driver, 60).until(
                        EC.element_to_be_clickable(
                            (By.XPATH, "//a[@href=\"javascript:Sauter('a',3);\"]")
                        )
                    )
                    next_page_button.click()

                    expected_number = ((i + 1) * 200) + 1

                    def check_page(driver):
                        element_text = driver.find_element(
                            By.XPATH, '//*[@id="tex"]'
                        ).text
                        pattern = r"من (\d+) إلى"
                        match = re.search(pattern, element_text)
                        if match:
                            found_number = int(match.group(1))
                            log_line = f"Found text: '{element_text}'. Extracted number: {found_number}. Expected number: {expected_number}."
                            page_logger.info(log_line)
                            main_logger.info(log_line)
                            print(log_line)

                            return found_number == expected_number
                        else:
                            log_line = f"No match found in text: '{element_text}'"
                            page_logger.info(log_line)
                            main_logger.info(log_line)
                            print(log_line)
                            return False

                    log_line = f"expected_number {expected_number}"
                    page_logger.info(log_line)
                    main_logger.info(log_line)
                    print(log_line)
                    WebDriverWait(driver, 180, 2).until(check_page)
                    log_line = f"Successfully navigated to page {i + 1}"
                    page_logger.info(log_line)
                    main_logger.info(log_line)
                    print(log_line)

                i = i + 1

        except TimeoutException as e:
            log_line = f"TimeoutException: {e} RETRYING..."
            print(log_line)
            main_logger.error(log_line)

        except Exception as e:
            log_line = f"ERROR !!!!: {e} RETRYING..."
            print(log_line)
            main_logger.error(log_line)
        finally:
            if driver is not None:
                driver.quit()
            j += 1

    log_line = f"PROGRAM ENDED AFTER {j + 1} TRIES FOR {kita3}!!!"
    print(log_line)
    main_logger.info(log_line)


def storeLawkita3(kita3, allLawsIds, page_logger):
    for law_id in allLawsIds:
        Session = sessionmaker(bind=engine)
        session = Session()
        try:
            # Check if the law text already exists
            existing_law_text = session.query(LawText).get(law_id)
            if existing_law_text:
                # Update existing record
                existing_law_text.field = kita3
            else:
                log_line = f"Law not found in db: LawId: {law_id}"
                page_logger.error(log_line)
            session.commit()
            log_line = f" ----------------- \n kita3 for laws stored in db successfully and committed...\n"
            page_logger.info(log_line)
        except Exception as e:
            session.rollback()
            log_line = f"Error inserting kita3 for a law: {e}"
            page_logger.error(log_line)
        finally:
            session.close()

if __name__ == "__main__":

    # Create database tables
    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    start_date = session.query(LastScrapingDate).first().kita3


    # Initialize ChromeOptions
    options = Options()
    options.add_argument("--disable-gpu")
    options.add_argument("--headless=new")
    driver = webdriver.Chrome(options=options)

    # Open the website
    driver.get("https://www.joradp.dz/HAR/Index.htm")

    wait = WebDriverWait(driver, 10)  # Timeout after 10 seconds
    frame = wait.until(EC.presence_of_element_located((By.XPATH, '//frame[@src="ATitre.htm"]')))
    # Switch to the frame with src="ATitre.htm"
    driver.switch_to.frame(frame)

    # Wait for an element on the page to indicate that it's fully loaded
    WebDriverWait(driver, 60).until(
        EC.presence_of_element_located(
            (By.XPATH, "/html/body/div/table[2]/tbody/tr/td[3]/a")
        )
    )

    # Now you can interact with elements inside this frame
    search_link = WebDriverWait(driver, 60).until(
        EC.element_to_be_clickable(
            (By.XPATH, "/html/body/div/table[2]/tbody/tr/td[3]/a")
        )
    )
    search_link.click()

    # Switch back to the default content before switching to another frame
    driver.switch_to.default_content()
    # Switch to the frame with name="FnCli"
    driver.switch_to.frame(driver.find_element(By.XPATH, '//frame[@name="FnCli"]'))

    select_input = WebDriverWait(driver, 60).until(
        EC.presence_of_element_located((By.NAME, "zsec"))
    )
    select_object = Select(select_input)

    kita3_types = []
    options = select_object.options
    for option in options:
        kita3_types.append(option.text)
    kita3_types = kita3_types[3:]    
    #kita3_types = ['الأمن العمومي', 'المناجم', 'المالية', 'الإتصال', 'المجاهدين', 'الإصلاح الإداري', 'الأشغال العمومية', 'النقل', 'الإقتصاد', 'الإعلام', 'البناء', 'التهيئة العمرانية', 'البيئة', 'التجهيز', 'التجارة', 'البحث العلمي', 'التخطيط', 'البرلمان', 'التربية والتعليم العالي', 'البريد', 'التضامن', 'التكوين المهني', 'الثقافة', 'الداخلية والجماعات المحلية', 'الدفاع الوطني', 'الري', 'الشؤون الإجتماعية', 'الشؤون الخارجية', 'الشؤون الدينية', 'الصناعة', 'الطاقة', 'الشباب والرياضة', 'السياحة', 'الصحة', 'الصيد', 'السكن', 'العمل', 'الغابات', 'العدل', 'الفلاحة', 'حقوق الإنسان', 'رئاسة الجمهورية', 'رئاسة الحكومة']
    print(kita3_types)
    main_logger.info(f"kita3_types : {kita3_types}")
    driver.quit()

    law_types_iterator = iter(kita3_types)
    with multiprocessing.Pool(processes=3) as pool:
        for result in pool.starmap(
            scrape_kita3_law_data, zip(law_types_iterator, itertools.repeat(start_date))
        ):
            pass
        
    last_scraping_date = session.query(LastScrapingDate).first()
    last_scraping_date.kita3 = dt.today()
    session.commit()
