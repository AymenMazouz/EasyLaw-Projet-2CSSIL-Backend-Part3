import multiprocessing
import os
import time
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
from dotenv import load_dotenv
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
    f"./pages_scraping_logs/main_program_logs.log",
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


class Association(Base):
    __tablename__ = "laws_associations"
    id_out = Column(Integer, primary_key=True)
    assoc_nom = Column(String, primary_key=True)
    ids_in = Column(ARRAY(Integer))


DB_URL = os.getenv("PG_URL")
engine = create_engine(DB_URL)

arabic_months = {
    "يناير": "01",
    "فبراير": "02",
    "مارس": "03",
    "أبريل": "04",
    "مايو": "05",
    "يونيو": "06",
    "يوليو": "07",
    "غشت": "08",
    "سبتمبر": "09",
    "أكتوبر": "10",
    "نوفمبر": "11",
    "ديسمبر": "12",
}


def scrape_law_data(law_type, start_date):
    number_of_pages = 0
    i = 0
    j = 0
    lawTexts = []
    allAssoc = []

    while i <= number_of_pages:
        i = 0
        log_line = f"TRY NUMBER {j + 1} FOR {law_type}!!!"
        main_logger.info(log_line)
        print(log_line)

        try:
            page_logger = setup_logger(
                f"page_{i}_{law_type}",
                f"./pages_scraping_logs/{law_type}/page{i}.log",
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

            # Switch to the frame with src="ATitre.htm"
            driver.switch_to.frame(
                driver.find_element(By.XPATH, '//frame[@src="ATitre.htm"]')
            )

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
                EC.presence_of_element_located((By.NAME, "znat"))
            )

            select_object = Select(select_input)
            select_object.select_by_visible_text(law_type)

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
                no_found_elements = driver.find_elements(By.XPATH, '//*[@id="tit"]')
                # No laws found
                if len(no_found_elements) > 0:
                    return True
                display_settings_link = driver.find_elements(
                    By.XPATH, "/html/body/div/table[1]/tbody/tr/td[1]/a"
                )
                if len(display_settings_link) > 0:
                    return True
                return False

            WebDriverWait(driver, 180, 2).until(page_ready)
            log_line = f"Page {i} of {law_type} ready"
            page_logger.info(log_line)
            main_logger.info(log_line)
            print(log_line)

            no_found_elements = driver.find_elements(By.XPATH, '//*[@id="tit"]')
            # No laws found
            if len(no_found_elements) > 0:
                log_line = f"Page {i} of {law_type}: No laws found!"
                page_logger.info(log_line)
                main_logger.info(log_line)
                print(log_line)
                break

            display_settings_link_elements = driver.find_elements(
                By.XPATH, "/html/body/div/table[1]/tbody/tr/td[1]/a"
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

            directory = f"./pages_scraping_logs/{law_type}"
            os.makedirs(directory, exist_ok=True)

            while i <= number_of_pages:
                lawTexts.clear()
                allAssoc.clear()
                matching_rows = driver.find_elements(
                    By.XPATH, '//tr[@bgcolor="#78a7b9"]'
                )
                # Iterate through the matching rows
                page_logger.info(f"Starting scrape for {law_type}, page {i}")
                row_number = 0
                for row in matching_rows:
                    row_number += 1
                    log_line = f"----------------- \n row: {row_number}\n"
                    page_logger.info(log_line)
                    object = {
                        "id": -1,
                        "textType": "",
                        "textNumber": "",
                        "journalDate": "",
                        "journalNum": "",
                        "journalPage": "",
                        "signatureDate": "",
                        "ministry": "",
                        "content": "",
                        "longContent": "",
                    }
                    # Find the a element within the current row
                    link_element = row.find_element(By.XPATH, ".//td[2]/a")
                    # Get the href attribute value and append it to the array
                    href_value = link_element.get_attribute("href")
                    page = re.search(
                        r'JoOpen\("(\d+)", *"(\d+)", *"(\d+)", *"([A-Za-z]+)"\)',
                        href_value,
                    )
                    if page:
                        (
                            journalYear,
                            object["journalNum"],
                            object["journalPage"],
                            letter,
                        ) = page.groups()

                    id_element = row.find_element(By.XPATH, ".//td[1]/a")
                    id_element_href = id_element.get_attribute("href")
                    match = re.search(r"#(\d+)", id_element_href)
                    id_number = match.group(1)
                    object["id"] = int(id_number)

                    next_siblings = []
                    assocObject = {"assoc": "", "idOut": object["id"], "idsIn": []}

                    log_line = f" ----------------- \n Getting siblings...\n"
                    page_logger.info(log_line)
                    script = """
                    var currentElement = arguments[0];
                    var siblings = [];
                    var nextSibling = currentElement.nextElementSibling;
                    while (nextSibling) {
                        if (nextSibling.getAttribute('bgcolor') === '#78a7b9') {
                            break; // Stop if the sibling with the specified bgcolor is found
                        }
                        siblings.push(nextSibling); // Add the sibling to the list
                        nextSibling = nextSibling.nextElementSibling; // Move to the next sibling
                    }
                    return siblings; // Return the collected siblings
                    """

                    # Execute the script and get the list of siblings until a specific bgcolor or the end
                    siblings = driver.execute_script(script, row)
                    log_line = f" ----------------- \n Got siblings...\n"
                    page_logger.info(log_line)

                    log_line = f" ----------------- \n Processing siblings...\n"
                    page_logger.info(log_line)

                    assocObject = {"assoc": "", "idOut": object["id"], "idsIn": []}
                    # Now 'siblings' will be a list of WebElement objects that you can loop through in Python
                    for following_sibling in siblings:
                        script = """
                        var followingSibling = arguments[0];
                        var tdElements = followingSibling.querySelectorAll('td');
                        var result = {
                            'siblingBgColor': followingSibling.getAttribute('bgcolor'),
                            'tdElements': Array.from(tdElements).map(td => ({
                                'colspan': td.getAttribute('colspan'),
                                'bgcolor': td.getAttribute('bgcolor'),
                                'text': td.querySelector('font') ? td.querySelector('font').textContent : '',
                                'href': td.querySelector('a') ? td.querySelector('a').getAttribute('href') : ''
                            }))
                        };

                        return result;                        
                        """
                        # Execute script
                        script_result = driver.execute_script(script, following_sibling)

                        # Process the result
                        td_elements = script_result["tdElements"]

                        if td_elements[0]["colspan"] == "6":
                            next_siblings.append(following_sibling)
                            log_line = (
                                f"Added to next_siblings: {following_sibling.text}\n"
                            )
                            page_logger.info(log_line)

                        elif td_elements[1]["colspan"] == "5":
                            if assocObject["assoc"] != "":
                                allAssoc.append(assocObject.copy())
                            assocObject["assoc"] = td_elements[1]["text"]
                            log_line = f"Association: {assocObject['assoc']}\n"
                            page_logger.info(log_line)
                        elif (
                            td_elements[0]["colspan"] == "2"
                            and len(td_elements) == 3
                            and td_elements[2]["bgcolor"] == "#9ec7d7"
                        ):
                            id_element_href = td_elements[1]["href"]
                            log_line = f"Association Law ID href: {id_element_href}\n"
                            page_logger.info(log_line)
                            match = re.search(r"#(\d+)", id_element_href)
                            if match:
                                id_number = match.group(1)
                                assocObject["idsIn"].append(id_number)
                    if assocObject["assoc"] != "":
                        allAssoc.append(assocObject.copy())

                    log_line = f" ----------------- \n Processed siblings...\n"
                    page_logger.info(log_line)

                    log_line = f" ----------------- \n Processing the law...\n"
                    page_logger.info(log_line)

                    if len(next_siblings) == 4:
                        var1 = next_siblings[0].text
                        object["textType"] = law_type
                        pattern = r"رقم (\S+)"
                        match = re.search(pattern, var1)
                        if match:
                            textNumber = match.group(1)
                        else:
                            textNumber = ""
                        object["textNumber"] = textNumber

                        # Define the regular expression pattern
                        pattern = r"في (\d+ [^\s]+ \d+)"
                        # Use re.search to find the match
                        match = re.search(pattern, var1)
                        # Check if there is a match and extract the result
                        if match:
                            full_date_str = match.group(1)
                            signatureDay, signatureMonth, signatureYear = (
                                full_date_str.split()
                            )
                            signatureMonth = arabic_months[signatureMonth]

                            object["signatureDate"] = (
                                signatureYear
                                + "-"
                                + str(signatureMonth)
                                + "-"
                                + signatureDay
                            )

                            object["signatureDate"] = dt.fromisoformat(
                                object["signatureDate"]
                            )
                        else:
                            object["signatureDate"] = dt.fromisoformat("9999-12-31")

                        object["ministry"] = next_siblings[1].text

                        date = next_siblings[2].text
                        # Define the regular expression pattern
                        pattern = r"في (\d+ [^\s]+ \d+)"
                        # Use re.search to find the match
                        match = re.search(pattern, date)
                        # Check if there is a match and extract the result
                        if match:
                            jornal_date_str = match.group(1)
                            journalDay, journalMonth, _ = jornal_date_str.split()
                            journalMonth = arabic_months[journalMonth]
                            object["journalDate"] = (
                                journalYear + "-" + str(journalMonth) + "-" + journalDay
                            )

                            object["journalDate"] = dt.fromisoformat(
                                object["journalDate"]
                            )
                        else:
                            object["journalDate"] = dt.fromisoformat("9999-12-31")

                        object["content"] = next_siblings[3].text
                        lawTexts.append(object.copy())

                    elif len(next_siblings) == 3:
                        var1 = next_siblings[0].text
                        object["textType"] = law_type
                        pattern = r"رقم (\S+)"
                        match = re.search(pattern, var1)
                        if match:
                            textNumber = match.group(1)
                        else:
                            textNumber = ""
                        object["textNumber"] = textNumber

                        # Define the regular expression pattern
                        pattern = r"في (\d+ [^\s]+ \d+)"
                        # Use re.search to find the match
                        match = re.search(pattern, var1)
                        # Check if there is a match and extract the result
                        if match:
                            full_date_str = match.group(1)
                            signatureDay, signatureMonth, signatureYear = (
                                full_date_str.split()
                            )
                            signatureMonth = arabic_months[signatureMonth]

                            object["signatureDate"] = (
                                signatureYear
                                + "-"
                                + str(signatureMonth)
                                + "-"
                                + signatureDay
                            )

                            object["signatureDate"] = dt.fromisoformat(
                                object["signatureDate"]
                            )
                        else:
                            object["signatureDate"] = dt.fromisoformat("9999-12-31")

                        date = next_siblings[1].text
                        # Define the regular expression pattern
                        pattern = r"في (\d+ [^\s]+ \d+)"
                        # Use re.search to find the match
                        match = re.search(pattern, date)
                        # Check if there is a match and extract the result
                        if match:
                            jornal_date_str = match.group(1)
                            journalDay, journalMonth, _ = jornal_date_str.split()
                            journalMonth = arabic_months[journalMonth]
                            object["journalDate"] = (
                                journalYear + "-" + str(journalMonth) + "-" + journalDay
                            )

                            object["journalDate"] = dt.fromisoformat(
                                object["journalDate"]
                            )
                        else:
                            object["journalDate"] = dt.fromisoformat("9999-12-31")

                        object["content"] = next_siblings[2].text
                        lawTexts.append(object.copy())
                    else:
                        log_line = f" \n \n \n ERROR\n"
                        page_logger.error(log_line)

                    log_line = f" ----------------- \n Processed the law...\n"
                    page_logger.info(log_line)

                log_line = f" \n \n \n ~~~~~~~~~~~~~~~~ \n lawTexts {lawTexts}\n"
                page_logger.info(log_line)
                log_line = f" \n \n \n ~~~~~~~~~~~~~~~~ \n length of lawTexts {len(lawTexts)}\n"
                page_logger.info(log_line)

                log_line = f" \n \n \n ~~~~~~~~~~~~~~~~ \n allAssoc {allAssoc}\n"
                page_logger.info(log_line)
                log_line = f" \n \n \n ~~~~~~~~~~~~~~~~ \n length of allAssoc {len(allAssoc)}\n"
                page_logger.info(log_line)

                log_line = f" ----------------- \n Storing the laws in db...\n"
                page_logger.info(log_line)

                storeLawText(lawTexts, page_logger)

                log_line = f" ----------------- \n Stored the laws in db...\n"
                page_logger.info(log_line)

                log_line = f" ----------------- \n Storing the assoc in db...\n"
                page_logger.info(log_line)

                storeLawAssociations(allAssoc, page_logger)

                log_line = f" ----------------- \n Stored the assoc in db...\n"
                page_logger.info(log_line)

                log_line = f" \n Finished scraping page {i} of {law_type} with {len(lawTexts)} law and {len(allAssoc)} assoc \n"
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
            driver.quit()
            j += 1

    log_line = f"PROGRAM ENDED AFTER {j + 1} TRIES FOR {law_type}!!!"
    print(log_line)
    main_logger.info(log_line)


def storeLawText(lawTexts, page_logger):
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        for law_text in lawTexts:
            # Check if the law text already exists
            existing_law_text = session.query(LawText).get(law_text["id"])
            if existing_law_text:
                # Update existing record
                existing_law_text.text_type = law_text["textType"]
                existing_law_text.text_number = law_text["textNumber"]
                existing_law_text.journal_date = law_text["journalDate"]
                existing_law_text.journal_num = law_text["journalNum"]
                existing_law_text.journal_page = law_text["journalPage"]
                existing_law_text.signature_date = law_text["signatureDate"]
                existing_law_text.ministry = law_text["ministry"]
                existing_law_text.content = law_text["content"]
            else:
                # Insert new record
                new_law_text = LawText(
                    id=law_text["id"],
                    text_type=law_text["textType"],
                    text_number=law_text["textNumber"],
                    journal_date=law_text["journalDate"],
                    journal_num=law_text["journalNum"],
                    journal_page=law_text["journalPage"],
                    signature_date=law_text["signatureDate"],
                    ministry=law_text["ministry"],
                    content=law_text["content"],
                )
                session.add(new_law_text)
        session.commit()
        log_line = f" ----------------- \n lawTexts stored in db successfully and committed...\n"
        page_logger.info(log_line)
    except Exception as e:
        session.rollback()
        log_line = f"Error inserting/updating law text: {e}"
        page_logger.error(log_line)
    finally:
        session.close()


def storeLawAssociations(associations, page_logger):
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        for assoc_data in associations:
            # Directly attempt to retrieve the specific association using both parts of the composite key
            existing_assoc = session.query(Association).get(
                {"id_out": assoc_data["idOut"], "assoc_nom": assoc_data["assoc"]}
            )

            if existing_assoc:
                # If the association exists, update the ids_in
                existing_assoc.ids_in = assoc_data["idsIn"]
            else:
                # If no such association exists, create a new one
                new_assoc = Association(
                    id_out=assoc_data["idOut"],
                    assoc_nom=assoc_data["assoc"],
                    ids_in=assoc_data["idsIn"],
                )
                session.add(new_assoc)

        # Commit the session once all associations have been processed
        session.commit()
        log_line = f" ----------------- \n associations stored in db successfully and committed...\n"
        page_logger.info(log_line)
    except Exception as e:
        # If any exception occurs, rollback the session to avoid partial commits
        session.rollback()
        log_line = f"Error in storing/updating associations: {e}"
        page_logger.error(log_line)
    finally:
        # Ensure the session is closed properly in a finally block
        session.close()


if __name__ == "__main__":
    

    # Create database tables
    # DONT FORGET TO CHECK IF THE TABLE EXISTS OR NOT BEFORE CREATING IT
    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    session = Session()

    start_date = session.query(LastScrapingDate).first().newspapers_scraper

    # Initialize ChromeOptions
    options = Options()
    options.add_argument("--disable-gpu")
    options.add_argument("--headless=new")
    driver = webdriver.Chrome(options=options)

    # Open the website
    driver.get("https://www.joradp.dz/HAR/Index.htm")

    # Switch to the frame with src="ATitre.htm"
    driver.switch_to.frame(driver.find_element(By.XPATH, '//frame[@src="ATitre.htm"]'))

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

    # Find the input field and enter '01/01/1964'
    select_input = WebDriverWait(driver, 60).until(
        EC.presence_of_element_located((By.NAME, "znat"))
    )
    select_object = Select(select_input)

    law_types = []
    options = select_object.options
    for option in options:
        law_types.append(option.text)
    law_types = law_types[1:]
    print(law_types)
    main_logger.info(f"law_types : {law_types}")
    driver.quit()

    # law_types = ['أمر', 'منشور', 'منشور وزاري مشترك', 'لائحة', 'مداولة', 'مداولة م-أ-للدولة', 'مرسوم', 'مرسوم تنفيذي', 'مرسوم تشريعي', 'مرسوم رئاسي', 'مقرر', 'مقرر وزاري مشترك', 'إعلان', 'نظام', 'اتفاقية', 'تصريح', 'تقرير', 'تعليمة', 'تعليمة وزارية مشتركة', 'جدول', 'رأي', 'قانون', 'قانون عضوي', 'قرار', 'قرار ولائي', 'قرار وزاري مشترك']

    law_types_iterator = iter(law_types)
    with multiprocessing.Pool(processes=3) as pool:
        for result in pool.starmap(
            scrape_law_data, zip(law_types_iterator, itertools.repeat(start_date))
        ):
            pass
    
    last_scraping_date = session.query(LastScrapingDate).first()
    last_scraping_date.laws_metadata_scraper = dt.today()
    session.commit()
