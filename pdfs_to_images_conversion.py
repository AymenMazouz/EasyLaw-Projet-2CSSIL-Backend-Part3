from multiprocessing import Pool, Value, Lock, current_process
import os
import logging
from pdf2image import convert_from_path
from datetime import date as dt
from sqlalchemy import create_engine, Column, Integer, Date
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import ARRAY
from dotenv import load_dotenv


load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    filename="conversion.log",
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s",
)

Base = declarative_base()

DB_URL = os.getenv("PG_URL")
engine = create_engine(DB_URL)

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
    

# Define the shared counter and lock globally so that they are inherited
total_files = None  # This will be set later
count = Value('i', 0)  # Shared counter
lock = Lock()  # Mutex for accessing the shared counter

def convert_pdf_to_images(pdf_path):
    try:
        logging.info(f"Starting conversion of {pdf_path}")
        # Create a directory for the images, stripping .pdf and replacing with nothing
        imgs_dir = pdf_path.rsplit(".", 1)[0]
        if not os.path.exists(imgs_dir):
            os.makedirs(imgs_dir)
            logging.info(f"Created directory {imgs_dir}")

        # Convert PDF to images
        convert_from_path(
            pdf_path=pdf_path,
            output_folder=imgs_dir,
            fmt='jpeg',
            output_file="journal",
            # Adjust the number of threads as necessary
            thread_count=2,
            paths_only=True,
        )
        logging.info(f"Saved images of {pdf_path} to {imgs_dir}")

        with lock:
            count.value += 1
            percentage_complete = (count.value / total_files) * 100
            print(f"Conversion progress: {percentage_complete:.2f}% ({count.value} / {total_files} files) completed by {current_process().name}.")
    except Exception as e:
        logging.error(f"Error converting {pdf_path}: {e}")

def convert_pdfs_to_images(base_dir):
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    year = session.query(LastScrapingDate).first().pdfs_to_images_conversion_journal_year
    journal_number = session.query(LastScrapingDate).first().pdfs_to_images_conversion_journal_number
    print(f"last scraping year: {year}, last scraping journal_number: {journal_number}")
    global total_files
    # Walk the directory to list all PDF files
    # Construct PDF file path and append it to the list
    pdf_files = [
        os.path.join(root, file)
        for root, _, files in os.walk(base_dir)
        for file in files 
        if (file.endswith(".pdf") and (int(file.split("_")[0]) > year or (int(file.split("_")[0]) == year and int(file.split("_")[1].split(".")[0]) > journal_number)))
    ]
        
    # Sort the list based on year and journal number
    pdf_files.sort(key=lambda x: (int(os.path.basename(x).split("_")[0]), int(os.path.basename(x).split("_")[1].split(".")[0])))
    
    print(f"pdf_files: {pdf_files}")
    total_files = len(pdf_files)  # Set the total number of files to be processed

    # Adjust the number of processes as necessary
    with Pool(processes=8) as pool:
        pool.map(convert_pdf_to_images, pdf_files)
    
    last_scraping_date = session.query(LastScrapingDate).first()
    if pdf_files:
        last_pdf_file = pdf_files[-1]  # Get the last PDF file after sorting
        last_scraping_date.pdfs_to_images_conversion_journal_year = int(os.path.basename(last_pdf_file).split("_")[0])
        last_scraping_date.pdfs_to_images_conversion_journal_number = int(os.path.basename(last_pdf_file).split("_")[1].split(".")[0])
    session.commit()

if __name__ == '__main__':
    # Adjust the path as necessary
    convert_pdfs_to_images("joradp_pdfs")
