from flask import Flask, jsonify
import subprocess
from sqlalchemy import create_engine, Column, Integer, String, Date, Boolean, Text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from sqlalchemy.orm import declarative_base
import os

app = Flask(__name__)

@app.route('/run-scraping', methods=['GET'])
def run_script():
    try:
        result = subprocess.run(['python', 'MAIN_SCRIPT.py'], capture_output=True, text=True)
        
        if result.returncode != 0:
            return jsonify({
                'status': 'error',
                'message': 'Script execution failed',
                'error': result.stderr
            }), 500

        return jsonify({
            'status': 'success',
            'message': 'Script executed successfully',
            'output': result.stdout
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500
        
        
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
        
load_dotenv()

DB_URL = os.getenv("PG_URL")
engine = create_engine(DB_URL)

"""Endpoint to get the last scarping dates"""
@app.route('/last-scraping-dates', methods=['GET'])
def get_last_scraping_dates():
    try:
        Session = sessionmaker(bind=engine)
        session = Session()
        last_scraping_dates = session.query(LastScrapingDate).first()

        if not last_scraping_dates:
            return jsonify({'message': 'No data found'}), 404

        result = {
            'newspapers_scraper': last_scraping_dates.newspapers_scraper,
            'laws_metadata_scraper': last_scraping_dates.laws_metadata_scraper,
            'kita3': last_scraping_dates.kita3,
            'fix_pages': last_scraping_dates.fix_pages,
            'ocr_images': last_scraping_dates.ocr_images,
            'pdfs_to_images_conversion_journal_year': last_scraping_dates.pdfs_to_images_conversion_journal_year,
            'pdfs_to_images_conversion_journal_number': last_scraping_dates.pdfs_to_images_conversion_journal_number,
            'text_extraction': last_scraping_dates.text_extraction,
            'fix_law_texts': last_scraping_dates.fix_law_texts
        }

        return jsonify(result)
    
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500
        
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
