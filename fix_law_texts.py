from sqlalchemy import create_engine, Column, Integer, String, Date, Boolean, Text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
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


def fix_law_texts():
    main_logger = setup_logger(
        f".",
        f"./law_texts_fixing_logs.log",
    )
    laws = session.query(LawText).all()
    for law in laws:
        if law.long_content:
            original_long_content = law.long_content
            # Replace multiple line breaks with a single line break
            law.long_content = law.long_content.replace("\n\n", "\n")
            # Remove leading and trailing whitespace, and remove lines containing only whitespace
            lines = law.long_content.strip().split('\n')
            lines = [line for line in lines if line.strip()]  # Filter out lines with only whitespace
            # Remove lines containing numbers only
            lines = [line for line in lines if not line.strip().isdigit()]
            # Join the non-empty lines back together
            law.long_content = '\n'.join(lines)
            if original_long_content != law.long_content:
                main_logger.info(f"Multiple line breaks replaced with single line break and lines containing only whitespace removed from long_content of LawText with id {law.id}.")
    session.commit()
    session.close()
fix_law_texts()