import os
import subprocess
import logging


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
    f"MAIN_SCRIPT_logs",
    f"./MAIN_SCRIPT_logs.log",
)

scripts = [
    ("newspapers_scraper.py", "scrapy runspider"),
    ("laws_metadata_scraper.py", "python"),
    ("9ita3.py", "python"),
    ("pdfs_to_images_conversion.py", "python"),
    ("ocr_images.py", "python"),
    ("text_extraction.py", "python"),
    ("fix_law_texts.py", "python"),
    ("delete_all_photos.py", "python")
]


def run_scripts(script_list):
    for script, command in script_list:
        try:
            main_logger.info(f"Executing {script} with command '{command}'...")
            if command == "scrapy runspider":
                result = subprocess.run(
                    ['scrapy', 'runspider', script], check=True)
            else:
                result = subprocess.run([command, script], check=True)

            if result.returncode == 0:
                main_logger.info(f"{script} executed successfully.")
            else:
                main_logger.error(f"{script} execution failed.")
                break
        except subprocess.CalledProcessError as e:
            main_logger.error(f"Execution failed: {e}")
            break
        except Exception as e:
            main_logger.info(f"An error occurred: {e}")
            break


if __name__ == "__main__":
    run_scripts(scripts)
