# EasyLaw

### Requirements:

```
python3 -m venv env
```

```
source env/bin/activate
```

```
pip install -r requirements.txt
```

### Pdf docs scraping:

```
scrapy runspider newspapers_scraper.py
```

### Laws and Laws associations scraping:

```
python3 joradp_db_population.py
```

### Database corrections:

```
python3 pages_fix_script.py
```

### Convert pdfs to images:

```
python3 convert_pdfs_to_images.py
```

### Perform ocr on images:

```
sudo apt install poppler-utils
```

```
sudo apt install tesseract-ocr
```

```
python3 ocr_images.py
```
