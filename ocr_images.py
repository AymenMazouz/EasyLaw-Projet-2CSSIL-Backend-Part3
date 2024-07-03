import os
from PIL import Image
import pytesseract
from concurrent.futures import ThreadPoolExecutor

def process_image(img_path, lang='ara'):
    try:
        # Extract sequence number from image file name
        sequence_number = int(os.path.basename(img_path).split("-")[-1].split(".")[0])
        
        # Construct text file path based on sequence number
        txt_file_path = os.path.join(os.path.dirname(img_path), f"{sequence_number}.txt")

        # Check if text path already exists
        if os.path.exists(txt_file_path):
            print(f"Text file already exists for {img_path}")
            return

        # Perform OCR using pytesseract
        text = pytesseract.image_to_string(Image.open(img_path), lang=lang)

        # Save the extracted text to a .txt file
        with open(txt_file_path, 'w', encoding='utf-8') as f:
            f.write(text)

        print(f"Processed {img_path} successfully")
    except Exception as e:
        print(f"Error processing {img_path}: {e}")

def image_to_text_parallel(base_dir, lang='ara', max_workers=None):
    # Use ThreadPoolExecutor to parallelize OCR processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Iterate through each image file in the directory
        for root, _, files in os.walk(base_dir):
            for file in files:
                if file.lower().endswith(('.png', '.jpg', '.jpeg')):
                    # Construct image file path
                    img_path = os.path.join(root, file)
                    
                    # Submit OCR task to the executor
                    executor.submit(process_image, img_path, lang=lang)

image_to_text_parallel('joradp_pdfs', 'ara', max_workers=6)  # Adjust max_workers as needed
