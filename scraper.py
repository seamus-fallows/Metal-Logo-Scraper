import requests
from bs4 import BeautifulSoup
import json
import os
import time
import random
from tqdm import tqdm
from PIL import Image
import io
import csv
import shutil
import threading
import queue
from typing import Union, Optional, Tuple, Any, List

"""
A script for downloading, processing, and saving metal band logos based on genre. It features two classes:

- `ImageDownloader`: Manages the download, resizing, conversion to grayscale, and saving of images. It supports handling errors and retrying downloads.

- `MetalLogoDownloader`: Automates scraping of metal band logos by genre, utilizing multithreading for efficiency. It filters bands by genre and excluded words, handles pagination, and saves band details and images to a CSV file.
"""

class ImageDownloader:
    def __init__(self, save_path: str, max_dim: int, genre: str, session: Optional[requests.Session] = None) -> None:
        self.save_path = os.path.join(save_path, f'{genre}_metal_logos')
        self.max_dim = max_dim
        self.session = session
        self._prepare_directory()

    def _prepare_directory(self) -> None:
        if os.path.exists(self.save_path):
            shutil.rmtree(self.save_path)
        os.makedirs(self.save_path)

    def _process_image(self, image: Image.Image) -> Tuple[Image.Image, Tuple[int, int]]:
        # Convert palette images with transparency to RGBA
        if image.mode == 'P':
            image = image.convert('RGBA')
        # Check if the image has an alpha channel and process accordingly
        if image.mode in ('RGBA', 'LA'):
            # Create a new image with a black background
            bg = Image.new('RGB', image.size, (0, 0, 0))
            
            if image.mode == 'RGBA':
                alpha = image.split()[3]
                bg.paste(image, mask=alpha)
            elif image.mode == 'LA':
                luminance, alpha = image.split()
                merged = Image.merge("LA", (luminance, alpha))
                bg.paste(merged.convert('RGB'), mask=alpha)

            image = bg

        # Convert to grayscale
        image = image.convert('L')

        # Resize the image
        original_size = image.size
        ratio = float(self.max_dim) / max(original_size)
        pre_pad_size = tuple(int(x * ratio) for x in original_size)
        image = image.resize(pre_pad_size, Image.LANCZOS)

        # Symmetric padding
        padding_horizontal = (self.max_dim - pre_pad_size[0]) // 2
        padding_vertical = (self.max_dim - pre_pad_size[1]) // 2
        final_image = Image.new('L', (self.max_dim, self.max_dim), color='black')
        final_image.paste(image, (padding_horizontal, padding_vertical))

        return final_image, pre_pad_size

    def _save_image(self, image: Image.Image, genre: str, count: int) -> str:
        save_format = 'JPEG'
        file_extension = '.jpg'
        image_name = f"{genre}_{count:05d}"
        file_path = os.path.join(self.save_path, image_name + file_extension)
        image.save(file_path, save_format)
        return image_name

    def download_and_process_image(self, image_url: str, genre: str, count: int, max_retries: int = 5) -> Tuple[Optional[str], Optional[Tuple[int, int]]]:
        retries = 0
        max_wait = 8
        while retries < max_retries:
            try:
                response = self.session.get(image_url, timeout=10) if self.session else requests.get(image_url, timeout=10)
                if response.status_code == 200:
                    # Converts binary data to image file
                    image = Image.open(io.BytesIO(response.content))
                    image, pre_pad_size = self._process_image(image)
                    return self._save_image(image, genre, count), pre_pad_size
                elif response.status_code == 429 or response.status_code >= 500:
                    # Handle rate limiting and server errors
                    wait = min(2**retries, max_wait)
                    print(f"Retrying in {wait} seconds due to error {response.status_code}.")
                    time.sleep(wait)
                    retries += 1
                else:
                    print(f'Failed to download the image for {image_url}. Status code: {response.status_code}')
                    return None, None
            except requests.exceptions.RequestException as e:
                print(f"An error occurred while downloading image for {image_url}: {e}")
                retries += 1
                time.sleep(3)

        print(f"Max retries reached for {image_url}")
        return None, None


class MetalLogoDownloader:
    def __init__(self, genre: str, 
                excluded_words: List[str] = [],
                total_pages: Optional[int] = None,
                max_dim: int = 224, 
                save_path: str = '', 
                num_threads: int = 4) -> None:
        self.genre = genre
        self.count = 0
        self.session = requests.Session()
        self.image_downloader = ImageDownloader(save_path, max_dim, self.genre, session=self.session)
        self.csv_save_path = save_path 
        self.download_queue = queue.Queue()
        self.num_threads = num_threads
        self.excluded_words = excluded_words
        self.lock = threading.Lock()
        # Headers to mimic a browser visit
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'})
        self.items_per_page = self._set_items_per_page()
        self.total_pages = self._calculate_total_pages() if not total_pages else total_pages

    def _set_items_per_page(self) -> int:
        ajax_url = self._create_ajax_url(0)
        response = self.session.get(ajax_url)
        if response.status_code == 200:
            data = json.loads(response.text)
            # Attempt to dynamically find the number of items per page from the response
            try:
                return len(data.get('aaData', []))
            except Exception as e:
                print(f"Failed to set items per page dynamically, using default value of 500: {e}")
                return 500
        else:
            print("Failed to fetch initial data for setting items per page, using default value of 500")
            return 500
        
    def _start_download_threads(self) -> None:
        for _ in range(self.num_threads):
            t = threading.Thread(target=self._download_worker)
            t.daemon = True
            t.start()

    def _download_worker(self) -> None:
        while True:
            item = self.download_queue.get()
            if item is None:
                break 
            band_link_tag, band_subgenre = item
            self._process_band(band_link_tag, band_subgenre)
            self.download_queue.task_done()

    def _create_ajax_url(self, start_index: int) -> str:
        return f'https://www.metal-archives.com/browse/ajax-genre/g/{self.genre}/json/1?&iDisplayStart={start_index}&iSortCol_0=2'
    
    def _calculate_total_pages(self) -> int:
            ajax_url = self._create_ajax_url(0)
            response = self.session.get(ajax_url)
            if response.status_code == 200:
                data = json.loads(response.text)
                total_bands = data.get('iTotalRecords', 0)
                return (total_bands + self.items_per_page - 1) // self.items_per_page
            return 0

    def _init_csv_writer(self) -> None:
        csv_file_path = os.path.join(self.csv_save_path, f'{self.genre}_metal_bands.csv')
        self.csv_file = open(csv_file_path, 'w', newline='', encoding='utf-8')
        self.csv_writer = csv.writer(self.csv_file)
        self.csv_writer.writerow(['Image Name', 'Band Name', 'Subgenre', 'Band URL', 'Pre-padding Size'])

    def _write_to_csv(self, image_name: str, band_name: str, band_subgenre: str, band_url: str, pre_pad_size: Tuple[int, int]) -> None:
        self.csv_writer.writerow([image_name, band_name, band_subgenre, band_url, pre_pad_size])
        self.csv_file.flush()

    def _process_page(self, ajax_url: str) -> None:
        response = self.session.get(ajax_url)

        if response.status_code == 200:
            # Parse the JSON data
            data = json.loads(response.text)
            bands = data.get('aaData', [])

            for band_info in tqdm(bands, desc=f"Processing bands in page", leave=False):
                band_subgenre = band_info[2].strip()

                if not any(excluded_word in band_subgenre for excluded_word in self.excluded_words):
                    band_link_html = band_info[0]
                    band_link_soup = BeautifulSoup(band_link_html, 'html.parser')
                    band_link_tag = band_link_soup.find('a')

                    if band_link_tag and band_link_tag.has_attr('href'):
                        self.download_queue.put((band_link_tag, band_subgenre))
                        time.sleep(random.uniform(0,2))

    def _process_band(self, band_link_tag: Any, band_subgenre: str) -> None:
        band_url = band_link_tag['href']
        band_name = band_link_tag.text.strip()
        band_response = self.session.get(band_url)

        if band_response.status_code == 200:
            band_soup = BeautifulSoup(band_response.text, 'html.parser')
            image_element = band_soup.select_one('div.band_name_img img')

            if image_element:
                image_url = image_element['src']
                with self.lock:  # Use self.lock here
                    image_name, pre_pad_size = self.image_downloader.download_and_process_image(image_url, self.genre, self.count)
                    if image_name:  # Check if the image was successfully downloaded and processed
                        self._write_to_csv(image_name, band_name, band_subgenre, band_url, pre_pad_size)
                        self.count += 1

    def download_logos(self) -> None:
        self._init_csv_writer()
        self._start_download_threads()
        try:
            for page in tqdm(range(self.total_pages)):
                start_index = page * self.items_per_page
                ajax_url = self._create_ajax_url(start_index)
                self._process_page(ajax_url)
                time.sleep(random.uniform(0,2))

            # Block until all tasks are done
            self.download_queue.join()

            # Stop workers
            for _ in range(self.num_threads):
                self.download_queue.put(None)

        finally:
            self.csv_file.close()
            self.session.close()

# Usage Example

if __name__ == "__main__":
    black_metal_downloader = MetalLogoDownloader(genre = 'black',  
                                                 excluded_words = ["death"],
                                                 save_path='test_run\\',
                                                 total_pages=1)
    black_metal_downloader.download_logos()
