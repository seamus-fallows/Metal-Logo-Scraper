# Metal-Logo-Scraper

## Overview
A Python script for downloading and processing metal band logos by genre from https://www.metal-archives.com/. It uses web scraping, image processing, and multithreading to efficiently collect logos, converting them to grayscale and saving alongside metadata in a CSV file.

## Key Features
- **Web Scraping:** Filters logos by genre and excludes specified keywords.
- **Image Processing:** Downloads, resizes, converts to grayscale, and saves images.
- **Multithreading:** Speeds up downloads by running multiple threads.
- **Error Handling:** Retries on failure and manages HTTP errors.
- **CSV Output:** Records band details and image information.

## Components
- `ImageDownloader`: Handles image downloading, resizing, and saving.
- `MetalLogoDownloader`: Manages web scraping, threading, and CSV writing based on genre.
