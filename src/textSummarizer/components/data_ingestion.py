import os
from urllib.request import urlretrieve
import zipfile
from textSummarizer.logging import logger
from textSummarizer.utils.common import get_size
from pathlib import Path
from textSummarizer.entity import DataIngestionConfig


class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config

    def _fix_github_url(self, url):
        """
        Convert GitHub blob URL to raw download URL
        """
        if "github.com" in url and "/blob/" in url:
            fixed_url = url.replace("github.com", "raw.githubusercontent.com")
            fixed_url = fixed_url.replace("/blob/", "/")
            logger.info(f"Fixed GitHub URL: {url} -> {fixed_url}")
            return fixed_url
        return url

    def _validate_zip_file(self, file_path):
        """
        Validate that the file is actually a ZIP file
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Check file size
        file_size = os.path.getsize(file_path)
        if file_size == 0:
            raise ValueError("Downloaded file is empty")

        # Check file signature
        with open(file_path, 'rb') as f:
            first_bytes = f.read(100)

        # Check if it's HTML (common issue with GitHub URLs)
        if first_bytes.startswith(b'<!DOCTYPE') or first_bytes.startswith(b'<html'):
            logger.error("Downloaded HTML page instead of ZIP file")
            # Show HTML content for debugging
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                html_content = f.read(300)
                logger.error(f"HTML content: {html_content}")
            raise ValueError("Downloaded HTML page instead of ZIP file. Check the URL.")

        # Check ZIP signature
        if not first_bytes.startswith(b'PK'):
            raise ValueError(f"File is not a ZIP file. First bytes: {first_bytes[:20].hex()}")

        # Validate ZIP file structure
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_test:
                file_list = zip_test.namelist()
                logger.info(f"ZIP file is valid with {len(file_list)} files")
                return True
        except zipfile.BadZipFile as e:
            raise zipfile.BadZipFile(f"Corrupted ZIP file: {e}")

    def download_file(self):
        """
        Download file with proper URL handling and validation
        """
        try:
            if not os.path.exists(self.config.local_data_file):
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(self.config.local_data_file), exist_ok=True)
                # Fix GitHub URL if needed
                download_url = self._fix_github_url(self.config.source_URL)

                logger.info(f"Downloading from: {download_url}")

                # Download the file
                filename, headers = urlretrieve(
                    url=download_url,
                    filename=self.config.local_data_file
                )

                logger.info(f"{filename} downloaded! Headers: \n{headers}")

                # Validate the downloaded file immediately
                self._validate_zip_file(self.config.local_data_file)
                logger.info("File validated successfully as ZIP")

            else:
                file_size = get_size(Path(self.config.local_data_file))
                logger.info(f"File already exists of size: {file_size}")

                # Still validate existing file
                try:
                    self._validate_zip_file(self.config.local_data_file)
                except (ValueError, zipfile.BadZipFile) as e:
                    logger.warning(f"Existing file is invalid: {e}")
                    logger.info("Removing invalid file and re-downloading...")
                    os.remove(self.config.local_data_file)
                    # Recursively call to re-download
                    return self.download_file()

        except Exception as e:
            # Clean up failed download
            if os.path.exists(self.config.local_data_file):
                os.remove(self.config.local_data_file)
                logger.error("Removed failed download")

            logger.error(f"Download failed: {e}")
            raise

    def extract_zip_file(self):
        """
        Extract ZIP file with validation and error handling
        """
        try:
            # Validate file before extraction
            self._validate_zip_file(self.config.local_data_file)

            unzip_path = self.config.unzip_dir
            os.makedirs(unzip_path, exist_ok=True)

            logger.info(f"Extracting ZIP file to: {unzip_path}")

            with zipfile.ZipFile(self.config.local_data_file, 'r') as zip_ref:
                # Get file list
                file_list = zip_ref.namelist()
                logger.info(f"Extracting {len(file_list)} files...")

                # Extract all files
                zip_ref.extractall(unzip_path)

                # Log some extracted files
                if file_list:
                    logger.info("Sample extracted files:")
                    for i, filename in enumerate(file_list[:5]):
                        logger.info(f"  {i+1}. {filename}")
                    if len(file_list) > 5:
                        logger.info(f"  ... and {len(file_list) - 5} more files")

                logger.info("ZIP extraction completed successfully!")

        except zipfile.BadZipFile as e:
            logger.error(f"ZIP file error: {e}")
            logger.error("Possible causes:")
            logger.error("1. Downloaded file is corrupted")
            logger.error("2. Wrong URL (downloaded HTML instead of ZIP)")
            logger.error("3. File is not actually a ZIP file")
            raise
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise
