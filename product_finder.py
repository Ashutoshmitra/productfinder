import asyncio
import aiohttp
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from typing import Optional, Callable, Dict, List, Tuple
from PIL import Image
import imagehash
from io import BytesIO
import gc
from functools import lru_cache
import concurrent.futures
import numpy as np
from cachetools import TTLCache
import time
import logging
import csv


logger = logging.getLogger(__name__)


class ProductFinder:
    def __init__(self, credentials_path: str, max_concurrent_requests: int = 10, cache_ttl: int = 3600):
        """Initialize with enhanced concurrency and caching."""
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        self.credentials = service_account.Credentials.from_service_account_file(
            credentials_path, scopes=self.SCOPES
        )
        self.service = build('sheets', 'v4', credentials=self.credentials)
        
        # Caching configurations
        self.image_hash_cache = TTLCache(maxsize=1000, ttl=cache_ttl)  # Cache with 1-hour TTL
        self.sheet_data_cache = TTLCache(maxsize=10, ttl=cache_ttl)
        
        # Concurrency settings
        self.max_concurrent_requests = max_concurrent_requests
        self.timeout = aiohttp.ClientTimeout(total=30)
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)

        self.match_threshold = 10

    @staticmethod
    def optimize_image(image: Image.Image, max_size: int = 300) -> Image.Image:
        """Optimize image for faster processing."""
        # Convert to grayscale for faster processing
        image = image.convert('L')
        
        # Resize if larger than max_size
        if max(image.size) > max_size:
            ratio = max_size / max(image.size)
            new_size = tuple(int(dim * ratio) for dim in image.size)
            image = image.resize(new_size, Image.Resampling.LANCZOS)
        
        return image

    @lru_cache(maxsize=1000)
    def get_image_hash(self, image_data: bytes) -> str:
        """Calculate and cache perceptual hash of an image."""
        try:
            img = Image.open(BytesIO(image_data))
            img = self.optimize_image(img)
            hash_value = str(imagehash.average_hash(img))
            return hash_value
        finally:
            if 'img' in locals():
                img.close()

    async def get_url_hash(self, url: str, session: aiohttp.ClientSession) -> Optional[str]:
        """Get perceptual hash of image from URL with concurrency control."""
        if not url or not isinstance(url, str):
            return None

        # Check cache first
        if url in self.image_hash_cache:
            return self.image_hash_cache[url]

        async with self.semaphore:  # Control concurrency
            try:
                async with session.get(url, timeout=self.timeout) as response:
                    if response.status != 200:
                        return None
                    
                    img_data = await response.read()
                    hash_value = self.get_image_hash(img_data)
                    self.image_hash_cache[url] = hash_value
                    return hash_value

            except Exception as e:
                print(f"Warning: Could not process URL {url}: {str(e)}")
                return None

    def get_sheet_data(self, spreadsheet_id: str, range_name: str) -> pd.DataFrame:
        """Fetch data from Google Sheets with caching."""
        cache_key = f"{spreadsheet_id}:{range_name}"
        
        # Check cache first
        if cache_key in self.sheet_data_cache:
            return self.sheet_data_cache[cache_key].copy()

        try:
            sheet = self.service.spreadsheets()
            result = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            
            data = result.get('values', [])
            if not data:
                raise ValueError("No data found in sheet")

            headers = data[0]
            df = pd.DataFrame(data[1:], columns=headers)
            
            # Cache the result
            self.sheet_data_cache[cache_key] = df.copy()
            
            # Clean up memory
            del data
            gc.collect()
            
            return df
        except Exception as e:
            raise Exception(f"Error fetching sheet data: {str(e)}")

    async def process_single_image(self, 
                            local_hash: str,
                            url: str,
                            df: pd.DataFrame,
                            session: aiohttp.ClientSession,
                            hash_threshold: int) -> Tuple[Optional[str], Optional[int], Optional[str], Optional[str]]:
        """Process a single image URL and return comparison results with title."""
        if not url or not isinstance(url, str) or not url.strip():
            return None, None, None, "Invalid URL"

        try:
            url_hash = await self.get_url_hash(url, session)
            if url_hash is None:
                return None, None, None, "Failed to process image"

            hash1 = imagehash.hex_to_hash(local_hash)
            hash2 = imagehash.hex_to_hash(url_hash)
            difference = hash1 - hash2

            # Get title for close matches
            title = None
            if difference is not None and difference <= 10:  # Only process title for close matches
                try:
                    # Debug prints
                    print(f"Processing URL: {url}")
                    print(f"DataFrame columns: {df.columns.tolist()}")
                    
                    # Find the row with matching Image Src
                    matching_rows = df[df['Image Src'] == url]
                    print(f"Found {len(matching_rows)} matching rows for URL")
                    
                    if not matching_rows.empty:
                        # Get the Handle
                        handle = matching_rows['Handle'].iloc[0]
                        print(f"Found handle: {handle}")
                        
                        # Get all rows with this Handle
                        handle_rows = df[df['Handle'] == handle]
                        print(f"Found {len(handle_rows)} rows with same handle")
                        
                        # Get the first non-null title
                        non_null_titles = handle_rows['Title'].dropna()
                        if not non_null_titles.empty:
                            title = non_null_titles.iloc[0]
                            if pd.isna(title) or title.strip() == '':
                                title = None
                            print(f"Found title: {title}")
                        else:
                            print("No non-null titles found")
                except Exception as e:
                    print(f"Error extracting title: {str(e)}")
                    title = None

            return url_hash, difference, title, None

        except Exception as e:
            print(f"Error in process_single_image: {str(e)}")
            return None, None, None, str(e)

    async def process_batch(self, 
                        local_hash: str,
                        urls: List[str],
                        df: pd.DataFrame,
                        session: aiohttp.ClientSession,
                        hash_threshold: int) -> List[Tuple[str, Optional[str], Optional[int], Optional[str], Optional[str]]]:
        """Process a batch of image URLs concurrently."""
        tasks = []
        for url in urls:
            if url:
                task = self.process_single_image(local_hash, url, df, session, hash_threshold)
                tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)
        processed_results = []
        
        for url, result in zip(urls, results):
            if isinstance(result, Exception):
                processed_results.append((url, None, None, None, str(result)))
            else:
                url_hash, difference, title, error = result
                if difference is not None and difference <= 10:
                    print(f"Close match found - URL: {url}, Title: {title}, Difference: {difference}")
                processed_results.append((url, url_hash, difference, title, error))
        
        return processed_results

    
    def get_file_hash(self, file_path: str) -> str:
        """Get perceptual hash of local image file."""
        try:
            with Image.open(file_path) as img:
                optimized_img = self.optimize_image(img)
                return str(imagehash.average_hash(optimized_img))
        except Exception as e:
            raise Exception(f"Error processing file {file_path}: {str(e)}")

    async def find_product_and_export_async(self,
                                            spreadsheet_id: str,
                                            sheet_name: str,
                                            local_image_path: str,
                                            output_csv_path: str,
                                            hash_threshold: int = 0,
                                            batch_size: int = 50,
                                            progress_callback: Callable[[str, int, dict], None] = None) -> Optional[str]:
        try:
            if progress_callback:
                progress_callback("Computing hash of local image...", 10, None)
            local_hash = self.get_file_hash(local_image_path)

            if progress_callback:
                progress_callback("Fetching data from Google Sheets...", 20, None)
            
            df = self.get_sheet_data(spreadsheet_id, f'{sheet_name}!A:AC')

            # Remove rows without image URLs
            df = df[df['Image Src'].notna() & (df['Image Src'] != '')]
            total_images = len(df)

            if progress_callback:
                progress_callback("Preparing product data", 25, {
                    'total_products': total_images
                })

            if total_images == 0:
                if progress_callback:
                    progress_callback("No valid image URLs found in the spreadsheet.", 100, None)
                return None

            best_match = None
            best_difference = float('inf')
            processed = 0

            connector = aiohttp.TCPConnector(limit=self.max_concurrent_requests)
            async with aiohttp.ClientSession(connector=connector) as session:
                url_batches = np.array_split(df['Image Src'].tolist(), 
                                        max(1, total_images // batch_size))

                for batch_urls in url_batches:
                    batch_results = await self.process_batch(
                        local_hash, batch_urls, df, session, hash_threshold
                    )

                    for url, url_hash, difference, title, error in batch_results:
                        processed += 1
                        progress = 25 + int((processed / total_images) * 70)

                        # Find the handle for this match
                        match_row = df[df['Image Src'] == url]
                        handle = match_row['Handle'].iloc[0] if not match_row.empty else None

                        # Include both exact matches (difference = 0) and close matches (0 < difference <= 10)
                        if difference is not None and difference <= 10:
                            details = {
                                'url': url,
                                'uploaded_hash': local_hash,
                                'url_hash': url_hash,
                                'difference': difference,
                                'processed': processed,
                                'total_products': total_images,
                                'title': title,
                                'handle': handle
                            }

                            if progress_callback:
                                progress_callback(
                                    f"Processing image {processed}/{total_images}",
                                    progress,
                                    details
                                )

                            if difference == 0:
                                # For exact match, export to CSV and return immediately
                                matching_handle = df[df['Image Src'] == url]['Handle'].iloc[0]
                                filtered_df = df[df['Handle'] == matching_handle]
                                filtered_df.to_csv(output_csv_path, index=False)
                                return filtered_df['Title'].iloc[0] if not filtered_df.empty else None
                            elif difference < best_difference:
                                # Update best match for close matches
                                best_difference = difference
                                best_match = (url, title, df[df['Image Src'] == url]['Handle'].iloc[0])

                            gc.collect()

                # If no exact match found, save the CSV for the best close match
                if best_match:
                    matching_handle = best_match[2]
                    filtered_df = df[df['Handle'] == matching_handle]
                    filtered_df.to_csv(output_csv_path, index=False)

                    if progress_callback:
                        progress_callback(
                            f"Best match found with difference of {best_difference}",
                            95,
                            {
                                'url': best_match[0],
                                'title': best_match[1],
                                'difference': best_difference,
                                'processed': processed,
                                'total_products': total_images,
                                'handle': matching_handle
                            }
                        )

                    # Return the title of the best match
                    return best_match[1] if len(filtered_df) > 0 else None

                if progress_callback:
                    progress_callback("No matches found", 95, {
                        'processed': processed,
                        'total_products': total_images
                    })

                return None

        except Exception as e:
            print(f"Error in find_product_and_export_async: {str(e)}")
            raise

    def filter_sheet_by_handle(self, spreadsheet_id: str, sheet_name: str, handle: str, output_csv_path: str):
        """
        Efficiently filter a large Google Sheet by handle and save to CSV.
        
        :param spreadsheet_id: ID of the Google Sheet
        :param sheet_name: Name of the sheet to filter
        :param handle: Handle to filter by
        :param output_csv_path: Path to save the filtered CSV
        """
        try:
            # Convert handle to string to ensure correct comparison
            handle_str = str(handle)
            
            # Fetch sheet values
            sheet = self.service.spreadsheets()
            
            # Get all values from the sheet
            result = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A:Z'  # Adjust range as needed
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                raise ValueError("No data found in sheet")
            
            # Identify the index of the Handle column
            headers = values[0]
            try:
                handle_col_index = headers.index('Handle')
            except ValueError:
                raise ValueError("Handle column not found in sheet")
            
            # Filter rows (skip header row)
            filtered_rows = [values[0]]  # Always include headers
            filtered_rows.extend([row for row in values[1:] if row[handle_col_index] == handle_str])
            
            # Validate filtering
            if len(filtered_rows) <= 1:
                raise ValueError(f"No rows found for handle: {handle}")
            
            # Write to CSV
            with open(output_csv_path, 'w', newline='') as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerows(filtered_rows)
            
            print(f"Filtered {len(filtered_rows) - 1} rows for handle {handle}")
            return len(filtered_rows) - 1
        
        except Exception as e:
            print(f"Error filtering sheet by handle: {e}")
            raise

    def find_product_and_export(self, 
                            spreadsheet_id: str, 
                            sheet_name: str,
                            local_image_path: str,
                            output_csv_path: str,
                            hash_threshold: int = 0,
                            progress_callback: Callable[[str, int, dict], None] = None) -> Optional[str]:
        """Synchronous wrapper for async find_product_and_export_async."""
        return asyncio.run(self.find_product_and_export_async(
            spreadsheet_id=spreadsheet_id,
            sheet_name=sheet_name,
            local_image_path=local_image_path,
            output_csv_path=output_csv_path,
            hash_threshold=hash_threshold,
            progress_callback=progress_callback
        ))