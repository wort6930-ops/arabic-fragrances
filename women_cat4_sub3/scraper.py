import requests
from bs4 import BeautifulSoup
import logging
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse, parse_qs, unquote
import time
from config import BASE_URL, REQUEST_TIMEOUT, MAX_RETRIES, RETRY_DELAY

# Try to import Playwright for JavaScript rendering
try:
    from playwright.async_api import async_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BoutiqaatScraper:
    """Web scraper for Boutiqaat arabic fragrances products"""

    def __init__(self):
        self.base_url = BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.playwright_available = HAS_PLAYWRIGHT
        
        if self.playwright_available:
            logger.info("Playwright available for JavaScript rendering")

    def _clean_url(self, url: str) -> str:
        """Clean up URLs by removing double slashes in path"""
        if not url:
            return url
        import re
        if '://' in url:
            parts = url.split('://', 1)
            protocol = parts[0] + '://'
            path = parts[1]
            path = re.sub(r'/+', '/', path)
            return protocol + path
        return url

    def _extract_image_url(self, image_url: str) -> str:
        """Extract actual image URL from Next.js optimization URLs"""
        if not image_url:
            return image_url
        
        if '/_next/image/' in image_url:
            try:
                if '?url=' in image_url:
                    encoded_url = image_url.split('?url=')[1].split('&')[0]
                    actual_url = unquote(encoded_url)
                    logger.debug(f"Extracted image URL from Next.js: {actual_url}")
                    return actual_url
            except Exception as e:
                logger.warning(f"Failed to extract Next.js image URL: {str(e)}")
        
        return image_url

    def _make_request(self, url: str, retries: int = MAX_RETRIES) -> Optional[BeautifulSoup]:
        """Make HTTP request with retry logic"""
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                return BeautifulSoup(response.content, 'html.parser')
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1}/{retries} failed for {url}: {str(e)}")
                if attempt < retries - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    logger.error(f"Failed to fetch {url} after {retries} attempts")
                    return None

    def _make_request_with_js(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch page with JS rendering, handling targeted infinite scroll."""
        if not self.playwright_available:
            logger.warning("Playwright not available, falling back to requests")
            return self._make_request(url)
        
        try:
            from playwright.sync_api import sync_playwright
            
            logger.info(f"Fetching with Playwright: {url}")
            
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                
                page.goto(url, wait_until='load', timeout=90000)
                
                if '/l/' in url:
                    logger.info("Handling infinite scroll by counting product containers...")
                    
                    scroll_attempts = 0
                    MAX_SCROLL_ATTEMPTS = 50
                    no_change_count = 0

                    while scroll_attempts < MAX_SCROLL_ATTEMPTS:
                        current_count = page.evaluate("document.querySelectorAll('div.single-product-wrap').length")
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        time.sleep(6)
                        
                        try:
                            page.wait_for_load_state('networkidle', timeout=8000)
                        except Exception:
                            time.sleep(3)
                        
                        new_count = page.evaluate("document.querySelectorAll('div.single-product-wrap').length")
                        logger.debug(f"Products: {current_count} -> {new_count}")
                        
                        if new_count == current_count:
                            no_change_count += 1
                            if no_change_count >= 5:
                                logger.info(f"Infinite scroll finished. Total products: {new_count}")
                                break
                        else:
                            no_change_count = 0
                        
                        scroll_attempts += 1

                    if scroll_attempts >= MAX_SCROLL_ATTEMPTS:
                        logger.warning("Max scroll attempts reached.")
                else:
                    logger.debug("Not a product listing page, skipping infinite scroll.")
                    time.sleep(2)

                html = page.content()
                browser.close()
                return BeautifulSoup(html, 'html.parser')
        except Exception as e:
            logger.error(f"Playwright failed for {url}: {str(e)}", exc_info=True)
            return self._make_request(url)

    def get_products(self, category_url: str) -> List[Dict]:
        """Scrape all products from a category page, handling infinite scroll."""
        logger.info(f"Fetching all products from {category_url} with infinite scroll")
        soup = self._make_request_with_js(category_url)
        
        if not soup:
            logger.error(f"Failed to load page content for {category_url}")
            return []

        all_products = self._extract_all_products(soup)
        logger.info(f"Found {len(all_products)} total products after scrolling.")
        return all_products
    
    def _extract_all_products(self, soup) -> List[Dict]:
        """Extract all products from a page by finding product containers."""
        products = []
        seen_urls = set()

        product_containers = soup.find_all('div', class_='single-product-wrap')
        logger.info(f"Found {len(product_containers)} product containers.")

        if not product_containers:
            logger.warning("No 'single-product-wrap' containers found.")
            return []

        for container in product_containers:
            try:
                link_elem = container.find('a', href=lambda x: x and '/p/' in str(x))
                if not link_elem or not link_elem.get('href'):
                    continue

                href = link_elem['href']
                full_url = self._clean_url(urljoin(self.base_url, href))

                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                details = self._extract_product_details(container)
                if details:
                    products.append(details)

            except Exception as e:
                logger.debug(f"Error processing product container: {str(e)}")
                continue

        logger.info(f"Extracted {len(products)} valid products from containers.")
        return products

    def _extract_product_details(self, product_elem) -> Optional[Dict]:
        """Extract details from a single product element (container)"""
        try:
            link_elem = product_elem.find('a', href=lambda x: x and '/p/' in str(x))
            if not link_elem or not link_elem.get('href'):
                logger.debug("No product link with '/p/' found in container.")
                return None

            product_url = urljoin(self.base_url, link_elem['href'])
            product_url = self._clean_url(product_url)

            name_elem = product_elem.find('span', class_='product-name-plp-h3')
            name = name_elem.get_text(strip=True) if name_elem else 'Unknown'
            if name == 'Unknown':
                name = link_elem.get('title', '') or link_elem.get_text(strip=True)

            brand_elem = product_elem.find('span', class_='brand-name')
            brand = brand_elem.get_text(strip=True) if brand_elem else 'Unknown'

            price_elem = product_elem.find('span', class_='new-price')
            price = price_elem.get_text(strip=True) if price_elem else 'N/A'

            img_elem = product_elem.find('img', class_='img-fluid')
            image_url = None
            if img_elem:
                image_url = img_elem.get('src') or img_elem.get('data-src')
                if image_url:
                    image_url = self._extract_image_url(image_url)

            colors = 'N/A'
            color_span = product_elem.find('span', text=lambda t: t and ('ألوان' in t or 'colors' in t.lower()))
            if color_span:
                colors = color_span.get_text(strip=True)

            return {
                'name': name,
                'brand': brand,
                'price': price,
                'url': product_url,
                'image_url': image_url,
                'colors': colors
            }
        except Exception as e:
            logger.warning(f"Error extracting product details from container: {str(e)}")
            return None

    def get_product_full_details(self, product_url: str) -> Optional[Dict]:
        """Scrape full details from product page"""
        product_url = self._clean_url(product_url)
        logger.info(f"Fetching full details from {product_url}")
        soup = self._make_request_with_js(product_url)
        
        if not soup:
            return None
        
        try:
            name_elem = soup.find('h1', class_='product-name-h1')
            name = name_elem.get_text(strip=True) if name_elem else 'Unknown'
            
            price_elem = soup.find('span', class_='new-price')
            price = price_elem.get_text(strip=True) if price_elem else 'N/A'
            
            old_price_elem = soup.find('span', class_='old-price')
            old_price = old_price_elem.get_text(strip=True) if old_price_elem else price
            
            discount_elem = soup.find('span', class_='discount-price')
            discount = discount_elem.get_text(strip=True) if discount_elem else 'N/A'
            
            brand_elem = soup.find('a', class_='brand-title')
            brand = brand_elem.get_text(strip=True) if brand_elem else 'Unknown'
            
            desc_elem = soup.find('div', class_='content-color')
            description = desc_elem.get_text(strip=True) if desc_elem else 'N/A'
            
            rating_elem = soup.find('span', class_='product-ratting')
            rating = 'N/A'
            if rating_elem:
                filled_stars = len(rating_elem.find_all('span', style=lambda x: x and 'width: 100%' in x))
                rating = f"{filled_stars}/5"
            
            review_elem = soup.find('a', href=lambda x: x and 'review' in str(x).lower())
            reviews = 'N/A'
            if review_elem:
                reviews = review_elem.get_text(strip=True)
            
            sku_elem = soup.find('span', class_='attr-level-val')
            sku = sku_elem.get_text(strip=True) if sku_elem else 'N/A'
            
            img_elem = soup.find('img', class_='img-fluid')
            main_image = img_elem.get('src') if img_elem else None
            if main_image:
                main_image = self._extract_image_url(main_image)
            
            return {
                'name': name,
                'brand': brand,
                'price': price,
                'old_price': old_price,
                'discount': discount,
                'description': description,
                'rating': rating,
                'reviews': reviews,
                'sku': sku,
                'image_url': main_image,
                'product_url': product_url
            }
        except Exception as e:
            logger.warning(f"Error extracting full details: {str(e)}")
            return None
