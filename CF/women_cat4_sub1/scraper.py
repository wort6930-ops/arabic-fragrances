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
        # Replace multiple consecutive slashes with single slash (except for :// protocol)
        import re
        # Keep the protocol part, then fix double slashes in the path
        if '://' in url:
            parts = url.split('://', 1)
            protocol = parts[0] + '://'
            path = parts[1]
            # Remove consecutive slashes
            path = re.sub(r'/+', '/', path)
            return protocol + path
        return url

    def _extract_image_url(self, image_url: str) -> str:
        """Extract actual image URL from Next.js optimization URLs"""
        if not image_url:
            return image_url
        
        # Check if it's a Next.js image optimization URL
        if '/_next/image/' in image_url:
            try:
                # Extract the 'url' parameter
                if '?url=' in image_url:
                    # The URL parameter is URL-encoded
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
                
                # Only apply infinite scroll logic for product listing pages (URLs with /l/)
                if '/l/' in url:
                    # --- Targeted Infinite Scroll Logic ---
                    logger.info("Handling infinite scroll by counting product containers...")
                    
                    scroll_attempts = 0
                    MAX_SCROLL_ATTEMPTS = 50
                    no_change_count = 0

                    while scroll_attempts < MAX_SCROLL_ATTEMPTS:
                        # Count current products
                        current_count = page.evaluate("document.querySelectorAll('div.single-product-wrap').length")
                        
                        # Scroll the window to the bottom
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        
                        # Wait longer for content to load (increased from 4 to 6 seconds for parallel execution)
                        time.sleep(6)
                        
                        # Try to wait for network idle but don't fail if it times out
                        try:
                            page.wait_for_load_state('networkidle', timeout=8000)
                        except Exception:
                            # Give it extra time if network is still busy
                            time.sleep(3)
                        
                        # Count products again
                        new_count = page.evaluate("document.querySelectorAll('div.single-product-wrap').length")
                        
                        logger.debug(f"Products: {current_count} -> {new_count}")
                        
                        if new_count == current_count:
                            # No new products loaded
                            no_change_count += 1
                            # Increased from 3 to 5 to be more patient
                            if no_change_count >= 5:
                                logger.info(f"Infinite scroll finished. Total products: {new_count}")
                                break
                        else:
                            # New products loaded, reset counter
                            no_change_count = 0
                        
                        scroll_attempts += 1

                    if scroll_attempts >= MAX_SCROLL_ATTEMPTS:
                        logger.warning("Max scroll attempts reached.")
                else:
                    # For non-listing pages (like category pages), just wait for initial load
                    logger.debug("Not a product listing page, skipping infinite scroll.")
                    time.sleep(2)

                html = page.content()
                browser.close()
                return BeautifulSoup(html, 'html.parser')
        except Exception as e:
            logger.error(f"Playwright failed for {url}: {str(e)}", exc_info=True)
            return self._make_request(url)

    def get_categories(self) -> List[Dict[str, str]]:
        """Scrape main categories from the main category page"""
        logger.info("Fetching categories from /ar-kw/women/arabic-fragrances/c/...")
        soup = self._make_request_with_js(f'{self.base_url}/ar-kw/women/arabic-fragrances/c/')
        
        if not soup:
            return []

        categories = []
        seen_urls = set()
        
        # Look for all links in the main category page
        all_links = soup.find_all('a', href=True)
        logger.info(f"Found {len(all_links)} total links on page")
        
        for link in all_links:
            href = link.get('href', '')
            
            # Match pattern: /arabic-fragrances/{category}/l/
            if href and '/arabic-fragrances/' in href and '/l/' in href and '/arabic-fragrances/c' not in href:
                if href.endswith('/l/') or '/l/' in href and (href.split('/l/')[1] == '' or href.split('/l/')[1].startswith('?')):
                    if href not in seen_urls:
                        seen_urls.add(href)
                        
                        # Extract category name from path
                        path_parts = href.strip('/').split('/')
                        if len(path_parts) >= 2:
                            # The part before /l/ is the category name
                            category_name = path_parts[-2]
                            
                            # Get display text from link
                            text = link.get_text(strip=True)
                            
                            if category_name and len(category_name) > 1:
                                full_url = urljoin(self.base_url, href)
                                categories.append({
                                    'name': text if text and len(text) > 0 else category_name,
                                    'url': full_url,
                                    'path': href
                                })
        
        logger.info(f"Found {len(categories)} main categories")
        if categories:
            for cat in categories[:5]:
                logger.info(f"  - {cat['name']}: {cat['url']}")
        
        return categories

    def get_subcategories(self, category_url: str) -> List[Dict[str, str]]:
        """Scrape subcategories from a main category page"""
        logger.info(f"Fetching subcategories from {category_url}")
        soup = self._make_request(category_url)
        
        if not soup:
            return []

        subcategories = []
        seen_urls = set()
        
        # Extract main category name from URL
        path_parts = category_url.strip('/').split('/')
        main_category = path_parts[-2] if len(path_parts) >= 2 else 'unknown'
        
        # Look for all links with pattern /arabic-fragrances/{subcategory}/l/
        all_links = soup.find_all('a', href=True)
        logger.info(f"Scanning {len(all_links)} links for subcategories")
        
        for link in all_links:
            href = link.get('href', '')
            
            # Match pattern: /arabic-fragrances/{subcategory}/l/
            if (href and '/arabic-fragrances/' in href and '/l/' in href and 
                '/arabic-fragrances/c' not in href and href not in seen_urls):
                
                # Extract the subcategory name
                path_parts = href.strip('/').split('/')
                if len(path_parts) >= 2:
                    subcategory_name = path_parts[-2]
                    
                    # Skip if it's the main category page itself
                    if subcategory_name != main_category:
                        seen_urls.add(href)
                        text = link.get_text(strip=True)
                        
                        if subcategory_name and len(subcategory_name) > 1:
                            full_url = urljoin(self.base_url, href)
                            subcategories.append({
                                'name': text if text and len(text) > 0 else subcategory_name,
                                'url': full_url,
                                'path': href
                            })
        
        logger.info(f"Found {len(subcategories)} subcategories")
        if subcategories:
            for sub in subcategories[:5]:
                logger.info(f"  - {sub['name']}: {sub['url']}")
        
        return subcategories

    def get_products(self, category_url: str) -> List[Dict]:
        """Scrape all products from a category page, handling infinite scroll."""
        logger.info(f"Fetching all products from {category_url} with infinite scroll")
        
        # Use Playwright to handle infinite scroll and get the full page content
        soup = self._make_request_with_js(category_url)
        
        if not soup:
            logger.error(f"Failed to load page content for {category_url}")
            return []

        # After scrolling, all products should be in the DOM.
        all_products = self._extract_all_products(soup)
        
        logger.info(f"Found {len(all_products)} total products after scrolling.")
        return all_products
    
    def _extract_products_with_subcategories(self, soup) -> List[Dict]:
        """Extract products from page while trying to identify subcategories"""
        # First try to find products organized by subcategory sections
        products = self._extract_by_sections(soup)
        if products:
            logger.info(f"Extracted {len(products)} products with subcategories")
            return products
        
        # Fallback: extract all products without subcategory grouping
        products = self._extract_all_products(soup)
        if products:
            logger.info(f"Extracted {len(products)} products (no subcategories found)")
        
        return products
    
    def _extract_by_sections(self, soup) -> List[Dict]:
        """Extract products organized by subcategory sections/headers"""
        products = []
        
        # Look for heading elements (h2, h3, h4) that indicate subcategories
        headings = soup.find_all(['h2', 'h3', 'h4'])
        
        for heading in headings:
            heading_text = heading.get_text(strip=True)
            if not heading_text or len(heading_text) < 2:
                continue
            
            # Find the next container with products
            container = heading.find_next(['div', 'section', 'ul', 'ol'])
            if not container:
                continue
            
            # Extract products from this container
            section_products = self._find_products_in_container(container)
            
            if section_products:
                logger.info(f"Section '{heading_text}': {len(section_products)} products")
                
                # Add subcategory info to each product
                for product_data in section_products:
                    if product_data:
                        product_data['subcategory'] = heading_text
                        products.append(product_data)
        
        return products
    
    def _extract_all_products(self, soup) -> List[Dict]:
        """Extract all products from a page by finding product containers."""
        products = []
        seen_urls = set()

        # Each product is in a 'single-product-wrap' div
        product_containers = soup.find_all('div', class_='single-product-wrap')
        logger.info(f"Found {len(product_containers)} product containers.")

        if not product_containers:
            logger.warning("No 'single-product-wrap' containers found.")
            return []

        for container in product_containers:
            try:
                # Find the main product link within the container
                link_elem = container.find('a', href=lambda x: x and '/p/' in str(x))
                if not link_elem or not link_elem.get('href'):
                    continue

                href = link_elem['href']
                full_url = self._clean_url(urljoin(self.base_url, href))

                # Avoid processing duplicate products
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                # Extract details from the container
                details = self._extract_product_details(container)
                if details:
                    products.append(details)

            except Exception as e:
                logger.debug(f"Error processing product container: {str(e)}")
                continue

        logger.info(f"Extracted {len(products)} valid products from containers.")
        return products
    
    def _find_products_in_container(self, container) -> List[Dict]:
        """Find and extract products from a container element"""
        products = []
        
        # Look for actual product links
        product_links = container.find_all('a', href=lambda x: x and '/p/' in str(x) and '/women/' in str(x))
        
        logger.debug(f"Found {len(product_links)} product links in container")
        
        for link in product_links:
            try:
                href = link.get('href', '')
                if not href or '/p/' not in href:
                    continue
                
                name = link.get_text(strip=True)
                if not name or len(name) < 2:
                    continue
                
                # Get image
                img_elem = link.find('img')
                if not img_elem:
                    parent = link.find_parent(['div', 'article', 'li'])
                    if parent:
                        img_elem = parent.find('img')
                
                image_url = None
                if img_elem:
                    image_url = img_elem.get('src') or img_elem.get('data-src')
                    if image_url and ('loader' in image_url.lower() or 'placeholder' in image_url.lower()):
                        image_url = None
                    # Extract actual URL from Next.js optimization URLs
                    if image_url:
                        image_url = self._extract_image_url(image_url)
                
                full_url = urljoin(self.base_url, href)
                full_url = self._clean_url(full_url)
                
                products.append({
                    'name': name,
                    'url': full_url,
                    'image_url': image_url,
                    'price': 'N/A',
                    'brand': 'Unknown'
                })
            except Exception as e:
                logger.debug(f"Error processing container product: {str(e)}")
                continue
        
        return products

    def _extract_product_details(self, product_elem) -> Optional[Dict]:
        """Extract details from a single product element (container)"""
        try:
            # The main link contains the URL and often the name
            link_elem = product_elem.find('a', href=lambda x: x and '/p/' in str(x))
            if not link_elem or not link_elem.get('href'):
                logger.debug("No product link with '/p/' found in container.")
                return None

            product_url = urljoin(self.base_url, link_elem['href'])
            product_url = self._clean_url(product_url)

            # Product name
            name_elem = product_elem.find('span', class_='product-name-plp-h3')
            name = name_elem.get_text(strip=True) if name_elem else 'Unknown'
            if name == 'Unknown':
                # Fallback to find name from link title or text
                name = link_elem.get('title', '') or link_elem.get_text(strip=True)

            # Brand
            brand_elem = product_elem.find('span', class_='brand-name')
            brand = brand_elem.get_text(strip=True) if brand_elem else 'Unknown'

            # Price
            price_elem = product_elem.find('span', class_='new-price')
            price = price_elem.get_text(strip=True) if price_elem else 'N/A'

            # Image URL
            img_elem = product_elem.find('img', class_='img-fluid')
            image_url = None
            if img_elem:
                # Playwright might load the final src, or it might be in data-src
                image_url = img_elem.get('src') or img_elem.get('data-src')
                if image_url:
                    image_url = self._extract_image_url(image_url)

            # Color options (optional)
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
        # Clean the URL first
        product_url = self._clean_url(product_url)
        logger.info(f"Fetching full details from {product_url}")
        soup = self._make_request_with_js(product_url)
        
        if not soup:
            return None
        
        try:
            # Product name
            name_elem = soup.find('h1', class_='product-name-h1')
            name = name_elem.get_text(strip=True) if name_elem else 'Unknown'
            
            # Price (new price after discount)
            price_elem = soup.find('span', class_='new-price')
            price = price_elem.get_text(strip=True) if price_elem else 'N/A'
            
            # Old price (original price before discount)
            old_price_elem = soup.find('span', class_='old-price')
            old_price = old_price_elem.get_text(strip=True) if old_price_elem else price  # If no discount, old_price = new price
            
            # Discount percentage
            discount_elem = soup.find('span', class_='discount-price')
            discount = discount_elem.get_text(strip=True) if discount_elem else 'N/A'
            
            # Brand
            brand_elem = soup.find('a', class_='brand-title')
            brand = brand_elem.get_text(strip=True) if brand_elem else 'Unknown'
            
            # Description
            desc_elem = soup.find('div', class_='content-color')
            description = desc_elem.get_text(strip=True) if desc_elem else 'N/A'
            
            # Rating
            rating_elem = soup.find('span', class_='product-ratting')
            rating = 'N/A'
            if rating_elem:
                # Count filled stars
                filled_stars = len(rating_elem.find_all('span', style=lambda x: x and 'width: 100%' in x))
                rating = f"{filled_stars}/5"
            
            # Review count
            review_elem = soup.find('a', href=lambda x: x and 'review' in str(x).lower())
            reviews = 'N/A'
            if review_elem:
                reviews = review_elem.get_text(strip=True)
            
            # SKU
            sku_elem = soup.find('span', class_='attr-level-val')
            sku = sku_elem.get_text(strip=True) if sku_elem else 'N/A'
            
            # Product image (from details page) - extract from Next.js URLs
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
