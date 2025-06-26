# parse_driver.py

import sqlite3
import requests
import re
from bs4 import BeautifulSoup
import config
from config import log
from fetch_api import get_country_code

def run_driver_parsers():
    """Runs all driver parsers."""
    log("Enriching Driver Data", 'HEADING')
    parse_driver_f1()

def parse_driver_f1():
    """Parses F1 official site for driver data and updates the database."""
    log("Parsing from Formula1.com", 'SUBHEADING')
    try:
        conn = sqlite3.connect(config.DB_FILE)
        cursor = conn.cursor()
        db_drivers = cursor.execute("SELECT driver_code, driver_name FROM driver").fetchall()
        
        for driver_code, driver_name in db_drivers:
            if not driver_name: continue

            # Generate URL slug from driver name (lowercase with dashes)
            url_name = driver_name.lower().replace(' ', '-')
            f1_url = f"https://www.formula1.com/en/drivers/{url_name}"
            
            log(f"Requesting data for '{driver_name}' from {f1_url}", 'INFO', indent=1)
            
            try:
                response = requests.get(f1_url, timeout=15)
                if response.status_code == 404:
                    log("Page not found (404), skipping.", 'WARNING', indent=2)
                    continue
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'lxml')
                
                country_name = None
                driver_image_url = None
                number_image_url = None
                
                # Find Country from specific paragraph structure
                country_elem = soup.find('p', class_=re.compile(r'typography-module_body-xs-semibold.*typography-module_lg_body-s-compact-semibold'))
                if country_elem:
                    country_name = country_elem.get_text(strip=True)
                
                # Find driver image URL
                driver_img = soup.find('img', class_=re.compile(r'absolute.*md:pt-px-32.*w-\[222px\].*md:w-\[305px\].*lg:w-\[360px\]'))
                if driver_img and (src := driver_img.get('src')):
                    driver_image_url = src if src.startswith('http') else 'https://www.formula1.com' + src
                
                # Find number image URL from div with mask-image style
                # Look for div with the specific class pattern and style containing mask-image
                number_div = soup.find('div', {
                    'class': re.compile(r'w-full.*h-\[342px\].*md:h-\[291px\].*lg:h-\[371px\]'),
                    'style': re.compile(r'mask-image:\s*url\(')
                })
                
                if number_div:
                    style = number_div.get('style', '')
                    # Extract URL from mask-image property, handling both &quot; and regular quotes
                    match = re.search(r'mask-image:\s*url\((?:&quot;|")?([^"&)]+)(?:&quot;|")?\)', style)
                    if match:
                        number_image_url = match.group(1)
                        if not number_image_url.startswith('http'):
                            number_image_url = 'https://www.formula1.com' + number_image_url
                
                update_data = {}
                country_code = None
                
                if country_name:
                    country_code = get_country_code(country_name)
                    if country_code:
                        cursor.execute("INSERT OR IGNORE INTO country (country_code, country_name) VALUES (?, ?)", (country_code, country_name))
                        update_data['country_fk'] = country_code
                
                if driver_image_url:
                    update_data['driver_image_url'] = driver_image_url
                
                if number_image_url:
                    update_data['number_image_url'] = number_image_url
                
                if update_data:
                    log("Found data", 'DATA', indent=2, data=update_data)
                    cursor.execute("""
                        UPDATE driver SET 
                        country_fk = COALESCE(?, country_fk), 
                        driver_image_url = COALESCE(?, driver_image_url), 
                        number_image_url = COALESCE(?, number_image_url)
                        WHERE driver_code = ?
                    """, (country_code, driver_image_url, number_image_url, driver_code))
                else:
                    log("No new data found on page.", 'WARNING', indent=2)
            except Exception as e:
                log("Could not fetch or parse data.", 'ERROR', indent=2, data={'url': config.Style.url(f1_url), 'error': str(e)})
        
        conn.commit()
        conn.close()
    except Exception as e:
        log("Could not parse driver data from F1 site.", 'ERROR', data={'reason': str(e)})
