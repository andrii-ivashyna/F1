# parse_driver.py

import sqlite3
import requests
import re
from bs4 import BeautifulSoup
import config
from config import log, show_progress_bar
from fetch_api import get_country_code
import time

def run_driver_parsers():
    """Runs all driver parsers."""
    log("Processing Driver Data...", 'SUBHEADING')
    parse_driver_f1()

def parse_driver_f1():
    """Parses F1 official site for driver data and updates the database."""
    try:
        conn = sqlite3.connect(config.DB_FILE)
        cursor = conn.cursor()
        db_drivers = cursor.execute("SELECT driver_code, driver_name FROM driver").fetchall()
        
        total_drivers = len(db_drivers)
        start_time_f1_drivers = time.time()
        for i, (driver_code, driver_name) in enumerate(db_drivers):
            show_progress_bar(i + 1, total_drivers, prefix_text='Formula1.com: Driver', start_time=start_time_f1_drivers)
            if not driver_name: continue

            url_name = driver_name.lower().replace(' ', '-')
            f1_url = f"https://www.formula1.com/en/drivers/{url_name}"
            
            try:
                response = requests.get(f1_url, timeout=15)
                if response.status_code == 404:
                    continue
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'lxml')
                
                country_name, driver_image_url, number_image_url = None, None, None
                
                if (elem := soup.find('p', class_=re.compile(r'typography-module_body-xs-semibold'))):
                    country_name = elem.get_text(strip=True)
                if (img := soup.find('img', class_=re.compile(r'absolute.*w-\[222px\].*'))) and (src := img.get('src')):
                    driver_image_url = src if src.startswith('http') else 'https://www.formula1.com' + src
                if (div := soup.find('div', style=re.compile(r'mask-image:\s*url\('))) and (style := div.get('style', '')) and (m := re.search(r'url\((?:&quot;|")?([^"&)]+)(?:&quot;|")?\)', style)):
                    number_image_url = m.group(1) if m.group(1).startswith('http') else 'https://www.formula1.com' + m.group(1)
                
                update_data = {}
                country_code = None
                
                if country_name and (country_code := get_country_code(country_name)):
                    cursor.execute("INSERT OR IGNORE INTO country (country_code, country_name) VALUES (?, ?)", (country_code, country_name))
                    update_data['country_fk'] = country_code
                if driver_image_url:
                    update_data['driver_image_url'] = driver_image_url
                if number_image_url:
                    update_data['number_image_url'] = number_image_url
                
                if update_data:
                    cursor.execute("""
                        UPDATE driver SET 
                        country_fk = COALESCE(?, country_fk), driver_image_url = COALESCE(?, driver_image_url), 
                        number_image_url = COALESCE(?, number_image_url)
                        WHERE driver_code = ?
                    """, (country_code, driver_image_url, number_image_url, driver_code))

            except Exception as e:
                log("Could not fetch or parse data.", 'ERROR', indent=2, data={'url': config.Style.url(f1_url), 'error': str(e)})
        
        conn.commit()
        conn.close()
    except Exception as e:
        log("Could not parse driver data from F1 site.", 'ERROR', data={'reason': str(e)})
