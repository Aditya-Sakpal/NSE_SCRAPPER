from concurrent.futures import ThreadPoolExecutor
import time
from datetime import datetime, timedelta
import json
import traceback

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

df = pd.read_csv('ind_nifty500list.csv')
symbols = df['Symbol'].tolist()

brsr_reports = {}
annual_reports = {}
announcements = {}

def init_driver():
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless") 
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
        
    driver = webdriver.Chrome(options=options)
    return driver


def extract_brsr_reports(symbol):
    url = f"https://www.nseindia.com/companies-listing/corporate-filings-bussiness-sustainabilitiy-reports?symbol={symbol}&tabIndex=equity"
    driver = init_driver()

    today_last_year = datetime.now().replace(year=datetime.now().year - 1)
    tomorrow_last_year_date = (today_last_year + timedelta(days=1)).strftime('%d-%m-%Y')    
    
    try:
        driver.get(url)
        print(f"Visited {url}")

        try:
            input_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, f"//input[@data-fromdate='{tomorrow_last_year_date}']"))
            )
            driver.execute_script("arguments[0].value = '01-01-1980';", input_element)
            print(f"Set value for symbol {symbol}")
        except Exception as e:
            print(f"Failed to set value for symbol {symbol}: {traceback.format_exc()}")
            raise e

        try:
            company_name_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Company Name']"))
            )

            input_value = driver.execute_script("return arguments[0].value;", company_name_input)
            if not input_value:
                driver.execute_script("arguments[0].value = arguments[1];", company_name_input, symbol)
                print(f"Set 'Company Name' input to {symbol}")
            else:
                print(f"'Company Name' input already has value: {input_value}")
        except Exception as e:
            print(f"Failed to locate or set 'Company Name' input for symbol {symbol}: {traceback.format_exc()}")
            raise e

        try:
            filter_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[class*='filterbtn']"))
            )
            filter_button.click()
            print(f"Clicked filter button for symbol {symbol}")
        except Exception as e:
            print(f"Failed to click filter button for symbol {symbol}: {traceback.format_exc()}")
            raise e

        time.sleep(5)

        try:
            download_link = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "CFBussinessSustainabilitiy-download"))
            )
            download_link.click()
            time.sleep(5)  

            tabs = driver.window_handles
            if len(tabs) > 1:
                driver.switch_to.window(tabs[1])
                redirected_url = driver.current_url
                print(f"Redirected URL for {symbol}: {redirected_url}")
                brsr_reports[symbol] = redirected_url
            else:
                print(f"No new tab opened for {symbol}")

        except Exception as e:
            print(f"Failed to capture download URL for {symbol}: {traceback.format_exc()}")
            raise e

    except Exception as e:
        print(f"Error visiting {url}: {e}")
        raise e
    finally:
        driver.quit()


def extract_annual_reports(symbol):
    url = f"https://www.nseindia.com/companies-listing/corporate-filings-annual-reports?symbol={symbol}&tabIndex=equity"
    driver = init_driver()

    try:
        driver.get(url)
        print(f"Visited {url}")
        
        time.sleep(10)

        try:
            equity_wrapper = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "AREquityWrapper"))
            )
            
            rows = equity_wrapper.find_elements(By.TAG_NAME, "tr")
            if len(rows) <= 1:
                print(f"No data rows found for symbol {symbol}.")
                raise Exception("No data rows found.")
            
            for row in rows[1:]:
                try:
                    tds = row.find_elements(By.TAG_NAME, "td")
                    if len(tds) == 4:
                        name = tds[0].text.strip()  
                        from_year = tds[1].text.strip() 
                        to_year = tds[2].text.strip()  
                        anchor = tds[3].find_element(By.TAG_NAME, "a")  
                        href = anchor.get_attribute("href")

                        print(f"Symbol: {symbol}, Report Name: {name}, From Year: {from_year}, To Year: {to_year}, Link: {href}")

                        if symbol not in annual_reports:
                            annual_reports[symbol] = []
                        annual_reports[symbol].append({
                            "from_year": from_year,
                            "to_year": to_year,
                            "link": href
                        })
                    else:
                        print(f"Row in symbol {symbol} does not have 4 <td> elements.")
                except Exception as e:
                    print(f"Failed to process a row for symbol {symbol}: {e}")
                    raise e

        except Exception as e:
            print(f"Failed to retrieve 'AREquityWrapper' for symbol {symbol}: {traceback.format_exc()}")
            raise e

    except Exception as e:
        print(f"Error visiting {url}: {e}")
        raise e
    finally:
        driver.quit()

def process_announcement_row(tr):
    try:
        tds = tr.find_elements(By.TAG_NAME, "td")

        if len(tds) < 9:
            print(f"Skipping row with insufficient columns: {tr.get_attribute('innerHTML')}")
            return None

        try:
            link = tds[4].find_element(By.TAG_NAME, "a").get_attribute("href")
        except Exception:
            link = ""
        
        return {
            "subject": tds[2].text,
            "details": tds[3].text,
            "link": link,
            "board_cast_date_n_time": tds[5].text,
        }
    except Exception as e:
        print(f"Error processing row: {traceback.format_exc()}")
        raise e

def extract_announcements(symbol):
    try:
        driver = init_driver()
        url = f"https://www.nseindia.com/companies-listing/corporate-filings-announcements?symbol={symbol}"
        
        driver.get(url)
        print(f"Visited {url}")
        time.sleep(10)
        
        table_div = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "table-CFanncEquity"))
        )
        
        tbody = table_div.find_element(By.TAG_NAME, "tbody")
        print("Found the div with ID 'table-CFanncEquity'")
        
        trs = tbody.find_elements(By.TAG_NAME, "tr")
        print(f"Found {len(trs)} rows in the table")
        
        records = []
        
        if len(trs) <= 1:
            print(f"No announcements found for symbol {symbol}")
            raise Exception("No announcements found.")

        with ThreadPoolExecutor(max_workers=1) as executor:
            results = list(executor.map(process_announcement_row, trs[1:]))

        # Filter out None results
        records = [record for record in results if record]
        
        print(f"Found {len(records)} announcements for symbol {symbol}")
        
        announcements[symbol] = records
                
    except Exception as e:
        print(f"Error processing {symbol}: {traceback.format_exc()}")
        print("")
        raise e
    finally:
        driver.quit()



def save_reports_to_json():
    with open('brsr_reports.json', 'w') as f:
        json.dump(brsr_reports, f, indent=4)

    with open('annual_reports.json', 'w') as f:
        json.dump(annual_reports, f, indent=4)
    
    with open('announcements.json', 'w') as f:
        json.dump(announcements, f, indent=4)

def extract_annual_reports_with_retries(symbol, max_retries=3):
    attempts = 0
    success = False

    while attempts < max_retries and not success:
        attempts += 1
        try:
            print(f"Attempt {attempts} for symbol : {symbol} while extracting annual reports")
            extract_annual_reports(symbol)
            success = True  
        except Exception as e:
            print(f"Error on attempt {attempts} for symbol {symbol} while extracting annual reports : {e}")
            if attempts < max_retries:
                print(f"Retrying symbol {symbol}... while extracting annual reports")
            else:
                print(f"Max retries reached for symbol {symbol} while extracting annual reports. Skipping.")

def extract_brsr_reports_with_retries(symbol, max_retries=3):
    attempts = 0
    success = False

    while attempts < max_retries and not success:
        attempts += 1
        try:
            print(f"Attempt {attempts} for symbol: {symbol} while extracting brsr reports")
            extract_brsr_reports(symbol)
            success = True 
        except Exception as e:
            print(f"Error on attempt {attempts} for symbol {symbol}: {e} while extracting brsr reports")
            if attempts < max_retries:
                print(f"Retrying symbol {symbol}... while extracting brsr reports")
            else:
                print(f"Max retries reached for symbol {symbol}. Skipping. while extracting brsr reports")

def extract_announcements_with_retries(symbol, max_retries=3):
    attempts = 0
    success = False

    while attempts < max_retries and not success:
        attempts += 1
        try:
            print(f"Attempt {attempts} for symbol: {symbol} while extracting announcements")
            extract_announcements(symbol)
            success = True 
        except Exception as e:
            print(f"Error on attempt {attempts} for symbol {symbol}: {e} while extracting announcements") 
            if attempts < max_retries:
                print(f"Retrying symbol {symbol}... while extracting announcements")
            else:
                print(f"Max retries reached for symbol {symbol}. Skipping. while extracting announcements")

max_workers = 1


with ThreadPoolExecutor(max_workers=max_workers) as executor:
    executor.map(lambda sym: extract_brsr_reports_with_retries(sym, max_retries=3), symbols[:200])
    executor.map(lambda sym: extract_annual_reports_with_retries(sym, max_retries=3), symbols[:200])
    executor.map(lambda sym: extract_announcements_with_retries(sym, max_retries=3), symbols[:1])
    
save_reports_to_json()
print("All tasks completed.")
print("BrSR Reports:", brsr_reports)
print("Annual Reports:", annual_reports)