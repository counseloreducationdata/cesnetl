# Scrape job postings from [CESNET-L](https://www.cesnet-l.net/)
# Emilio Lehoucq
# Script to run with GitHub Actions

##################################### Importing libraries #####################################
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from datetime import datetime
from time import sleep
import os
from dotenv import load_dotenv
import logging
from datetime import datetime
from shared_scripts.text_extractor import extract_text
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload
from shared_scripts.url_extractor import extract_urls
from shared_scripts.scraper import get_selenium_response
import json
import sys
from shared_scripts.salary_functions import check_salary

##################################### Configure the logging settings #####################################

# Current timestamp
ts = datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Logging configured")

##################################### Defining functions for this script #####################################

# I'm not turning the next three functions into a class with three methods because it doesn't work within
# soup_compilation.find_all('a', href = True, string = Class.method)
def contains_posting(text):
    '''
    Function to filter HTML <a> elements containing postings.

    Input: text in an <a> element.
    Outupt: boolean--True if contains posting, False otherwise.
    '''
    return text and ('faculty' in text.lower() or 'professor' in text.lower() or 'position' in text.lower() or 'instructor' in text.lower())

def contains_plain_text(text):
    '''
    Function to filter HTML <a> elements containing 'text/plain'.

    Input: text in an <a> element.
    Outupt: boolean--True if contains 'text/plain', False otherwise.
    '''
    return text and ('text/plain' in text.lower())

def contains_html(text):
    '''
    Function to filter HTML <a> elements containing 'text/html'.

    Input: text in an <a> element.
    Outupt: boolean--True if contains 'text/html', False otherwise.
    '''
    return text and ('text/html' in text.lower())

def login_cesnet(driver, password):
    """
    Function to log in to the CESNET-L website.

    Inputs:
    - driver: the web driver.
    - password: the password to log in to the website.
    Output: None.

    Dependencies: selenium.webdriver, logging
    """
    # Log in to the website
    # Find password input field and insert password
    driver.find_element("id", "Password").send_keys(password)
    logger.info("Inside login_cesnet: password inserted.")

    # Click log in button
    driver.find_element("name", "e").click()
    logger.info("Inside login_cesnet: clicked log in button.")

def check_login_required(source_code):
    """
    Function to check if the source code indicates that a login is required.

    Input: source code of a webpage.
    Output: boolean--True if login is required, False otherwise.

    Dependencies: logging
    """
    login_text = 'Please enter your email address and your LISTSERV password and click on the "Log In" button.'
    logger.info(f"Inside check_login_required: returning {login_text in source_code}.")
    return login_text in source_code

def upload_file(element_id, file_suffix, content, folder_id, service, logger):
    """
    Function to upload a file to Google Drive.

    Inputs:
    - element_id: ID of the job post
    - file_suffix: suffix of the file name
    - content: content of the file
    - folder_id: ID of the folder in Google Drive
    - service: service for Google Drive
    - logger: logger

    Outputs: None

    Dependencies: from googleapiclient.http import MediaFileUpload, os
    """
    
    logger.info(f"Inside upload_file: uploading ID {element_id} to Google Drive.")

    try:
        # Prepare the file name
        file_name = f"{element_id}_{file_suffix}.txt"
        logger.info(f"Inside upload_file: prepared the name of the file for the {file_suffix}")

        # Write the content to a temporary file
        with open(file_name, 'w') as temp_file:
            temp_file.write(content)
        logger.info(f"Inside upload_file: wrote the {file_suffix} string to a temporary file")

        # Prepare the file metadata
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]
        }
        logger.info(f"Inside upload_file: prepared the file metadata for the {file_suffix}")

        # Prepare the file media
        media = MediaFileUpload(file_name, mimetype='text/plain')
        logger.info(f"Inside upload_file: prepared the file media for the {file_suffix}")

        # Upload the file to the Drive folder
        service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        logger.info(f"Inside upload_file: uploaded the file to the shared folder for the {file_suffix}")

        # Remove the temporary file after uploading
        os.remove(file_name)
        logger.info(f"Inside upload_file: removed the temporary file after uploading for the {file_suffix}")
    
    except Exception as e:
        logger.info(f"Inside upload_file: something went wrong. Error: {e}")

    return None

logger.info('Functions defined.')

##################################### Setting parameters #####################################

# Define https://listserv.kent.edu/ credentials
load_dotenv()
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')
logger.info("Username and password obtained.")

# Set sleep time (this is a lot, but the website is slow sometimes)
sleep_time = 15

# Set number of tries
ntries = 15

# Set a delay between retries
retry_delay = 15

# Define URL base
url_base = 'https://listserv.kent.edu'

# Define URL login
url_login = "https://listserv.kent.edu/cgi-bin/wa.exe?LOGON"

logger.info('All parameters set.')

##################################### SETTING UP GOOGLE APIS AND GET THE COMPILATIONS THAT I ALREADY SCRAPED #####################################

# LOCAL MACHINE -- Set the environment variable for the service account credentials 
#TODO: comment for GH Actions
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

# Authenticate using the service account
# LOCAL MACHINE
#TODO: comment for GH Actions
# credentials = service_account.Credentials.from_service_account_file(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
# GITHUB ACTIONS
# TODO: uncomment for GH Actions
credentials = service_account.Credentials.from_service_account_info(json.loads(os.getenv('GOOGLE_APPLICATION_CREDENTIALS')))
logger.info("Authenticated with Google Sheets")

# Create service
service = build("sheets", "v4", credentials=credentials)
logger.info("Created service for Google Sheets")

# Get the values from the Google Sheet with the postings
# https://docs.google.com/spreadsheets/d/1APvXQ2H1MWvpk3T7mHTyr4rkDEIOgZYZplK3a2XNspI/edit?gid=0#gid=0
spreadsheet_postings_id = "1APvXQ2H1MWvpk3T7mHTyr4rkDEIOgZYZplK3a2XNspI"
result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_postings_id, range='B:B').execute()
rows = result.get("values", []) # Example output: [['test1'], ['abc'], ['123']]
logger.info("Got data from Google Sheets with the postings")

# Get list of weeks
weeks = list(dict.fromkeys([row[0] for row in rows]))
logger.info(f"List of weeks obtained. Last three weeks: {weeks[-3:]}.")

# Get the last value from the compilations
last_compilation_collected = weeks[-1]
logger.info(f"Last compilation collected obtained (last_compilation_collected): {last_compilation_collected}.")

# Get the previous to last value from the compilations
previous_to_last_compilation_collected = weeks[-2]
logger.info(f"Previous to last compilation collected obtained (previous_to_last_compilation_collected): {previous_to_last_compilation_collected}.")

# Get number of existing compilations
n_compilations = len(rows)
logger.info(f"Number of existing compilations obtained: {n_compilations}.")

# Get the values from the Google Sheet with the URLs inside the messages
# https://docs.google.com/spreadsheets/d/1Ao34BRLA9bFZ-I-koC4Qd1kGSP65X4akcOF3SPhuT18/edit?gid=0#gid=0
spreadsheet_urls_id = "1Ao34BRLA9bFZ-I-koC4Qd1kGSP65X4akcOF3SPhuT18"
result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_urls_id, range='A:A').execute()
rows = result.get("values", []) # Example output: [['test1'], ['abc'], ['123']]
logger.info("Got data from Google Sheets with the URLs inside the messages")

# Get the number of existing URLs inside the messages
n_urls = len(rows)
logger.info(f"Number of existing URLs inside the messages obtained: {n_urls}.")

##################################### Scrape the compilation #####################################

# Initialize the web driver
options = Options()
options.add_argument("--headless") # TODO: GITHUB ACTIONS UNCOMMENT
driver = webdriver.Chrome(options=options)
logger.info("Web driver initialized.")

# Retry block in case of failure
for attempt in range(ntries):

    logger.info(f"First re-try block. Attempt {attempt + 1}.")

    try:
        # Go to the login URL
        driver.get(url_login)
        logger.info(f"Web driver went to the login URL: {url_login}.")
        sleep(sleep_time)

        # Log in to the website
        # Not using the function to login because here it requires username in addition to password
        # Find username input field and insert username
        driver.find_element("id", "Email Address").send_keys(username)
        logger.info("Inside login_cesnet: username inserted.")

        # Find password input field and insert password
        driver.find_element("id", "Password").send_keys(password)
        logger.info("Inside login_cesnet: password inserted.")

        # Click log in button
        driver.find_element("name", "e").click()
        logger.info("Inside login_cesnet: clicked log in button.")
        sleep(sleep_time)

        # Find the CESNET-L listserv and click on it to get to the archive
        cesnet_archive_element = driver.find_element(By.LINK_TEXT, "CESNET-L")
        cesnet_archive_element.click()
        logger.info("CESNET-L listserv found and clicked.")
        sleep(sleep_time)

        # Find all <li> elements
        li_elements = driver.find_elements(By.CSS_SELECTOR, "li")
        logger.info("All <li> elements found.")

        # Get the text of each compilation
        li_elements_text = [li.text for li in li_elements]
        logger.info("Text of each compilation obtained.")

        # Get the previous to latest compilation
        previous_to_latest_compilation = li_elements_text[1]
        logger.info(f"Previous to latest compilation obtained (previous_to_latest_compilation): {previous_to_latest_compilation}.")

        # Get the previous to previous to latest compilation
        previous_to_previous_to_latest_compilation = li_elements_text[2]
        logger.info(f"Previous to previous to latest compilation obtained (previous_to_previous_to_latest_compilation): {previous_to_previous_to_latest_compilation}.")

        # If I already have the previous to latest compilation and the previous to previous to latest compilation, end the script
        if previous_to_latest_compilation == last_compilation_collected and previous_to_previous_to_latest_compilation == previous_to_last_compilation_collected:
            logger.info("Previous to latest compilation and previous to previous to latest compilation are the same as the ones I collected. Ending the script.")
            sys.exit()
        # If I don't have the previous to latest compilation and the previous to previous to latest compilation
        else:
            logger.info("Missing at least one compilation. Continuing with the script.")

            # Get URL for the previous to latest compilation
            previous_to_latest_compilation_url = li_elements[1].find_element(By.TAG_NAME, "a").get_attribute("href")
            logger.info(f'URL of previous to latest compilation obtained: {previous_to_latest_compilation_url}.')

            # Get URL for the previous to previous to latest compilation
            previous_to_previous_to_latest_compilation_url = li_elements[2].find_element(By.TAG_NAME, "a").get_attribute("href")
            logger.info(f'URL of previous to previous to latest compilation obtained: {previous_to_previous_to_latest_compilation_url}.')

            # Define list to store info about the compilation(s) that I'm missing
            missing_compilations_data = []
            logger.info("List to store info about the compilation(s) that I'm missing initialized.")

            # If I'm missing the previous to previous to latest compilation
            if previous_to_previous_to_latest_compilation != last_compilation_collected:
                logger.info(f"{previous_to_previous_to_latest_compilation} (previous_to_previous_to_latest_compilation) != {last_compilation_collected} (last_compilation_collected).")

                # Create list to store the data for the weekly compilation
                missing_compilation = []

                # Go to the previous to previous to latest compilation
                driver.get(previous_to_previous_to_latest_compilation_url)
                sleep(sleep_time)
                logger.info(f"Web driver went to the previous to previous to latest compilation.")

                # Check if login is required
                if check_login_required(driver.page_source):
                    logger.info("Login required. Logging in.")
                    # Log in to the website
                    login_cesnet(driver, password)
                    sleep(sleep_time)

                # Get the source code for the compilation
                source_code = driver.page_source
                logger.info("Source code for the compilation obtained.")

                # Parse the source code for the compilation
                soup_compilation = BeautifulSoup(source_code, 'html.parser')
                logger.info("Source code for the compilation parsed.")

                # Get the week of the compilation (the second h2 element)
                week = soup_compilation.find_all('h2')[1].text.strip()
                logger.info(f"Week of the compilation obtained: {week}.")

                # Append the week of the compilation to the list
                missing_compilation.append(week)
                logger.info("Week of the compilation appended to the list.")

                # Find the URLs for the postings
                urls = [a['href'] for a in soup_compilation.find_all('a', href=True, string=contains_posting) if 'https' in a['href']]
                logger.info("URLs for the postings obtained.")

                # Append the URLs for the postings to the list
                missing_compilation.append(urls)
                logger.info("URLs for the postings appended to the list.")

                # Append the data for the weekly compilation to the list
                missing_compilations_data.append(missing_compilation)
                logger.info("Data for the weekly compilation appended to the list.")
            else:
                logger.info("I'm not missing the previous to previous to latest compilation.")

            # There's a fair amount of repetition here, but it's only twice, so that's ok
            # If I'm missing the previous to latest compilation
            if previous_to_latest_compilation != last_compilation_collected:
                logger.info(f"{previous_to_latest_compilation} (previous_to_latest_compilation) != {last_compilation_collected} (last_compilation_collected).")

                # Create list to store the data for the weekly compilation
                missing_compilation = []

                # Go to the previous to latest compilation
                driver.get(previous_to_latest_compilation_url)
                sleep(sleep_time)
                logger.info(f"Web driver went to the previous to latest compilation.")

                # Check if login is required
                if check_login_required(driver.page_source):
                    logger.info("Login required. Logging in.")
                    # Log in to the website
                    login_cesnet(driver, password)
                    sleep(sleep_time)

                # Get the source code for the compilation
                source_code = driver.page_source
                logger.info("Source code for the compilation obtained.")

                # Parse the source code for the compilation
                soup_compilation = BeautifulSoup(source_code, 'html.parser')
                logger.info("Source code for the compilation parsed.")

                # Get the week of the compilation (the second h2 element)
                week = soup_compilation.find_all('h2')[1].text.strip()  # e.g., August 2024, Week 3
                logger.info(f"Week of the compilation obtained: {week}.")

                # Append the week of the compilation to the list
                missing_compilation.append(week)
                logger.info("Week of the compilation appended to the list.")

                # Find the URLs for the postings
                urls = [a['href'] for a in soup_compilation.find_all('a', href=True, string=contains_posting) if 'https' in a['href']]
                logger.info("URLs for the postings obtained.")

                # Append the URLs for the postings to the list
                missing_compilation.append(urls)
                logger.info("URLs for the postings appended to the list.")

                # Append the data for the weekly compilation to the list
                missing_compilations_data.append(missing_compilation)
                logger.info("Data for the weekly compilation appended to the list.")
            else:
                logger.info("I'm not missing the previous to latest compilation.")

            # Break the loop if successful
            logger.info("First re-try block successful. About to break the loop.")
            break

    except Exception as e:
        logger.info(f"First re-try block. Attempt {attempt + 1} failed. Error: {e}")

        if attempt < ntries - 1:  # Check if we have retries left
            logger.info("First re-try block. Sleeping before retry.")
            sleep(retry_delay)
        else:
            logger.info("First re-try block. All retries exhausted.")
            raise  # Re-raise the last exception if all retries are exhausted

##################################### Scrape the messages within the compilation #####################################

# Create list to store the data for the weekly compilations
data_compilation = []

# Iterate over the missing compilations
for missing_compilation in missing_compilations_data:
    logger.info("Starting loop for the missing compilations.")

    # Define variable with the week of the compilation
    week = missing_compilation[0]
    logger.info(f"Week of the compilation: {week}.")

    # Define variable with the URLs for the postings
    urls = missing_compilation[1]
    logger.info(f"URLs for the postings obtained. len(urls): {len(urls)}.")

    # Check that there are actually URLs
    if len(urls) > 0:
        logger.info(f"Checked if if len(urls) > 0. First URL: {urls[0]}.")
    
        # Iterate over the URLs for the postings
        for url in urls:
            logger.info(f"Starting loop for the URLs for the postings. URL: {url}.")
    
            # Create list to store the data for the posting
            data_posting = []
    
            # Store the ID for the posting
            data_posting.append(n_compilations + len(data_compilation) + 1)
    
            # Store the week of the compilation
            data_posting.append(week)
    
            # Store the URL for the posting
            data_posting.append(url)
    
            # Store the current timestamp
            data_posting.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
            logger.info("Data for the posting initialized.")
    
            # Retry block in case of failure
            for attempt in range(ntries):
                logger.info(f"Second re-try block. Attempt {attempt + 1}.")
    
                try:
                    # Go to the URL of the posting
                    driver.get(url)
                    logger.info("Web driver went to the URL of the posting.")
                    sleep(sleep_time)
    
                    # Check if login is required
                    if check_login_required(driver.page_source):
                        logger.info("Login required. Logging in.")
                        # Log in to the website
                        login_cesnet(driver, password)
                        sleep(sleep_time)
    
                    # Get the source code for the posting
                    source_code_posting = driver.page_source
                    logger.info("Source code for the posting obtained.")
    
                    # Parse the source code for the posting
                    soup_posting = BeautifulSoup(source_code_posting, 'html.parser')
                    logger.info("Source code for the posting parsed.")
    
                    # Try getting the plain text data
                    try:
                        logger.info("Trying to find the plain text message.")
    
                        # Find the URL of the plain text message
                        url_message = url_base + soup_posting.find('a', href = True, string = contains_plain_text)['href']
                        logger.info(f"URL for the plain text message obtained: {url_message}.")
    
                        # Go to the message URL
                        driver.get(url_message)
                        logger.info("Web driver went to the message URL.")
                        sleep(sleep_time)
    
                        # Check if login is required
                        if check_login_required(driver.page_source):
                            logger.info("Login required. Logging in.")
                            # Log in to the website
                            login_cesnet(driver, password)
                            sleep(sleep_time)
    
                        # Get the source code for the message
                        source_code_message = driver.page_source
                        logger.info("Source code for the message obtained.")
    
                        # Extract the text from the source code of the message
                        text = extract_text(source_code_message)
                        logger.info("Text for the message extracted.")
    
                        # Check if there seems to be salary info
                        salary_flag = check_salary(text)
                        logger.info(f"salary_flag: {salary_flag}.")
            
                        # Store salary flag
                        data_posting.append(salary_flag)
                        logger.info("Salary flag appended to the list.")
    
                        # Store the source code for the message
                        data_posting.append(source_code_message)
                        logger.info("Source code for the message stored.")
                        
                        # Store the text for the message
                        data_posting.append(text)
                        logger.info("Text for the message stored.")
    
                    except:
                        logger.info("Something went wrong with finding the plain text message. Trying with the HTML message.")
    
                        # Get the HTML data (if there's no plain text, there's HTML)
                        # Pretty much the same as for the plain text. Not creating a function since it's only two. Maybe I should though...
                        # Find the URL of the HTML message
                        url_message = url_base + soup_posting.find('a', href = True, string = contains_html)['href']
                        logger.info(f"URL for the HTML message obtained: {url_message}.")
    
                        # Go to the message URL
                        driver.get(url_message)
                        logger.info("Web driver went to the message URL.")
                        sleep(sleep_time)
    
                        # Check if login is required
                        if check_login_required(driver.page_source):
                            logger.info("Login required. Logging in.")
                            # Log in to the website
                            login_cesnet(driver, password)
                            sleep(sleep_time)
    
                        # Get the source code for the message
                        source_code_message = driver.page_source
                        logger.info("Source code for the message obtained.")
    
                        # Extract the text from the source code of the message
                        text = extract_text(source_code_message)
                        logger.info("Text for the message extracted.")
    
                        # Check if there seems to be salary info
                        salary_flag = check_salary(text)
                        logger.info(f"salary_flag: {salary_flag}.")
            
                        # Store salary flag
                        data_posting.append(salary_flag)
                        logger.info("Salary flag appended to the list.")
                        
                        # Store the source code for the message
                        data_posting.append(source_code_message)
                        logger.info("Source code for the message stored.")
                        
                        # Store the text for the message
                        data_posting.append(text)
                        logger.info("Text for the message stored.")
    
                    # Break the loop if successful
                    logger.info("Second re-try block successful. About to break the loop.")
                    break
    
                except Exception as e:
                    logger.info(f"Second re-try block. Attempt {attempt + 1} failed. Error: {e}")
    
                    if attempt < ntries - 1:  # Check if we have retries left
                        logger.info("Second re-try block. Sleeping before retry.")
                        sleep(retry_delay)
                    else:
                        logger.info("Second re-try block. All retries exhausted.")
    
                        # Store 'FAILURE' for the salary flag
                        data_posting.append('FAILURE')
                        
                        # Store 'FAILURE' for the source code for the message
                        data_posting.append('FAILURE')
    
                        # Store 'FAILURE' for the text for the message
                        data_posting.append('FAILURE')
    
                        logger.info("Data for the posting stored as 'FAILURE'.")
    
                        # Append the data for the posting to the data for the compilation
                        data_compilation.append(data_posting)
                        logger.info("Data for the posting appended to the data for the compilation after failure.")
    
            # Append the data for the posting to the data for the compilation
            data_compilation.append(data_posting)
            logger.info("Data for the posting appended to the data for the compilation after success.")

    # If there are no URLs
    else:
        # Add data for two purposes:
        # 1. To add the specific week to the spreadsheet, so that it's in the record
        # 2. So that the script keeps running if a week there are no postings (e.g., December 2024, Week 5), but the next week there are
        data_compilation.append([n_compilations + len(data_compilation) + 1, week, url, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), None, None, None]) # None instead of salary flag, source code, and text
        
# Quit the driver
driver.quit()
logger.info("Web driver quit.")

##################################### Scrape the URLs inside the messages #####################################

# Create list to store data for the compilation
data_compilation_urls_inside_messages = []

# Iterate over the data for the compilation
for data_posting in data_compilation:
    logger.info("Starting loop to get the URLs inside the messages for the compilation.")

    # Get the text of the message
    text = data_posting[-1]
    logger.info("Text of the message obtained.")

    # Extract the URLs from the text of the message
    urls_in_message = extract_urls(text)
    logger.info("URLs in the message extracted.")

    # Iterate over the URLs found
    for url_in_message in urls_in_message:
        logger.info(f"Starting loop for the URLs in the message. URL: {url_in_message}.")

        # Create a list to store the data for the URL in the message
        data_url_in_message = []

        # Store the ID for the posting
        data_url_in_message.append(data_posting[0])

        # Store the ID for the URL in the message
        data_url_in_message.append(n_urls + len(data_compilation_urls_inside_messages) + 1)

        # Store the week of the compilation
        data_url_in_message.append(data_posting[1])

        # Store the URL for the posting
        data_url_in_message.append(data_posting[2])

        # Store the URL in the message
        data_url_in_message.append(url_in_message)

        # Store the current timestamp
        data_url_in_message.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        logger.info("Data for the URL in the message initialized.")

        # Scrape the URL
        source_code_url_in_message = get_selenium_response(url_in_message)
        logger.info(f"Source code for the URL in the message obtained.")

        # Extract the text from the response
        text_in_url_in_message = extract_text(source_code_url_in_message)
        logger.info(f"Text extracted from the URL in the message.")

        # Check if there seems to be salary info
        salary_flag = check_salary(text_in_url_in_message)
        logger.info(f"salary_flag: {salary_flag}.")

        # Store salary flag
        data_url_in_message.append(salary_flag)
        logger.info("Salary flag appended to the list.")
        
        # Store the source code for the URL in the message
        data_url_in_message.append(source_code_url_in_message)
        logger.info(f"Source code for the URL in the message stored.")
        
        # Store the text for the URL in the message
        data_url_in_message.append(text_in_url_in_message)
        logger.info(f"Text for the URL in the message stored.")

        # Append the data for the URL in the message to the data for the compilation
        data_compilation_urls_inside_messages.append(data_url_in_message)
        logger.info("Data for the URL in the message appended to the data for the compilation.")

logger.info("Scraping finished.")

####################################### WRITE NEW DATA TO GOOGLE SHEETS #######################################

# Data for the postings

# Retry block in case of failure
for attempt in range(ntries):

    try:
        logger.info(f"Re-try block for data for postings (Google Sheets). Attempt {attempt + 1}.")

        # Range to write the data
        range_sheet="A"+str(n_compilations+1)+":E10000000"
        logger.info("Prepared range to write the data for the postings")

        # Body of the request
        # The last two elements of each element in data are the source code and the text, which are not written to the Google Sheet
        body={"values": [element[:-2] for element in data_compilation]}
        logger.info("Prepared body of the request for the postings")

        # Execute the request
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_postings_id,
            range=range_sheet,
            valueInputOption="USER_ENTERED",
            body=body
            ).execute()
        logger.info("Wrote new data to Google Sheets for the postings")

        # Break the loop if successful
        logger.info("Re-try block for data for postings successful. About to break the loop.")
        break
    
    except Exception as e:
        logger.info(f"Re-try block for data for postings. Attempt {attempt + 1} failed. Error: {e}")

        if attempt < ntries - 1:
            logger.info("Re-try block for data for postings. Sleeping before retry.")
            sleep(retry_delay)
        else:
            logger.info("Re-try block for data for postings. All retries exhausted.")
            raise

# Data for the URLs inside the messages

# Retry block in case of failure
for attempt in range(ntries):

    try:
        logger.info(f"Re-try block for data for URLs inside the messages (Google Sheets). Attempt {attempt + 1}.")

        # Range to write the data
        range_sheet="A"+str(n_urls+1)+":G10000000"
        logger.info("Prepared range to write the data for the URLs inside the messages")

        # Body of the request
        # The last two elements of each element in data are the source code and the text, which are not written to the Google Sheet
        body={"values": [element[:-2] for element in data_compilation_urls_inside_messages]}
        logger.info("Prepared body of the request for the URLs inside the messages")

        # Execute the request
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_urls_id,
            range=range_sheet,
            valueInputOption="USER_ENTERED",
            body=body
            ).execute()
        logger.info("Wrote new data to Google Sheets for the URLs inside the messages")

        # Break the loop if successful
        logger.info("Re-try block for data for URLs inside the messages successful. About to break the loop.")
        break
    
    except Exception as e:
        logger.info(f"Re-try block for data for URLs inside the messages. Attempt {attempt + 1} failed. Error: {e}")

        if attempt < ntries - 1:
            logger.info("Re-try block for data for URLs inside the messages. Sleeping before retry.")
            sleep(retry_delay)
        else:
            logger.info("Re-try block for data for URLs inside the messages. All retries exhausted.")
            raise

####################################### WRITE NEW DATA TO GOOGLE DRIVE #######################################

# Note: if there's already a file with the same name in the folder, this code will add another with the same name

# Data for the postings

# Folder ID
# https://drive.google.com/drive/u/4/folders/1qx2CMXHTj0Km3LGaD7K1dB-2jhLBya6y
folder_id = "1qx2CMXHTj0Km3LGaD7K1dB-2jhLBya6y" 

# Retry block in case of failure
for attempt in range(ntries):

    try:
        logger.info(f"Re-try block for data for the postings (Google Drive). Attempt {attempt + 1}.")

        # Authenticate using the service account (for Google Drive, not Sheets)
        service = build('drive', 'v3', credentials=credentials)
        logger.info("Created service for Google Drive")
                    
        # Iterate over each of the job posts (list)
        for element in data_compilation:
            logger.info("Iterating over each of the postings")
            # Get the source code of the job post
            source_code = element[-2]
            logger.info("Got the source code of the post")
            # Get the text of the job post
            text = element[-1]
            logger.info("Got the text of the post")
            # Upload the source code to Google Drive
            upload_file(element[0], "source_code", source_code, folder_id, service, logger)
            # Upload the text to Google Drive
            upload_file(element[0], "text", text, folder_id, service, logger)
        logger.info("Wrote new data for the postings (if available) to Google Drive.")

        # Break the loop if successful
        logger.info("Re-try block for data for the postings successful. About to break the loop.")
        break

    except Exception as e:
        logger.info(f"Re-try block for data for the postings. Attempt {attempt + 1} failed. Error: {e}")

        if attempt < ntries - 1:
            logger.info("Re-try block for data for the postings. Sleeping before retry.")
            sleep(retry_delay)
        else:
            logger.info("Re-try block for data for the postings. All retries exhausted.")
            raise

# Data for the URLs inside the messages

# Folder ID
# https://drive.google.com/drive/u/4/folders/1du_dluC7hiGmk4EuQHCCH8Y0Rw9zxsmg
folder_id = "1du_dluC7hiGmk4EuQHCCH8Y0Rw9zxsmg"

# Retry block in case of failure
for attempt in range(ntries):

    try:
        logger.info(f"Re-try block for data for the URLs inside the messages (Google Drive). Attempt {attempt + 1}.")

        # Iterate over each of the URLs inside the messages (list)
        for element in data_compilation_urls_inside_messages:
            logger.info("Iterating over each of the URLs inside the messages")
            # Get the source code of the URL inside the message
            source_code = element[-2]
            logger.info("Got the source code of the URL inside the message")
            # Get the text of the URL inside the message
            text = element[-1]
            logger.info("Got the text of the URL inside the message")
            # Upload the source code to Google Drive
            upload_file(element[1], "source_code", source_code, folder_id, service, logger)
            # Upload the text to Google Drive
            upload_file(element[1], "text", text, folder_id, service, logger)
        logger.info("Wrote new data for the URLs inside the messages (if available) to Google Drive.")

        # Break the loop if successful
        logger.info("Re-try block for data for the URLs inside the messages successful. About to break the loop.")
        break

    except Exception as e:
        logger.info(f"Re-try block for data for the URLs inside the messages. Attempt {attempt + 1} failed. Error: {e}")

        if attempt < ntries - 1:
            logger.info("Re-try block for data for the URLs inside the messages. Sleeping before retry.")
            sleep(retry_delay)
        else:
            logger.info("Re-try block for data for the URLs inside the messages. All retries exhausted.")
            raise

logger.info("Script finished successfully.")
