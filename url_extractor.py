# Module to extract URLs from text strings
# Emilio Lehoucq

# Import libraries

import re
from urllib.parse import urlparse

# Define functions

def is_valid_url(url):
    """
    Function to check if a given string is a valid URL.

    Input: url (str): A string containing a URL.
    Output: valid (bool): True if the input is a valid URL, False otherwise.

    Dependencies: urllib.parse.urlparse

    Taken from https://proxiesapi.com/articles/extracting-urls-from-text-in-python
    """
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False
    
def extract_urls(text):
    """
    Function to extract valid URLs from a given text string.

    Input: text (str): A string containing text with URLs.
    Output: extracted_urls (list): A list of URLs extracted from the input text.

    Dependencies: re and from urllib.parse import urlparse

    Started from here and used the help of ChatGPT and GitHub Copilot: https://stackoverflow.com/questions/839994/extracting-a-url-in-python
    """

    # Check if the input is a string
    if not isinstance(text, str):
        raise TypeError("Input must be a string.")
    
    # Regular expression pattern to match URLs
    url_pattern = r"(?:https?://|www\.|ftp://)[^\s\"]+"

    # Find all URLs in the input text
    extracted_urls = re.findall(url_pattern, text, re.IGNORECASE)

    # # If any of the extracted URLs end with a punctuation mark or other unwanted symbol, remove it
    # extracted_urls = [re.sub(r'[.,!?;)\]]$', '', url) for url in extracted_urls]

    # Clean for unwanted characters at the end
    # A problem is if the URL actually ends with any of these
    unwanted_chars = {'.', ',', '!', '?', ';', ')', ']'}
    extracted_urls = [url.rstrip(''.join(unwanted_chars)) for url in extracted_urls]

    # If any of the extracted URLs doesn't include a valid domain, remove it
    # TODO: check with Gideon
    # valid_domains = ['.com', '.edu', '.org', '.net', '.gov', '.int', '.mil']
    valid_domains = ['.edu']
    extracted_urls = [url for url in extracted_urls if any(domain in url.lower() for domain in valid_domains)]

    # If there are any of the common valid_domains followed immediately by http, https, www, or ftp (or with a comma in between), split the string between the valid domain and the http, https, www, or ftp and keep both URLs
    # This only works for two URLs in the string, it fails with three
    for domain in valid_domains:
        for url in extracted_urls:
            if domain in url:
                split_part = url.split(domain)[1]
                if split_part.startswith(('http://', 'https://', 'www.', 'ftp://', ',')):
                    extracted_urls[extracted_urls.index(url)] = url.split(domain)[0] + domain
                    extracted_urls.append(split_part.lstrip(',') + domain)

    # If the extracted URL starts with 'www', add 'http://' to the beginning
    # Is this correct? Or should it be https instead?
    extracted_urls = ['http://' + url if url.startswith('www.') else url for url in extracted_urls]

    # Include only valid URLs
    extracted_urls = [url for url in extracted_urls if is_valid_url(url)]

    # Exclude URLs that include listserv.kent.edu
    extracted_urls = [url for url in extracted_urls if 'listserv.kent.edu' not in url]

    # Return the list of extracted URLs
    return extracted_urls

if __name__ == '__main__':
    print("Running script as main program.")

    # # https://chatgpt.com/share/6708180c-4acc-8004-82a4-6fab17a13976

    # # Case 1: Test with an empty string
    # print("Case 1")
    # print(extract_urls(''))
    # assert extract_urls('') == []
    # print("Case 1 passed.")

    # # Case 2: Test with a string without any URLs
    # print("Case 2")
    # print(extract_urls('This is a test string with no URLs.'))
    # assert extract_urls('This is a test string with no URLs.') == []
    # print("Case 2 passed.")

    # # Case 3: Test with a valid URL starting with 'http'
    # print("Case 3")
    # print(extract_urls('Here is a link: http://example.com'))
    # assert extract_urls('Here is a link: http://example.com') == ['http://example.com']
    # print("Case 3 passed.")

    # # Case 4: Test with a valid URL starting with 'https'
    # print("Case 4")
    # print(extract_urls('Here is a secure link: https://example.com'))
    # assert extract_urls('Here is a secure link: https://example.com') == ['https://example.com']
    # print("Case 4 passed.")

    # # Case 5: Test with a URL starting with 'www'
    # print("Case 5")
    # print(extract_urls('Go to www.example.com for more info.'))
    # assert extract_urls('Go to www.example.com for more info.') == ['http://www.example.com']
    # print("Case 5 passed.")

    # # Case 6: Test with multiple URLs in one string
    # print("Case 6")
    # print(extract_urls('Check these: http://example.com, https://example.org, www.example.net'))
    # assert extract_urls('Check these: http://example.com, https://example.org, www.example.net') == ['http://example.com', 'https://example.org', 'http://www.example.net']
    # print("Case 6 passed.")

    # # Case 7: Test with URLs followed by punctuation
    # print("Case 7")
    # print(extract_urls('Visit www.example.com! Or check out https://example.com, for updates.'))
    # assert extract_urls('Visit www.example.com! Or check out https://example.com, for updates.') == ['http://www.example.com', 'https://example.com']
    # print("Case 7 passed.")

    # # Case 8: Test with URLs ending in different domains
    # print("Case 8")
    # print(extract_urls('Visit http://example.org and http://example.edu for details.'))
    # assert extract_urls('Visit http://example.org and http://example.edu for details.') == ['http://example.org', 'http://example.edu']
    # print("Case 8 passed.")

    # # Case 9: Test with URLs ending with a slash
    # print("Case 9")
    # print(extract_urls('Check https://example.com/ for the homepage.'))
    # assert extract_urls('Check https://example.com/ for the homepage.') == ['https://example.com/']
    # print("Case 9 passed.")

    # # Case 10: Test with mixed case URLs
    # print("Case 10")
    # print(extract_urls('Visit Https://Example.com for secure content.'))
    # assert extract_urls('Visit Https://Example.com for secure content.') == ['Https://Example.com']
    # print("Case 10 passed.")

    # # Case 11: Test with a URL having query parameters
    # print("Case 11")
    # print(extract_urls('Check https://example.com/search?q=test for more.'))
    # assert extract_urls('Check https://example.com/search?q=test for more.') == ['https://example.com/search?q=test']
    # print("Case 11 passed.")

    # # Case 13: Test with a string that is not a valid URL (e.g., missing TLD)
    # print("Case 13")
    # print(extract_urls('Invalid: http://example'))
    # assert extract_urls('Invalid: http://example') == []
    # print("Case 13 passed.")

    # # Case 14: Test with input that is not a string (e.g., an integer)
    # print("Case 14")
    # try:
    #     extract_urls(12345)
    #     print(extract_urls(12345))
    # except TypeError:
    #     print("TypeError: Input must be a string.")
    #     pass  # Expecting TypeError for non-string input
    # print("Case 14 passed.")

    # # Case 15: Test with a URL inside parentheses
    # print("Case 15")
    # print(extract_urls('Visit (https://example.com) for more info.'))
    # assert extract_urls('Visit (https://example.com) for more info.') == ['https://example.com']
    # print("Case 15 passed.")

    # # Case 15B: Test with a URL inside parentheses
    # print("Case 15B")
    # print(extract_urls('Visit [https://example.com] for more info.'))
    # assert extract_urls('Visit [https://example.com] for more info.') == ['https://example.com']
    # print("Case 15B passed.")

    # # Case 16: Test with a string containing file paths but no URLs
    # print("Case 16")
    # print(extract_urls('Open the file at C:/Users/Documents/file.txt or /usr/local/bin/script.sh'))
    # assert extract_urls('Open the file at C:/Users/Documents/file.txt or /usr/local/bin/script.sh') == []
    # print("Case 16 passed.")

    # # Case 17: Test with a URL followed by other text without a space
    # print("Case 17")
    # print(extract_urls('Check https://example.com/more-text'))
    # assert extract_urls('Check https://example.com/more-text') == ['https://example.com/more-text']
    # print("Case 17 passed.")

    # # https://chatgpt.com/share/670817f7-242c-8004-9104-0fc9432296f8

    # # Case 18: Test with a simple URL
    # print("Case 18")
    # print(extract_urls('Visit https://example.com'))
    # assert extract_urls('Visit https://example.com') == ['https://example.com']
    # print("Case 18 passed.")

    # # Case 19: Test with an HTTP URL
    # print("Case 19")
    # print(extract_urls('Go to http://example.com'))
    # assert extract_urls('Go to http://example.com') == ['http://example.com']
    # print("Case 19 passed.")

    # # Case 20: Test with no URL in the string
    # print("Case 20")
    # print(extract_urls('No URL here'))
    # assert extract_urls('No URL here') == []
    # print("Case 20 passed.")

    # # Case 21: Test with multiple URLs in a single string
    # print("Case 21")
    # print(extract_urls('Check https://example.com and http://another-example.com'))
    # assert extract_urls('Check https://example.com and http://another-example.com') == ['https://example.com', 'http://another-example.com']
    # print("Case 21 passed.")

    # # Case 22: Test with URLs without HTTP/HTTPS scheme
    # print("Case 22")
    # print(extract_urls('Go to www.example.com'))
    # assert extract_urls('Go to www.example.com') == ['http://www.example.com']
    # print("Case 22 passed.")

    # # Case 23: Test with URL containing query parameters
    # print("Case 23")
    # print(extract_urls('Search https://example.com?q=python'))
    # assert extract_urls('Search https://example.com?q=python') == ['https://example.com?q=python']
    # print("Case 23 passed.")

    # # Case 24: Test with URL containing special characters
    # print("Case 24")
    # print(extract_urls('Look at https://example.com/page#section'))
    # assert extract_urls('Look at https://example.com/page#section') == ['https://example.com/page#section']
    # print("Case 24 passed.")

    # # Case 25: Test with parentheses around the URL
    # print("Case 25")
    # print(extract_urls('Here is the link (https://example.com)'))
    # assert extract_urls('Here is the link (https://example.com)') == ['https://example.com']
    # print("Case 25 passed.")

    # # Case 26: Test with a URL followed by punctuation
    # print("Case 26")
    # print(extract_urls('Visit https://example.com.'))
    # assert extract_urls('Visit https://example.com.') == ['https://example.com']
    # print("Case 26 passed.")

    # # Case 27: Test with a URL embedded in HTML
    # print("Case 27")
    # print(extract_urls('<a href="https://example.com">Link</a>'))
    # assert extract_urls('<a href="https://example.com">Link</a>') == ['https://example.com']
    # print("Case 27 passed.")

    # # Case 28: Test with a malformed URL (missing a dot)
    # print("Case 28")
    # print(extract_urls('Visit http://examplecom'))
    # assert extract_urls('Visit http://examplecom') == []
    # print("Case 28 passed.")

    # # Case 29: Test with an incomplete URL (missing the domain)
    # print("Case 29")
    # print(extract_urls('Visit https://'))
    # assert extract_urls('Visit https://') == []
    # print("Case 29 passed.")

    # # Case 30: Test with a long URL with multiple subdomains
    # print("Case 30")
    # print(extract_urls('Find more at https://sub.domain.example.com/page'))
    # assert extract_urls('Find more at https://sub.domain.example.com/page') == ['https://sub.domain.example.com/page']
    # print("Case 30 passed.")

    # # Case 31: Test with an FTP URL
    # print("Case 31")
    # print(extract_urls('Download from ftp://example.com/file'))
    # assert extract_urls('Download from ftp://example.com/file') == ['ftp://example.com/file']
    # print("Case 31 passed.")

    # # Case 32: Test with a URL containing port number
    # print("Case 32")
    # print(extract_urls('Access the service at https://example.com:8080/service'))
    # assert extract_urls('Access the service at https://example.com:8080/service') == ['https://example.com:8080/service']
    # print("Case 32 passed.")

    # # Case 34: Test with a URL containing Unicode characters
    # print("Case 34")
    # print(extract_urls('Find at https://example.com/привет'))
    # assert extract_urls('Find at https://example.com/привет') == ['https://example.com/привет']
    # print("Case 34 passed.")

    # # Case 35: Test with a URL followed by text with no space
    # print("Case 35")
    # print(extract_urls('See https://example.com/somethingelse'))
    # assert extract_urls('See https://example.com/somethingelse') == ['https://example.com/somethingelse']
    # print("Case 35 passed.")

    # # Case 36: Test with a URL in a JSON-like structure
    # print("Case 36")
    # print(extract_urls('{"url": "https://example.com/api"}'))
    # assert extract_urls('{"url": "https://example.com/api"}') == ['https://example.com/api']
    # print("Case 36 passed.")

    # # https://chatgpt.com/share/67081978-927c-8004-af27-a5e77b0ec552

    # # Case 37: Test with a URL containing query parameters and fragment
    # print("Case 37")
    # print(extract_urls('Go to https://example.com/page?query=param#section'))
    # assert extract_urls('Go to https://example.com/page?query=param#section') == ['https://example.com/page?query=param#section']
    # print("Case 37 passed.")

    # # Case 38: Test with multiple URLs separated by spaces
    # print("Case 38")
    # print(extract_urls('Visit https://site1.com and https://site2.com for details.'))
    # assert extract_urls('Visit https://site1.com and https://site2.com for details.') == ['https://site1.com', 'https://site2.com']
    # print("Case 38 passed.")

    # # Case 39: Test with a URL inside parentheses
    # print("Case 39")
    # print(extract_urls('Check this (https://example.com) for reference.'))
    # assert extract_urls('Check this (https://example.com) for reference.') == ['https://example.com']
    # print("Case 39 passed.")

    # # Case 40: Test with a URL followed by punctuation
    # print("Case 40")
    # print(extract_urls('Go to https://example.com!'))
    # assert extract_urls('Go to https://example.com!') == ['https://example.com']
    # print("Case 40 passed.")

    # # Case 41: Test with URL in a sentence followed by a comma
    # print("Case 41")
    # print(extract_urls('Check out https://example.com, it’s amazing.'))
    # assert extract_urls('Check out https://example.com, it’s amazing.') == ['https://example.com']
    # print("Case 41 passed.")

    # # Case 42: Test with no URL in the text
    # print("Case 42")
    # print(extract_urls('This sentence has no URLs.'))
    # assert extract_urls('This sentence has no URLs.') == []
    # print("Case 42 passed.")

    # # Case 43: Test with a URL without http/https scheme
    # print("Case 43")
    # print(extract_urls('Visit www.example.com for details.'))
    # assert extract_urls('Visit www.example.com for details.') == ["http://www.example.com"]
    # print("Case 43 passed.")

    # # Case 44: Test with a malformed URL (missing domain)
    # print("Case 44")
    # print(extract_urls('Go to https:// for details.'))
    # assert extract_urls('Go to https:// for details.') == []
    # print("Case 44 passed.")

    # # Case 45: Test with an email address (should not be extracted)
    # print("Case 45")
    # print(extract_urls('Contact us at info@example.com.'))
    # assert extract_urls('Contact us at info@example.com.') == []
    # print("Case 45 passed.")

    # # Case 46: Test with an FTP URL
    # print("Case 46")
    # print(extract_urls('Download from ftp://ftp.example.com'))
    # assert extract_urls('Download from ftp://ftp.example.com') == ['ftp://ftp.example.com']
    # print("Case 46 passed.")

    # # Case 47: Test with a URL in a very long text
    # print("Case 47")
    # long_text = 'This is a long text with a URL somewhere in it. Here it is: https://example.com/page. The rest of the text follows.'
    # print(extract_urls(long_text))
    # assert extract_urls(long_text) == ['https://example.com/page']
    # print("Case 47 passed.")

    # # Case 48: Test with multiple URLs with mixed schemes
    # print("Case 48")
    # print(extract_urls('Check https://example.com and ftp://ftp.example.com'))
    # assert extract_urls('Check https://example.com and ftp://ftp.example.com') == ['https://example.com', 'ftp://ftp.example.com']
    # print("Case 48 passed.")

    # # Case 49: Test with a URL in an HTML anchor tag
    # print("Case 49")
    # print(extract_urls('<a href="https://example.com">Link</a>'))
    # assert extract_urls('<a href="https://example.com">Link</a>') == ['https://example.com']
    # print("Case 49 passed.")

    # # Case 50: Test with a URL in square brackets
    # print("Case 50")
    # print(extract_urls('Visit [https://example.com] for details.'))
    # assert extract_urls('Visit [https://example.com] for details.') == ['https://example.com']
    # print("Case 50 passed.")

    # # Case 51: Test with a string containing only a URL
    # print("Case 51")
    # print(extract_urls('https://example.com'))
    # assert extract_urls('https://example.com') == ['https://example.com']
    # print("Case 51 passed.")

    # # Case 53: Test with a very long URL
    # print("Case 53")
    # long_url = 'https://example.com/' + 'a' * 100
    # print(extract_urls(f'Check out this long URL: {long_url}'))
    # assert extract_urls(f'Check out this long URL: {long_url}') == [long_url]
    # print("Case 53 passed.")

    # # Case 54: Test with a URL containing special characters
    # print("Case 54")
    # print(extract_urls('Special characters in URL: https://example.com/this!is*a_test'))
    # assert extract_urls('Special characters in URL: https://example.com/this!is*a_test') == ['https://example.com/this!is*a_test']
    # print("Case 54 passed.")

    # # Case 55: Test with a URL surrounded by whitespace
    # print("Case 55")
    # print(extract_urls('    https://example.com    '))
    # assert extract_urls('    https://example.com    ') == ['https://example.com']
    # print("Case 55 passed.")

    # # https://chatgpt.com/share/67081bae-deb0-8004-b063-cc0e8d2968a7

    # # Case 55: Test URL with www and no scheme
    # print("Case 55")
    # print(extract_urls('Visit www.example.com for more info'))
    # assert extract_urls('Visit www.example.com for more info') == ['http://www.example.com']
    # print("Case 55 passed.")

    # # Case 56: Test URL with special characters in the query string
    # print("Case 56")
    # print(extract_urls('Check out https://example.com?search=test&sort=asc#anchor'))
    # assert extract_urls('Check out https://example.com?search=test&sort=asc#anchor') == ['https://example.com?search=test&sort=asc#anchor']
    # print("Case 56 passed.")

    # # Case 57: Test malformed URL with missing domain
    # print("Case 57")
    # print(extract_urls('Check out https:// for more info'))
    # assert extract_urls('Check out https:// for more info') == []
    # print("Case 57 passed.")

    # # Case 58: Test URL embedded within parentheses
    # print("Case 58")
    # print(extract_urls('Find more details (https://example.com/info).'))
    # assert extract_urls('Find more details (https://example.com/info).') == ['https://example.com/info']
    # print("Case 58 passed.")

    # # Case 59: Test URL inside a sentence with punctuation
    # print("Case 59")
    # print(extract_urls('This is an important link: https://example.com, and you should visit it.'))
    # assert extract_urls('This is an important link: https://example.com, and you should visit it.') == ['https://example.com']
    # print("Case 59 passed.")

    # # Case 60: Test URL with mixed-case scheme
    # print("Case 60")
    # print(extract_urls('Go to HtTpS://example.com for more info'))
    # assert extract_urls('Go to HtTpS://example.com for more info') == ['HtTpS://example.com']
    # print("Case 60 passed.")

    # # Case 61: Test URL followed by newline
    # print("Case 61")
    # print(extract_urls('Here is the link:\nhttps://example.com'))
    # assert extract_urls('Here is the link:\nhttps://example.com') == ['https://example.com']
    # print("Case 61 passed.")

    # # Case 62: Test text with multiple URLs
    # print("Case 62")
    # print(extract_urls('Go to https://example.com and https://another.com for details.'))
    # assert extract_urls('Go to https://example.com and https://another.com for details.') == ['https://example.com', 'https://another.com']
    # print("Case 62 passed.")

    # # Case 63: Test URL with port number
    # print("Case 63")
    # print(extract_urls('Visit http://example.com:8080 for access'))
    # assert extract_urls('Visit http://example.com:8080 for access') == ['http://example.com:8080']
    # print("Case 63 passed.")

    # # Case 67: Test broken URL with scheme but no domain
    # print("Case 67")
    # print(extract_urls('Broken URL https:// for more info'))
    # assert extract_urls('Broken URL https:// for more info') == []
    # print("Case 67 passed.")

    # # Case 68: Test URL with uncommon protocol (FTP)
    # print("Case 68")
    # print(extract_urls('Download from ftp://example.com/resource.zip'))
    # assert extract_urls('Download from ftp://example.com/resource.zip') == ['ftp://example.com/resource.zip']
    # print("Case 68 passed.")

    # # Case 69: Test URL within HTML tag
    # print("Case 69")
    # print(extract_urls('Visit <a href="https://example.com">our website</a>'))
    # assert extract_urls('Visit <a href="https://example.com">our website</a>') == ['https://example.com']
    # print("Case 69 passed.")

    # # Case 70: Test URL in a JSON string
    # print("Case 70")
    # print(extract_urls('{"url": "https://example.com"}'))
    # assert extract_urls('{"url": "https://example.com"}') == ['https://example.com']
    # print("Case 70 passed.")

    # # Case 71: Test URL with a username and password
    # print("Case 71")
    # print(extract_urls('Visit http://user:password@example.com for access'))
    # assert extract_urls('Visit http://user:password@example.com for access') == ['http://user:password@example.com']
    # print("Case 71 passed.")

    # # Case 72: Test URL with emoji in query string
    # print("Case 72")
    # print(extract_urls('Go to https://example.com/search?query=😊 for more info'))
    # assert extract_urls('Go to https://example.com/search?query=😊 for more info') == ['https://example.com/search?query=😊']
    # print("Case 72 passed.")

    # # Case 73: Test URL with encoded characters
    # print("Case 73")
    # print(extract_urls('Download from https://example.com/resource%20name.zip'))
    # assert extract_urls('Download from https://example.com/resource%20name.zip') == ['https://example.com/resource%20name.zip']
    # print("Case 73 passed.")

    # # Case 74: Test URL with trailing dot
    # print("Case 74")
    # print(extract_urls('Visit https://example.com. for more info'))
    # assert extract_urls('Visit https://example.com. for more info') == ['https://example.com']
    # print("Case 74 passed.")

    # # Case 75: Test URL surrounded by commas
    # print("Case 75")
    # print(extract_urls('Check out this website,https://example.com, for more info'))
    # assert extract_urls('Check out this website,https://example.com, for more info') == ['https://example.com']
    # print("Case 75 passed.")

    # # Case 76: Test text with invalid URL scheme
    # print("Case 76")
    # print(extract_urls('This is not a valid URL scheme: mailto:user@example.com'))
    # assert extract_urls('This is not a valid URL scheme: mailto:user@example.com') == []
    # print("Case 76 passed.")

    # # Case 77: Test text with a file path that looks like a URL
    # print("Case 77")
    # print(extract_urls('Here is a file path: C:\\Program Files\\example'))
    # assert extract_urls('Here is a file path: C:\\Program Files\\example') == []
    # print("Case 77 passed.")

    # # Case 78: Test URL with fragment identifier
    # print("Case 78")
    # print(extract_urls('Visit https://example.com#section1 for details'))
    # assert extract_urls('Visit https://example.com#section1 for details') == ['https://example.com#section1']
    # print("Case 78 passed.")

    # # https://chatgpt.com/share/67081d74-8654-8004-8cb2-7851e47b25e7

    # # Case 79: Test with multiple URLs in the same string
    # print("Case 79")
    # print(extract_urls('Here are two URLs: https://example.com and https://another.com'))
    # assert extract_urls('Here are two URLs: https://example.com and https://another.com') == ['https://example.com', 'https://another.com']
    # print("Case 79 passed.")

    # # Case 80: Test with no URLs
    # print("Case 80")
    # print(extract_urls('No URLs here'))
    # assert extract_urls('No URLs here') == []
    # print("Case 80 passed.")

    # # Case 81: Test with an invalid URL format
    # print("Case 81")
    # print(extract_urls('Check this URL: htt://example.com'))
    # assert extract_urls('Check this URL: htt://example.com') == []
    # print("Case 81 passed.")

    # # Case 82: Test with URL surrounded by punctuation
    # print("Case 82")
    # print(extract_urls('Here is a URL: (https://example.com)!'))
    # assert extract_urls('Here is a URL: (https://example.com)!') == ['https://example.com']
    # print("Case 82 passed.")

    # # Case 83: Test with URL without protocol
    # print("Case 83")
    # print(extract_urls('Visit example.com for more information'))
    # assert extract_urls('Visit example.com for more information') == []
    # print("Case 83 passed.")

    # # Case 84: Test with www URL without protocol
    # print("Case 84")
    # print(extract_urls('Visit www.example.com for more information'))
    # assert extract_urls('Visit www.example.com for more information') == ['http://www.example.com']
    # print("Case 84 passed.")

    # # Case 85: Test with a URL that includes query parameters
    # print("Case 85")
    # print(extract_urls('Check this URL with query: https://example.com?query=test'))
    # assert extract_urls('Check this URL with query: https://example.com?query=test') == ['https://example.com?query=test']
    # print("Case 85 passed.")

    # # Case 86: Test with a URL that includes fragments
    # print("Case 86")
    # print(extract_urls('Check this fragment URL: https://example.com#section2'))
    # assert extract_urls('Check this fragment URL: https://example.com#section2') == ['https://example.com#section2']
    # print("Case 86 passed.")

    # # Case 87: Test with multiple consecutive URLs without spaces
    # print("Case 87")
    # print(extract_urls('https://first.comhttps://second.com'))
    # assert extract_urls('https://first.comhttps://second.com') == ['https://first.com', 'https://second.com']
    # print("Case 87 passed.")

    # # Case 88: Test with a URL containing port number
    # print("Case 88")
    # print(extract_urls('Check this URL with port: https://example.com:8080'))
    # assert extract_urls('Check this URL with port: https://example.com:8080') == ['https://example.com:8080']
    # print("Case 88 passed.")

    # # Case 90: Test with a URL in the middle of a long string
    # print("Case 90")
    # print(extract_urls('Before the URL https://example.com after the URL'))
    # assert extract_urls('Before the URL https://example.com after the URL') == ['https://example.com']
    # print("Case 90 passed.")

    # # Case 91: Test with a URL in parentheses
    # print("Case 91")
    # print(extract_urls('(https://example.com)'))
    # assert extract_urls('(https://example.com)') == ['https://example.com']
    # print("Case 91 passed.")

    # # Case 92: Test with a URL followed by a period
    # print("Case 92")
    # print(extract_urls('Check this URL: https://example.com. It is useful.'))
    # assert extract_urls('Check this URL: https://example.com. It is useful.') == ['https://example.com']
    # print("Case 92 passed.")

    # # Case 93: Test with a URL in HTML-like structure
    # print("Case 93")
    # print(extract_urls('<a href="https://example.com">Example</a>'))
    # assert extract_urls('<a href="https://example.com">Example</a>') == ['https://example.com']
    # print("Case 93 passed.")

    # # Case 94: Test with a URL that has a subdomain
    # print("Case 94")
    # print(extract_urls('Visit https://sub.example.com for more info'))
    # assert extract_urls('Visit https://sub.example.com for more info') == ['https://sub.example.com']
    # print("Case 94 passed.")

    # # Case 95: Test with a URL followed by special characters
    # print("Case 95")
    # print(extract_urls('Check https://example.com?! for more info'))
    # assert extract_urls('Check https://example.com?! for more info') == ['https://example.com']
    # print("Case 95 passed.")

    # # Case 96: Test with malformed URL that starts with https
    # print("Case 96")
    # print(extract_urls('Check https:///example.com'))
    # assert extract_urls('Check https:///example.com') == []
    # print("Case 96 passed.")

    # # Case 97: Test with an FTP URL
    # print("Case 97")
    # print(extract_urls('Download file from ftp://example.com/file.txt'))
    # assert extract_urls('Download file from ftp://example.com/file.txt') == ['ftp://example.com/file.txt']
    # print("Case 97 passed.")

    # # Case 98: Test with a very long URL
    # print("Case 98")
    # print(extract_urls('This is a very long URL: https://example.com/' + 'a'*1000))
    # assert extract_urls('This is a very long URL: https://example.com/' + 'a'*1000) == ['https://example.com/' + 'a'*1000]
    # print("Case 98 passed.")

    # # Case 99: Test with an empty string
    # print("Case 99")
    # print(extract_urls(''))
    # assert extract_urls('') == []
    # print("Case 99 passed.")

    # # Case 100: Test with URL inside a sentence with no spaces
    # print("Case 100")
    # print(extract_urls('ThisisnotanURLbutthisoneishttps://example.com'))
    # assert extract_urls('ThisisnotanURLbutthisoneishttps://example.com') == ['https://example.com']
    # print("Case 100 passed.")

    # # https://chatgpt.com/share/67082386-5948-8004-948f-2bc5e51af734

    # # Case 101: Test a simple HTTP URL
    # print("Case 101")
    # print(extract_urls('Visit http://example.com'))
    # assert extract_urls('Visit http://example.com') == ['http://example.com']
    # print("Case 101 passed.")

    # # Case 102: Test a simple HTTPS URL
    # print("Case 102")
    # print(extract_urls('Visit https://example.com'))
    # assert extract_urls('Visit https://example.com') == ['https://example.com']
    # print("Case 102 passed.")

    # # Case 103: Test with a URL with query parameters
    # print("Case 103")
    # print(extract_urls('Check out https://example.com/search?q=test'))
    # assert extract_urls('Check out https://example.com/search?q=test') == ['https://example.com/search?q=test']
    # print("Case 103 passed.")

    # # Case 104: Test with multiple URLs in the string
    # print("Case 104")
    # print(extract_urls('Visit http://example.com and https://another.com'))
    # assert extract_urls('Visit http://example.com and https://another.com') == ['http://example.com', 'https://another.com']
    # print("Case 104 passed.")

    # # Case 105: Test with a URL followed by punctuation
    # print("Case 105")
    # print(extract_urls('Go to http://example.com, now!'))
    # assert extract_urls('Go to http://example.com, now!') == ['http://example.com']
    # print("Case 105 passed.")

    # # Case 106: Test with a URL without "www."
    # print("Case 106")
    # print(extract_urls('Try https://example.org/path/to/page'))
    # assert extract_urls('Try https://example.org/path/to/page') == ['https://example.org/path/to/page']
    # print("Case 106 passed.")

    # # Case 107: Test with a URL with a subdomain
    # print("Case 107")
    # print(extract_urls('Visit https://sub.example.com'))
    # assert extract_urls('Visit https://sub.example.com') == ['https://sub.example.com']
    # print("Case 107 passed.")

    # # # Case 108: Test with a URL that has special characters
    # # print("Case 108")
    # # print(extract_urls('Check https://example.com/path/to/@file!'))
    # # assert extract_urls('Check https://example.com/path/to/@file!') == ['https://example.com/path/to/@file!']
    # # print("Case 108 passed.")

    # # Case 109: Test with a URL missing the protocol
    # print("Case 109")
    # print(extract_urls('www.example.com/path/to/page'))
    # assert extract_urls('www.example.com/path/to/page') == ['http://www.example.com/path/to/page']
    # print("Case 109 passed.")

    # # Case 110: Test with a malformed URL (missing TLD)
    # print("Case 110")
    # print(extract_urls('Check out http://example'))
    # assert extract_urls('Check out http://example') == []
    # print("Case 110 passed.")

    # # Case 111: Test with multiple URLs separated by commas
    # print("Case 111")
    # print(extract_urls('Visit http://example.com,https://another.com'))
    # assert extract_urls('Visit http://example.com,https://another.com') == ['http://example.com', 'https://another.com']
    # print("Case 111 passed.")

    # # Case 113: Test with a URL containing port number
    # print("Case 113")
    # print(extract_urls('Visit http://example.com:8080/path'))
    # assert extract_urls('Visit http://example.com:8080/path') == ['http://example.com:8080/path']
    # print("Case 113 passed.")

    # # Case 114: Test with FTP protocol
    # print("Case 114")
    # print(extract_urls('Download from ftp://example.com/file'))
    # assert extract_urls('Download from ftp://example.com/file') == ['ftp://example.com/file']
    # print("Case 114 passed.")

    # # Case 116: Test with URL inside parentheses
    # print("Case 116")
    # print(extract_urls('Find more info (https://example.com) here'))
    # assert extract_urls('Find more info (https://example.com) here') == ['https://example.com']
    # print("Case 116 passed.")

    # # Case 117: Test with multiple URLs inside parentheses
    # print("Case 117")
    # print(extract_urls('(http://example.com) and (https://another.com)'))
    # assert extract_urls('(http://example.com) and (https://another.com)') == ['http://example.com', 'https://another.com']
    # print("Case 117 passed.")

    # # Case 118: Test with a URL missing the protocol but prefixed with a space
    # print("Case 118")
    # print(extract_urls(' Go to www.example.com for more'))
    # assert extract_urls(' Go to www.example.com for more') == ["http://www.example.com"]
    # print("Case 118 passed.")

    # # Case 119: Test with a URL with a long subdomain
    # print("Case 119")
    # print(extract_urls('Visit https://sub.sub.example.com/path'))
    # assert extract_urls('Visit https://sub.sub.example.com/path') == ['https://sub.sub.example.com/path']
    # print("Case 119 passed.")

    # # Case 120: Test with URL containing hash fragment
    # print("Case 120")
    # print(extract_urls('Check out https://example.com/page#section'))
    # assert extract_urls('Check out https://example.com/page#section') == ['https://example.com/page#section']
    # print("Case 120 passed.")

    # # Case 121: Test with email address (should not be extracted as URL)
    # print("Case 121")
    # print(extract_urls('Contact me at test@example.com'))
    # assert extract_urls('Contact me at test@example.com') == []
    # print("Case 121 passed.")

    # # Case 123: Test with URL with hyphenated domain
    # print("Case 123")
    # print(extract_urls('Go to http://my-example.com'))
    # assert extract_urls('Go to http://my-example.com') == ['http://my-example.com']
    # print("Case 123 passed.")

    # # # Case 124: Test with URL with underscore in domain (invalid URL)
    # # print("Case 124")
    # # print(extract_urls('Visit http://my_example.com'))
    # # assert extract_urls('Visit http://my_example.com') == []
    # # print("Case 124 passed.")

    # # Case 125: Test with a very long URL
    # print("Case 125")
    # long_url = 'https://example.com/' + 'a' * 500
    # print(extract_urls(f'Check out {long_url}'))
    # assert extract_urls(f'Check out {long_url}') == [long_url]
    # print("Case 125 passed.")

    # # Case 126: Test with a URL containing escaped characters
    # print("Case 126")
    # print(extract_urls('Visit https://example.com/path%20with%20spaces'))
    # assert extract_urls('Visit https://example.com/path%20with%20spaces') == ['https://example.com/path%20with%20spaces']
    # print("Case 126 passed.")

    # # https://chatgpt.com/share/67082564-2f9c-8004-9a7b-1b862c9f322d

    # # Case 127: Test with a URL inside parentheses
    # print("Case 127")
    # print(extract_urls('Here is a link (https://example.com)'))
    # assert extract_urls('Here is a link (https://example.com)') == ['https://example.com']
    # print("Case 127 passed.")

    # # Case 128: Test with a URL surrounded by quotation marks
    # print("Case 128")
    # print(extract_urls('"https://example.com" is the site'))
    # assert extract_urls('"https://example.com" is the site') == ['https://example.com']
    # print("Case 128 passed.")

    # # Case 129: Test with a URL with a fragment identifier
    # print("Case 129")
    # print(extract_urls('Go to https://example.com/page#section1'))
    # assert extract_urls('Go to https://example.com/page#section1') == ['https://example.com/page#section1']
    # print("Case 129 passed.")

    # # Case 130: Test with multiple URLs in the same string
    # print("Case 130")
    # print(extract_urls('Links: https://example.com and https://another.com'))
    # assert extract_urls('Links: https://example.com and https://another.com') == ['https://example.com', 'https://another.com']
    # print("Case 130 passed.")

    # # Case 131: Test with a URL that has query parameters
    # print("Case 131")
    # print(extract_urls('Search here: https://example.com/search?q=test'))
    # assert extract_urls('Search here: https://example.com/search?q=test') == ['https://example.com/search?q=test']
    # print("Case 131 passed.")

    # # Case 132: Test with URL that contains a port number
    # print("Case 132")
    # print(extract_urls('Go to https://example.com:8080/path'))
    # assert extract_urls('Go to https://example.com:8080/path') == ['https://example.com:8080/path']
    # print("Case 132 passed.")

    # # Case 133: Test with a URL that has internationalized domain name
    # print("Case 133")
    # print(extract_urls('Visit https://xn--fsq.com'))
    # assert extract_urls('Visit https://xn--fsq.com') == ['https://xn--fsq.com']
    # print("Case 133 passed.")

    # # Case 134: Test with a URL that is broken (missing scheme)
    # print("Case 134")
    # print(extract_urls('Check example.com/path'))
    # assert extract_urls('Check example.com/path') == []
    # print("Case 134 passed.")

    # # Case 135: Test with a URL that uses ftp scheme
    # print("Case 135")
    # print(extract_urls('Download here: ftp://example.com/file'))
    # assert extract_urls('Download here: ftp://example.com/file') == ['ftp://example.com/file']
    # print("Case 135 passed.")

    # # Case 136: Test with an email address mistaken for a URL
    # print("Case 136")
    # print(extract_urls('Contact us at test@example.com'))
    # assert extract_urls('Contact us at test@example.com') == []
    # print("Case 136 passed.")

    # # Case 137: Test with a URL containing special characters
    # print("Case 137")
    # print(extract_urls('Visit https://example.com/path?name=John&age=25!'))
    # assert extract_urls('Visit https://example.com/path?name=John&age=25!') == ['https://example.com/path?name=John&age=25']
    # print("Case 137 passed.")

    # # Case 138: Test with a URL surrounded by non-breaking spaces
    # print("Case 138")
    # print(extract_urls('Here is a link: https://example.com/path'))
    # assert extract_urls('Here is a link: https://example.com/path') == ['https://example.com/path']
    # print("Case 138 passed.")

    # # Case 139: Test with a URL with underscore in domain
    # print("Case 139")
    # print(extract_urls('Check https://sub_domain.example.com'))
    # assert extract_urls('Check https://sub_domain.example.com') == ['https://sub_domain.example.com']
    # print("Case 139 passed.")

    # # Case 140: Test with a very long URL
    # print("Case 140")
    # long_url = 'https://example.com/' + 'a'*500
    # print(extract_urls(f'Long URL: {long_url}'))
    # assert extract_urls(f'Long URL: {long_url}') == [long_url]
    # print("Case 140 passed.")

    # # Case 141: Test with a URL missing a TLD
    # print("Case 141")
    # print(extract_urls('This URL is broken: https://example'))
    # assert extract_urls('This URL is broken: https://example') == []
    # print("Case 141 passed.")

    # # Case 143: Test with an empty string
    # print("Case 143")
    # print(extract_urls(''))
    # assert extract_urls('') == []
    # print("Case 143 passed.")

    # # Case 144: Test with a URL in a sentence without protocol (http)
    # print("Case 144")
    # print(extract_urls('Visit www.example.com for more info.'))
    # assert extract_urls('Visit www.example.com for more info.') == ["http://www.example.com"]
    # print("Case 144 passed.")

    # # Case 145: Test with URL including subdomain
    # print("Case 145")
    # print(extract_urls('Access https://subdomain.example.com'))
    # assert extract_urls('Access https://subdomain.example.com') == ['https://subdomain.example.com']
    # print("Case 145 passed.")

    # # Case 147: Test with URL containing path traversal (..)
    # print("Case 147")
    # print(extract_urls('Access https://example.com/../../etc/passwd'))
    # assert extract_urls('Access https://example.com/../../etc/passwd') == ['https://example.com/../../etc/passwd']
    # print("Case 147 passed.")

    # # Case 148: Test with a URL followed by punctuation
    # print("Case 148")
    # print(extract_urls('Go to https://example.com/path.'))
    # assert extract_urls('Go to https://example.com/path.') == ['https://example.com/path']
    # print("Case 148 passed.")

    # # Case 149: Test with URL starting with http only (without https)
    # print("Case 149")
    # print(extract_urls('Visit http://example.com'))
    # assert extract_urls('Visit http://example.com') == ['http://example.com']
    # print("Case 149 passed.")

    # # https://chatgpt.com/share/6708276f-22b4-8004-bf8d-e20e821ba61d

    # # Case 150: Test with a simple URL
    # print("Case 150")
    # print(extract_urls('Visit https://example.com'))
    # assert extract_urls('Visit https://example.com') == ['https://example.com']
    # print("Case 150 passed.")

    # # Case 151: Test with multiple URLs in one string
    # print("Case 151")
    # print(extract_urls('Check https://example.com and http://test.org'))
    # assert extract_urls('Check https://example.com and http://test.org') == ['https://example.com', 'http://test.org']
    # print("Case 151 passed.")

    # # Case 152: Test with URLs with query parameters
    # print("Case 152")
    # print(extract_urls('Search https://example.com/search?q=python'))
    # assert extract_urls('Search https://example.com/search?q=python') == ['https://example.com/search?q=python']
    # print("Case 152 passed.")

    # # Case 153: Test with URLs containing fragments
    # print("Case 153")
    # print(extract_urls('Navigate to https://example.com/page#section'))
    # assert extract_urls('Navigate to https://example.com/page#section') == ['https://example.com/page#section']
    # print("Case 153 passed.")

    # # Case 154: Test with a URL that doesn't have a protocol
    # print("Case 154")
    # print(extract_urls('Visit www.example.com'))
    # assert extract_urls('Visit www.example.com') == ["http://www.example.com"]
    # print("Case 154 passed.")

    # # Case 155: Test with a URL with different protocol (ftp)
    # print("Case 155")
    # print(extract_urls('Download via ftp://ftp.example.com/file'))
    # assert extract_urls('Download via ftp://ftp.example.com/file') == ['ftp://ftp.example.com/file']
    # print("Case 155 passed.")

    # # Case 156: Test with a URL followed by punctuation
    # print("Case 156")
    # print(extract_urls('Visit https://example.com!'))
    # assert extract_urls('Visit https://example.com!') == ['https://example.com']
    # print("Case 156 passed.")

    # # Case 157: Test with URL and text without space between them
    # print("Case 157")
    # print(extract_urls('Here is the link:https://example.com.'))
    # assert extract_urls('Here is the link:https://example.com.') == ['https://example.com']
    # print("Case 157 passed.")

    # # Case 158: Test with URL inside parentheses
    # print("Case 158")
    # print(extract_urls('Visit (https://example.com) for details.'))
    # assert extract_urls('Visit (https://example.com) for details.') == ['https://example.com']
    # print("Case 158 passed.")

    # # Case 161: Test with incomplete URL (missing top-level domain)
    # print("Case 161")
    # print(extract_urls('Check http://example for details'))
    # assert extract_urls('Check http://example for details') == []
    # print("Case 161 passed.")

    # # Case 163: Test with a mix of text and multiple URLs with spaces
    # print("Case 163")
    # print(extract_urls('Links: https://a.com https://b.org http://c.net'))
    # assert extract_urls('Links: https://a.com https://b.org http://c.net') == ['https://a.com', 'https://b.org', 'http://c.net']
    # print("Case 163 passed.")

    # # Case 164: Test with a URL containing dashes
    # print("Case 164")
    # print(extract_urls('Visit https://example-site.com for more info.'))
    # assert extract_urls('Visit https://example-site.com for more info.') == ['https://example-site.com']
    # print("Case 164 passed.")

    # # Case 165: Test with a URL containing underscores
    # print("Case 165")
    # print(extract_urls('Visit https://example_site.com for more info.'))
    # assert extract_urls('Visit https://example_site.com for more info.') == ['https://example_site.com']
    # print("Case 165 passed.")

    # # Case 166: Test with a URL inside HTML tag
    # print("Case 166")
    # print(extract_urls('<a href="https://example.com">Click here</a>'))
    # assert extract_urls('<a href="https://example.com">Click here</a>') == ['https://example.com']
    # print("Case 166 passed.")

    # # Case 167: Test with a URL containing special characters in the query string
    # print("Case 167")
    # print(extract_urls('Find it at https://example.com/search?q=a+b&sort=asc'))
    # assert extract_urls('Find it at https://example.com/search?q=a+b&sort=asc') == ['https://example.com/search?q=a+b&sort=asc']
    # print("Case 167 passed.")

    # # # Case 168: Test with URLs separated by commas
    # # # TODO: fix this test case
    # # print("Case 168")
    # # print(extract_urls('Here are the sites: https://a.com,http://b.org,https://c.net'))
    # # assert extract_urls('Here are the sites: https://a.com,http://b.org,https://c.net') == ['https://a.com', 'http://b.org', 'https://c.net']
    # # print("Case 168 passed.")

    # # Case 169: Test with a URL containing Unicode characters
    # print("Case 169")
    # print(extract_urls('Visit https://exämple.com for more details.'))
    # assert extract_urls('Visit https://exämple.com for more details.') == ['https://exämple.com']
    # print("Case 169 passed.")

    # # Case 170: Test with an empty string
    # print("Case 170")
    # print(extract_urls(''))
    # assert extract_urls('') == []
    # print("Case 170 passed.")

    # # Case 171: Test with only text and no URL
    # print("Case 171")
    # print(extract_urls('This is a test without any links.'))
    # assert extract_urls('This is a test without any links.') == []
    # print("Case 171 passed.")

    # # Case 172: Test with URL followed by emoji
    # print("Case 172")
    # print(extract_urls('Check this out: https://example.com 😃'))
    # assert extract_urls('Check this out: https://example.com 😃') == ['https://example.com']
    # print("Case 172 passed.")

    # # Case 173: Test with URL followed by newline character
    # print("Case 173")
    # print(extract_urls('Here is the link:\nhttps://example.com\nCheck it out.'))
    # assert extract_urls('Here is the link:\nhttps://example.com\nCheck it out.') == ['https://example.com']
    # print("Case 173 passed.")

    # # Case 174: Test with a URL in uppercase
    # print("Case 174")
    # print(extract_urls('VISIT HTTPS://EXAMPLE.COM'))
    # assert extract_urls('VISIT HTTPS://EXAMPLE.COM') == ['HTTPS://EXAMPLE.COM']
    # print("Case 174 passed.")

    # # Case 175: Test with a URL surrounded by quotes
    # print("Case 175")
    # print(extract_urls('"https://example.com" is the link'))
    # assert extract_urls('"https://example.com" is the link') == ['https://example.com']
    # print("Case 175 passed.")

    # # https://chatgpt.com/share/67082ce4-5958-8004-9fa1-c971ee736d18

    # # Case 176: Test with a basic valid URL
    # print("Case 176")
    # print(extract_urls('Visit https://example.com'))
    # assert extract_urls('Visit https://example.com') == ['https://example.com']
    # print("Case 176 passed.")

    # # Case 177: Test with a URL that lacks "https://"
    # print("Case 177")
    # print(extract_urls('Visit example.com'))
    # assert extract_urls('Visit example.com') == []
    # print("Case 177 passed.")

    # # Case 178: Test with a URL followed by other text without a space
    # print("Case 178")
    # print(extract_urls('Check https://example.com/more-textnow'))
    # assert extract_urls('Check https://example.com/more-textnow') == ['https://example.com/more-textnow']
    # print("Case 178 passed.")

    # # Case 179: Test with a URL containing a query string
    # print("Case 179")
    # print(extract_urls('Check https://example.com/path?query=123&name=test'))
    # assert extract_urls('Check https://example.com/path?query=123&name=test') == ['https://example.com/path?query=123&name=test']
    # print("Case 179 passed.")

    # # Case 180: Test with a URL containing a fragment
    # print("Case 180")
    # print(extract_urls('Check https://example.com/path#section'))
    # assert extract_urls('Check https://example.com/path#section') == ['https://example.com/path#section']
    # print("Case 180 passed.")

    # # Case 181: Test with a URL in parentheses
    # print("Case 181")
    # print(extract_urls('Visit (https://example.com/path)'))
    # assert extract_urls('Visit (https://example.com/path)') == ['https://example.com/path']
    # print("Case 181 passed.")

    # # Case 182: Test with multiple URLs in a string
    # print("Case 182")
    # print(extract_urls('Check https://example1.com and https://example2.com'))
    # assert extract_urls('Check https://example1.com and https://example2.com') == ['https://example1.com', 'https://example2.com']
    # print("Case 182 passed.")

    # # Case 183: Test with URLs separated by special characters
    # print("Case 183")
    # print(extract_urls('Check https://example.com, https://another.com'))
    # assert extract_urls('Check https://example.com, https://another.com') == ['https://example.com', 'https://another.com']
    # print("Case 183 passed.")

    # # Case 184: Test with a URL with no top-level domain
    # print("Case 184")
    # print(extract_urls('Check https://example'))
    # assert extract_urls('Check https://example') == []
    # print("Case 184 passed.")

    # # Case 186: Test with a URL in square brackets
    # print("Case 186")
    # print(extract_urls('See [https://example.com] for more info'))
    # assert extract_urls('See [https://example.com] for more info') == ['https://example.com']
    # print("Case 186 passed.")

    # # Case 187: Test with a URL with unusual but valid characters in the path
    # print("Case 187")
    # print(extract_urls('Go to https://example.com/~user/test_this-thing123'))
    # assert extract_urls('Go to https://example.com/~user/test_this-thing123') == ['https://example.com/~user/test_this-thing123']
    # print("Case 187 passed.")

    # # Case 188: Test with a URL containing percent encoding
    # print("Case 188")
    # print(extract_urls('Check https://example.com/path%20with%20spaces'))
    # assert extract_urls('Check https://example.com/path%20with%20spaces') == ['https://example.com/path%20with%20spaces']
    # print("Case 188 passed.")

    # # Case 189: Test with a URL with no path after domain
    # print("Case 189")
    # print(extract_urls('Visit https://example.com'))
    # assert extract_urls('Visit https://example.com') == ['https://example.com']
    # print("Case 189 passed.")

    # # Case 190: Test with a URL starting with www and no scheme
    # print("Case 190")
    # print(extract_urls('Visit www.example.com'))
    # assert extract_urls('Visit www.example.com') == ["http://www.example.com"]
    # print("Case 190 passed.")

    # # Case 191: Test with a URL containing a port number
    # print("Case 191")
    # print(extract_urls('Check https://example.com:8080/path'))
    # assert extract_urls('Check https://example.com:8080/path') == ['https://example.com:8080/path']
    # print("Case 191 passed.")

    # # Case 192: Test with a URL in a sentence with punctuation
    # print("Case 192")
    # print(extract_urls('The website https://example.com is down.'))
    # assert extract_urls('The website https://example.com is down.') == ['https://example.com']
    # print("Case 192 passed.")

    # # Case 193: Test with an invalid URL containing spaces
    # print("Case 193")
    # print(extract_urls('Visit https://exa mple.com now'))
    # assert extract_urls('Visit https://exa mple.com now') == []
    # print("Case 193 passed.")

    # # Case 194: Test with a malformed URL (too many slashes)
    # print("Case 194")
    # print(extract_urls('Check https://////example.com/path'))
    # assert extract_urls('Check https://////example.com/path') == []
    # print("Case 194 passed.")

    # # Case 195: Test with multiple URLs on separate lines
    # print("Case 195")
    # print(extract_urls('First https://example1.com\nSecond https://example2.com'))
    # assert extract_urls('First https://example1.com\nSecond https://example2.com') == ['https://example1.com', 'https://example2.com']
    # print("Case 195 passed.")

    # # Case 196: Test with a non-ASCII character in the URL
    # print("Case 196")
    # print(extract_urls('Visit https://exámple.com/path'))
    # assert extract_urls('Visit https://exámple.com/path') == ['https://exámple.com/path']
    # print("Case 196 passed.")

    # # Case 197: Test with an email address
    # print("Case 197")
    # print(extract_urls('Contact at email@example.com'))
    # assert extract_urls('Contact at email@example.com') == []
    # print("Case 197 passed.")

    # # Case 198: Test with a URL at the end of a sentence
    # print("Case 198")
    # print(extract_urls('The website is available at https://example.com.'))
    # assert extract_urls('The website is available at https://example.com.') == ['https://example.com']
    # print("Case 198 passed.")

    # # Case 199: Test with a URL containing listserv.kent.edu
    # print("Case 199")
    # print(extract_urls("To unsubscribe from the CESNET-L list, click the following link:\nhttps://listserv.kent.edu/cgi-bin/wa.exe?SUBED1=CESNET-L&A=1"))
    # assert extract_urls("To unsubscribe from the CESNET-L list, click the following link:\nhttps://listserv.kent.edu/cgi-bin/wa.exe?SUBED1=CESNET-L&A=1") == []
    # print("Case 199 passed.")

    # print("All test cases passed!")

    # Tests for .edu only

    # Case 1: Test with an empty string
    print("Case 1")
    print(extract_urls(''))
    assert extract_urls('') == []
    print("Case 1 passed.")

    # Case 2: Test with a string without any URLs
    print("Case 2")
    print(extract_urls('This is a test string with no URLs.'))
    assert extract_urls('This is a test string with no URLs.') == []
    print("Case 2 passed.")

    # Case 3: Test with a valid URL starting with 'http'
    print("Case 3")
    print(extract_urls('Here is a link: http://example.edu'))
    assert extract_urls('Here is a link: http://example.edu') == ['http://example.edu']
    print("Case 3 passed.")

    # Case 4: Test with a valid URL starting with 'https'
    print("Case 4")
    print(extract_urls('Here is a secure link: https://example.edu'))
    assert extract_urls('Here is a secure link: https://example.edu') == ['https://example.edu']
    print("Case 4 passed.")

    # Case 5: Test with a URL starting with 'www'
    print("Case 5")
    print(extract_urls('Go to www.example.edu for more info.'))
    assert extract_urls('Go to www.example.edu for more info.') == ['http://www.example.edu']
    print("Case 5 passed.")

    # Case 6: Test with multiple URLs in one string
    print("Case 6")
    print(extract_urls('Check these: http://example.edu, https://example.edu, www.example.edu'))
    assert extract_urls('Check these: http://example.edu, https://example.edu, www.example.edu') == ['http://example.edu', 'https://example.edu', 'http://www.example.edu']
    print("Case 6 passed.")

    # Case 7: Test with URLs followed by punctuation
    print("Case 7")
    print(extract_urls('Visit www.example.edu! Or check out https://example.edu, for updates.'))
    assert extract_urls('Visit www.example.edu! Or check out https://example.edu, for updates.') == ['http://www.example.edu', 'https://example.edu']
    print("Case 7 passed.")

    # Case 8: Test with URLs ending in different domains
    print("Case 8")
    print(extract_urls('Visit http://example.edu and http://example.edu for details.'))
    assert extract_urls('Visit http://example.edu and http://example.edu for details.') == ['http://example.edu', 'http://example.edu']
    print("Case 8 passed.")

    # Case 9: Test with URLs ending with a slash
    print("Case 9")
    print(extract_urls('Check https://example.edu/ for the homepage.'))
    assert extract_urls('Check https://example.edu/ for the homepage.') == ['https://example.edu/']
    print("Case 9 passed.")

    # Case 10: Test with mixed case URLs
    print("Case 10")
    print(extract_urls('Visit Https://Example.edu for secure content.'))
    assert extract_urls('Visit Https://Example.edu for secure content.') == ['Https://Example.edu']
    print("Case 10 passed.")

    # Case 11: Test with a URL having query parameters
    print("Case 11")
    print(extract_urls('Check https://example.edu/search?q=test for more.'))
    assert extract_urls('Check https://example.edu/search?q=test for more.') == ['https://example.edu/search?q=test']
    print("Case 11 passed.")

    # Case 13: Test with a string that is not a valid URL (e.g., missing TLD)
    print("Case 13")
    print(extract_urls('Invalid: http://example'))
    assert extract_urls('Invalid: http://example') == []
    print("Case 13 passed.")

    # Case 14: Test with input that is not a string (e.g., an integer)
    print("Case 14")
    try:
        extract_urls(12345)
        print(extract_urls(12345))
    except TypeError:
        print("TypeError: Input must be a string.")
        pass  # Expecting TypeError for non-string input
    print("Case 14 passed.")

    # Case 15: Test with a URL inside parentheses
    print("Case 15")
    print(extract_urls('Visit (https://example.edu) for more info.'))
    assert extract_urls('Visit (https://example.edu) for more info.') == ['https://example.edu']
    print("Case 15 passed.")

    # Case 15B: Test with a URL inside parentheses
    print("Case 15B")
    print(extract_urls('Visit [https://example.edu] for more info.'))
    assert extract_urls('Visit [https://example.edu] for more info.') == ['https://example.edu']
    print("Case 15B passed.")

    # Case 16: Test with a string containing file paths but no URLs
    print("Case 16")
    print(extract_urls('Open the file at C:/Users/Documents/file.txt or /usr/local/bin/script.sh'))
    assert extract_urls('Open the file at C:/Users/Documents/file.txt or /usr/local/bin/script.sh') == []
    print("Case 16 passed.")

    # Case 17: Test with a URL followed by other text without a space
    print("Case 17")
    print(extract_urls('Check https://example.edu/more-text'))
    assert extract_urls('Check https://example.edu/more-text') == ['https://example.edu/more-text']
    print("Case 17 passed.")

    # Case 18: Test with a simple URL
    print("Case 18")
    print(extract_urls('Visit https://example.edu'))
    assert extract_urls('Visit https://example.edu') == ['https://example.edu']
    print("Case 18 passed.")

    # Case 19: Test with an HTTP URL
    print("Case 19")
    print(extract_urls('Go to http://example.edu'))
    assert extract_urls('Go to http://example.edu') == ['http://example.edu']
    print("Case 19 passed.")

    # Case 20: Test with no URL in the string
    print("Case 20")
    print(extract_urls('No URL here'))
    assert extract_urls('No URL here') == []
    print("Case 20 passed.")

    # Case 21: Test with multiple URLs in a single string
    print("Case 21")
    print(extract_urls('Check https://example.edu and http://another-example.edu'))
    assert extract_urls('Check https://example.edu and http://another-example.edu') == ['https://example.edu', 'http://another-example.edu']
    print("Case 21 passed.")

    # Case 22: Test with URLs without HTTP/HTTPS scheme
    print("Case 22")
    print(extract_urls('Go to www.example.edu'))
    assert extract_urls('Go to www.example.edu') == ['http://www.example.edu']
    print("Case 22 passed.")

    # Case 23: Test with URL containing query parameters
    print("Case 23")
    print(extract_urls('Search https://example.edu?q=python'))
    assert extract_urls('Search https://example.edu?q=python') == ['https://example.edu?q=python']
    print("Case 23 passed.")

    # Case 24: Test with URL containing special characters
    print("Case 24")
    print(extract_urls('Look at https://example.edu/page#section'))
    assert extract_urls('Look at https://example.edu/page#section') == ['https://example.edu/page#section']
    print("Case 24 passed.")

    # Case 25: Test with a URL inside parentheses
    print("Case 25")
    print(extract_urls('Here is the link (https://example.edu)'))
    assert extract_urls('Here is the link (https://example.edu)') == ['https://example.edu']
    print("Case 25 passed.")

    # Case 26: Test with a URL followed by punctuation
    print("Case 26")
    print(extract_urls('Visit https://example.edu.'))
    assert extract_urls('Visit https://example.edu.') == ['https://example.edu']
    print("Case 26 passed.")

    # Case 27: Test with a URL embedded in HTML
    print("Case 27")
    print(extract_urls('<a href="https://example.edu">Link</a>'))
    assert extract_urls('<a href="https://example.edu">Link</a>') == ['https://example.edu']
    print("Case 27 passed.")

    # Case 28: Test with a malformed URL (missing a dot)
    print("Case 28")
    print(extract_urls('Visit http://exampleedu'))
    assert extract_urls('Visit http://exampleedu') == []
    print("Case 28 passed.")

    # Case 29: Test with an incomplete URL (missing the domain)
    print("Case 29")
    print(extract_urls('Visit https://'))
    assert extract_urls('Visit https://') == []
    print("Case 29 passed.")

    # Case 30: Test with a long URL with multiple subdomains
    print("Case 30")
    print(extract_urls('Find more at https://sub.domain.example.edu/page'))
    assert extract_urls('Find more at https://sub.domain.example.edu/page') == ['https://sub.domain.example.edu/page']
    print("Case 30 passed.")

    # Case 31: Test with an FTP URL
    print("Case 31")
    print(extract_urls('Download from ftp://example.edu/file'))
    assert extract_urls('Download from ftp://example.edu/file') == ['ftp://example.edu/file']
    print("Case 31 passed.")

    # Case 32: Test with a URL containing port number
    print("Case 32")
    print(extract_urls('Access the service at https://example.edu:8080/service'))
    assert extract_urls('Access the service at https://example.edu:8080/service') == ['https://example.edu:8080/service']
    print("Case 32 passed.")

    # Case 34: Test with a URL containing Unicode characters
    print("Case 34")
    print(extract_urls('Find at https://example.edu/привет'))
    assert extract_urls('Find at https://example.edu/привет') == ['https://example.edu/привет']
    print("Case 34 passed.")

    # Case 35: Test with a URL followed by text with no space
    print("Case 35")
    print(extract_urls('See https://example.edu/somethingelse'))
    assert extract_urls('See https://example.edu/somethingelse') == ['https://example.edu/somethingelse']
    print("Case 35 passed.")

    # Case 36: Test with a URL in a JSON-like structure
    print("Case 36")
    print(extract_urls('{"url": "https://example.edu/api"}'))
    assert extract_urls('{"url": "https://example.edu/api"}') == ['https://example.edu/api']
    print("Case 36 passed.")

    # Case 37: Test with a URL containing query parameters and fragment
    print("Case 37")
    print(extract_urls('Go to https://example.edu/page?query=param#section'))
    assert extract_urls('Go to https://example.edu/page?query=param#section') == ['https://example.edu/page?query=param#section']
    print("Case 37 passed.")

    # Case 38: Test with multiple URLs separated by spaces
    print("Case 38")
    print(extract_urls('Visit https://site1.edu and https://site2.edu for details.'))
    assert extract_urls('Visit https://site1.edu and https://site2.edu for details.') == ['https://site1.edu', 'https://site2.edu']
    print("Case 38 passed.")

    # Case 39: Test with a URL inside parentheses
    print("Case 39")
    print(extract_urls('Check this (https://example.edu) for reference.'))
    assert extract_urls('Check this (https://example.edu) for reference.') == ['https://example.edu']
    print("Case 39 passed.")

    # Case 40: Test with a URL followed by punctuation
    print("Case 40")
    print(extract_urls('Go to https://example.edu!'))
    assert extract_urls('Go to https://example.edu!') == ['https://example.edu']
    print("Case 40 passed.")

    # Case 41: Test with URL in a sentence followed by a comma
    print("Case 41")
    print(extract_urls('Check out https://example.edu, it’s amazing.'))
    assert extract_urls('Check out https://example.edu, it’s amazing.') == ['https://example.edu']
    print("Case 41 passed.")

    # Case 42: Test with no URL in the text
    print("Case 42")
    print(extract_urls('This sentence has no URLs.'))
    assert extract_urls('This sentence has no URLs.') == []
    print("Case 42 passed.")

    # Case 43: Test with a URL without http/https scheme
    print("Case 43")
    print(extract_urls('Visit www.example.edu for details.'))
    assert extract_urls('Visit www.example.edu for details.') == ["http://www.example.edu"]
    print("Case 43 passed.")

    # Case 44: Test with a malformed URL (missing domain)
    print("Case 44")
    print(extract_urls('Go to https:// for details.'))
    assert extract_urls('Go to https:// for details.') == []
    print("Case 44 passed.")

    # Case 45: Test with an email address (should not be extracted)
    print("Case 45")
    print(extract_urls('Contact us at info@example.edu.'))
    assert extract_urls('Contact us at info@example.edu.') == []
    print("Case 45 passed.")

    # Case 46: Test with an FTP URL
    print("Case 46")
    print(extract_urls('Download from ftp://ftp.example.edu'))
    assert extract_urls('Download from ftp://ftp.example.edu') == ['ftp://ftp.example.edu']
    print("Case 46 passed.")

    # Case 47: Test with a URL in a very long text
    print("Case 47")
    long_text = 'This is a long text with a URL somewhere in it. Here it is: https://example.edu/page. The rest of the text follows.'
    print(extract_urls(long_text))
    assert extract_urls(long_text) == ['https://example.edu/page']
    print("Case 47 passed.")

    # Case 48: Test with multiple URLs with mixed schemes
    print("Case 48")
    print(extract_urls('Check https://example.edu and ftp://ftp.example.edu'))
    assert extract_urls('Check https://example.edu and ftp://ftp.example.edu') == ['https://example.edu', 'ftp://ftp.example.edu']
    print("Case 48 passed.")

    # Case 49: Test with a URL in an HTML anchor tag
    print("Case 49")
    print(extract_urls('<a href="https://example.edu">Link</a>'))
    assert extract_urls('<a href="https://example.edu">Link</a>') == ['https://example.edu']
    print("Case 49 passed.")

    # Case 50: Test with a URL in square brackets
    print("Case 50")
    print(extract_urls('Visit [https://example.edu] for details.'))
    assert extract_urls('Visit [https://example.edu] for details.') == ['https://example.edu']
    print("Case 50 passed.")

    # Case 51: Test with a string containing only a URL
    print("Case 51")
    print(extract_urls('https://example.edu'))
    assert extract_urls('https://example.edu') == ['https://example.edu']
    print("Case 51 passed.")

    # Case 53: Test with a very long URL
    print("Case 53")
    long_url = 'https://example.edu/' + 'a' * 100
    print(extract_urls(f'Check out this long URL: {long_url}'))
    assert extract_urls(f'Check out this long URL: {long_url}') == [long_url]
    print("Case 53 passed.")

    # Case 54: Test with a URL containing special characters
    print("Case 54")
    print(extract_urls('Special characters in URL: https://example.edu/this!is*a_test'))
    assert extract_urls('Special characters in URL: https://example.edu/this!is*a_test') == ['https://example.edu/this!is*a_test']
    print("Case 54 passed.")

    # Case 55: Test with a URL surrounded by whitespace
    print("Case 55")
    print(extract_urls('    https://example.edu    '))
    assert extract_urls('    https://example.edu    ') == ['https://example.edu']
    print("Case 55 passed.")

    # Case 55: Test URL with www and no scheme
    print("Case 55")
    print(extract_urls('Visit www.example.edu for more info'))
    assert extract_urls('Visit www.example.edu for more info') == ['http://www.example.edu']
    print("Case 55 passed.")

    # Case 56: Test URL with special characters in the query string
    print("Case 56")
    print(extract_urls('Check out https://example.edu?search=test&sort=asc#anchor'))
    assert extract_urls('Check out https://example.edu?search=test&sort=asc#anchor') == ['https://example.edu?search=test&sort=asc#anchor']
    print("Case 56 passed.")

    # Case 57: Test malformed URL with missing domain
    print("Case 57")
    print(extract_urls('Check out https:// for more info'))
    assert extract_urls('Check out https:// for more info') == []
    print("Case 57 passed.")

    # Case 58: Test URL embedded within parentheses
    print("Case 58")
    print(extract_urls('Find more details (https://example.edu/info).'))
    assert extract_urls('Find more details (https://example.edu/info).') == ['https://example.edu/info']
    print("Case 58 passed.")

    # Case 59: Test URL inside a sentence with punctuation
    print("Case 59")
    print(extract_urls('This is an important link: https://example.edu, and you should visit it.'))
    assert extract_urls('This is an important link: https://example.edu, and you should visit it.') == ['https://example.edu']
    print("Case 59 passed.")

    # Case 60: Test URL with mixed-case scheme
    print("Case 60")
    print(extract_urls('Go to HtTpS://example.edu for more info'))
    assert extract_urls('Go to HtTpS://example.edu for more info') == ['HtTpS://example.edu']
    print("Case 60 passed.")

    # Case 61: Test URL followed by newline
    print("Case 61")
    print(extract_urls('Here is the link:\nhttps://example.edu'))
    assert extract_urls('Here is the link:\nhttps://example.edu') == ['https://example.edu']
    print("Case 61 passed.")

    # Case 62: Test text with multiple URLs
    print("Case 62")
    print(extract_urls('Go to https://example.edu and https://another.edu for details.'))
    assert extract_urls('Go to https://example.edu and https://another.edu for details.') == ['https://example.edu', 'https://another.edu']
    print("Case 62 passed.")

    # Case 63: Test URL with port number
    print("Case 63")
    print(extract_urls('Visit http://example.edu:8080 for access'))
    assert extract_urls('Visit http://example.edu:8080 for access') == ['http://example.edu:8080']
    print("Case 63 passed.")

    # Case 67: Test broken URL with scheme but no domain
    print("Case 67")
    print(extract_urls('Broken URL https:// for more info'))
    assert extract_urls('Broken URL https:// for more info') == []
    print("Case 67 passed.")

    # Case 68: Test URL with uncommon protocol (FTP)
    print("Case 68")
    print(extract_urls('Download from ftp://example.edu/resource.zip'))
    assert extract_urls('Download from ftp://example.edu/resource.zip') == ['ftp://example.edu/resource.zip']
    print("Case 68 passed.")

    # Case 69: Test URL within HTML tag
    print("Case 69")
    print(extract_urls('Visit <a href="https://example.edu">our website</a>'))
    assert extract_urls('Visit <a href="https://example.edu">our website</a>') == ['https://example.edu']
    print("Case 69 passed.")

    # Case 70: Test URL in a JSON string
    print("Case 70")
    print(extract_urls('{"url": "https://example.edu"}'))
    assert extract_urls('{"url": "https://example.edu"}') == ['https://example.edu']
    print("Case 70 passed.")

    # Case 71: Test URL with a username and password
    print("Case 71")
    print(extract_urls('Visit http://user:password@example.edu for access'))
    assert extract_urls('Visit http://user:password@example.edu for access') == ['http://user:password@example.edu']
    print("Case 71 passed.")

    # Case 72: Test URL with emoji in query string
    print("Case 72")
    print(extract_urls('Go to https://example.edu/search?query=😊 for more info'))
    assert extract_urls('Go to https://example.edu/search?query=😊 for more info') == ['https://example.edu/search?query=😊']
    print("Case 72 passed.")

    # Case 73: Test URL with encoded characters
    print("Case 73")
    print(extract_urls('Download from https://example.edu/resource%20name.zip'))
    assert extract_urls('Download from https://example.edu/resource%20name.zip') == ['https://example.edu/resource%20name.zip']
    print("Case 73 passed.")

    # Case 74: Test URL with trailing dot
    print("Case 74")
    print(extract_urls('Visit https://example.edu. for more info'))
    assert extract_urls('Visit https://example.edu. for more info') == ['https://example.edu']
    print("Case 74 passed.")

    # Case 75: Test URL surrounded by commas
    print("Case 75")
    print(extract_urls('Check out this website,https://example.edu, for more info'))
    assert extract_urls('Check out this website,https://example.edu, for more info') == ['https://example.edu']
    print("Case 75 passed.")

    # Case 76: Test text with invalid URL scheme
    print("Case 76")
    print(extract_urls('This is not a valid URL scheme: mailto:user@example.edu'))
    assert extract_urls('This is not a valid URL scheme: mailto:user@example.edu') == []
    print("Case 76 passed.")

    # Case 77: Test text with a file path that looks like a URL
    print("Case 77")
    print(extract_urls('Here is a file path: C:\\Program Files\\example'))
    assert extract_urls('Here is a file path: C:\\Program Files\\example') == []
    print("Case 77 passed.")

    # Case 78: Test URL with fragment identifier
    print("Case 78")
    print(extract_urls('Visit https://example.edu#section1 for details'))
    assert extract_urls('Visit https://example.edu#section1 for details') == ['https://example.edu#section1']
    print("Case 78 passed.")

    # Case 79: Test with multiple URLs in the same string
    print("Case 79")
    print(extract_urls('Here are two URLs: https://example.edu and https://another.edu'))
    assert extract_urls('Here are two URLs: https://example.edu and https://another.edu') == ['https://example.edu', 'https://another.edu']
    print("Case 79 passed.")

    # Case 80: Test with no URLs
    print("Case 80")
    print(extract_urls('No URLs here'))
    assert extract_urls('No URLs here') == []
    print("Case 80 passed.")

    # Case 81: Test with an invalid URL format
    print("Case 81")
    print(extract_urls('Check this URL: htt://example.edu'))
    assert extract_urls('Check this URL: htt://example.edu') == []
    print("Case 81 passed.")

    # Case 82: Test with URL surrounded by punctuation
    print("Case 82")
    print(extract_urls('Here is a URL: (https://example.edu)!'))
    assert extract_urls('Here is a URL: (https://example.edu)!') == ['https://example.edu']
    print("Case 82 passed.")

    # Case 83: Test with URL without protocol
    print("Case 83")
    print(extract_urls('Visit example.edu for more information'))
    assert extract_urls('Visit example.edu for more information') == []
    print("Case 83 passed.")

    # Case 84: Test with www URL without protocol
    print("Case 84")
    print(extract_urls('Visit www.example.edu for more information'))
    assert extract_urls('Visit www.example.edu for more information') == ['http://www.example.edu']
    print("Case 84 passed.")

    # Case 85: Test with a URL that includes query parameters
    print("Case 85")
    print(extract_urls('Check this URL with query: https://example.edu?query=test'))
    assert extract_urls('Check this URL with query: https://example.edu?query=test') == ['https://example.edu?query=test']
    print("Case 85 passed.")

    # Case 86: Test with a URL that includes fragments
    print("Case 86")
    print(extract_urls('Check this fragment URL: https://example.edu#section2'))
    assert extract_urls('Check this fragment URL: https://example.edu#section2') == ['https://example.edu#section2']
    print("Case 86 passed.")

    # Case 87: Test with multiple consecutive URLs without spaces
    print("Case 87")
    print(extract_urls('https://first.eduhttps://second.edu'))
    assert extract_urls('https://first.eduhttps://second.edu') == ['https://first.edu', 'https://second.edu']
    print("Case 87 passed.")

    # Case 88: Test with a URL containing port number
    print("Case 88")
    print(extract_urls('Check this URL with port: https://example.edu:8080'))
    assert extract_urls('Check this URL with port: https://example.edu:8080') == ['https://example.edu:8080']
    print("Case 88 passed.")

    # Case 90: Test with a URL in the middle of a long string
    print("Case 90")
    print(extract_urls('Before the URL https://example.edu after the URL'))
    assert extract_urls('Before the URL https://example.edu after the URL') == ['https://example.edu']
    print("Case 90 passed.")

    # Case 91: Test with a URL in parentheses
    print("Case 91")
    print(extract_urls('(https://example.edu)'))
    assert extract_urls('(https://example.edu)') == ['https://example.edu']
    print("Case 91 passed.")

    # Case 92: Test with a URL followed by a period
    print("Case 92")
    print(extract_urls('Check this URL: https://example.edu. It is useful.'))
    assert extract_urls('Check this URL: https://example.edu. It is useful.') == ['https://example.edu']
    print("Case 92 passed.")

    # Case 93: Test with a URL in HTML-like structure
    print("Case 93")
    print(extract_urls('<a href="https://example.edu">Example</a>'))
    assert extract_urls('<a href="https://example.edu">Example</a>') == ['https://example.edu']
    print("Case 93 passed.")

    # Case 94: Test with a URL that has a subdomain
    print("Case 94")
    print(extract_urls('Visit https://sub.example.edu for more info'))
    assert extract_urls('Visit https://sub.example.edu for more info') == ['https://sub.example.edu']
    print("Case 94 passed.")

    # Case 95: Test with a URL followed by special characters
    print("Case 95")
    print(extract_urls('Check https://example.edu?! for more info'))
    assert extract_urls('Check https://example.edu?! for more info') == ['https://example.edu']
    print("Case 95 passed.")

    # Case 96: Test with malformed URL that starts with https
    print("Case 96")
    print(extract_urls('Check https:///example.edu'))
    assert extract_urls('Check https:///example.edu') == []
    print("Case 96 passed.")

    # Case 97: Test with an FTP URL
    print("Case 97")
    print(extract_urls('Download file from ftp://example.edu/file.txt'))
    assert extract_urls('Download file from ftp://example.edu/file.txt') == ['ftp://example.edu/file.txt']
    print("Case 97 passed.")

    # Case 98: Test with a very long URL
    print("Case 98")
    print(extract_urls('This is a very long URL: https://example.edu/' + 'a'*1000))
    assert extract_urls('This is a very long URL: https://example.edu/' + 'a'*1000) == ['https://example.edu/' + 'a'*1000]
    print("Case 98 passed.")

    # Case 99: Test with an empty string
    print("Case 99")
    print(extract_urls(''))
    assert extract_urls('') == []
    print("Case 99 passed.")

    # Case 100: Test with URL inside a sentence with no spaces
    print("Case 100")
    print(extract_urls('ThisisnotanURLbutthisoneishttps://example.edu'))
    assert extract_urls('ThisisnotanURLbutthisoneishttps://example.edu') == ['https://example.edu']
    print("Case 100 passed.")

    # Case 101: Test a simple HTTP URL
    print("Case 101")
    print(extract_urls('Visit http://example.edu'))
    assert extract_urls('Visit http://example.edu') == ['http://example.edu']
    print("Case 101 passed.")

    # Case 102: Test a simple HTTPS URL
    print("Case 102")
    print(extract_urls('Visit https://example.edu'))
    assert extract_urls('Visit https://example.edu') == ['https://example.edu']
    print("Case 102 passed.")

    # Case 103: Test with a URL with query parameters
    print("Case 103")
    print(extract_urls('Check out https://example.edu/search?q=test'))
    assert extract_urls('Check out https://example.edu/search?q=test') == ['https://example.edu/search?q=test']
    print("Case 103 passed.")

    # Case 104: Test with multiple URLs in the string
    print("Case 104")
    print(extract_urls('Visit http://example.edu and https://another.edu'))
    assert extract_urls('Visit http://example.edu and https://another.edu') == ['http://example.edu', 'https://another.edu']
    print("Case 104 passed.")

    # Case 105: Test with a URL followed by punctuation
    print("Case 105")
    print(extract_urls('Go to http://example.edu, now!'))
    assert extract_urls('Go to http://example.edu, now!') == ['http://example.edu']
    print("Case 105 passed.")

    # Case 106: Test with a URL without "www."
    print("Case 106")
    print(extract_urls('Try https://example.edu/path/to/page'))
    assert extract_urls('Try https://example.edu/path/to/page') == ['https://example.edu/path/to/page']
    print("Case 106 passed.")

    # Case 107: Test with a URL with a subdomain
    print("Case 107")
    print(extract_urls('Visit https://sub.example.edu'))
    assert extract_urls('Visit https://sub.example.edu') == ['https://sub.example.edu']
    print("Case 107 passed.")

    # Case 109: Test with a URL missing the protocol
    print("Case 109")
    print(extract_urls('www.example.edu/path/to/page'))
    assert extract_urls('www.example.edu/path/to/page') == ['http://www.example.edu/path/to/page']
    print("Case 109 passed.")

    # Case 110: Test with a malformed URL (missing TLD)
    print("Case 110")
    print(extract_urls('Check out http://example'))
    assert extract_urls('Check out http://example') == []
    print("Case 110 passed.")

    # Case 111: Test with multiple URLs separated by commas
    print("Case 111")
    print(extract_urls('Visit http://example.edu,https://another.edu'))
    assert extract_urls('Visit http://example.edu,https://another.edu') == ['http://example.edu', 'https://another.edu']
    print("Case 111 passed.")

    # Case 113: Test with a URL containing port number
    print("Case 113")
    print(extract_urls('Visit http://example.edu:8080/path'))
    assert extract_urls('Visit http://example.edu:8080/path') == ['http://example.edu:8080/path']
    print("Case 113 passed.")

    # Case 114: Test with FTP protocol
    print("Case 114")
    print(extract_urls('Download from ftp://example.edu/file'))
    assert extract_urls('Download from ftp://example.edu/file') == ['ftp://example.edu/file']
    print("Case 114 passed.")

    # Case 116: Test with URL inside parentheses
    print("Case 116")
    print(extract_urls('Find more info (https://example.edu) here'))
    assert extract_urls('Find more info (https://example.edu) here') == ['https://example.edu']
    print("Case 116 passed.")

    # Case 117: Test with multiple URLs inside parentheses
    print("Case 117")
    print(extract_urls('(http://example.edu) and (https://another.edu)'))
    assert extract_urls('(http://example.edu) and (https://another.edu)') == ['http://example.edu', 'https://another.edu']
    print("Case 117 passed.")

    # Case 118: Test with a URL missing the protocol but prefixed with a space
    print("Case 118")
    print(extract_urls(' Go to www.example.edu for more'))
    assert extract_urls(' Go to www.example.edu for more') == ["http://www.example.edu"]
    print("Case 118 passed.")

    # Case 119: Test with a URL with a long subdomain
    print("Case 119")
    print(extract_urls('Visit https://sub.sub.example.edu/path'))
    assert extract_urls('Visit https://sub.sub.example.edu/path') == ['https://sub.sub.example.edu/path']
    print("Case 119 passed.")

    # Case 120: Test with URL containing hash fragment
    print("Case 120")
    print(extract_urls('Check out https://example.edu/page#section'))
    assert extract_urls('Check out https://example.edu/page#section') == ['https://example.edu/page#section']
    print("Case 120 passed.")

    # Case 121: Test with email address (should not be extracted as URL)
    print("Case 121")
    print(extract_urls('Contact me at test@example.edu'))
    assert extract_urls('Contact me at test@example.edu') == []
    print("Case 121 passed.")

    # Case 123: Test with URL with hyphenated domain
    print("Case 123")
    print(extract_urls('Go to http://my-example.edu'))
    assert extract_urls('Go to http://my-example.edu') == ['http://my-example.edu']
    print("Case 123 passed.")

    # Case 125: Test with a very long URL
    print("Case 125")
    long_url = 'https://example.edu/' + 'a' * 500
    print(extract_urls(f'Check out {long_url}'))
    assert extract_urls(f'Check out {long_url}') == [long_url]
    print("Case 125 passed.")

    # Case 126: Test with a URL containing escaped characters
    print("Case 126")
    print(extract_urls('Visit https://example.edu/path%20with%20spaces'))
    assert extract_urls('Visit https://example.edu/path%20with%20spaces') == ['https://example.edu/path%20with%20spaces']
    print("Case 126 passed.")

    # Case 127: Test with a URL inside parentheses
    print("Case 127")
    print(extract_urls('Here is a link (https://example.edu)'))
    assert extract_urls('Here is a link (https://example.edu)') == ['https://example.edu']
    print("Case 127 passed.")

    # Case 128: Test with a URL surrounded by quotation marks
    print("Case 128")
    print(extract_urls('"https://example.edu" is the site'))
    assert extract_urls('"https://example.edu" is the site') == ['https://example.edu']
    print("Case 128 passed.")

    # Case 129: Test with a URL with a fragment identifier
    print("Case 129")
    print(extract_urls('Go to https://example.edu/page#section1'))
    assert extract_urls('Go to https://example.edu/page#section1') == ['https://example.edu/page#section1']
    print("Case 129 passed.")

    # Case 130: Test with multiple URLs in the same string
    print("Case 130")
    print(extract_urls('Links: https://example.edu and https://another.edu'))
    assert extract_urls('Links: https://example.edu and https://another.edu') == ['https://example.edu', 'https://another.edu']
    print("Case 130 passed.")

    # Case 131: Test with a URL that has query parameters
    print("Case 131")
    print(extract_urls('Search here: https://example.edu/search?q=test'))
    assert extract_urls('Search here: https://example.edu/search?q=test') == ['https://example.edu/search?q=test']
    print("Case 131 passed.")

    # Case 132: Test with URL that contains a port number
    print("Case 132")
    print(extract_urls('Go to https://example.edu:8080/path'))
    assert extract_urls('Go to https://example.edu:8080/path') == ['https://example.edu:8080/path']
    print("Case 132 passed.")

    # Case 133: Test with a URL that has internationalized domain name
    print("Case 133")
    print(extract_urls('Visit https://xn--fsq.edu'))
    assert extract_urls('Visit https://xn--fsq.edu') == ['https://xn--fsq.edu']
    print("Case 133 passed.")

    # Case 134: Test with a URL that is broken (missing scheme)
    print("Case 134")
    print(extract_urls('Check example.edu/path'))
    assert extract_urls('Check example.edu/path') == []
    print("Case 134 passed.")

    # Case 135: Test with a URL that uses ftp scheme
    print("Case 135")
    print(extract_urls('Download here: ftp://example.edu/file'))
    assert extract_urls('Download here: ftp://example.edu/file') == ['ftp://example.edu/file']
    print("Case 135 passed.")

    # Case 136: Test with an email address mistaken for a URL
    print("Case 136")
    print(extract_urls('Contact us at test@example.edu'))
    assert extract_urls('Contact us at test@example.edu') == []
    print("Case 136 passed.")

    # Case 137: Test with a URL containing special characters
    print("Case 137")
    print(extract_urls('Visit https://example.edu/path?name=John&age=25!'))
    assert extract_urls('Visit https://example.edu/path?name=John&age=25!') == ['https://example.edu/path?name=John&age=25']
    print("Case 137 passed.")

    # Case 138: Test with a URL surrounded by non-breaking spaces
    print("Case 138")
    print(extract_urls('Here is a link: https://example.edu/path'))
    assert extract_urls('Here is a link: https://example.edu/path') == ['https://example.edu/path']
    print("Case 138 passed.")

    # Case 139: Test with a URL with underscore in domain
    print("Case 139")
    print(extract_urls('Check https://sub_domain.example.edu'))
    assert extract_urls('Check https://sub_domain.example.edu') == ['https://sub_domain.example.edu']
    print("Case 139 passed.")

    # Case 140: Test with a very long URL
    print("Case 140")
    long_url = 'https://example.edu/' + 'a'*500
    print(extract_urls(f'Long URL: {long_url}'))
    assert extract_urls(f'Long URL: {long_url}') == [long_url]
    print("Case 140 passed.")

    # Case 141: Test with a URL missing a TLD
    print("Case 141")
    print(extract_urls('This URL is broken: https://example'))
    assert extract_urls('This URL is broken: https://example') == []
    print("Case 141 passed.")

    # Case 143: Test with an empty string
    print("Case 143")
    print(extract_urls(''))
    assert extract_urls('') == []
    print("Case 143 passed.")

    # Case 144: Test with a URL in a sentence without protocol (http)
    print("Case 144")
    print(extract_urls('Visit www.example.edu for more info.'))
    assert extract_urls('Visit www.example.edu for more info.') == ["http://www.example.edu"]
    print("Case 144 passed.")

    # Case 145: Test with URL including subdomain
    print("Case 145")
    print(extract_urls('Access https://subdomain.example.edu'))
    assert extract_urls('Access https://subdomain.example.edu') == ['https://subdomain.example.edu']
    print("Case 145 passed.")

    # Case 147: Test with URL containing path traversal (..)
    print("Case 147")
    print(extract_urls('Access https://example.edu/../../etc/passwd'))
    assert extract_urls('Access https://example.edu/../../etc/passwd') == ['https://example.edu/../../etc/passwd']
    print("Case 147 passed.")

    # Case 148: Test with a URL followed by punctuation
    print("Case 148")
    print(extract_urls('Go to https://example.edu/path.'))
    assert extract_urls('Go to https://example.edu/path.') == ['https://example.edu/path']
    print("Case 148 passed.")

    # Case 149: Test with URL starting with http only (without https)
    print("Case 149")
    print(extract_urls('Visit http://example.edu'))
    assert extract_urls('Visit http://example.edu') == ['http://example.edu']
    print("Case 149 passed.")

    # Case 150: Test with a simple URL
    print("Case 150")
    print(extract_urls('Visit https://example.edu'))
    assert extract_urls('Visit https://example.edu') == ['https://example.edu']
    print("Case 150 passed.")

    # Case 151: Test with multiple URLs in one string
    print("Case 151")
    print(extract_urls('Check https://example.edu and http://test.edu'))
    assert extract_urls('Check https://example.edu and http://test.edu') == ['https://example.edu', 'http://test.edu']
    print("Case 151 passed.")

    # Case 152: Test with URLs with query parameters
    print("Case 152")
    print(extract_urls('Search https://example.edu/search?q=python'))
    assert extract_urls('Search https://example.edu/search?q=python') == ['https://example.edu/search?q=python']
    print("Case 152 passed.")

    # Case 153: Test with URLs containing fragments
    print("Case 153")
    print(extract_urls('Navigate to https://example.edu/page#section'))
    assert extract_urls('Navigate to https://example.edu/page#section') == ['https://example.edu/page#section']
    print("Case 153 passed.")

    # Case 154: Test with a URL that doesn't have a protocol
    print("Case 154")
    print(extract_urls('Visit www.example.edu'))
    assert extract_urls('Visit www.example.edu') == ["http://www.example.edu"]
    print("Case 154 passed.")

    # Case 155: Test with a URL with different protocol (ftp)
    print("Case 155")
    print(extract_urls('Download via ftp://ftp.example.edu/file'))
    assert extract_urls('Download via ftp://ftp.example.edu/file') == ['ftp://ftp.example.edu/file']
    print("Case 155 passed.")

    # Case 156: Test with a URL followed by punctuation
    print("Case 156")
    print(extract_urls('Visit https://example.edu!'))
    assert extract_urls('Visit https://example.edu!') == ['https://example.edu']
    print("Case 156 passed.")

    # Case 157: Test with URL and text without space between them
    print("Case 157")
    print(extract_urls('Here is the link:https://example.edu.'))
    assert extract_urls('Here is the link:https://example.edu.') == ['https://example.edu']
    print("Case 157 passed.")

    # Case 158: Test with URL inside parentheses
    print("Case 158")
    print(extract_urls('Visit (https://example.edu) for details.'))
    assert extract_urls('Visit (https://example.edu) for details.') == ['https://example.edu']
    print("Case 158 passed.")

    # Case 161: Test with incomplete URL (missing top-level domain)
    print("Case 161")
    print(extract_urls('Check http://example for details'))
    assert extract_urls('Check http://example for details') == []
    print("Case 161 passed.")

    # Case 163: Test with a mix of text and multiple URLs with spaces
    print("Case 163")
    print(extract_urls('Links: https://a.edu https://b.edu http://c.edu'))
    assert extract_urls('Links: https://a.edu https://b.edu http://c.edu') == ['https://a.edu', 'https://b.edu', 'http://c.edu']
    print("Case 163 passed.")

    # Case 164: Test with a URL containing dashes
    print("Case 164")
    print(extract_urls('Visit https://example-site.edu for more info.'))
    assert extract_urls('Visit https://example-site.edu for more info.') == ['https://example-site.edu']
    print("Case 164 passed.")

    # Case 165: Test with a URL containing underscores
    print("Case 165")
    print(extract_urls('Visit https://example_site.edu for more info.'))
    assert extract_urls('Visit https://example_site.edu for more info.') == ['https://example_site.edu']
    print("Case 165 passed.")

    # Case 166: Test with a URL inside HTML tag
    print("Case 166")
    print(extract_urls('<a href="https://example.edu">Click here</a>'))
    assert extract_urls('<a href="https://example.edu">Click here</a>') == ['https://example.edu']
    print("Case 166 passed.")

    # Case 167: Test with a URL containing special characters in the query string
    print("Case 167")
    print(extract_urls('Find it at https://example.edu/search?q=a+b&sort=asc'))
    assert extract_urls('Find it at https://example.edu/search?q=a+b&sort=asc') == ['https://example.edu/search?q=a+b&sort=asc']
    print("Case 167 passed.")

    # Case 169: Test with a URL containing Unicode characters
    print("Case 169")
    print(extract_urls('Visit https://exämple.edu for more details.'))
    assert extract_urls('Visit https://exämple.edu for more details.') == ['https://exämple.edu']
    print("Case 169 passed.")

    # Case 170: Test with an empty string
    print("Case 170")
    print(extract_urls(''))
    assert extract_urls('') == []
    print("Case 170 passed.")

    # Case 171: Test with only text and no URL
    print("Case 171")
    print(extract_urls('This is a test without any links.'))
    assert extract_urls('This is a test without any links.') == []
    print("Case 171 passed.")

    # Case 172: Test with URL followed by emoji
    print("Case 172")
    print(extract_urls('Check this out: https://example.edu 😃'))
    assert extract_urls('Check this out: https://example.edu 😃') == ['https://example.edu']
    print("Case 172 passed.")

    # Case 173: Test with URL followed by newline character
    print("Case 173")
    print(extract_urls('Here is the link:\nhttps://example.edu\nCheck it out.'))
    assert extract_urls('Here is the link:\nhttps://example.edu\nCheck it out.') == ['https://example.edu']
    print("Case 173 passed.")

    # Case 174: Test with a URL in uppercase
    print("Case 174")
    print(extract_urls('VISIT HTTPS://EXAMPLE.EDU'))
    assert extract_urls('VISIT HTTPS://EXAMPLE.EDU') == ['HTTPS://EXAMPLE.EDU']
    print("Case 174 passed.")

    # Case 175: Test with a URL surrounded by quotes
    print("Case 175")
    print(extract_urls('"https://example.edu" is the link'))
    assert extract_urls('"https://example.edu" is the link') == ['https://example.edu']
    print("Case 175 passed.")

    # Case 176: Test with a basic valid URL
    print("Case 176")
    print(extract_urls('Visit https://example.edu'))
    assert extract_urls('Visit https://example.edu') == ['https://example.edu']
    print("Case 176 passed.")

    # Case 177: Test with a URL that lacks "https://"
    print("Case 177")
    print(extract_urls('Visit example.edu'))
    assert extract_urls('Visit example.edu') == []
    print("Case 177 passed.")

    # Case 178: Test with a URL followed by other text without a space
    print("Case 178")
    print(extract_urls('Check https://example.edu/more-textnow'))
    assert extract_urls('Check https://example.edu/more-textnow') == ['https://example.edu/more-textnow']
    print("Case 178 passed.")

    # Case 179: Test with a URL containing a query string
    print("Case 179")
    print(extract_urls('Check https://example.edu/path?query=123&name=test'))
    assert extract_urls('Check https://example.edu/path?query=123&name=test') == ['https://example.edu/path?query=123&name=test']
    print("Case 179 passed.")

    # Case 180: Test with a URL containing a fragment
    print("Case 180")
    print(extract_urls('Check https://example.edu/path#section'))
    assert extract_urls('Check https://example.edu/path#section') == ['https://example.edu/path#section']
    print("Case 180 passed.")

    # Case 181: Test with a URL in parentheses
    print("Case 181")
    print(extract_urls('Visit (https://example.edu/path)'))
    assert extract_urls('Visit (https://example.edu/path)') == ['https://example.edu/path']
    print("Case 181 passed.")

    # Case 182: Test with multiple URLs in a string
    print("Case 182")
    print(extract_urls('Check https://example1.edu and https://example2.edu'))
    assert extract_urls('Check https://example1.edu and https://example2.edu') == ['https://example1.edu', 'https://example2.edu']
    print("Case 182 passed.")

    # Case 183: Test with URLs separated by special characters
    print("Case 183")
    print(extract_urls('Check https://example.edu, https://another.edu'))
    assert extract_urls('Check https://example.edu, https://another.edu') == ['https://example.edu', 'https://another.edu']
    print("Case 183 passed.")

    # Case 184: Test with a URL with no top-level domain
    print("Case 184")
    print(extract_urls('Check https://example'))
    assert extract_urls('Check https://example') == []
    print("Case 184 passed.")

    # Case 186: Test with a URL in square brackets
    print("Case 186")
    print(extract_urls('See [https://example.edu] for more info'))
    assert extract_urls('See [https://example.edu] for more info') == ['https://example.edu']
    print("Case 186 passed.")

    # Case 187: Test with a URL with unusual but valid characters in the path
    print("Case 187")
    print(extract_urls('Go to https://example.edu/~user/test_this-thing123'))
    assert extract_urls('Go to https://example.edu/~user/test_this-thing123') == ['https://example.edu/~user/test_this-thing123']
    print("Case 187 passed.")

    # Case 188: Test with a URL containing percent encoding
    print("Case 188")
    print(extract_urls('Check https://example.edu/path%20with%20spaces'))
    assert extract_urls('Check https://example.edu/path%20with%20spaces') == ['https://example.edu/path%20with%20spaces']
    print("Case 188 passed.")

    # Case 189: Test with a URL with no path after domain
    print("Case 189")
    print(extract_urls('Visit https://example.edu'))
    assert extract_urls('Visit https://example.edu') == ['https://example.edu']
    print("Case 189 passed.")

    # Case 190: Test with a URL starting with www and no scheme
    print("Case 190")
    print(extract_urls('Visit www.example.edu'))
    assert extract_urls('Visit www.example.edu') == ["http://www.example.edu"]
    print("Case 190 passed.")

    # Case 191: Test with a URL containing a port number
    print("Case 191")
    print(extract_urls('Check https://example.edu:8080/path'))
    assert extract_urls('Check https://example.edu:8080/path') == ['https://example.edu:8080/path']
    print("Case 191 passed.")

    # Case 192: Test with a URL in a sentence with punctuation
    print("Case 192")
    print(extract_urls('The website https://example.edu is down.'))
    assert extract_urls('The website https://example.edu is down.') == ['https://example.edu']
    print("Case 192 passed.")

    # Case 193: Test with an invalid URL containing spaces
    print("Case 193")
    print(extract_urls('Visit https://exa mple.edu now'))
    assert extract_urls('Visit https://exa mple.edu now') == []
    print("Case 193 passed.")

    # Case 194: Test with a malformed URL (too many slashes)
    print("Case 194")
    print(extract_urls('Check https://////example.edu/path'))
    assert extract_urls('Check https://////example.edu/path') == []
    print("Case 194 passed.")

    # Case 195: Test with multiple URLs on separate lines
    print("Case 195")
    print(extract_urls('First https://example1.edu\nSecond https://example2.edu'))
    assert extract_urls('First https://example1.edu\nSecond https://example2.edu') == ['https://example1.edu', 'https://example2.edu']
    print("Case 195 passed.")

    # Case 196: Test with a non-ASCII character in the URL
    print("Case 196")
    print(extract_urls('Visit https://exámple.edu/path'))
    assert extract_urls('Visit https://exámple.edu/path') == ['https://exámple.edu/path']
    print("Case 196 passed.")

    # Case 197: Test with an email address
    print("Case 197")
    print(extract_urls('Contact at email@example.edu'))
    assert extract_urls('Contact at email@example.edu') == []
    print("Case 197 passed.")

    # Case 198: Test with a URL at the end of a sentence
    print("Case 198")
    print(extract_urls('The website is available at https://example.edu.'))
    assert extract_urls('The website is available at https://example.edu.') == ['https://example.edu']
    print("Case 198 passed.")

    # Case 199: Test with a URL containing listserv.kent.edu
    print("Case 199")
    print(extract_urls("To unsubscribe from the CESNET-L list, click the following link:\nhttps://listserv.kent.edu/cgi-bin/wa.exe?SUBED1=CESNET-L&A=1"))
    assert extract_urls("To unsubscribe from the CESNET-L list, click the following link:\nhttps://listserv.kent.edu/cgi-bin/wa.exe?SUBED1=CESNET-L&A=1") == []
    print("Case 199 passed.")

    print("All test cases passed!")
